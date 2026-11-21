#!/usr/bin/env python3
"""End-to-end smoke test against a running Offline Tile Packer.

Usage: smoke_test.py BASE_URL TILES_DIR

Covers:
  1. slow first tile: later tiles convert first but wait; tar order kept;
     two workers convert concurrently; quota gates the third tile.
  2. two batches packing concurrently while one fails on a broken tile:
     failure names the tile, staging is cleaned, the other batch completes.
  3. a single tile exceeding the staging limit fails its batch.
  4. a non-PNG upload fails its batch at the header check.
  5. download is refused until the whole tar is complete.
"""
from __future__ import annotations

import io
import sys
import tarfile
import time
from pathlib import Path

import httpx

FAILURES: list[str] = []


def check(cond: bool, label: str, detail: str = "") -> None:
    mark = "PASS" if cond else "FAIL"
    print(f"    [{mark}] {label}" + (f" -- {detail}" if detail else ""))
    if not cond:
        FAILURES.append(label)


def submit(client: httpx.Client, tiles_dir: Path, names: list[str]) -> dict:
    handles = [open(tiles_dir / n, "rb") for n in names]
    try:
        files = [("files", (n, h, "image/png")) for n, h in zip(names, handles)]
        r = client.post("/batches", files=files)
        r.raise_for_status()
        return r.json()
    finally:
        for h in handles:
            h.close()


def wait_terminal(client: httpx.Client, batch_id: str, timeout: float = 60) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        info = client.get(f"/batches/{batch_id}").json()
        if info["state"] in ("done", "failed"):
            return info
        time.sleep(0.05)
    raise TimeoutError(f"batch {batch_id} did not finish in {timeout}s")


def watch_batch(client: httpx.Client, batch_id: str, timeout: float = 60) -> list[dict]:
    """Poll batch+status until terminal; return timeline samples."""
    samples = []
    deadline = time.monotonic() + timeout
    t0 = time.monotonic()
    while True:
        info = client.get(f"/batches/{batch_id}").json()
        status = client.get("/status").json()
        samples.append({
            "ms": (time.monotonic() - t0) * 1000,
            "started": info["tiles_started"],
            "packed": info["tiles_packed"],
            "held": info["staging_bytes_held"],
            "inflight": status["conversions_in_flight"],
            "used": status["ppm_staging_used_bytes"],
            "state": info["state"],
            "tile_states": [t["state"] for t in info["tiles"]],
        })
        if info["state"] in ("done", "failed"):
            return samples
        if time.monotonic() > deadline:
            raise TimeoutError(f"batch {batch_id} watch timeout")
        time.sleep(0.02)


def tar_names(content: bytes) -> list[str]:
    with tarfile.open(fileobj=io.BytesIO(content)) as tf:
        return tf.getnames()


def main() -> None:
    base_url, tiles_dir = sys.argv[1], Path(sys.argv[2])
    client = httpx.Client(base_url=base_url, timeout=30)

    status = client.get("/status").json()
    limit = status["ppm_staging_limit_bytes"]
    print(f"service up: {status}")
    print(f"staging limit = {limit:,} B\n")

    # ------------------------------------------------------------------
    print("TEST 1: slow first tile, in-order tar, concurrency, quota gating")
    names1 = ["big.png", "small1.png", "small2.png", "small3.png", "small4.png"]
    b1 = submit(client, tiles_dir, names1)
    bid1 = b1["batch_id"]
    print(f"  batch {bid1}: {names1}")
    samples = watch_batch(client, bid1)
    final1 = samples[-1]
    check(final1["state"] == "done", "batch completes", f"state={final1['state']}")

    saw_inflight2 = any(s["inflight"] >= 2 for s in samples)
    check(saw_inflight2, "two workers converted concurrently (inflight==2 observed)")

    later_first = any(
        s["tile_states"][0] == "converting" and s["tile_states"][1] == "converted"
        for s in samples
    )
    check(later_first, "small tile #1 converted while big tile #0 still converting")

    # tile #2 must NOT start while #0 converts and #1 holds staged quota:
    # 15.6MB + 3.1MB + 3.1MB > 20MiB, so started stays at 2 until first pack.
    gated = any(
        s["started"] == 2 and s["packed"] == 0 and s["inflight"] == 1 for s in samples
    )
    check(gated, "tile #2 held back by quota while #0 converting and #1 staged")

    never_over = all(s["used"] <= limit for s in samples)
    check(never_over, "staging usage never exceeded the limit",
          f"max used={max(s['used'] for s in samples):,}")

    r = client.get(f"/batches/{bid1}/download")
    check(r.status_code == 200, "download opens after completion")
    got = tar_names(r.content)
    want = [f"{i:06d}_{n.rsplit('.', 1)[0]}.ppm" for i, n in enumerate(names1)]
    check(got == want, "tar entries follow submission order", f"{got}")

    print("  timeline (transitions):")
    prev = None
    for s in samples:
        key = (s["started"], s["packed"], s["inflight"], s["used"])
        if key != prev:
            print(f"    t={s['ms']:7.0f}ms started={s['started']} packed={s['packed']} "
                  f"inflight={s['inflight']} used={s['used']:,}")
            prev = key

    # ------------------------------------------------------------------
    print("\nTEST 2: concurrent batches, one fails on a broken tile")
    # Batch A leads with a slow tile; its broken second tile predicts 15.6MB,
    # so quota keeps it undispatched until the first tile packs. All three
    # submits therefore land while A and B are both active and C must queue.
    b_a = submit(client, tiles_dir, ["medium.png", "truncbig.png", "small2.png"])
    b_b = submit(client, tiles_dir, ["small3.png", "medium.png", "small4.png", "small1.png"])
    b_c = submit(client, tiles_dir, ["small2.png", "small3.png"])
    ida, idb, idc = b_a["batch_id"], b_b["batch_id"], b_c["batch_id"]

    saw_two_active = saw_c_queued = False
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline and not (saw_two_active and saw_c_queued):
        st = client.get("/status").json()
        saw_two_active = saw_two_active or (
            ida in st["active_batches"] and idb in st["active_batches"])
        saw_c_queued = saw_c_queued or idc in st["queued_batches"]
        time.sleep(0.01)
    check(saw_two_active, "two batches active at once")
    check(saw_c_queued, "third batch queued while two packs were in flight")

    fa = wait_terminal(client, ida)
    fb = wait_terminal(client, idb)
    fc = wait_terminal(client, idc)
    check(fa["state"] == "failed", "batch with broken tile failed")
    check(fa["failed_tile"] == "truncbig.png", "failure names the tile",
          f"failed_tile={fa['failed_tile']!r}")
    check("conversion failed" in (fa["error"] or ""), "error describes conversion failure",
          fa["error"])
    check(fa["staging_bytes_held"] == 0, "failed batch staging cleaned (0 bytes held)")
    check(fb["state"] == "done", "other batch completes normally", f"state={fb['state']}")
    check(fc["state"] == "done", "queued batch activated and completes", f"state={fc['state']}")

    r = client.get(f"/batches/{ida}/download")
    check(r.status_code == 409, "failed batch download refused", f"HTTP {r.status_code}")
    r = client.get(f"/batches/{idb}/download")
    names_b = ["small3.png", "medium.png", "small4.png", "small1.png"]
    want_b = [f"{i:06d}_{n.rsplit('.', 1)[0]}.ppm" for i, n in enumerate(names_b)]
    check(r.status_code == 200 and tar_names(r.content) == want_b,
          "other batch tar complete and ordered", f"{tar_names(r.content) if r.status_code == 200 else r.status_code}")

    # ------------------------------------------------------------------
    print("\nTEST 3: single tile exceeding the staging limit")
    b3 = submit(client, tiles_dir, ["small1.png", "huge.png"])
    f3 = wait_terminal(client, b3["batch_id"])
    check(f3["state"] == "failed", "batch failed")
    check(f3["failed_tile"] == "huge.png", "oversize tile named", f"{f3['failed_tile']!r}")
    check("exceeds" in (f3["error"] or ""), "error mentions the limit", f3["error"])
    check(f3["staging_bytes_held"] == 0, "staging cleaned after oversize failure")
    r = client.get(f"/batches/{b3['batch_id']}/download")
    check(r.status_code == 409, "download refused for oversize-failed batch")

    # ------------------------------------------------------------------
    print("\nTEST 4: non-PNG upload")
    b4 = submit(client, tiles_dir, ["junk.png"])
    f4 = wait_terminal(client, b4["batch_id"])
    check(f4["state"] == "failed" and f4["failed_tile"] == "junk.png",
          "junk tile named in failure", f"{f4['failed_tile']!r}: {f4['error']}")

    # ------------------------------------------------------------------
    print("\nTEST 5: download refused while packing")
    b5 = submit(client, tiles_dir, ["big.png", "small1.png", "small2.png"])
    r = client.get(f"/batches/{b5['batch_id']}/download")
    check(r.status_code == 409, "download refused before completion", f"HTTP {r.status_code}")
    f5 = wait_terminal(client, b5["batch_id"])
    r = client.get(f"/batches/{b5['batch_id']}/download")
    check(f5["state"] == "done" and r.status_code == 200, "download opens once done")

    # ------------------------------------------------------------------
    print("\nfinal status:", client.get("/status").json())
    st = client.get("/status").json()
    check(st["ppm_staging_used_bytes"] == 0, "all quota released at the end")

    print()
    if FAILURES:
        print(f"{len(FAILURES)} CHECK(S) FAILED:")
        for f in FAILURES:
            print(f"  - {f}")
        sys.exit(1)
    print("ALL CHECKS PASSED")


if __name__ == "__main__":
    main()
