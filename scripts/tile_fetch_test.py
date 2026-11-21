#!/usr/bin/env python3
"""End-to-end tests for the single-tile fetch API.

Usage: tile_fetch_test.py BASE_URL TILES_DIR

Covers:
  1. each tile fetch returns exactly that tile's PPM (matched against the
     full-tar extraction), with the original upload name in the headers;
  2. out-of-range / negative / non-numeric indices are rejected and can never
     return another tile's bytes;
  3. tile fetch is refused while the batch is still packing (409), for a
     failed batch (409), for an unknown batch (404), and when the index
     sidecar is missing (503);
  4. concurrent tile fetches + full-tar downloads do not change batch state;
  5. the index sidecar is published atomically alongside the tar.
"""
from __future__ import annotations

import io
import json
import os
import sys
import tarfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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


def main() -> None:
    base_url, tiles_dir = sys.argv[1], Path(sys.argv[2])
    client = httpx.Client(base_url=base_url, timeout=60)

    names = ["big.png", "small1.png", "small2.png", "small3.png", "medium.png"]
    print(f"TEST 1: tile fetch returns the exact indexed tile ({len(names)} tiles)")
    info = submit(client, tiles_dir, names)
    bid = info["batch_id"]
    final = wait_terminal(client, bid)
    check(final["state"] == "done", "batch completes", final["state"])
    check(final["index_published"] is True, "batch reports index_published=true")
    check(
        all(t["tile_url"] == f"/batches/{bid}/tiles/{i}" for i, t in enumerate(final["tiles"])),
        "batch detail advertises a tile_url per tile",
    )

    # Ground truth: extract every member from the full tar download.
    tar_bytes = client.get(f"/batches/{bid}/download").content
    with tarfile.open(fileobj=io.BytesIO(tar_bytes)) as tf:
        truth = {m.name: tf.extractfile(m).read() for m in tf.getmembers()}
    check(len(truth) == len(names), "full tar still downloadable with all entries",
          f"{len(truth)} entries")

    fetched: dict[int, bytes] = {}
    for i, name in enumerate(names):
        r = client.get(f"/batches/{bid}/tiles/{i}")
        check(r.status_code == 200, f"tile {i} fetch HTTP 200", f"HTTP {r.status_code}")
        check(
            r.headers.get("content-type") == "image/x-portable-pixmap",
            f"tile {i} content-type is PPM",
            r.headers.get("content-type", ""),
        )
        check(
            r.headers.get("x-tile-index") == str(i),
            f"tile {i} X-Tile-Index header",
            r.headers.get("x-tile-index", ""),
        )
        check(
            r.headers.get("x-tile-name") == name,
            f"tile {i} X-Tile-Name carries original file name",
            r.headers.get("x-tile-name", ""),
        )
        arc = f"{i:06d}_{name.rsplit('.', 1)[0]}.ppm"
        check(
            r.headers.get("x-tile-arcname") == arc,
            f"tile {i} X-Tile-Arcname matches tar entry",
            r.headers.get("x-tile-arcname", ""),
        )
        check(
            f'filename="{arc}"' in r.headers.get("content-disposition", ""),
            f"tile {i} Content-Disposition names the PPM",
        )
        check(
            r.headers.get("x-tile-name") in r.headers.get("content-disposition", ""),
            f"tile {i} Content-Disposition also carries original name",
        )
        body = r.content
        check(int(r.headers["content-length"]) == len(body) == len(truth[arc]),
              f"tile {i} length matches tar member",
              f"http={len(body)} tar={len(truth[arc])}")
        check(body == truth[arc], f"tile {i} bytes identical to tar member {arc}")
        check(body.startswith(b"P6\n"), f"tile {i} payload is a P6 PPM")
        fetched[i] = body

    # Tiles must be pairwise different (different source PNGs), proving an
    # index lookup can never silently return another tile.
    check(
        len({b for b in fetched.values()}) == len(names),
        "every fetched tile has distinct bytes (no cross-tile leakage)",
    )

    print("\nTEST 2: index error cases never return another tile")
    last = len(names) - 1
    for bad in [last + 1, last + 100, 10_000]:
        r = client.get(f"/batches/{bid}/tiles/{bad}")
        check(r.status_code == 404, f"out-of-range index {bad} -> 404",
              f"HTTP {r.status_code}: {r.text[:100]}")
        check(r.content not in fetched.values(),
              f"out-of-range index {bad} response body is not any tile's bytes")
    r = client.get(f"/batches/{bid}/tiles/-1")
    check(r.status_code in (404, 422), "negative index rejected (404/422)",
          f"HTTP {r.status_code}")
    r = client.get(f"/batches/{bid}/tiles/abc")
    check(r.status_code == 422, "non-numeric index -> 422", f"HTTP {r.status_code}")
    r = client.get(f"/batches/nonexistent00/tiles/0")
    check(r.status_code == 404, "unknown batch -> 404", f"HTTP {r.status_code}")

    print("\nTEST 3: fetch refused before publication / on failure")
    packing = submit(client, tiles_dir, ["big.png", "small1.png", "small2.png"])
    pid = packing["batch_id"]
    r = client.get(f"/batches/{pid}/tiles/0")
    check(r.status_code == 409, "tile fetch while packing -> 409", f"HTTP {r.status_code}")
    r = client.get(f"/batches/{pid}/download")
    check(r.status_code == 409, "full download while packing stays 409")
    wait_terminal(client, pid)

    failed = submit(client, tiles_dir, ["small1.png", "huge.png"])
    fid = failed["batch_id"]
    f = wait_terminal(client, fid)
    check(f["state"] == "failed", "oversize batch fails as before")
    r = client.get(f"/batches/{fid}/tiles/0")
    check(r.status_code == 409, "tile fetch on failed batch -> 409", f"HTTP {r.status_code}")
    r = client.get(f"/batches/{fid}/download")
    check(r.status_code == 409, "full download on failed batch stays 409")

    print("\nTEST 4: missing index sidecar -> explicit 503, no other tile's bytes")
    # Locate the published sidecar through the host data dir convention: the
    # test runs against a local server with DATA_DIR under /tmp; ask the batch
    # detail first, then have an operator-side check remove the sidecar.
    data_dir = Path(os.environ["DATA_DIR"]) if "DATA_DIR" in os.environ else None
    if data_dir is None:
        check(False, "DATA_DIR env set for sidecar-removal test")
    else:
        sidecar = data_dir / "out" / f"{bid}.tar.index.json"
        check(sidecar.exists(), "index sidecar exists on disk", str(sidecar))
        saved = sidecar.read_bytes()
        sidecar.unlink()
        try:
            r = client.get(f"/batches/{bid}/tiles/0")
            check(r.status_code == 503, "missing index -> 503", f"HTTP {r.status_code}")
            check("index" in r.text.lower() and "missing" in r.text.lower(),
                  "error message names the missing index", r.text[:120])
            check(r.content not in fetched.values(),
                  "missing-index response never carries tile bytes")
            # Whole-tar download is unaffected by the index: requirement keeps
            # the original interface working.
            r = client.get(f"/batches/{bid}/download")
            check(r.status_code == 200 and r.content == tar_bytes,
                  "full tar download still serves the identical archive")
        finally:
            sidecar.write_bytes(saved)  # restore for the remaining tests
        r = client.get(f"/batches/{bid}/tiles/0")
        check(r.status_code == 200, "fetch works again after index restored")

        # Corrupted (but syntactically present) index must also be rejected.
        bad_doc = json.loads(saved)
        bad_doc["entries"][0]["header_offset"] = 999_999_999
        sidecar.write_text(json.dumps(bad_doc))
        r = client.get(f"/batches/{bid}/tiles/0")
        check(r.status_code == 503, "inconsistent index -> 503", f"HTTP {r.status_code}")
        check(r.content not in fetched.values(),
              "inconsistent-index response never carries tile bytes")
        sidecar.write_bytes(saved)
        r = client.get(f"/batches/{bid}/tiles/0")
        check(r.status_code == 200, "fetch works again after index repaired")

    print("\nTEST 5: concurrent fetches + full downloads leave state untouched")
    before = client.get(f"/batches/{bid}").json()
    st_before = client.get("/status").json()

    def hit(i: int):
        if i % 3 == 0:
            r = client.get(f"/batches/{bid}/download")
            return ("download", r.status_code, r.content)
        tile = i % len(names)
        r = client.get(f"/batches/{bid}/tiles/{tile}")
        return (f"tile-{tile}", r.status_code, r.content)

    n = 60
    with ThreadPoolExecutor(max_workers=12) as pool:
        results = [fut.result() for fut in as_completed(
            [pool.submit(hit, i) for i in range(n)]
        )]
    downloads = [r for r in results if r[0] == "download"]
    tiles = [r for r in results if r[0].startswith("tile-")]
    check(len(downloads) > 0 and len(tiles) > 0, "mixed workload ran",
          f"{len(downloads)} downloads, {len(tiles)} tile fetches")
    check(all(code == 200 for _, code, _ in results), "all concurrent requests 200")
    check(all(body == tar_bytes for _, _, body in downloads),
          "every concurrent full download is byte-identical")
    for label, _, body in tiles:
        tile = int(label.split("-")[1])
        check(body == fetched[tile], f"concurrent {label} bytes still exact")
    after = client.get(f"/batches/{bid}").json()
    st_after = client.get("/status").json()
    check(after["state"] == "done" and after["index_published"] is True,
          "batch state unchanged after concurrent load")
    check(
        {k: before[k] for k in ("tiles_packed", "tiles_started", "staging_bytes_held")}
        == {k: after[k] for k in ("tiles_packed", "tiles_started", "staging_bytes_held")},
        "batch counters unchanged after concurrent load",
    )
    check(st_before == st_after, "global scheduler status unchanged",
          f"before={st_before} after={st_after}")

    print()
    if FAILURES:
        print(f"{len(FAILURES)} CHECK(S) FAILED:")
        for f in FAILURES:
            print(f"  - {f}")
        sys.exit(1)
    print("ALL TILE-FETCH CHECKS PASSED")


if __name__ == "__main__":
    main()
