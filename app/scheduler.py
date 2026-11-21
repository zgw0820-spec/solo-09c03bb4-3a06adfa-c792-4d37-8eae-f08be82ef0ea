"""Batch scheduler: shared PPM staging quota, in-order dispatch, in-order tar packing.

Design invariants
-----------------
* Tiles of a batch are *dispatched* strictly in submission order. A later
  tile therefore never holds staging quota before the next tile that must
  enter the tar has started, so staged later tiles can never crowd out the
  space needed to start that next tile.
* A tile holds quota at its full PPM size from the moment it is dispatched
  (converting) until its staged PPM has been appended to the tar and
  deleted (packed). Only then is the quota released.
* Tar entries are appended strictly in submission order. A tile converted
  early stays staged until every preceding tile has been packed.
* At most ``max_active_batches`` batches pack concurrently; the rest queue.
* Any single tile whose PPM exceeds the quota limit, or any conversion
  failure, fails the whole batch: the tile name is reported, the batch's
  staged bytes and partial tar are cleaned up, and other batches continue.
"""
from __future__ import annotations

import asyncio
import logging
import multiprocessing
import shutil
import tarfile
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path

from .config import Settings
from .worker import run_worker

log = logging.getLogger("tiler")

# tile states
PENDING = "pending"        # not dispatched yet
CONVERTING = "converting"  # dispatched to a worker, holding quota
CONVERTED = "converted"    # PPM staged, waiting for its tar turn, holding quota
PACKED = "packed"          # in the tar, staging removed, quota released
ABORTED = "aborted"        # batch failed before/while this tile was in flight

# batch states
QUEUED = "queued"    # waiting for a free active slot
PACKING = "packing"  # active: converting / packing
DONE = "done"        # tar complete, downloadable
FAILED = "failed"    # aborted, staging cleaned


@dataclass
class Tile:
    index: int
    name: str             # original (sanitized) upload name, used in error reports
    arcname: str          # entry name inside the tar
    png_path: Path
    ppm_path: Path
    ppm_size: int | None  # exact PPM size predicted from the PNG header
    state: str = PENDING
    reserved: int = 0     # staging quota bytes currently held


@dataclass
class Batch:
    id: str
    tiles: list[Tile]
    tar_path: Path
    created_at: float
    state: str = QUEUED
    next_to_start: int = 0
    next_to_pack: int = 0
    failed_tile: str | None = None
    error: str | None = None
    tar: tarfile.TarFile | None = None
    pack_lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    @property
    def held_bytes(self) -> int:
        return sum(t.reserved for t in self.tiles)


class Scheduler:
    """Owns all mutable scheduling state. Every mutating method runs on the
    API process's event loop, so no extra locking is needed beyond the
    per-batch pack lock that serializes tar appends."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.uploads_dir = settings.data_dir / "uploads"
        self.stage_dir = settings.data_dir / "stage"
        self.out_dir = settings.data_dir / "out"
        self.batches: dict[str, Batch] = {}
        self.active: list[Batch] = []        # batches currently packing (FIFO)
        self.waiting: deque[Batch] = deque()  # submitted but not yet active
        self.used = 0                         # staging bytes currently reserved
        self.inflight = 0                     # conversions dispatched, not yet reported
        self._task_q: multiprocessing.Queue | None = None
        self._result_q: multiprocessing.Queue | None = None
        self._workers: list[multiprocessing.Process] = []
        self._reader_task: asyncio.Task | None = None

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------
    def start(self) -> None:
        # In-memory state is authoritative; drop leftovers from previous runs
        # so a restart never resurrects half-packed staging or stale tars.
        for d in (self.uploads_dir, self.stage_dir, self.out_dir):
            d.mkdir(parents=True, exist_ok=True)
            for child in d.iterdir():
                if child.is_dir():
                    shutil.rmtree(child, ignore_errors=True)
                else:
                    child.unlink(missing_ok=True)
        try:
            ctx = multiprocessing.get_context("fork")
        except ValueError:  # pragma: no cover - non-POSIX fallback
            ctx = multiprocessing.get_context("spawn")
        self._task_q = ctx.Queue()
        self._result_q = ctx.Queue()
        for i in range(self.settings.workers):
            p = ctx.Process(
                target=run_worker,
                args=(self._task_q, self._result_q),
                name=f"ppm-worker-{i}",
                daemon=True,
            )
            p.start()
            self._workers.append(p)
        self._reader_task = asyncio.create_task(self._result_reader())
        log.info(
            "scheduler up: workers=%d staging_limit=%d bytes max_active_batches=%d data_dir=%s",
            self.settings.workers, self.settings.ppm_staging_bytes,
            self.settings.max_active_batches, self.settings.data_dir,
        )

    async def stop(self) -> None:
        if self._reader_task:
            self._reader_task.cancel()
        if self._task_q:
            for _ in self._workers:
                self._task_q.put(None)
        for p in self._workers:
            p.join(timeout=3)
            if p.is_alive():
                p.terminate()

    # ------------------------------------------------------------------
    # intake
    # ------------------------------------------------------------------
    def new_batch(self, tiles: list[Tile], batch_id: str | None = None) -> Batch:
        batch = Batch(
            id=batch_id or uuid.uuid4().hex[:12],
            tiles=tiles,
            tar_path=self.out_dir / "pending.tar",  # replaced below
            created_at=time.time(),
        )
        batch.tar_path = self.out_dir / f"{batch.id}.tar"
        self.batches[batch.id] = batch
        self.waiting.append(batch)
        self._activate()
        self._try_dispatch()
        return batch

    def _activate(self) -> None:
        while self.waiting and len(self.active) < self.settings.max_active_batches:
            batch = self.waiting.popleft()
            batch.state = PACKING
            self.active.append(batch)
            log.info(
                "batch=%s ACTIVATED tiles=%d active=%d queued=%d",
                batch.id, len(batch.tiles), len(self.active), len(self.waiting),
            )

    # ------------------------------------------------------------------
    # dispatch (quota gate)
    # ------------------------------------------------------------------
    def _try_dispatch(self) -> None:
        """Start conversions while workers are free and quota allows.

        Batches are scanned oldest-first; each batch offers its next
        unstarted tile (strict in-order start per batch). A tile starts only
        if its full PPM size fits in the remaining shared quota.
        """
        while self.inflight < self.settings.workers:
            progressed = False
            for batch in list(self.active):
                if self.inflight >= self.settings.workers:
                    break
                if batch.next_to_start >= len(batch.tiles):
                    continue
                tile = batch.tiles[batch.next_to_start]
                if tile.ppm_size is None:
                    self._fail_batch(batch, tile, "invalid PNG: cannot parse signature/IHDR")
                    progressed = True
                    break  # active list changed; rescan from the oldest batch
                if tile.ppm_size > self.settings.ppm_staging_bytes:
                    self._fail_batch(
                        batch, tile,
                        f"single tile PPM is {tile.ppm_size} bytes, exceeds the "
                        f"{self.settings.ppm_staging_bytes} byte staging limit",
                    )
                    progressed = True
                    break
                if self.used + tile.ppm_size > self.settings.ppm_staging_bytes:
                    continue  # quota blocked; give other batches a chance
                self._dispatch(batch, tile)
                progressed = True
            if not progressed:
                return

    def _dispatch(self, batch: Batch, tile: Tile) -> None:
        tile.state = CONVERTING
        tile.reserved = tile.ppm_size or 0
        batch.next_to_start += 1
        self.used += tile.reserved
        self.inflight += 1
        self._task_q.put((batch.id, tile.index, tile.name,
                          str(tile.png_path), str(tile.ppm_path)))
        log.info(
            "batch=%s tile=%d name=%s DISPATCH reserve=%d used=%d/%d inflight=%d",
            batch.id, tile.index, tile.name, tile.reserved,
            self.used, self.settings.ppm_staging_bytes, self.inflight,
        )

    # ------------------------------------------------------------------
    # worker results
    # ------------------------------------------------------------------
    async def _result_reader(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            result = await loop.run_in_executor(None, self._result_q.get)
            try:
                await self._on_result(result)
            except Exception:  # pragma: no cover - defensive
                log.exception("failed to handle worker result %r", result)

    async def _on_result(self, result) -> None:
        batch_id, index, name, size, error, duration = result
        self.inflight -= 1
        batch = self.batches.get(batch_id)
        if batch is None or batch.state != PACKING:
            # Batch already failed while this tile was in flight: drop any
            # file the worker just wrote; its quota was released at failure.
            if batch is not None:
                try:
                    batch.tiles[index].ppm_path.unlink(missing_ok=True)
                except OSError:
                    pass
            log.warning(
                "batch=%s tile=%d name=%s result DISCARDED (batch state=%s)",
                batch_id, index, name, batch.state if batch else "unknown",
            )
        elif error is not None:
            self._fail_batch(batch, batch.tiles[index], f"conversion failed: {error}")
        else:
            tile = batch.tiles[index]
            tile.state = CONVERTED
            if size != tile.reserved:  # keep accounting exact if size differed
                self.used += size - tile.reserved
                tile.reserved = size
            log.info(
                "batch=%s tile=%d name=%s CONVERTED size=%d duration=%.0fms used=%d/%d",
                batch.id, index, name, size, duration * 1000,
                self.used, self.settings.ppm_staging_bytes,
            )
            await self._pack_ready(batch)
        self._try_dispatch()

    # ------------------------------------------------------------------
    # packing (strict tar order)
    # ------------------------------------------------------------------
    async def _pack_ready(self, batch: Batch) -> None:
        async with batch.pack_lock:
            while batch.state == PACKING and batch.next_to_pack < len(batch.tiles):
                tile = batch.tiles[batch.next_to_pack]
                if tile.state != CONVERTED:
                    break  # earlier tile not ready: later ones keep waiting
                await asyncio.to_thread(self._append_and_clear, batch, tile)
                self.used -= tile.reserved
                tile.reserved = 0
                tile.state = PACKED
                batch.next_to_pack += 1
                log.info(
                    "batch=%s tile=%d name=%s PACKED as=%s used=%d/%d",
                    batch.id, tile.index, tile.name, tile.arcname,
                    self.used, self.settings.ppm_staging_bytes,
                )
            if batch.state == PACKING and batch.next_to_pack == len(batch.tiles):
                await asyncio.to_thread(self._close_tar, batch)
                batch.state = DONE
                self.active.remove(batch)
                shutil.rmtree(self.stage_dir / batch.id, ignore_errors=True)
                log.info("batch=%s DONE tar=%s", batch.id, batch.tar_path)
                self._activate()

    @staticmethod
    def _append_and_clear(batch: Batch, tile: Tile) -> None:
        if batch.tar is None:
            batch.tar_path.parent.mkdir(parents=True, exist_ok=True)
            batch.tar = tarfile.open(batch.tar_path, "w")
        batch.tar.add(tile.ppm_path, arcname=tile.arcname)
        tile.ppm_path.unlink()  # staging cleared; quota is released by the caller

    @staticmethod
    def _close_tar(batch: Batch) -> None:
        if batch.tar is not None:
            batch.tar.close()
            batch.tar = None

    # ------------------------------------------------------------------
    # failure handling
    # ------------------------------------------------------------------
    def _fail_batch(self, batch: Batch, tile: Tile, error: str) -> None:
        batch.state = FAILED
        batch.failed_tile = tile.name
        batch.error = error
        released = 0
        for t in batch.tiles:
            if t.reserved:
                released += t.reserved
                self.used -= t.reserved
                t.reserved = 0
            if t.state in (PENDING, CONVERTING, CONVERTED):
                t.state = ABORTED
        if batch in self.active:
            self.active.remove(batch)
        else:
            try:
                self.waiting.remove(batch)
            except ValueError:
                pass
        log.error(
            "batch=%s FAILED tile=%s error=%r released=%d used=%d/%d",
            batch.id, tile.name, error, released,
            self.used, self.settings.ppm_staging_bytes,
        )
        # File cleanup runs under the pack lock: a tar append for this batch
        # may be in flight in a worker thread right now.
        asyncio.get_running_loop().create_task(self._cleanup_failed(batch))
        self._activate()

    async def _cleanup_failed(self, batch: Batch) -> None:
        """Remove the failed batch's staged PPMs and partial tar."""
        try:
            async with batch.pack_lock:
                if batch.tar is not None:
                    batch.tar.close()
                    batch.tar = None
                batch.tar_path.unlink(missing_ok=True)
                for t in batch.tiles:
                    try:
                        t.ppm_path.unlink(missing_ok=True)
                    except OSError:
                        pass
                shutil.rmtree(self.stage_dir / batch.id, ignore_errors=True)
        except Exception:  # pragma: no cover - defensive
            log.exception("batch=%s cleanup failed", batch.id)

    # ------------------------------------------------------------------
    # introspection for the API
    # ------------------------------------------------------------------
    def status(self) -> dict:
        return {
            "ppm_staging_limit_bytes": self.settings.ppm_staging_bytes,
            "ppm_staging_used_bytes": self.used,
            "workers": self.settings.workers,
            "conversions_in_flight": self.inflight,
            "active_batches": [b.id for b in self.active],
            "queued_batches": [b.id for b in self.waiting],
        }

    def batch_info(self, batch: Batch) -> dict:
        return {
            "batch_id": batch.id,
            "state": batch.state,
            "tiles_total": len(batch.tiles),
            "tiles_started": batch.next_to_start,
            "tiles_packed": batch.next_to_pack,
            "staging_bytes_held": batch.held_bytes,
            "failed_tile": batch.failed_tile,
            "error": batch.error,
            "download_url": f"/batches/{batch.id}/download" if batch.state == DONE else None,
            "tiles": [
                {
                    "index": t.index,
                    "name": t.name,
                    "state": t.state,
                    "ppm_size": t.ppm_size,
                    "arcname": t.arcname,
                }
                for t in batch.tiles
            ],
        }
