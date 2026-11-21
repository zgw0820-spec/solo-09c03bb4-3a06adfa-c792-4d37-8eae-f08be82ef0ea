"""FastAPI surface: submit PNG batches, track them, download finished tars."""
from __future__ import annotations

import asyncio
import logging
import re
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse, RedirectResponse, StreamingResponse

from .config import load_settings
from .pngutil import ppm_size_for_png
from .scheduler import DONE, FAILED, Scheduler, Tile
from .tileindex import TileIndexError, load_tile_index, open_tile_ppm

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)-5s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("tiler")

settings = load_settings()
sched = Scheduler(settings)


@asynccontextmanager
async def lifespan(app: FastAPI):
    sched.start()
    yield
    await sched.stop()


app = FastAPI(
    title="Offline Tile Packer",
    version="1.1.0",
    description=(
        "Receives batches of PNG tiles, converts them to PPM with two local "
        "worker processes under a shared staging-byte quota, and packs each "
        "batch into a tar whose entries follow the submission order. "
        "Download opens only once the whole tar is complete. When a batch "
        "finishes, a read-only per-tile index is published alongside the tar: "
        "GET /batches/{id}/tiles/{index} then returns one tile's PPM directly "
        "from its indexed tar offset -- the archive is never scanned or "
        "extracted for a single-tile fetch."
    ),
    lifespan=lifespan,
)

_SAFE_CHARS = re.compile(r"[^A-Za-z0-9._-]+")


def _sanitize(name: str) -> str:
    base = name.rsplit("/", 1)[-1].rsplit("\\", 1)[-1]
    base = _SAFE_CHARS.sub("_", base).strip("._")
    return base[:80] or "tile"


@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse("/docs")


@app.get("/status", summary="Global scheduler/quota status")
async def get_status():
    return sched.status()


@app.get("/batches", summary="List all batches")
async def list_batches():
    return [sched.batch_info(b) for b in sched.batches.values()]


@app.post("/batches", status_code=201, summary="Submit a batch of PNG tiles")
async def create_batch(
    files: list[UploadFile] = File(
        ..., description="PNG tiles; the tar preserves this attachment order"
    ),
):
    """Save the uploaded PNGs, predict each tile's PPM size from its IHDR,
    register the batch, and let the scheduler convert/pack it."""
    if not files:
        raise HTTPException(400, "at least one PNG file is required")
    batch_id = uuid.uuid4().hex[:12]
    upload_dir = sched.uploads_dir / batch_id
    stage_dir = sched.stage_dir / batch_id
    upload_dir.mkdir(parents=True, exist_ok=False)
    stage_dir.mkdir(parents=True, exist_ok=False)

    tiles: list[Tile] = []
    for i, uf in enumerate(files):
        name = _sanitize(uf.filename or f"tile_{i}.png")
        png_path = upload_dir / f"{i:06d}_{name}"
        with open(png_path, "wb") as out:
            while chunk := await uf.read(1 << 20):
                out.write(chunk)
        stem = name.rsplit(".", 1)[0] if "." in name else name
        tiles.append(
            Tile(
                index=i,
                name=name,
                arcname=f"{i:06d}_{stem}.ppm",
                png_path=png_path,
                ppm_path=stage_dir / f"{i:06d}.ppm",
                ppm_size=ppm_size_for_png(png_path),
            )
        )
    log.info("batch=%s SUBMITTED tiles=%d names=%s",
             batch_id, len(tiles), [t.name for t in tiles])
    batch = sched.new_batch(tiles, batch_id=batch_id)
    return sched.batch_info(batch)


@app.get("/batches/{batch_id}", summary="Batch detail")
async def get_batch(batch_id: str):
    batch = sched.batches.get(batch_id)
    if batch is None:
        raise HTTPException(404, "batch not found")
    return sched.batch_info(batch)


@app.get(
    "/batches/{batch_id}/download",
    summary="Download the finished tar (only once the batch is done)",
    responses={
        200: {
            "description": "The complete tar archive",
            "content": {
                "application/x-tar": {
                    "schema": {"type": "string", "format": "binary"}
                }
            },
        },
        409: {"description": "Batch not finished yet or failed"},
    },
)
async def download_batch(batch_id: str):
    batch = sched.batches.get(batch_id)
    if batch is None:
        raise HTTPException(404, "batch not found")
    if batch.state == FAILED:
        raise HTTPException(
            409,
            f"batch failed at tile '{batch.failed_tile}': {batch.error}",
        )
    if batch.state != DONE:
        raise HTTPException(
            409,
            f"batch is {batch.state} ({batch.next_to_pack}/{len(batch.tiles)} "
            "tiles packed); download opens when the whole tar is complete",
        )
    return FileResponse(
        batch.tar_path,
        media_type="application/x-tar",
        filename=f"tiles_{batch.id}.tar",
    )


@app.get(
    "/batches/{batch_id}/tiles/{index}",
    summary="Fetch one tile's PPM by submission index (read-only index lookup)",
    responses={
        200: {
            "description": (
                "The single tile's binary P6 PPM, read directly from the tar "
                "at its indexed byte offset (the whole tar is never read or "
                "extracted). The original upload name is in "
                "Content-Disposition and the X-Tile-* headers."
            ),
            "content": {
                "image/x-portable-pixmap": {
                    "schema": {"type": "string", "format": "binary"}
                }
            },
            "headers": {
                "X-Tile-Index": {"description": "Submission index of the tile", "schema": {"type": "integer"}},
                "X-Tile-Name": {"description": "Original sanitized upload file name", "schema": {"type": "string"}},
                "X-Tile-Arcname": {"description": "Entry name inside the tar", "schema": {"type": "string"}},
            },
        },
        404: {"description": "Unknown batch, or tile index outside the batch's range"},
        409: {"description": "Archive not finished yet (or batch failed)"},
        422: {"description": "Index is not a non-negative integer"},
        503: {"description": "Tar or read-only index missing/incomplete/inconsistent"},
    },
)
async def get_tile(batch_id: str, index: int):
    batch = sched.batches.get(batch_id)
    if batch is None:
        raise HTTPException(404, "batch not found")
    if batch.state == FAILED:
        raise HTTPException(
            409,
            f"batch failed at tile '{batch.failed_tile}': {batch.error}",
        )
    if batch.state != DONE:
        raise HTTPException(
            409,
            f"batch is {batch.state} ({batch.next_to_pack}/{len(batch.tiles)} "
            "tiles packed); tile fetch opens only once the tar and its index "
            "are both published",
        )
    # In-memory gate first: DONE is assigned only after the atomic index
    # rename, so a fetch can never observe DONE without a published index.
    if not batch.index_published:
        raise HTTPException(
            503,
            "tile index has not been published for this batch; refuse to "
            "serve until tar and index are both complete",
        )
    # Range check before touching the sidecar so an out-of-range index can
    # never resolve to another tile's slot.
    if index < 0 or index >= len(batch.tiles):
        raise HTTPException(
            404,
            f"tile index {index} out of range; batch has {len(batch.tiles)} "
            f"tiles (valid indices 0..{len(batch.tiles) - 1})",
        )
    try:
        tile_index = await asyncio.to_thread(load_tile_index, batch.tar_path)
        entry = tile_index.entry(index)
        # Open+verify the single 512-byte tar header at the recorded offset in
        # a worker thread; only this tile's payload region is ever read.
        fh = await asyncio.to_thread(open_tile_ppm, batch.tar_path, entry)
    except TileIndexError as exc:
        raise HTTPException(
            503,
            f"published tar/index is incomplete or inconsistent: {exc}",
        ) from exc

    def _stream():
        try:
            remaining = entry.size
            while remaining > 0:
                chunk = fh.read(min(1 << 20, remaining))
                if not chunk:  # archive truncated after publication?
                    log.error(
                        "batch=%s tile=%d tar truncated mid-payload",
                        batch.id, index,
                    )
                    break
                remaining -= len(chunk)
                yield chunk
        finally:
            fh.close()

    # Filename tokens come from _sanitize() ([A-Za-z0-9._-]), so they are
    # safe as header values; keep them quoted regardless.
    headers = {
        "Content-Length": str(entry.size),
        "X-Tile-Index": str(entry.index),
        "X-Tile-Name": entry.name,
        "X-Tile-Arcname": entry.arcname,
        "Content-Disposition": (
            f'attachment; filename="{entry.arcname}"; '
            f'filename*=UTF-8\'\'{entry.name}'
        ),
    }
    return StreamingResponse(
        _stream(),
        media_type="image/x-portable-pixmap",
        headers=headers,
    )
