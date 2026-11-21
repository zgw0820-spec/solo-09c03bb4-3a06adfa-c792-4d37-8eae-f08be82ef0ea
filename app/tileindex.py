"""Read-only per-tile index published together with a finished tar.

A tile index is a small JSON sidecar (``<batch>.tar.index.json``) written
atomically *after* the tar has been closed and fully verified. It maps each
tile's submission index to the original upload name and to the exact byte
offset/size of its entry inside the tar. Fetching one tile therefore needs
exactly one 512-byte tar header plus one contiguous payload read at a known
offset: the whole tar is never scanned or extracted.

Index files are treated as immutable once published. Every load re-validates
the structure and the cross-references against the on-disk tar, so a missing,
truncated or tampered sidecar produces an explicit error instead of data
from another tile.
"""
from __future__ import annotations

import json
import logging
import os
import tarfile
import tempfile
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger("tiler")

INDEX_VERSION = 1
BLOCKSIZE = 512


class TileIndexError(Exception):
    """Raised when an index is missing, unreadable, malformed or inconsistent."""


@dataclass(frozen=True)
class TileEntry:
    index: int          # submission order (0-based)
    name: str           # original (sanitized) upload file name
    arcname: str        # entry name inside the tar
    header_offset: int  # tar header position in bytes
    size: int           # PPM payload size in bytes


@dataclass(frozen=True)
class TileIndex:
    version: int
    tar_name: str
    tar_size: int
    entries: tuple[TileEntry, ...]

    def entry(self, index: int) -> TileEntry:
        if not isinstance(index, int) or index < 0 or index >= len(self.entries):
            raise TileIndexError(
                f"tile index {index} out of range; batch has "
                f"{len(self.entries)} tiles (valid indices 0..{len(self.entries) - 1})"
            )
        entry = self.entries[index]
        if entry.index != index:
            # Structural guard: entries must be stored in submission order.
            raise TileIndexError(f"index entry {index} is not stored at its slot")
        return entry


def build_tile_index(
    tar_path: Path,
    records: list[tuple[int, str, str, int, int]],
) -> TileIndex:
    """Build and verify an index for a just-closed tar.

    ``records`` holds, per tile in submission order,
    ``(index, original_name, arcname, header_offset, ppm_size)`` as captured
    while the tar was appended. Every recorded header is re-parsed directly
    from the tar at its offset and checked for name, size, checksum (via
    :meth:`TarInfo.frombuf`) and ordering. This reads one 512-byte block per
    tile -- it never streams or extracts the payloads.
    """
    if not records:
        raise TileIndexError("cannot build an index for an empty batch")
    entries: list[TileEntry] = []
    expected_end = 0
    with open(tar_path, "rb") as fh:
        tar_size = fh.seek(0, os.SEEK_END)
        for slot, (idx, name, arcname, header_offset, size) in enumerate(records):
            if idx != slot:
                raise TileIndexError(
                    f"record {slot}: submission index {idx} out of order"
                )
            if not isinstance(header_offset, int) or header_offset < 0:
                raise TileIndexError(f"record {slot}: invalid header offset")
            if not isinstance(size, int) or size < 0:
                raise TileIndexError(f"record {slot}: invalid size")
            # Entries are written back-to-back in 512-byte blocks; the first
            # header starts at 0 and each subsequent header must follow the
            # previous header block + payload (padded to a full block).
            if slot == 0:
                if header_offset != 0:
                    raise TileIndexError(
                        f"record {slot}: first header not at offset 0 "
                        f"(got {header_offset})"
                    )
            elif header_offset != expected_end:
                raise TileIndexError(
                    f"record {slot}: gap/overlap before header "
                    f"(offset {header_offset}, expected {expected_end})"
                )
            payload_blocks = (size + BLOCKSIZE - 1) // BLOCKSIZE
            expected_end = header_offset + BLOCKSIZE + payload_blocks * BLOCKSIZE
            if expected_end > tar_size:
                raise TileIndexError(
                    f"record {slot}: entry extends beyond the tar"
                )
            fh.seek(header_offset)
            buf = fh.read(BLOCKSIZE)
            if len(buf) != BLOCKSIZE or buf == b"\0" * BLOCKSIZE:
                raise TileIndexError(
                    f"record {slot}: no tar header at offset {header_offset}"
                )
            try:
                info = tarfile.TarInfo.frombuf(
                    buf, tarfile.ENCODING, "surrogateescape"
                )
            except (tarfile.ReadError, tarfile.InvalidHeaderError) as exc:
                raise TileIndexError(
                    f"record {slot}: bad tar header at offset {header_offset}: {exc}"
                ) from exc
            if not info.isreg():
                raise TileIndexError(f"record {slot}: tar entry is not a regular file")
            if info.name != arcname:
                raise TileIndexError(
                    f"record {slot}: tar entry name {info.name!r} != recorded {arcname!r}"
                )
            if info.size != size:
                raise TileIndexError(
                    f"record {slot}: tar entry size {info.size} != recorded {size}"
                )
            entries.append(
                TileEntry(
                    index=idx,
                    name=name,
                    arcname=arcname,
                    header_offset=header_offset,
                    size=size,
                )
            )
        # A valid tar ends with two zero blocks right after the last entry.
        if expected_end + 2 * BLOCKSIZE > tar_size:
            raise TileIndexError("tar is missing its terminating zero blocks")
        fh.seek(expected_end)
        if fh.read(2 * BLOCKSIZE) != b"\0" * (2 * BLOCKSIZE):
            raise TileIndexError("tar terminator blocks are not zero")
    return TileIndex(
        version=INDEX_VERSION,
        tar_name=tar_path.name,
        tar_size=tar_size,
        entries=tuple(entries),
    )


def index_path_for(tar_path: Path) -> Path:
    return tar_path.with_name(tar_path.name + ".index.json")


def write_tile_index_atomic(tar_path: Path, index: TileIndex) -> Path:
    """Write the sidecar via temp-file + atomic rename.

    The tar is already closed at this point, and the index only becomes
    visible under its final name once the full content has been fsynced.
    """
    path = index_path_for(tar_path)
    payload = {
        "version": index.version,
        "tar_name": index.tar_name,
        "tar_size": index.tar_size,
        "entries": [
            {
                "index": e.index,
                "name": e.name,
                "arcname": e.arcname,
                "header_offset": e.header_offset,
                "size": e.size,
            }
            for e in index.entries
        ],
    }
    fd, tmp_name = tempfile.mkstemp(
        prefix=path.name + ".", suffix=".tmp", dir=str(path.parent)
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_name, path)
        log.info(
            "index published: %s entries=%d tar=%s (%d bytes)",
            path.name, len(index.entries), index.tar_name, index.tar_size,
        )
        return path
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


def load_tile_index(tar_path: Path) -> TileIndex:
    """Load and fully validate a published index against the on-disk tar."""
    path = index_path_for(tar_path)
    try:
        with open(path, "r", encoding="utf-8") as fh:
            doc = json.load(fh)
    except FileNotFoundError as exc:
        raise TileIndexError(f"tile index {path.name} is missing") from exc
    except (OSError, json.JSONDecodeError) as exc:
        raise TileIndexError(f"tile index {path.name} is unreadable: {exc}") from exc
    try:
        if doc.get("version") != INDEX_VERSION:
            raise TileIndexError(
                f"unsupported index version {doc.get('version')!r}"
            )
        if doc.get("tar_name") != tar_path.name:
            raise TileIndexError("index belongs to a different tar file")
        raw_entries = doc["entries"]
        if not isinstance(raw_entries, list) or not raw_entries:
            raise TileIndexError("index contains no entries")
        entries: list[TileEntry] = []
        for slot, raw in enumerate(raw_entries):
            entry = TileEntry(
                index=int(raw["index"]),
                name=str(raw["name"]),
                arcname=str(raw["arcname"]),
                header_offset=int(raw["header_offset"]),
                size=int(raw["size"]),
            )
            if entry.index != slot:
                raise TileIndexError(f"entry slot {slot} has index {entry.index}")
            if not entry.name or not entry.arcname:
                raise TileIndexError(f"entry {slot} has an empty name")
            if entry.header_offset < 0 or entry.size < 0:
                raise TileIndexError(f"entry {slot} has negative offset/size")
            entries.append(entry)
        index = TileIndex(
            version=int(doc["version"]),
            tar_name=str(doc["tar_name"]),
            tar_size=int(doc["tar_size"]),
            entries=tuple(entries),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise TileIndexError(f"tile index {path.name} is malformed: {exc}") from exc
    # Cross-check the sidecar against the tar that is actually on disk.
    try:
        actual_size = tar_path.stat().st_size
    except OSError as exc:
        raise TileIndexError(f"tar {tar_path.name} is missing or unreadable") from exc
    if actual_size != index.tar_size:
        raise TileIndexError(
            f"tar size {actual_size} does not match indexed size {index.tar_size}; "
            "the archive may have been modified after publication"
        )
    # Bounds checks only (no I/O on payloads): every referenced header block
    # and its payload region must lie inside the published tar. The single
    # requested tile's header is re-parsed (with checksum verification) at
    # fetch time in open_tile_ppm, so serving one tile never reads the whole
    # archive -- just one 512-byte header plus one contiguous payload range.
    for entry in entries:
        payload_blocks = (entry.size + BLOCKSIZE - 1) // BLOCKSIZE
        region_end = entry.header_offset + BLOCKSIZE + payload_blocks * BLOCKSIZE
        if region_end + 2 * BLOCKSIZE > actual_size:
            raise TileIndexError(
                f"entry for tile {entry.index} lies outside the published tar"
            )
    return index


def open_tile_ppm(tar_path: Path, entry: TileEntry):
    """Open a fresh file handle positioned at the tile's PPM payload.

    Reads only the 512-byte header block and the single contiguous payload
    region recorded in the index. The caller is responsible for closing the
    handle. Raises :class:`TileIndexError` if the header does not verify, so
    a corrupted/relocated entry can never surface another tile's bytes.
    """
    fh = open(tar_path, "rb")
    try:
        fh.seek(entry.header_offset)
        buf = fh.read(BLOCKSIZE)
        if len(buf) != BLOCKSIZE or buf == b"\0" * BLOCKSIZE:
            raise TileIndexError(
                f"tar header for tile {entry.index} is missing at its offset"
            )
        try:
            info = tarfile.TarInfo.frombuf(
                buf, tarfile.ENCODING, "surrogateescape"
            )
        except (tarfile.ReadError, tarfile.InvalidHeaderError) as exc:
            raise TileIndexError(
                f"tar header for tile {entry.index} failed verification: {exc}"
            ) from exc
        if info.name != entry.arcname or info.size != entry.size or not info.isreg():
            raise TileIndexError(
                f"tar entry for tile {entry.index} does not match its index record"
            )
        fh.seek(entry.header_offset + BLOCKSIZE)
        return fh
    except BaseException:
        fh.close()
        raise
