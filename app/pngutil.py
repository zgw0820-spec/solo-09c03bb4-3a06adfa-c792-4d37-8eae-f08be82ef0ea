"""PNG header inspection used to predict the exact PPM size before conversion.

We always emit 8-bit RGB P6 files with the header ``P6\\n<w> <h>\\n255\\n``,
so the final size is known exactly once width/height are read from IHDR.
"""
from __future__ import annotations

import struct
from pathlib import Path

PNG_SIG = b"\x89PNG\r\n\x1a\n"


def ppm_header(width: int, height: int) -> bytes:
    return f"P6\n{width} {height}\n255\n".encode("ascii")


def ppm_size_for_png(path: Path) -> int | None:
    """Exact size of the P6 PPM this PNG will produce, or None if the
    PNG signature/IHDR cannot be parsed (i.e. not a well-formed PNG)."""
    try:
        with open(path, "rb") as f:
            if f.read(8) != PNG_SIG:
                return None
            raw = f.read(4)
            if len(raw) != 4:
                return None
            (length,) = struct.unpack(">I", raw)
            if f.read(4) != b"IHDR" or length != 13:
                return None
            ihdr = f.read(13)
            if len(ihdr) != 13:
                return None
            width, height = struct.unpack(">II", ihdr[:8])
    except OSError:
        return None
    if width == 0 or height == 0:
        return None
    return len(ppm_header(width, height)) + 3 * width * height
