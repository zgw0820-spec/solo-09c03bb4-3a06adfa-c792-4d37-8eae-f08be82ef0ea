#!/usr/bin/env python3
"""Generate the PNG tiles used by the smoke test.

Usage: gen_tiles.py OUTDIR

Sizes are chosen against the default demo quota of 20 MiB
(PPM_STAGING_BYTES=20971520):

* big.png    2600x2000 -> 15,600,017 B PPM: fits, and fits together with one
             small tile, but a second small tile must wait for quota.
* smallN.png 1024x1024 ->  3,145,745 B PPM each.
* medium.png 2048x2048 -> 12,582,929 B PPM.
* huge.png   3000x3000 -> 27,000,018 B PPM: alone exceeds the 20 MiB limit.
* trunc.png  valid PNG header, truncated body -> conversion fails in worker.
* junk.png   not a PNG at all -> rejected by the header check.
"""
import sys
from pathlib import Path

from PIL import Image, ImageDraw


def make_png(width: int, height: int, path: Path, seed: int = 0) -> None:
    im = Image.new("RGB", (width, height))
    d = ImageDraw.Draw(im)
    for y in range(0, height, 4):
        d.rectangle(
            [0, y, width, min(y + 4, height)],
            fill=((y * 7 + seed * 31) % 256, (y * 3 + seed * 17) % 256,
                  (y * 11 + seed * 5) % 256),
        )
    for x in range(0, width, 200):
        d.rectangle([x, 0, x + 60, height // 3], outline=(255, 255, 255))
    im.save(path, "PNG")


def main() -> None:
    out = Path(sys.argv[1])
    out.mkdir(parents=True, exist_ok=True)
    make_png(2600, 2000, out / "big.png")
    for i in range(1, 5):
        make_png(1024, 1024, out / f"small{i}.png", seed=i)
    make_png(2048, 2048, out / "medium.png", seed=9)
    make_png(3000, 3000, out / "huge.png", seed=13)

    data = (out / "small1.png").read_bytes()
    (out / "trunc.png").write_bytes(data[:80])  # IHDR intact, body truncated
    big = (out / "big.png").read_bytes()
    # truncated big tile: predicted PPM is 15.6MB, so it only gets dispatched
    # once earlier tiles have packed (used by the concurrent-failure test)
    (out / "truncbig.png").write_bytes(big[:80])
    (out / "junk.png").write_bytes(b"this is not a png file at all\n" * 4)

    for p in sorted(out.iterdir()):
        print(f"  {p.name:12s} {p.stat().st_size:>10,} B")


if __name__ == "__main__":
    main()
