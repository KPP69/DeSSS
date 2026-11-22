"""Generate placeholder app icons (binaries, not committed).

Writes src-tauri/icons/icon.png (512x512) and src-tauri/icons/icon.ico
(256x256 PNG embedded in an ICO container, required by tauri-build on
Windows for the .exe resource file). Pure stdlib, no Pillow needed.
"""
import pathlib
import struct
import zlib

root = pathlib.Path(__file__).resolve().parent.parent
icons_dir = root / "src-tauri" / "icons"
icons_dir.mkdir(parents=True, exist_ok=True)

RGB = (27, 99, 235, 255)  # solid blue RGBA


def png_bytes(size: int) -> bytes:
    raw = b""
    row = b"\x00" + bytes(RGB) * size
    for _ in range(size):
        raw += row

    def chunk(tag: bytes, data: bytes) -> bytes:
        c = struct.pack(">I", len(data)) + tag + data
        return c + struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF)

    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", size, size, 8, 6, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(raw))
        + chunk(b"IEND", b"")
    )


png_path = icons_dir / "icon.png"
png_path.write_bytes(png_bytes(512))
print(f"wrote {png_path}")

# ICO container with a single PNG-compressed 256x256 image.
# Width/height bytes of 0 mean 256.
ico_png = png_bytes(256)
header = struct.pack("<HHH", 0, 1, 1)
entry = struct.pack(
    "<BBBBHHII",
    0,  # width = 256
    0,  # height = 256
    0,  # palette colors
    0,  # reserved
    1,  # color planes
    32,  # bits per pixel
    len(ico_png),
    len(header) + 16,  # offset of image data
)
ico_path = icons_dir / "icon.ico"
ico_path.write_bytes(header + entry + ico_png)
print(f"wrote {ico_path}")
