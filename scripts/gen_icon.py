"""Generate a placeholder 512x512 icon at src-tauri/icons/icon.png (binary, not committed)."""
import pathlib, struct, zlib
root = pathlib.Path(__file__).resolve().parent.parent
out = root/"src-tauri"/"icons"/"icon.png"; out.parent.mkdir(parents=True, exist_ok=True)
w=h=512; raw=b""
for _ in range(h): raw+=b"\x00"+bytes([27,99,235,255]*w)
def chunk(t,d):
    c=struct.pack(">I",len(d))+t+d; return c+struct.pack(">I", zlib.crc32(t+d)&0xffffffff)
out.write_bytes(b"\x89PNG\r\n\x1a\n"+chunk(b"IHDR",struct.pack(">IIBBBBB",w,h,8,6,0,0,0))+chunk(b"IDAT",zlib.compress(raw))+chunk(b"IEND",b""))
print(f"wrote {out}")
