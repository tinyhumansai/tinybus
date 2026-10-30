"""Regenerate the independent ZIP fixtures used by zip_extract_tests.rs.

Run from the TinyBus component root with Python 3's standard library only:

    python3 crates/tinybus/src/module/fixtures/generate.py
"""

from __future__ import annotations

import io
import pathlib
import stat
import struct
import warnings
import zipfile

ROOT = pathlib.Path(__file__).parent


class NonSeekable(io.BytesIO):
    def seekable(self) -> bool:
        return False

    def seek(self, *args: object) -> int:
        raise io.UnsupportedOperation("fixture is intentionally streaming")


def archive(rows: list[tuple[str, bytes, int | None, int]], *, streaming: bool = False) -> bytes:
    output: io.BytesIO = NonSeekable() if streaming else io.BytesIO()
    with zipfile.ZipFile(output, "w") as writer:
        for name, body, mode, compression in rows:
            info = zipfile.ZipInfo(name)
            info.compress_type = compression
            if mode is not None:
                info.external_attr = mode << 16
            writer.writestr(info, body)
    return output.getvalue()


def write(name: str, body: bytes) -> bytearray:
    data = bytearray(body)
    (ROOT / name).write_bytes(data)
    return data


def central_records(data: bytes | bytearray) -> list[int]:
    offsets: list[int] = []
    offset = 0
    while (offset := data.find(b"PK\x01\x02", offset)) >= 0:
        offsets.append(offset)
        offset += 4
    return offsets


def stored(rows: list[tuple[str, bytes, int | None]]) -> bytes:
    return archive([(name, body, mode, zipfile.ZIP_STORED) for name, body, mode in rows])


write(
    "compat_store.zip",
    stored(
        [
            ("folder/", b"", None),
            ("folder/module.so", b"module-payload", None),
            ("empty", b"", None),
        ]
    ),
)
write(
    "compat_deflate_descriptor.zip",
    archive(
        [("folder/module.so", b"deflated-module-payload" * 16, None, zipfile.ZIP_DEFLATED)],
        streaming=True,
    ),
)
write("unsafe_parent.zip", stored([("../outside.txt", b"outside", None)]))
write("windows_drive.zip", stored([("C:/outside.txt", b"outside", None)]))
write("windows_backslash.zip", stored([("..\\outside.txt", b"outside", None)]))
write("unix_link_mode.zip", stored([("link", b"../outside.txt", stat.S_IFLNK | 0o777)]))
write(
    "suid_mode.zip",
    stored([("module.so", b"module-payload", stat.S_IFREG | stat.S_ISUID | stat.S_ISGID | 0o777)]),
)
with warnings.catch_warnings():
    warnings.simplefilter("ignore", UserWarning)
    write("duplicate.zip", stored([("module.so", b"first", None), ("module.so", b"second", None)]))
write("archive_collision.zip", stored([("fixture.zip", b"must not replace the archive", None)]))
write("case_collision.zip", stored([("foo/module.so", b"first", None), ("FOO/MODULE.SO", b"second", None)]))
write("parent_case_collision.zip", stored([("A/one", b"first", None), ("a/two", b"second", None)]))
write("file_directory_collision.zip", stored([("a", b"file", None), ("a/b", b"child", None)]))
write(
    "canonical_parent_collision.zip",
    stored([("é/one", b"first", None), ("e\u0301/two", b"second", None)]),
)
write(
    "canonical_leaf_collision.zip",
    stored([("dir/café", b"first", None), ("dir/cafe\u0301", b"second", None)]),
)
write(
    "casefold_collision.zip",
    stored([("dir/Straße", b"first", None), ("dir/STRASSE", b"second", None)]),
)
write(
    "unicode_paths.zip",
    stored([("café/module.so", b"accented", None), ("数据/module.so", b"cjk", None)]),
)

zip64_local = io.BytesIO()
with zipfile.ZipFile(zip64_local, "w") as writer:
    info = zipfile.ZipInfo("module.so")
    info.compress_type = zipfile.ZIP_STORED
    with writer.open(info, "w", force_zip64=True) as entry:
        entry.write(b"zip64-local-module")
write("compat_zip64_local.zip", zip64_local.getvalue())

zip64_streaming = NonSeekable()
with zipfile.ZipFile(zip64_streaming, "w") as writer:
    info = zipfile.ZipInfo("module.so")
    info.compress_type = zipfile.ZIP_DEFLATED
    with writer.open(info, "w", force_zip64=True) as entry:
        entry.write(b"zip64 streaming descriptor")
write("compat_deflate_descriptor_zip64_local.zip", zip64_streaming.getvalue())

depth_name = "/".join(["a"] * 130) + "/module.so"
write("deep_path.zip", stored([(depth_name, b"", None)]))
write("one_module.zip", stored([("module.so", b"payload", None)]))

encrypted = write("encrypted.zip", stored([("module.so", b"module", None)]))
local = encrypted.find(b"PK\x03\x04")
central = central_records(encrypted)[0]
struct.pack_into("<H", encrypted, local + 6, 1)
struct.pack_into("<H", encrypted, central + 8, 1)
(ROOT / "encrypted.zip").write_bytes(encrypted)

unsupported = write("unsupported_method.zip", stored([("module.so", b"module", None)]))
local = unsupported.find(b"PK\x03\x04")
central = central_records(unsupported)[0]
struct.pack_into("<H", unsupported, local + 8, 99)
struct.pack_into("<H", unsupported, central + 10, 99)
(ROOT / "unsupported_method.zip").write_bytes(unsupported)

overlap = write("overlap.zip", stored([("a.bin", b"first", None), ("b.bin", b"second", None)]))
first_central, second_central = central_records(overlap)
first_local = overlap.find(b"PK\x03\x04")
first_data = first_local + 30 + struct.unpack_from("<H", overlap, first_local + 26)[0]
first_data += struct.unpack_from("<H", overlap, first_local + 28)[0]
second_local = struct.unpack_from("<I", overlap, second_central + 42)[0]
second_end = second_local + 30 + struct.unpack_from("<H", overlap, second_local + 26)[0]
second_end += struct.unpack_from("<H", overlap, second_local + 28)[0]
second_end += struct.unpack_from("<I", overlap, second_local + 18)[0]
overlap_size = second_end - first_data
for offset in (first_local + 18, first_local + 22, first_central + 20, first_central + 24):
    struct.pack_into("<I", overlap, offset, overlap_size)
(ROOT / "overlap.zip").write_bytes(overlap)
