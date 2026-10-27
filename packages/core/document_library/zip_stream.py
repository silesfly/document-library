"""Flat ZIP64 STORED profile, bounded originals and exact source-byte offsets."""
from dataclasses import dataclass
import struct
import zlib

from .library_contracts import archive_name_v1


@dataclass(frozen=True)
class ZipEntry:
    name: str
    size: int


@dataclass(frozen=True)
class EntrySpan:
    entry: ZipEntry
    header_start: int
    data_start: int
    data_end: int


@dataclass(frozen=True)
class ArchiveLayout:
    spans: tuple
    central_start: int
    total_bytes: int

    def source_bytes(self, sent):
        """Successful ASGI body calls form a prefix, including on failure."""
        if type(sent) is not int or not 0 <= sent <= self.total_bytes:
            raise ValueError('Archive byte count is outside the fixed representation')
        return tuple(max(0, min(sent, span.data_end)-span.data_start) for span in self.spans)


def archive_layout(entries):
    entries = tuple(entries)
    if len(entries) > 200 or any(not isinstance(entry, ZipEntry) for entry in entries):
        raise ValueError('At most 200 explicit archive members required')
    names, spans, cursor = set(), [], 0
    for entry in entries:
        if (not archive_name_v1(entry.name) or '/' in entry.name
                or entry.name.casefold() in names or type(entry.size) is not int
                or not 0 <= entry.size < 2**64):
            raise ValueError('Safe unique flat name and unsigned size required')
        names.add(entry.name.casefold())
        start = cursor + 50 + len(entry.name.encode('utf-8'))
        spans.append(EntrySpan(entry, cursor, start, start+entry.size))
        cursor = start + entry.size + 24
    central = cursor
    cursor += sum(74+len(span.entry.name.encode('utf-8')) for span in spans)
    return ArchiveLayout(tuple(spans), central, cursor+98)


def zip_chunks(layout, read_chunks):
    """read_chunks(index) yields original bytes, each <=64KiB; no archive spool."""
    if not isinstance(layout, ArchiveLayout) or archive_layout(s.entry for s in layout.spans) != layout:
        raise ValueError('An exact immutable archive layout is required')
    central = []
    for index, span in enumerate(layout.spans):
        name, size = span.entry.name.encode('utf-8'), span.entry.size
        extra = struct.pack('<HHQQ', 1, 16, size, size)
        yield struct.pack('<IHHHHHIIIHH', 0x04034b50,45,0x808,0,0,33,0,0xffffffff,0xffffffff,len(name),len(extra))+name+extra
        crc, received = 0, 0
        for data in read_chunks(index):
            if not isinstance(data, bytes) or not 0 < len(data) <= 65536:
                raise ValueError('Bounded original chunks required')
            received += len(data)
            if received > size:
                raise ValueError('Original exceeds fixed archive size')
            crc = zlib.crc32(data, crc)
            yield data
        if received != size:
            raise ValueError('Original is shorter than fixed archive size')
        yield struct.pack('<IIQQ', 0x08074b50,crc,size,size)
        extra = struct.pack('<HHQQQ', 1, 24, size, size, span.header_start)
        central.append(struct.pack('<IHHHHHHIIIHHHHHII',0x02014b50,45,45,0x808,0,0,33,crc,
                                   0xffffffff,0xffffffff,len(name),len(extra),0,0,0,0,0xffffffff)+name+extra)
    for record in central:
        yield record
    count = len(layout.spans)
    central_size = layout.total_bytes-98-layout.central_start
    zip64_offset = layout.total_bytes-98
    yield struct.pack('<IQHHIIQQQQ',0x06064b50,44,45,45,0,0,count,count,central_size,layout.central_start)
    yield struct.pack('<IIQI',0x07064b50,0,zip64_offset,1)
    yield struct.pack('<IHHHHIIH',0x06054b50,0,0,0xffff,0xffff,0xffffffff,0xffffffff,0)
