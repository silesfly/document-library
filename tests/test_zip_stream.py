"""Independent stdlib ZIP decoder checks for the exact streaming profile."""
import hashlib
import io
import struct
import tracemalloc
import unittest
import zipfile
import zlib

from document_library.zip_stream import ZipEntry, archive_layout, zip_chunks


class ZipStreamAcceptance(unittest.TestCase):
    def test_exact_profile_crc_unicode_empty_and_duplicate_content(self):
        payloads = (b'',b'original\x00bytes'*10000,b'original\x00bytes'*10000,'한글'.encode())
        names = ('empty.txt','edition-one.cbz','edition-two.cbz','원본.txt')
        layout = archive_layout(ZipEntry(name,len(data)) for name,data in zip(names,payloads))
        raw = b''.join(zip_chunks(layout,lambda i:(payloads[i][j:j+65536] for j in range(0,len(payloads[i]),65536))))
        self.assertEqual(len(raw),98+sum(len(data)+148+2*len(name.encode()) for name,data in zip(names,payloads)))
        self.assertEqual(layout.total_bytes,len(raw))
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            self.assertEqual(archive.namelist(),list(names))
            self.assertIsNone(archive.testzip())
            for name,payload in zip(names,payloads):
                info = archive.getinfo(name)
                self.assertEqual(hashlib.sha256(archive.read(name)).digest(),hashlib.sha256(payload).digest())
                self.assertEqual(info.CRC,zlib.crc32(payload))
                self.assertEqual(info.compress_type,zipfile.ZIP_STORED)
                self.assertTrue(info.flag_bits & 0x800)
        for span in layout.spans:
            self.assertEqual(struct.unpack_from('<I',raw,span.header_start)[0],0x04034b50)
        for sent in (0,layout.spans[1].data_start+11,len(raw)):
            self.assertEqual(layout.source_bytes(sent),tuple(max(0,min(sent,s.data_end)-s.data_start) for s in layout.spans))

    def test_empty_archive_is_independently_decodable(self):
        layout=archive_layout(())
        raw=b''.join(zip_chunks(layout,lambda i:()))
        self.assertEqual(len(raw),98)
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            self.assertEqual(archive.namelist(),[])

    def test_no_unsafe_or_ambiguous_entry_and_bounded_members(self):
        for name in ('../x','a/b','\\x','a:b','.hidden','a.','a ','a\x00','Ａ.txt'):
            with self.subTest(name=name),self.assertRaises(ValueError):
                archive_layout((ZipEntry(name,1),))
        for entries in ((ZipEntry('a',1),ZipEntry('A',2)),(ZipEntry('x',-1),),tuple(ZipEntry(str(i),0) for i in range(201))):
            with self.assertRaises(ValueError):
                archive_layout(entries)

    def test_short_long_and_oversized_reader_chunks_fail(self):
        for size,data in ((2,b'a'),(0,b'a'),(70000,b'a'*70000)):
            with self.subTest(size=size),self.assertRaises(ValueError):
                list(zip_chunks(archive_layout((ZipEntry('a',size),)),lambda i:(data,)))

    def test_200_members_large_virtual_stream_is_bounded(self):
        block=b'x'*65536
        layout=archive_layout(ZipEntry(f'asset-{i:03d}.bin',len(block)*16) for i in range(200))
        tracemalloc.start()
        total=sum(len(data) for data in zip_chunks(layout,lambda i:(block for _ in range(16))))
        _,peak=tracemalloc.get_traced_memory()
        tracemalloc.stop()
        self.assertEqual(total,layout.total_bytes)
        self.assertLess(peak,512*1024)
        self.assertEqual(layout.source_bytes(total),(1024**2,)*200)

    def test_exact_2gib_cap_includes_all_zip_overhead(self):
        cap=2*1024**3
        overhead=archive_layout((ZipEntry('a',0),)).total_bytes
        self.assertEqual(archive_layout((ZipEntry('a',cap-overhead),)).total_bytes,cap)
        self.assertEqual(archive_layout((ZipEntry('a',cap-overhead+1),)).total_bytes,cap+1)


if __name__=='__main__':
    unittest.main()
