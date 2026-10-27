"""Synthetic ingestion contents and descriptor boundaries; no external input."""
from dataclasses import replace
import hashlib
import io
import json
import os
from pathlib import Path
import stat
import subprocess
import tempfile
import traceback
import unittest
from unittest.mock import patch
import warnings
import zipfile

from PIL import Image
from pypdf import PdfWriter
from pypdf.generic import DictionaryObject, NameObject, TextStringObject

from document_library.ingest_formats import FormatError, Limits, inspect_bytes, inspect_fd, parse_metadata


def image_bytes(fmt='PNG', size=(3, 2)):
    output = io.BytesIO()
    Image.new('RGB', size, 'navy').save(output, format=fmt)
    return output.getvalue()


def archive_bytes(entries, compression=zipfile.ZIP_STORED):
    output = io.BytesIO()
    with zipfile.ZipFile(output, 'w', compression=compression) as archive:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', UserWarning)
            for name, data in entries:
                archive.writestr(name, data)
    return output.getvalue()


class FormatAcceptance(unittest.TestCase):
    def test_content_gaps_and_independent_completion_candidate(self):
        text = '제1화 시작\r\n내용\r\n제3화 끝\r\n내용'
        result = inspect_bytes(text.encode(), 'ＳＦ 1984 1-3화 (완결).txt')
        meta = result['metadata']
        self.assertEqual(meta['title_display'], 'SF 1984')
        self.assertEqual(meta['title_match_key'], 'sf1984')
        self.assertEqual(meta['coverage'], {'unit': 'episode', 'intervals': [[1, 1], [3, 3]], 'gaps': [[2, 2]], 'confidence': 'verified'})
        self.assertEqual(meta['review_reason'], 'filename_content_disagree')
        self.assertEqual(meta['publication_state'], 'unknown')
        self.assertEqual(set(meta['episode_hashes']), {'1', '3'})
        self.assertEqual(result['sha256'], hashlib.sha256(text.encode()).hexdigest())

    def test_first_episode_hash_includes_nonblank_preamble(self):
        plain = parse_metadata('Book.txt', '1화\nbody1\n2화\nbody2\n')
        prefaced = parse_metadata('Book.txt', 'unique preface\r\n1화\r\nbody1\r\n2화\r\nbody2\r\n')
        self.assertNotEqual(plain['episode_hashes']['1'], prefaced['episode_hashes']['1'])
        self.assertEqual(plain['episode_hashes']['2'], prefaced['episode_hashes']['2'])
        spaced = parse_metadata('Book.txt', ' \r\n\t1화\nbody1\n2화\nbody2\n')
        self.assertEqual(plain['episode_hashes'], spaced['episode_hashes'])
        empty = parse_metadata('Book.txt', 'preface\n1화\n2화\nbody2\n')
        self.assertEqual(empty['review_reason'], 'headings_without_body')
        self.assertEqual(empty['episode_hashes'], {})

    def test_ambiguous_metadata_is_not_authority(self):
        for text in ('1화\na\n1화\nb', '2화\na\n1화\nb', '1권\na\n2화\nb'):
            with self.subTest(text=text):
                result = parse_metadata('A 1-20화.txt', text)
                self.assertEqual(result['coverage']['confidence'], 'unknown')
                self.assertEqual(result['episode_hashes'], {})
        self.assertEqual(parse_metadata('1984.txt')['title_display'], '1984')
        self.assertEqual(parse_metadata('A 1권-2화.txt')['coverage']['confidence'], 'unknown')
        self.assertEqual(parse_metadata('A 1-150화.epub')['coverage']['confidence'], 'inferred')
        self.assertEqual(parse_metadata('A 4권.txt')['coverage']['unit'], 'volume')
        self.assertEqual(parse_metadata('A.txt', '1화 시작\n2화 끝')['coverage']['confidence'], 'unknown')
        with self.assertRaisesRegex(FormatError, 'episode_limit'):
            parse_metadata('A.txt', '1화\na\n2화\nb', max_entries=1)

    def test_text_encodings_and_binary_or_path_rejection(self):
        for encoding in ('utf-8-sig', 'utf-16', 'cp949'):
            with self.subTest(encoding=encoding):
                self.assertIn('본문', inspect_bytes('1화\n본문'.encode(encoding), 'A.txt')['text_excerpt'])
        for data, name in ((b'a\x00b', 'A.txt'), (b'abc', '../A.txt'), (b'abc', 'A.exe'), (b'', 'A.txt'), (b'abc', 'A\u0000.txt'), (b'abc', 'x／A.txt')):
            with self.subTest(name=name):
                with self.assertRaises(FormatError):
                    inspect_bytes(data, name)

    def test_markup_extracts_plain_content_and_rejects_active_resources(self):
        result = inspect_bytes('<h1>1화</h1><p>본문 &amp; text</p>'.encode(), 'A.html')
        self.assertIn('본문 & text', result['text_excerpt'])
        self.assertEqual(result['metadata']['coverage']['intervals'], [[1, 1]])
        for markup in ('<script>alert(1)</script>', '<img src="https://example.invalid/a">', '<a href="&#106;avascript:alert(1)">a</a>', '<img onerror="x">', '<svg><use xlink:href="https://example.invalid/a"/></svg>', '<style>@import "x";</style>'):
            with self.subTest(markup=markup):
                with self.assertRaises(FormatError):
                    inspect_bytes(markup.encode(), 'A.html')
        with self.assertRaises(FormatError):
            inspect_bytes(b'<!DOCTYPE x [<!ENTITY a "hi">]><x>&a;</x>', 'A.fb2')
        self.assertEqual(inspect_bytes('<body><p>제1화 <b>시작</b></p><p>본문</p></body>'.encode(), 'A.fb2')['metadata']['coverage']['intervals'], [[1, 1]])

    def test_image_actual_decode_and_limits(self):
        result = inspect_bytes(image_bytes(), 'A.png')
        self.assertEqual(result['image']['dimensions'], [[3, 2]])
        self.assertEqual(result['metadata']['coverage']['unit'], 'unknown')
        for data, name, limits in ((image_bytes(), 'A.jpg', Limits()), (b'not an image', 'A.png', Limits()), (image_bytes(), 'A.png', replace(Limits(), pixels=5))):
            with self.assertRaises(FormatError):
                inspect_bytes(data, name, limits)
        output = io.BytesIO()
        Image.new('RGB', (2, 2), 'red').save(output, format='GIF', save_all=True, append_images=[Image.new('RGB', (2, 2), 'blue')])
        with self.assertRaisesRegex(FormatError, 'frame_limit'):
            inspect_bytes(output.getvalue(), 'A.gif', replace(Limits(), frames=1))

    def test_imagepack_natural_order_and_page_is_not_episode(self):
        data = archive_bytes([(f'page{n}.png', image_bytes()) for n in (10, 2, 1)])
        result = inspect_bytes(data, 'A.cbz')
        self.assertEqual([p['name'] for p in result['pages']], ['page1.png', 'page2.png', 'page10.png'])
        self.assertEqual(result['representation'], 'imagepack')
        self.assertEqual(result['metadata']['coverage']['confidence'], 'unknown')

    def test_archive_paths_aliases_duplicates_and_file_prefixes(self):
        pixel = image_bytes()
        cases = [[('../a.png', pixel)], [('/a.png', pixel)], [('a\\b.png', pixel)], [('a/./b.png', pixel)], [('a//b.png', pixel)], [('a.png', pixel), ('a.png', pixel)], [('a/1.png', pixel), ('A/2.png', pixel)], [('a.png', pixel), ('a.png/2.png', pixel)], [('a.png/2.png', pixel), ('a.png', pixel)], [('a\x00.png', pixel)], [('x' * 1025 + '.png', pixel)]]
        for entries in cases:
            with self.subTest(names=[str(p[0])[:40] for p in entries]):
                with self.assertRaises(FormatError):
                    inspect_bytes(archive_bytes(entries), 'A.zip')
        for kind in (stat.S_IFLNK, stat.S_IFIFO, stat.S_IFDIR):
            info = zipfile.ZipInfo('a.png')
            info.create_system = 3
            info.external_attr = (kind | 0o600) << 16
            with self.assertRaises(FormatError):
                inspect_bytes(archive_bytes([(info, pixel)]), 'A.zip')

    def test_archive_aggregate_pixel_and_frame_budget(self):
        data = archive_bytes([('a.png', image_bytes()), ('b.png', image_bytes())])
        for limits in (replace(Limits(), archive_pixels=10), replace(Limits(), archive_frames=1)):
            with self.assertRaisesRegex(FormatError, 'archive_image_budget'):
                inspect_bytes(data, 'A.zip', limits)
        self.assertEqual(len(inspect_bytes(data, 'A.zip', replace(Limits(), archive_pixels=12, archive_frames=2))['pages']), 2)

    def test_archive_bombs_nesting_entry_limits_and_crc(self):
        for entries, limits in (([('a.png', image_bytes())], replace(Limits(), member_bytes=5)), ([('a.png', image_bytes()), ('b.png', image_bytes())], replace(Limits(), entries=1)), ([('readme.txt', b'a' * 10000)], replace(Limits(), ratio=2)), ([('a.png', b'PK\x03\x04fake')], Limits()), ([('run.exe', b'abc')], Limits())):
            with self.assertRaises(FormatError):
                inspect_bytes(archive_bytes(entries, zipfile.ZIP_DEFLATED), 'A.zip', limits)
        raw = archive_bytes([('a.png', image_bytes())])
        offset = raw.index(b'\x89PNG')
        damaged = raw[:offset] + b'X' + raw[offset + 1:]
        with self.assertRaises(FormatError):
            inspect_bytes(damaged, 'A.zip')

    def test_epub_spine_order_not_zip_order_and_missing_member(self):
        entries = [
            ('mimetype', b'application/epub+zip'),
            ('META-INF/container.xml', b'<container><rootfiles><rootfile full-path="OPS/book.opf"/></rootfiles></container>'),
            ('OPS/book.opf', b'<package><manifest><item id="a" href="one.xhtml" media-type="application/xhtml+xml"/><item id="b" href="two.xhtml" media-type="application/xhtml+xml"/></manifest><spine><itemref idref="a"/><itemref idref="b"/></spine></package>'),
            ('OPS/two.xhtml', '<html><body><p>2화</p><p>둘</p></body></html>'.encode()),
            ('OPS/one.xhtml', '<html><body><p>1화</p><p>하나</p></body></html>'.encode()),
        ]
        result = inspect_bytes(archive_bytes(entries), 'A.epub')
        self.assertEqual(result['metadata']['coverage']['intervals'], [[1, 2]])
        self.assertLess(result['text_excerpt'].index('하나'), result['text_excerpt'].index('둘'))
        with self.assertRaises(FormatError):
            inspect_bytes(archive_bytes(entries[:-1]), 'A.epub')

    def test_office_external_relationship_and_plain_document(self):
        body = '<document><p>1화</p><p>본문</p></document>'.encode()
        result = inspect_bytes(archive_bytes([('word/document.xml', body)]), 'A.docx')
        self.assertEqual(result['metadata']['coverage']['intervals'], [[1, 1]])
        for relationship in (b'<Relationships><Relationship Target="https://example.invalid/a"/></Relationships>', b'<Relationships><Relationship Target="relative" TargetMode="External"/></Relationships>'):
            with self.assertRaises(FormatError):
                inspect_bytes(archive_bytes([('word/document.xml', body), ('word/_rels/document.xml.rels', relationship)]), 'A.docx')
        self.assertIn('본문', inspect_bytes(archive_bytes([('mimetype', b'application/vnd.oasis.opendocument.text'), ('content.xml', body)]), 'A.odt')['text_excerpt'])

    def test_pdf_valid_blank_is_document_not_episode_and_active_rejected(self):
        def pdf(active=False, encrypted=False):
            writer = PdfWriter()
            writer.add_blank_page(width=100, height=100)
            if active:
                writer._root_object[NameObject('/OpenAction')] = DictionaryObject({NameObject('/S'): NameObject('/URI'), NameObject('/URI'): TextStringObject('https://example.invalid')})
            if encrypted:
                writer.encrypt('synthetic-test-only')
            output = io.BytesIO()
            writer.write(output)
            return output.getvalue()
        result = inspect_bytes(pdf(), 'A.pdf')
        self.assertEqual(result['page_count'], 1)
        self.assertEqual(result['representation'], 'document')
        self.assertEqual(result['metadata']['coverage']['unit'], 'unknown')
        for data in (b'fake', pdf(active=True), pdf(encrypted=True)):
            with self.assertRaises(FormatError):
                inspect_bytes(data, 'A.pdf')

    def test_optional_formats_are_explicitly_unvalidated(self):
        for suffix in ('7z', 'rar', 'mobi', 'azw3', 'rtf', 'cbr'):
            result = inspect_bytes(b'opaque preserved original', 'A.' + suffix)
            self.assertEqual((result['status'], result['preview']), ('review', 'unavailable'))

    def test_pdf_direct_attachments_actions_and_external_streams_rejected(self):
        from pypdf.generic import ArrayObject, DecodedStreamObject
        for kind in ('attachment', 'gotoe', 'external_stream'):
            with self.subTest(kind=kind):
                writer = PdfWriter()
                page = writer.add_blank_page(width=100, height=100)
                if kind == 'attachment':
                    stream = DecodedStreamObject()
                    stream.set_data(b'synthetic embedded bytes')
                    spec = DictionaryObject({NameObject('/Type'): NameObject('/Filespec'), NameObject('/F'): TextStringObject('embedded.txt'), NameObject('/EF'): DictionaryObject({NameObject('/F'): writer._add_object(stream)})})
                    annotation = DictionaryObject({NameObject('/Subtype'): NameObject('/FileAttachment'), NameObject('/FS'): writer._add_object(spec)})
                    page[NameObject('/Annots')] = ArrayObject([writer._add_object(annotation)])
                elif kind == 'gotoe':
                    action = DictionaryObject({NameObject('/S'): NameObject('/GoToE'), NameObject('/F'): TextStringObject('external.pdf')})
                    page[NameObject('/A')] = writer._add_object(action)
                else:
                    stream = DecodedStreamObject()
                    stream.set_data(b'')
                    stream[NameObject('/F')] = TextStringObject('external.txt')
                    writer._root_object[NameObject('/Custom')] = writer._add_object(stream)
                output = io.BytesIO()
                writer.write(output)
                with self.assertRaisesRegex(FormatError, 'active_pdf'):
                    inspect_bytes(output.getvalue(), 'A.pdf')

    def test_markup_alternate_urls_and_css_obfuscation_rejected(self):
        cases = [
            '<img srcset="https://example.invalid/a.png 1x">',
            '<meta http-equiv="refresh" content="0;url=https://example.invalid/">',
            '<video poster="https://example.invalid/a.png"></video>',
            r'<p style="background:u\72l(https://example.invalid/a)">text</p>',
            r'<style>p { background:u/**/rl(https://example.invalid/a) }</style>',
            '<svg><animate attributeName="href" to="https://example.invalid/a"/></svg>',
            '<svg><rect fill="url(https://example.invalid/paint.svg#paint)"/></svg>',
            '<a ping="https://example.invalid/log">link</a>',
            '<svg xml:base="https://example.invalid/"><image href="a.png"/></svg>',
        ]
        for markup in cases:
            with self.subTest(markup=markup):
                with self.assertRaises(FormatError):
                    inspect_bytes(markup.encode(), 'A.html')
        for function in ('image-set', '-webkit-image-set', 'image', 'src'):
            css = f'background-image:{function}("https://example.invalid/a.png" 1x)'
            for markup, suffix in ((f"<p style='{css}'>body</p>", 'html'), (f'<body><style>p {{{css}}}</style></body>', 'fb2')):
                with self.subTest(function=function, suffix=suffix):
                    with self.assertRaises(FormatError):
                        inspect_bytes(markup.encode(), 'A.' + suffix)
        with self.assertRaises(FormatError):
            inspect_bytes(r'<body><style>p {background:u\72l(https://example.invalid/a)}</style></body>'.encode(), 'A.fb2')
        with self.assertRaises(FormatError):
            inspect_bytes(b'<body><svg><rect filter="url(https://example.invalid/filter.svg#x)"/></svg></body>', 'A.fb2')
        self.assertIn('text', inspect_bytes(b'<p style="margin:0;color:navy">text</p>', 'A.html')['text_excerpt'])
        plain = inspect_bytes(b'<style>p { color:navy }</style><p>image-set() and url() are prose</p>', 'A.html')['text_excerpt']
        self.assertIn('image-set() and url() are prose', plain)
        self.assertNotIn('color:navy', plain)
        for tag in ('<style/>', '<style />', '<STYLE/>'):
            with self.subTest(tag=tag):
                with self.assertRaisesRegex(FormatError, 'ambiguous_style_element'):
                    inspect_bytes((tag + 'p{background:url(https://example.invalid/a.png)}</style><p>body</p>').encode(), 'A.html')

    def test_pdf_type3_glyph_and_resource_names_are_not_actions(self):
        from pypdf.generic import ArrayObject, DecodedStreamObject, FloatObject, NumberObject
        writer = PdfWriter()
        page = writer.add_blank_page(width=100, height=100)
        glyph = DecodedStreamObject()
        glyph.set_data(b'500 0 0 0 500 500 d1 0 0 500 500 re f')
        glyph_ref = writer._add_object(glyph)
        font = DictionaryObject({
            NameObject('/Type'): NameObject('/Font'), NameObject('/Subtype'): NameObject('/Type3'),
            NameObject('/FontBBox'): ArrayObject([NumberObject(n) for n in (0, 0, 500, 500)]),
            NameObject('/FontMatrix'): ArrayObject([FloatObject(n) for n in (.001, 0, 0, .001, 0, 0)]),
            NameObject('/FirstChar'): NumberObject(65), NameObject('/LastChar'): NumberObject(65),
            NameObject('/Widths'): ArrayObject([NumberObject(500)]),
            NameObject('/Encoding'): DictionaryObject({NameObject('/Type'): NameObject('/Encoding'), NameObject('/Differences'): ArrayObject([NumberObject(65), NameObject('/A')])}),
            NameObject('/CharProcs'): DictionaryObject({NameObject(n): glyph_ref for n in ('/A', '/F', '/S', '/AA', '/JS')}),
            NameObject('/Resources'): DictionaryObject(),
        })
        page[NameObject('/Resources')] = DictionaryObject({NameObject('/Font'): DictionaryObject({NameObject('/A'): writer._add_object(font)})})
        content = DecodedStreamObject()
        content.set_data(b'BT /A 12 Tf 10 10 Td (A) Tj ET')
        page[NameObject('/Contents')] = writer._add_object(content)
        output = io.BytesIO()
        writer.write(output)
        self.assertEqual(inspect_bytes(output.getvalue(), 'A.pdf')['text_excerpt'].strip(), 'A')
        glyph[NameObject('/A')] = DictionaryObject({NameObject('/S'): NameObject('/JavaScript'), NameObject('/JS'): TextStringObject('synthetic')})
        output = io.BytesIO()
        writer.write(output)
        with self.assertRaisesRegex(FormatError, 'active_pdf'):
            inspect_bytes(output.getvalue(), 'A.pdf')

    def test_child_descriptor_hash_position_size_review_and_invalid_access(self):
        with tempfile.TemporaryDirectory(prefix='document-library-format-test-') as directory:
            path = Path(directory).resolve() / 'input'
            data = '1화\n합성 본문'.encode()
            path.write_bytes(data)
            with path.open('rb') as source:
                source.seek(3)
                result = inspect_fd(source.fileno(), 'A.txt')
                self.assertEqual(source.tell(), 3)
                self.assertEqual(result['sha256'], hashlib.sha256(data).hexdigest())
                self.assertEqual(result['byte_size'], len(data))
                self.assertNotIn('full_text', result)
                self.assertEqual(inspect_fd(source.fileno(), 'A.txt', full_text=False), result)
                self.assertEqual(source.tell(), 3)
                self.assertEqual(result['metadata']['coverage']['intervals'], [[1, 1]])
                result = inspect_fd(source.fileno(), 'A.txt', replace(Limits(), parse_bytes=2), full_text=True)
                self.assertEqual(result['status'], 'review')
                self.assertEqual(result['sha256'], hashlib.sha256(data).hexdigest())
                self.assertEqual(result['byte_size'], len(data))
                self.assertNotIn('full_text', result)
                self.assertEqual(source.tell(), 3)
                self.assertEqual(source.read(), data[3:])
            with path.open('r+b') as source:
                with self.assertRaisesRegex(FormatError, 'readonly_descriptor'):
                    inspect_fd(source.fileno(), 'A.txt')
            os.link(path, path.with_name('alias'))
            with path.open('rb') as source:
                with self.assertRaisesRegex(FormatError, 'invalid_source'):
                    inspect_fd(source.fileno(), 'A.txt')
        self.assertFalse(path.parent.exists())

    def test_child_full_parser_text_unicode_round_trip(self):
        sentinel = 'G4_FULL_PARSER_TEXT_END_끝😀'
        text = '한😀문' * 24000 + '\n' + sentinel
        data = text.encode('utf-8')
        self.assertGreater(len(text), 65536)
        self.assertGreater(len(data), len(text))
        with tempfile.TemporaryDirectory(prefix='document-library-format-test-') as directory:
            path = Path(directory).resolve() / 'input'
            path.write_bytes(data)
            with path.open('rb') as source:
                source.seek(7)
                result = inspect_fd(source.fileno(), 'A.txt', full_text=True)
                self.assertEqual(result['full_text'], text)
                self.assertTrue(result['full_text'].endswith(sentinel))
                self.assertEqual(result['text_excerpt'], text[:65536])
                self.assertNotIn(sentinel, result['text_excerpt'])
                self.assertEqual(result['sha256'], hashlib.sha256(data).hexdigest())
                self.assertEqual(result['byte_size'], len(data))
                self.assertEqual(source.tell(), 7)
                self.assertEqual(source.read(1), data[7:8])
                self.assertFalse(source.closed)
        self.assertFalse(path.parent.exists())

    def test_child_full_text_flag_requires_exact_bool_before_descriptor_access(self):
        with patch('document_library.ingest_formats.subprocess.run') as run, \
                patch('document_library.ingest_formats.os.lseek') as seek, \
                patch('document_library.ingest_formats._display_name') as display_name:
            for value in (0, 1, None, 'true', [], object()):
                with self.subTest(value=value):
                    with self.assertRaisesRegex(FormatError, '^invalid_full_text_flag$'):
                        inspect_fd(-1, 'A.txt', full_text=value)
            with self.assertRaises(TypeError):
                inspect_fd(-1, 'A.txt', Limits(), True)
            run.assert_not_called()
            seek.assert_not_called()
            display_name.assert_not_called()

    def test_child_full_text_parser_and_serialized_unicode_limits(self):
        output_text = '한' * 700000 + 'G4_OUTPUT_LIMIT_END😀'
        self.assertLess(len(output_text), Limits().text_chars)
        self.assertGreater(len(output_text.encode('utf-8')), 2 * 1024**2)
        cases = (
            ('text', '한😀' * 100, replace(Limits(), text_chars=5), 'text_limit'),
            ('output', output_text, Limits(), 'inspection_output_limit'),
        )
        with tempfile.TemporaryDirectory(prefix='document-library-format-test-') as directory:
            path = Path(directory).resolve() / 'input'
            for name, text, limits, code in cases:
                with self.subTest(limit=name):
                    data = text.encode('utf-8')
                    path.write_bytes(data)
                    with path.open('rb') as source:
                        source.seek(4)
                        if name == 'output':
                            report = inspect_fd(source.fileno(), 'A.txt')
                            self.assertNotIn('full_text', report)
                            self.assertEqual(report['sha256'], hashlib.sha256(data).hexdigest())
                            self.assertEqual(report['text_excerpt'], text[:65536])
                            self.assertEqual(source.tell(), 4)
                        with self.assertRaisesRegex(FormatError, '^' + code + '$') as caught:
                            inspect_fd(source.fileno(), 'A.txt', limits, full_text=True)
                        self.assertIsNone(caught.exception.__cause__)
                        self.assertIsNone(caught.exception.__context__)
                        self.assertEqual(source.tell(), 4)
                        self.assertEqual(source.read(1), data[4:5])
                        self.assertFalse(source.closed)
        self.assertFalse(path.parent.exists())

    def test_child_worker_errors_restore_descriptor_without_body_exception_chain(self):
        sentinel = 'G4_PRIVATE_BODY_SENTINEL_비밀😀'
        raw = sentinel.encode('utf-8')
        parse_json = json.loads
        cases = [
            ('timeout', None, 0, 'inspection_timeout'),
            ('truncated_json', b'{"body":"' + raw, 0, 'inspection_worker_failed'),
            ('invalid_utf8', raw + b'\xff', 0, 'inspection_worker_failed'),
            ('nonobject_array', json.dumps([sentinel]).encode(), 0, 'inspection_worker_failed'),
            ('nonobject_string', json.dumps(sentinel).encode(), 0, 'inspection_worker_failed'),
            ('nonobject_number', b'7', 0, 'inspection_worker_failed'),
            ('nonobject_null', b'null', 0, 'inspection_worker_failed'),
            ('nonobject_bool', b'true', 0, 'inspection_worker_failed'),
            ('integer_digit_limit', b'9' * 5000, 0, 'inspection_worker_failed'),
            ('decoder_recursion', b'{}', 0, 'inspection_worker_failed'),
            ('returncode', raw, 1, 'inspection_worker_failed'),
            ('parent_output_limit', raw + b'x' * (2 * 1024**2), 0, 'inspection_worker_failed'),
        ]
        malformed = (None, True, 7, [], {'body': sentinel}, '', sentinel, 'UPPER',
                     'two words', 'line\nbreak', '_prefix', 'suffix_', 'two__parts', 'é', 'a' * 65)
        cases.extend(('malformed_error_' + str(index), json.dumps({'error': value, 'body': sentinel}).encode(),
                      0, 'inspection_worker_failed') for index, value in enumerate(malformed))
        cases.extend(('stable_error_' + str(index), json.dumps({'error': code, 'body': sentinel}).encode(),
                      0, code) for index, code in enumerate(
                          ('text_limit', 'inspection_output_limit', 'readonly_descriptor_required', 'code_2', 'a' * 64)))
        data = b'synthetic descriptor contents'
        with tempfile.TemporaryDirectory(prefix='document-library-format-test-') as directory:
            path = Path(directory).resolve() / 'input'
            path.write_bytes(data)
            with path.open('rb') as source:
                for name, stdout, returncode, code in cases:
                    with self.subTest(case=name):
                        source.seek(4)
                        def run(command, **kwargs):
                            self.assertEqual(kwargs['pass_fds'], (source.fileno(),))
                            self.assertTrue(parse_json(command[-1])['full_text'])
                            os.lseek(source.fileno(), 1, os.SEEK_SET)
                            if stdout is None:
                                raise subprocess.TimeoutExpired(command, kwargs['timeout'], output=raw)
                            return subprocess.CompletedProcess(command, returncode, stdout=stdout)
                        def decode(value):
                            if name == 'decoder_recursion':
                                raise RecursionError(sentinel)
                            return parse_json(value)
                        with patch('document_library.ingest_formats.subprocess.run', side_effect=run) as worker, \
                                patch('document_library.ingest_formats.json.loads', side_effect=decode):
                            try:
                                inspect_fd(source.fileno(), 'A.txt', full_text=True)
                            except FormatError as failure:
                                error = failure
                                formatted = ''.join(traceback.format_exception(failure))
                            else:
                                self.fail('Expected stable inspection failure')
                            worker.assert_called_once()
                        self.assertEqual(str(error), code)
                        self.assertEqual(error.args, (code,))
                        self.assertIsNone(error.__cause__)
                        self.assertIsNone(error.__context__)
                        rendered = (str(error), repr(error), repr(error.args), formatted)
                        for value in rendered:
                            self.assertNotIn(sentinel, value)
                            self.assertNotIn('G4_PRIVATE_BODY_SENTINEL', value)
                        self.assertEqual(source.tell(), 4)
                        self.assertEqual(source.read(1), data[4:5])
                        self.assertFalse(source.closed)
        self.assertFalse(path.parent.exists())

    def test_child_full_text_optional_and_empty_parser_results_keep_status(self):
        cases = (
            ('A.rtf', b'synthetic opaque original', 'review', 'unavailable', 'document'),
            ('A.png', image_bytes(), 'validated', 'image', 'single_image'),
        )
        with tempfile.TemporaryDirectory(prefix='document-library-format-test-') as directory:
            path = Path(directory).resolve() / 'input'
            for filename, data, status, preview, representation in cases:
                with self.subTest(filename=filename):
                    path.write_bytes(data)
                    with path.open('rb') as source:
                        source.seek(3)
                        result = inspect_fd(source.fileno(), filename, full_text=True)
                        self.assertEqual((result['status'], result['preview'], result['representation']),
                                         (status, preview, representation))
                        self.assertEqual(result['full_text'], '')
                        self.assertEqual(result['text_excerpt'], '')
                        self.assertEqual(result['metadata']['coverage']['confidence'], 'unknown')
                        self.assertEqual(result['sha256'], hashlib.sha256(data).hexdigest())
                        self.assertEqual(result['byte_size'], len(data))
                        self.assertEqual(source.tell(), 3)
                        self.assertEqual(source.read(1), data[3:4])
                        self.assertFalse(source.closed)
        self.assertFalse(path.parent.exists())


if __name__ == '__main__':
    unittest.main()
