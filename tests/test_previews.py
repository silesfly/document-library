"""Actual descriptor-only child previews on disposable synthetic storage."""
from dataclasses import replace
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
import uuid
import zipfile

from PIL import Image

from document_library.ingest_formats import FormatError, Limits
from document_library.jobs import Lease
from document_library.previews import build_preview, _BoundedOutput
from document_library.storage import StorageRoot
from test_ingest_formats import archive_bytes, image_bytes
from test_storage import SyntheticVolume


class PreviewAcceptance(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='document-library-preview-')
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name).resolve()
        self.root = StorageRoot(self.path, SyntheticVolume())
        self.addCleanup(self.root.close)
        self.lease = Lease(str(uuid.uuid4()), 1, 'preview-worker')

    def stage(self, content):
        with self.root.stage(self.lease) as (path, fd):
            os.write(fd, content)
        return path

    def bundle(self, content, filename, **kwargs):
        path = self.stage(content)
        result = build_preview(self.root, self.lease, path, filename, **kwargs)
        raw = (self.path / result['bundle_relpath']).read_bytes()
        self.assertEqual(result['source_sha256'], hashlib.sha256(content).hexdigest())
        self.assertEqual(result['bundle_sha256'], hashlib.sha256(raw).hexdigest())
        self.assertEqual(result['bundle_size'], len(raw))
        self.assertTrue(result['bundle_relpath'].startswith(self.lease.staging_key + '/'))
        self.assertEqual((self.path / path).read_bytes(), content)
        return result, zipfile.ZipFile(io.BytesIO(raw))

    def test_full_multibyte_text_and_utf8_offsets_beyond_excerpt(self):
        value = '1화\n' + ('합성 📚 본문\n' * 15000)
        result, bundle = self.bundle(value.encode(), 'Book.txt')
        with bundle:
            self.assertEqual(bundle.read('text.txt').decode(), value)
            self.assertGreater(result['text_chars'], 65536)
            index = json.loads(bundle.read('text-index.json'))
            raw = bundle.read('text.txt')
            self.assertEqual(index['byte_size'], len(raw))
            self.assertEqual(index['characters'], len(value))
            reconstructed = ''.join(raw[c['byte_start']:c['byte_end']].decode() for c in index['chunks'])
            self.assertEqual(reconstructed, value)
            for chunk in index['chunks']:
                self.assertEqual(raw[chunk['byte_start']:chunk['byte_end']].decode(),
                                 value[chunk['char_start']:chunk['char_end']])

    def test_markup_preview_is_plain_utf8_and_manifest_binds_members(self):
        _, bundle = self.bundle(b'<h1>1</h1><p>Safe &amp; plain</p>', 'Book.html')
        with bundle:
            body = bundle.read('text.txt')
            self.assertIn(b'Safe & plain', body)
            self.assertNotIn(b'<p>', body)
            manifest = json.loads(bundle.read('manifest.json'))
            self.assertEqual(set(bundle.namelist()), {'text.txt', 'text-index.json', 'manifest.json'})
            for item in manifest['entries']:
                payload = bundle.read(item['name'])
                self.assertEqual(item['sha256'], hashlib.sha256(payload).hexdigest())
                self.assertEqual(item['byte_size'], len(payload))

    def test_imagepack_natural_order_generated_names_and_thumbnails(self):
        entries = []
        for number, color in ((10, 'blue'), (2, 'green'), (1, 'red')):
            stream = io.BytesIO()
            Image.new('RGB', (300, 200), color).save(stream, format='PNG')
            entries.append((f'folder/page{number}.png', stream.getvalue()))
        result, bundle = self.bundle(archive_bytes(entries), 'Images.cbz')
        self.assertEqual(result['page_count'], 3)
        with bundle:
            for ordinal, color in enumerate(((255, 0, 0), (0, 128, 0), (0, 0, 255)), 1):
                with Image.open(io.BytesIO(bundle.read(f'pages/{ordinal:06d}.png'))) as page:
                    self.assertEqual(page.getpixel((0, 0)), color)
                    self.assertEqual(page.size, (300, 200))
                with Image.open(io.BytesIO(bundle.read(f'thumbnails/{ordinal:06d}.png'))) as thumb:
                    self.assertLessEqual(max(thumb.size), 256)
            self.assertFalse(any('folder' in name for name in bundle.namelist()))

    def test_exif_orientation_applied_and_original_unchanged(self):
        stream = io.BytesIO()
        image = Image.new('RGB', (3, 2), 'red')
        exif = Image.Exif()
        exif[274] = 6
        image.save(stream, format='JPEG', exif=exif)
        _, bundle = self.bundle(stream.getvalue(), 'Photo.jpg')
        with bundle, Image.open(io.BytesIO(bundle.read('pages/000001.png'))) as page:
            self.assertEqual(page.size, (2, 3))
            self.assertNotIn(274, page.getexif())

    def test_unvalidated_and_limit_outputs_are_unavailable_and_no_bundle_remains(self):
        for filename, content, limits in [('Unknown.rar', b'opaque original', Limits()),
                                          ('Large.txt', b'larger than parser budget', replace(Limits(), parse_bytes=4))]:
            with self.subTest(filename=filename):
                path = self.stage(content)
                files = set(self.path.rglob('*.part'))
                result = build_preview(self.root, self.lease, path, filename, limits=limits)
                self.assertEqual(result['kind'], 'unavailable')
                self.assertNotIn('bundle_relpath', result)
                self.assertEqual(set(self.path.rglob('*.part')), files)

    def test_wrong_source_report_wrong_generation_and_active_html_rejected(self):
        for content, filename, report, reason in [
            (b'valid text', 'Book.txt', dict(sha256='f' * 64, byte_size=10), 'inspection_mismatch'),
            (b'<script>unsafe</script>', 'Book.html', None, 'active_document')]:
            path = self.stage(content)
            files = set(self.path.rglob('*.part'))
            with self.assertRaisesRegex(FormatError, reason):
                build_preview(self.root, self.lease, path, filename, report=report)
            self.assertEqual(set(self.path.rglob('*.part')), files)
        stale = replace(self.lease, generation=2)
        with self.assertRaisesRegex(FormatError, 'stage_generation'):
            build_preview(self.root, stale, path, 'Book.txt')

    def test_writer_enforces_actual_bundle_bound_before_write(self):
        path = self.stage(b'unchanged')
        with self.root.stage(self.lease) as (_, fd):
            output = _BoundedOutput(fd)
            output.position = 128 * 1024**2
            with self.assertRaisesRegex(FormatError, 'preview_output_limit'):
                output.write(b'x')
            self.assertEqual(os.fstat(fd).st_size, 0)
        self.assertEqual((self.path / path).read_bytes(), b'unchanged')


if __name__ == '__main__':
    unittest.main()
