"""Installed SDK contract without a Telegram connection, session, or credentials."""
import asyncio
from datetime import datetime, timezone
import tempfile
import unittest

from telethon.client.downloads import DownloadMethods
from telethon.tl import types

from document_library.source_adapter import SourceAdapter, _DownloadSink
from document_library.sources import SourceError


class InstalledSdkContractTests(unittest.TestCase):
    @staticmethod
    def message(size):
        date = datetime(2026, 1, 1, tzinfo=timezone.utc)
        document = types.Document(id=23, access_hash=0, file_reference=b'', date=date,
                                  mime_type='text/plain', size=size, dc_id=1,
                                  attributes=[types.DocumentAttributeFilename('../sample.txt')])
        return types.Message(id=17, peer_id=types.PeerChannel(5), date=date,
                             edit_date=date, grouped_id=31,
                             media=types.MessageMediaDocument(document=document))

    def test_real_sdk_document_metadata_is_allowlisted(self):
        observed = SourceAdapter._observation(self.message(6))
        self.assertEqual(observed, dict(message_id=17, media_index=0, state='attached', metadata={
            'filename': '../sample.txt', 'byte_size': 6, 'document_id': 23,
            'edited_at': 1767225600, 'grouped_id': 31}))

    def test_actual_sdk_download_dispatch_and_stream_writer(self):
        class OfflineDownload(DownloadMethods):
            async def _iter_download(self, location, **kwargs):
                self.location = location
                for chunk in (b'abc', b'def'):
                    yield chunk

        client = OfflineDownload()
        with tempfile.TemporaryFile() as output:
            sink = _DownloadSink(output.fileno(), 6)
            returned = asyncio.run(client.download_media(self.message(6), file=sink))
            self.assertIs(returned, sink)
            output.seek(0)
            self.assertEqual(output.read(), b'abcdef')
            self.assertEqual(sink.size, 6)
            self.assertIsInstance(client.location, types.InputDocumentFileLocation)
            self.assertEqual(client.location.id, 23)
            self.assertFalse(output.closed)

    def test_sdk_cannot_write_more_than_observed_size(self):
        class OversizedDownload(DownloadMethods):
            async def _iter_download(self, location, **kwargs):
                yield b'abc'
                yield b'defg'

        with tempfile.TemporaryFile() as output:
            sink = _DownloadSink(output.fileno(), 6)
            with self.assertRaisesRegex(SourceError, 'exceeds observed size'):
                asyncio.run(OversizedDownload().download_media(self.message(6), file=sink))
            output.seek(0)
            self.assertEqual(output.read(), b'abc')
            self.assertEqual(sink.size, 3)


if __name__ == '__main__':
    unittest.main()
