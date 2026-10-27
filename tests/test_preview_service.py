"""Real ingestion lineage, immutable repair receipts and verified UTF8/cache streams."""
from contextlib import contextmanager
from dataclasses import asdict,replace
import hashlib
import io
import json
import os
import struct
import threading
import time
import tracemalloc
import unittest
import uuid
import zipfile
from unittest.mock import patch

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from document_library.auth import AuthError
from document_library.cursors import CursorError,decode_cursor,encode_cursor
from document_library.database import write_transaction
from document_library.ingest_formats import Limits
from document_library.jobs import JobError,LostLease
from document_library.library_contracts import AssetSelection,canonical_json
from document_library.library_policy import LibraryReadPolicy
from document_library.preview_service import CURSOR_KIND,PreviewService,PreviewError,PreviewNotReady,_Damaged
from document_library.previews import build_preview,preview_profile
from document_library.storage import StorageBusy
import test_ingestion as ingestion_fixtures
from test_ingest_formats import archive_bytes,image_bytes
from test_streaming import run_response
from library_helpers import LibraryFixture


class VerifiedPreviewAcceptance(LibraryFixture):
    def setUp(self):
        self.h = ingestion_fixtures.IngestionAcceptance();self.addCleanup(self.h.doCleanups);self.h.setUp()
        self.policy = LibraryReadPolicy(self.h.auth)
        self.service = PreviewService(self.h.publisher,self.policy)
        self.engine,self.root,self.jobs = self.h.engine,self.h.root,self.h.jobs

    def publish(self, payload=b'1\xed\x99\x94\nsynthetic chapter body\n', *, filename='Book.txt', previous=None,edition='e'):
        _,lease = self.h.queued(payload,filename=filename,previous=previous,edition=edition)
        return self.h.ingest.process_upload(lease),lease

    def fresh(self, revision, position=0):
        def resolve():
            with self.engine.connect() as conn:
                return self.policy.resolve_selection_in(conn,self.h.uploader.token,(AssetSelection(revision,positions=(position,)),))
        return resolve

    def read(self, revision, position=0, **kwargs):
        messages = run_response(self.service.response(self.fresh(revision,position),**kwargs))
        return dict(messages[0]['headers']),b''.join(m.get('body',b'') for m in messages)

    def manifest(self, lease):
        return json.loads(self.scalar('SELECT preview_json FROM ingestion_runs WHERE job_id=:id',id=lease.job_id))

    def damage(self, lease):
        value = self.manifest(lease)
        self.root.unlink(value['bundle_relpath'])

    def missing(self, revision, position=0):
        with self.assertRaises(PreviewNotReady) as failure:
            self.read(revision,position)
        return failure.exception

    def claim(self):
        lease = self.jobs.claim('synthetic-preview-repair',kind='preview.repair',lease_ms=300000)
        self.assertIsNotNone(lease)
        return lease

    def test_inherited_original_preview_uses_its_actual_ingestion_run(self):
        first,old = self.publish(b'150\xed\x99\x94\nold member body\n')
        current,new = self.publish(b'151\xed\x99\x94\nnew member body\n',previous=first)
        for position,word in ((0,b'old member'),(1,b'new member')):
            headers,body = self.read(current,position)
            self.assertIn(word,body)
            selected = self.fresh(current,position)().assets[0]
            expected = old if position == 0 else new
            self.assertEqual(selected.previews[0].source.job_id,expected.job_id)
        self.assertEqual(self.scalar("SELECT count(*) FROM jobs WHERE kind='preview.repair'"),0)

    def test_mixed_utf8_chunks_reconstruct_and_cursor_is_logical_and_bound(self):
        value = '1화\n'+('ASCII 한글 📚\n'*15000)
        revision,lease = self.publish(value.encode())
        cursor,parts = None,[]
        while True:
            headers,body = self.read(revision,cursor=cursor)
            body.decode('utf-8');parts.append(body)
            self.assertLessEqual(len(body),65536)
            next_cursor = headers.get(b'x-preview-next-cursor')
            if next_cursor is None:
                break
            self.assertGreaterEqual(len(body),32768)
            cursor = next_cursor.decode()
            payload = decode_cursor(cursor,expected_kind=CURSOR_KIND)
            self.assertIs(type(payload['ordinal']),int)
            self.assertNotIn('byte_start',payload)
        self.assertEqual(b''.join(parts),value.encode())
        binding = decode_cursor(cursor,expected_kind=CURSOR_KIND)
        for field,wrong in [('revision_id','wrong'),('asset_position',True),('source_sha256','0'*64),
                            ('bundle_sha256','f'*64),('renderer',True),('ordinal',True),('ordinal',99999)]:
            with self.subTest(field=field,wrong=wrong),self.assertRaises(CursorError):
                self.read(revision,cursor=encode_cursor(CURSOR_KIND,dict(binding,**{field:wrong})))
        with self.assertRaises(CursorError):
            self.read(revision,cursor=encode_cursor('library.list',binding))

    def test_sanitized_html_chapter_char_and_byte_offsets(self):
        content = '<h1>1화</h1><p>첫 본문 📚</p><h1>2화</h1><p>둘째 본문</p>'.encode()
        revision,_ = self.publish(content,filename='Book.html')
        _,body = self.read(revision)
        _,chapters_raw = self.read(revision,kind='chapters')
        chapters = json.loads(chapters_raw)['chapters']
        self.assertEqual(len(chapters),2)
        self.assertNotIn(b'<h1>',body)
        value = body.decode()
        for i,chapter in enumerate(chapters):
            raw = body[chapter['byte_start']:chapter['byte_end']]
            self.assertEqual(raw.decode(),value[chapter['char_start']:chapter['char_end']])
            self.assertIn(str(i+1)+'화',raw.decode())
            _,jumped = self.read(revision,chapter=i)
            self.assertIn(raw,jumped)

    def test_ambiguous_headings_and_filename_ranges_do_not_invent_offsets(self):
        from document_library.previews import _text_index
        for value in ('1화\nbody\n1화\nother\n','2화\nbody\n1화\nother\n','1화\n2화\nbody\n'):
            index = _text_index(value)
            self.assertTrue(index['chapter_review_reason']);self.assertEqual(index['chapters'],[])
        index = _text_index('plain text without chapter headings')
        self.assertEqual(index['chapters'],[])

    def test_missing_cache_closes_readers_before_enqueue_and_public409(self):
        revision,lease = self.publish();self.damage(lease)
        original = self.service.enqueue
        observed = []
        def enqueue(*args,**kwargs):
            asset = args[1].assets[0]
            with self.h.publisher.locks.mutation([asset.object_sha],blocking=False):
                observed.append(True)
            return original(*args,**kwargs)
        with patch.object(self.service,'enqueue',side_effect=enqueue):
            error = self.missing(revision)
        self.assertEqual(observed,[True])
        self.assertEqual(error.status,409);self.assertEqual(error.code,'PREVIEW_NOT_READY')
        self.assertEqual(error.public(),dict(state='repair_queued',retry_after_seconds=1))
        self.assertNotIn('job',json.dumps(error.public()))
        self.assertEqual(self.scalar("SELECT count(*) FROM jobs WHERE kind='preview.repair'"),1)

    def test_same_damage_collapses_repair_and_running_state(self):
        revision,lease = self.publish();self.damage(lease)
        for _ in range(3):
            self.assertEqual(self.missing(revision).state,'repair_queued')
        repair = self.claim()
        self.assertEqual(self.missing(revision).state,'repair_running')
        self.assertEqual(self.scalar("SELECT count(*) FROM jobs WHERE kind='preview.repair'"),1)
        before = self.scalar('SELECT preview_json FROM ingestion_runs WHERE job_id=:id',id=lease.job_id)
        self.assertEqual(self.service.process(repair),'text')
        self.assertEqual(self.read(revision)[1],b'1\xed\x99\x94\nsynthetic chapter body\n')
        self.assertEqual(self.scalar('SELECT preview_json FROM ingestion_runs WHERE job_id=:id',id=lease.job_id),before)
        self.assertEqual(self.jobs.get(repair.job_id)['state'],'completed')
        self.assertEqual(self.scalar('SELECT count(*) FROM preview_cache_receipts'),1)

    def test_new_damage_new_job_and_stale_head_cas(self):
        revision,lease = self.publish();self.damage(lease);self.missing(revision)
        first = self.claim();self.service.process(first)
        manifest = json.loads(self.scalar('SELECT manifest_json FROM preview_cache_receipts'))
        self.root.unlink(manifest['bundle_relpath']);self.missing(revision)
        second = self.claim()
        self.assertNotEqual(first.job_id,second.job_id)
        self.assertEqual(self.scalar("SELECT count(*) FROM jobs WHERE kind='preview.repair'"),2)
        # A concurrent trusted head replacement after compute invalidates the fixed CAS.
        real = build_preview
        def changed_head(*args,**kwargs):
            result = real(*args,**kwargs)
            with write_transaction(self.engine) as conn:
                conn.execute(text('DELETE FROM preview_cache_heads'))
            return result
        with self.assertRaisesRegex(JobError,'head changed'):
            self.service.process(second,preview_builder=changed_head)
        self.assertEqual(self.scalar('SELECT count(*) FROM preview_cache_receipts'),1)
        self.assertEqual(self.jobs.get(second.job_id)['state'],'running')

    def test_terminal_unavailable_prevents_same_version_requeue(self):
        revision,lease = self.publish();self.damage(lease);self.missing(revision)
        repair = self.claim()
        def unavailable(_root,_lease,_source,_filename,*,report,limits):
            return dict(preview_profile(limits),kind='unavailable',source_sha256=report['sha256'],source_byte_size=report['byte_size'],text_chars=0,page_count=0)
        self.assertEqual(self.service.process(repair,preview_builder=unavailable),'unavailable')
        for _ in range(3):
            self.assertEqual(self.missing(revision).public(),dict(state='unavailable'))
        self.assertEqual(self.scalar("SELECT count(*) FROM jobs WHERE kind='preview.repair'"),1)

    def test_profile_transitions_preserve_heads_and_fixed_worker_limits(self):
        raw = b'1\xed\x99\x94\nactual source body beyond strict parse budget\n'
        revision,seed = self.publish(raw)
        strict = replace(Limits(),parse_bytes=8)
        self.service = PreviewService(self.h.publisher,self.policy,limits=strict)
        for _ in range(2):
            self.assertEqual(self.missing(revision).state,'repair_queued')
        strict_job = self.claim()
        # A worker hosted by a different current configuration must use the
        # job's fixed limits for generation, validation and head CAS.
        self.service = PreviewService(self.h.publisher,self.policy)
        self.assertEqual(self.service.process(strict_job),'unavailable')
        self.assertEqual(self.read(revision)[1],raw)
        self.damage(seed)
        for _ in range(2):
            self.assertEqual(self.missing(revision).state,'repair_queued')
        default_job = self.claim()
        self.service = PreviewService(self.h.publisher,self.policy,limits=strict)
        self.assertEqual(self.service.process(default_job),'text')
        self.assertEqual(self.missing(revision).state,'unavailable')
        self.service = PreviewService(self.h.publisher,self.policy)
        self.assertEqual(self.read(revision)[1],raw)
        self.assertEqual(self.scalar('SELECT count(*) FROM preview_cache_heads'),2)
        self.assertEqual(self.scalar('SELECT count(*) FROM preview_cache_receipts'),2)
        self.assertEqual(self.scalar("SELECT count(*) FROM jobs WHERE kind='preview.repair'"),2)
        with self.engine.connect() as conn:
            rows = conn.execute(text('SELECT profile_sha256,manifest_json FROM preview_cache_receipts')).all()
        self.assertEqual({r.profile_sha256 for r in rows},{preview_profile(strict)['profile_sha256'],preview_profile(Limits())['profile_sha256']})
        for row in rows:
            manifest = json.loads(row.manifest_json)
            self.assertEqual(manifest['profile_sha256'],row.profile_sha256)
            self.assertEqual(preview_profile(Limits(**manifest['limits']))['profile_sha256'],row.profile_sha256)

    def test_missing_profile_manifest_requires_repair(self):
        revision,lease = self.publish()
        manifest = self.manifest(lease)
        del manifest['profile_sha256']
        with patch.object(self.service,'_candidates',return_value=([(canonical_json(manifest),lease.job_id,lease.generation)],None)):
            self.assertEqual(self.missing(revision).state,'repair_queued')

    def test_worker_rejects_forged_profile_and_cleans_uncommitted_output(self):
        revision,seed = self.publish();self.damage(seed);self.missing(revision)
        repair = self.claim()
        def forged(*args,**kwargs):
            result = build_preview(*args,**kwargs)
            result['limits'] = dict(result['limits'],parse_bytes=8)
            return result
        with self.assertRaisesRegex(JobError,'profile differs'):
            self.service.process(repair,preview_builder=forged)
        self.assertEqual(self.scalar('SELECT count(*) FROM preview_cache_receipts'),0)
        self.assertFalse(list((self.h.path/repair.staging_key).glob('*.part')))

    def test_fixed_job_headers_and_payload_cannot_redirect_repair(self):
        revision,lease = self.publish();self.damage(lease);self.missing(revision);repair = self.claim()
        original = self.jobs.get(repair.job_id)
        for column,value in [('input_revision_id',None),('object_sha',None),('work_id',None),
                             ('kind','source.download'),('payload_json','{}')]:
            with write_transaction(self.engine) as conn:
                conn.execute(text('UPDATE jobs SET '+column+'=:value WHERE id=:id'),dict(value=value,id=repair.job_id))
            with self.subTest(column=column),self.assertRaises(JobError):
                self.service.process(repair)
            with write_transaction(self.engine) as conn:
                conn.execute(text('UPDATE jobs SET '+column+'=:value WHERE id=:id'),dict(value=original[column],id=repair.job_id))
        self.assertEqual(self.scalar('SELECT count(*) FROM preview_cache_receipts'),0)
        self.assertNotIn(self.h.uploader.token,original['payload_json'])
        self.assertNotIn('session',original['payload_json'])

    def test_repair_input_and_receipt_are_immutable_evidence(self):
        revision,lease = self.publish();self.damage(lease);self.missing(revision);repair = self.claim()
        self.service.process(repair)
        for sql in ("UPDATE preview_repair_inputs SET renderer=2", "DELETE FROM preview_repair_inputs",
                    "UPDATE preview_cache_receipts SET reason='changed'", "DELETE FROM preview_cache_receipts",
                    "UPDATE preview_cache_heads SET profile_sha256='"+'0'*64+"'"):
            with self.subTest(sql=sql),self.assertRaises(IntegrityError):
                with write_transaction(self.engine) as conn:
                    conn.execute(text(sql))

    def test_logout_does_not_revoke_service_compute_but_user_response_denied(self):
        revision,lease = self.publish();self.damage(lease);self.missing(revision);repair = self.claim()
        with write_transaction(self.engine) as conn:
            conn.execute(text('DELETE FROM auth_sessions'))
        self.assertEqual(self.service.process(repair),'text')
        with self.assertRaises(AuthError):
            self.read(revision)

    def _assert_commit_loss(self, action):
        revision,lease = self.publish()
        self.damage(lease);self.missing(revision);repair = self.claim()
        def revoked(*args,**kwargs):
            result = build_preview(*args,**kwargs)
            if action == 'lease':
                self.jobs.cancel(repair.job_id)
            else:
                with write_transaction(self.engine) as conn:
                    conn.execute(text("UPDATE publication_plans SET rights_status='blocked' WHERE job_id=:id"),dict(id=lease.job_id))
            return result
        with self.assertRaises((JobError,AuthError)):
            self.service.process(repair,preview_builder=revoked)
        self.assertEqual(self.scalar('SELECT count(*) FROM preview_cache_receipts'),0)
        self.assertEqual(self.scalar('SELECT count(*) FROM preview_cache_heads'),0)

    def test_source_rights_loss_before_commit_rolls_back(self):
        self._assert_commit_loss('rights')

    def test_lease_loss_before_commit_rolls_back(self):
        self._assert_commit_loss('lease')

    def test_copy_reader_closes_before_child_and_cancel_gc_cas(self):
        revision,lease = self.publish();self.damage(lease);self.missing(revision);repair = self.claim()
        asset = self.fresh(revision)().assets[0]
        def after_copy(*args,**kwargs):
            # Child phase has no original reader: exclusive mutation is available.
            with self.h.publisher.locks.mutation([asset.object_sha],blocking=False):
                pass
            self.jobs.cancel(repair.job_id)
            self.assertFalse(self.scalar("SELECT count(*) FROM jobs WHERE id=:id AND state IN ('queued','running','retry_wait')",id=repair.job_id))
            result = build_preview(*args,**kwargs)
            return result
        with self.assertRaises(LostLease):
            self.service.process(repair,preview_builder=after_copy)
        self.assertEqual(self.scalar('SELECT count(*) FROM preview_cache_receipts'),0)
        self.assertFalse(list((self.h.path/repair.staging_key).glob('*.part')))

    def test_actual_copy_cancellation_does_not_release_live_reader(self):
        revision,lease = self.publish();self.damage(lease);self.missing(revision);repair = self.claim()
        sha = self.fresh(revision)().assets[0].object_sha
        original = self.h.publisher.read_selected
        entered,release = threading.Event(),threading.Event()
        errors = []
        @contextmanager
        def hold(*args,**kwargs):
            with original(*args,**kwargs) as opened:
                entered.set();release.wait(5)
                yield opened
        def worker():
            try:
                self.service.process(repair)
            except BaseException as error:
                errors.append(error)
        with patch.object(self.h.publisher,'read_selected',side_effect=hold):
            thread = threading.Thread(target=worker);thread.start();self.assertTrue(entered.wait(3))
            self.jobs.cancel(repair.job_id)
            with self.assertRaises(StorageBusy):
                with self.h.publisher.locks.mutation([sha],blocking=False):
                    self.fail('cancelled job released physical reader')
            release.set();thread.join(8);self.assertFalse(thread.is_alive())
        self.assertTrue(any(isinstance(e,LostLease) for e in errors))
        with self.h.publisher.locks.mutation([sha],blocking=False):
            pass

    def test_generated_page_and_thumbnail_order_only_numeric_selection(self):
        payload = archive_bytes([('10.png',image_bytes(size=(5,2))),('2.png',image_bytes(size=(4,2))),('1.png',image_bytes(size=(3,2)))])
        with write_transaction(self.engine) as conn:
            conn.execute(text("INSERT INTO work_editions(id,work_id,edition_key,representation,edition_label,created_at) VALUES('packs','w','packs','imagepack','Packs','test')"))
        revision,_ = self.publish(payload,filename='Book.cbz',edition='packs')
        for page in (1,2,3):
            for thumbnail in (False,True):
                headers,body = self.read(revision,kind='image',page=page,thumbnail=thumbnail)
                self.assertEqual(headers[b'content-type'],b'image/png');self.assertTrue(body.startswith(b'\x89PNG'))
                self.assertEqual(headers[b'x-preview-page-count'],b'3')
                self.assertEqual(headers[b'x-preview-page'],str(page).encode('ascii'))
                head_headers,head_body=self.read(revision,kind='image',page=page,thumbnail=thumbnail,method='HEAD')
                self.assertEqual(head_body,b'')
                self.assertEqual(head_headers[b'x-preview-page-count'],b'3')
                self.assertEqual(head_headers[b'x-preview-page'],str(page).encode('ascii'))
        for page in (0,4,True,'../1'):
            with self.subTest(page=page),self.assertRaises(PreviewError):
                self.read(revision,kind='image',page=page)

    def test_corrupt_bundle_hash_and_legacy_renderer_queue_v2_repair(self):
        revision,lease = self.publish()
        manifest = self.manifest(lease)
        path = self.h.path/manifest['bundle_relpath'];raw = bytearray(path.read_bytes());raw[10] ^= 1;path.write_bytes(raw)
        self.assertEqual(self.missing(revision).state,'repair_queued')
        self.jobs.cancel(self.scalar("SELECT id FROM jobs WHERE kind='preview.repair'"))
        old = dict(manifest,renderer=1,schema=1)
        with patch.object(self.service,'_candidates',return_value=([(canonical_json(old),lease.job_id,lease.generation)],None)):
            self.assertEqual(self.missing(revision).state,'repair_queued')

    def test_archive_extra_duplicate_compressed_traversal_and_entry_sha_rejected(self):
        revision,lease = self.publish();asset = self.fresh(revision)().assets[0]
        manifest = self.manifest(lease)
        original = self.h.path/manifest['bundle_relpath']
        with zipfile.ZipFile(original) as archive:
            entries = {i.filename:archive.read(i) for i in archive.infolist()}
        for mode in ('extra','duplicate','compressed','traversal','entry_sha','source_sha','encrypted','unsupported_flag','local_flag_mismatch',
                     'inner_limits','inner_profile','outer_limits','outer_profile'):
            output = io.BytesIO()
            content = dict(entries)
            if mode == 'entry_sha':
                inner = json.loads(content['manifest.json']);inner['entries'][0]['sha256'] = '0'*64
                content['manifest.json'] = canonical_json(inner).encode()
            if mode == 'source_sha':
                inner = json.loads(content['manifest.json']);inner['source_sha256'] = '0'*64
                content['manifest.json'] = canonical_json(inner).encode()
            if mode in ('inner_limits','inner_profile'):
                inner = json.loads(content['manifest.json'])
                if mode == 'inner_limits':
                    inner['limits']['parse_bytes'] = 8
                else:
                    inner['profile_sha256'] = '0'*64
                content['manifest.json'] = canonical_json(inner).encode()
            with zipfile.ZipFile(output,'w',compression=zipfile.ZIP_DEFLATED if mode == 'compressed' else zipfile.ZIP_STORED) as archive:
                for name,body in content.items():
                    archive.writestr(name,body)
                if mode in ('extra','traversal','duplicate'):
                    archive.writestr('../escape' if mode == 'traversal' else 'text.txt' if mode == 'duplicate' else 'extra.txt',b'extra')
            raw = output.getvalue()
            if mode in ('encrypted','unsupported_flag','local_flag_mismatch'):
                raw = bytearray(raw)
                flag = 1 if mode == 'encrypted' else 64
                for signature,offset in ((b'PK\x03\x04',6),(b'PK\x01\x02',8)):
                    if mode == 'local_flag_mismatch' and offset == 8:
                        continue
                    start = raw.find(signature)
                    while start >= 0:
                        struct.pack_into('<H',raw,start+offset,flag)
                        start = raw.find(signature,start+4)
                raw = bytes(raw)
            with self.root.stage(lease) as (path,fd):
                os.write(fd,raw)
            forged = dict(manifest,bundle_relpath=path,bundle_sha256=hashlib.sha256(raw).hexdigest(),bundle_size=len(raw))
            if mode == 'outer_limits':
                forged['limits'] = dict(forged['limits'],parse_bytes=8)
            if mode == 'outer_profile':
                forged['profile_sha256'] = '0'*64
            with self.subTest(mode=mode),self.assertRaises((_Damaged,ValueError,KeyError)):
                with self.h.publisher.read_selected(self.fresh(revision)) as opened:
                    self.service._bundle(opened,asset,canonical_json(forged),lease.job_id,lease.generation)

    def test_near_limit_cache_entry_hashing_and_streaming_has_bounded_buffers(self):
        # Actual >120MiB stored PNG-shaped entry; verification is byte/hash based,
        # renderer decoding authority is already represented by the bound proof.
        revision,lease = self.publish();asset = self.fresh(revision)().assets[0]
        size = 120*1024**2
        block = b'x'*65536
        digest = hashlib.sha256()
        for _ in range(size//len(block)):
            digest.update(block)
        proof = dict(preview_profile(Limits()),source_sha256=asset.object_sha,source_byte_size=asset.byte_size,
                     kind='images',text_chars=0,page_count=1)
        entries = [dict(name='pages/000001.png',byte_size=size,sha256=digest.hexdigest()),
                   dict(name='thumbnails/000001.png',byte_size=1,sha256=hashlib.sha256(b't').hexdigest())]
        with self.root.stage(lease) as (path,fd):
            # Borrow FD without memory-sized output or an original path reopen.
            from document_library.previews import _BoundedOutput
            with zipfile.ZipFile(_BoundedOutput(fd),'w',compression=zipfile.ZIP_STORED) as archive:
                with archive.open('pages/000001.png','w') as entry:
                    for _ in range(size//len(block)):
                        entry.write(block)
                archive.writestr('thumbnails/000001.png',b't')
                archive.writestr('manifest.json',canonical_json(dict(proof,entries=entries)).encode())
        hasher = hashlib.sha256();total = 0
        with self.root.file(path) as fd:
            while chunk := os.read(fd,65536):
                hasher.update(chunk);total += len(chunk)
        proof.update(bundle_relpath=path,bundle_sha256=hasher.hexdigest(),bundle_size=total)
        tracemalloc.start()
        with self.h.publisher.read_selected(self.fresh(revision)) as opened:
            bundle = self.service._bundle(opened,asset,canonical_json(proof),lease.job_id,lease.generation)
            amount = sum(len(c) for c in bundle.chunks('pages/000001.png'))
        _,peak = tracemalloc.get_traced_memory();tracemalloc.stop()
        self.assertEqual(amount,size);self.assertLess(peak,8*1024**2)


if __name__ == '__main__':
    unittest.main()
