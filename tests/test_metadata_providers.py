import dataclasses
import io
import json
import os
from pathlib import Path
import struct
import tempfile
import unittest
from unittest.mock import patch
import zlib

from PIL import Image

from document_library.ingest_formats import FormatError
from document_library.metadata_providers import (
    BibliographicQuery, GoogleBooksAdapter, LinkOnly, OpenLibraryAdapter,
    PermittedMetadata, ScopedPolicyReceipt, WorkIdentity, evaluate_policy,
    inspect_permitted_cover, match_observation, parse_observation,
)
from document_library.metadata_service import MetadataService
from document_library.safe_public_fetch import OpenLibraryPolicy


class ProviderTests(unittest.TestCase):
    def setUp(self):
        self.work = WorkIdentity('local-work','東京1',('著者',),('9780306406157',),
                                 'ja','出版',provider_editions=(('openlibrary','OL1M'),))
        self.query = BibliographicQuery.from_work(self.work)
        self.data = {'key':'/books/OL1M','title':'東京1','author_name':['著者'],
                     'isbn':['9780306406157'],'language':['jpn'],'publisher':['出版'],'cover_i':17,
                     'description':{'value':'PRIVATE UNLICENSED PROSE'},
                     'imageLinks':{'thumbnail':'https://evil.test/secret.jpg'}}

    def observations(self, data=None, provider='openlibrary', work=None):
        return parse_observation(provider, json.dumps(self.data if data is None else data).encode(),
                                 query_digest=(work or self.work).digest)

    def observation(self, **changes):
        return self.observations(dict(self.data, **changes))[0]

    def policy(self, observation, **changes):
        receipt = ScopedPolicyReceipt(observation.provider,observation.external_id,observation.source_url,
            'metadata',observation.source_url,self.work.digest,'a'*64,2000,'verified',
            True,True,False,'© explicit test license',1500,True,False)
        return dataclasses.replace(receipt, **changes)

    def test_only_bounded_work_fields_exported_no_local_ids_or_body(self):
        plan = OpenLibraryAdapter().plan(self.query)
        self.assertEqual(len(plan.requests), 1)
        self.assertEqual(plan.maximum_exchanges, 6)
        target = OpenLibraryPolicy().validate(plan.requests[0])
        self.assertEqual(target.host, 'openlibrary.org')
        self.assertIn('limit=5', plan.requests[0].url)
        self.assertIn('isbn=9780306406157', plan.requests[0].url)
        self.assertNotIn('local-work', plan.requests[0].url)
        self.assertNotIn('body', dataclasses.asdict(self.query))
        with self.assertRaises(TypeError):
            BibliographicQuery.from_work({'title':'x','body':'private','endpoint':'https://evil.test'})

    def test_google_unconfigured_zero_network_or_environment_key_access(self):
        with (patch('socket.getaddrinfo', side_effect=AssertionError('network forbidden')),
              patch('socket.socket', side_effect=AssertionError('socket forbidden'))):
            plan = GoogleBooksAdapter().plan(self.query)
        self.assertEqual(plan.requests, ())
        self.assertEqual(plan.reason, 'application_identification_not_configured')

    def test_unknown_prose_images_and_google_access_flags_never_in_observation(self):
        observation = self.observation()
        selected = json.dumps(dataclasses.asdict(observation),ensure_ascii=False)
        self.assertNotIn('PRIVATE UNLICENSED PROSE', selected)
        self.assertNotIn('evil.test', selected)
        self.assertEqual(observation.source_url, 'https://openlibrary.org/books/OL1M')
        link = evaluate_policy(observation, match_observation(observation,self.work), None, now_ms=1000)
        self.assertIsInstance(link, LinkOnly)
        self.assertEqual(set(dataclasses.asdict(link)), {'source_url','external_id','reasons'})

    def test_unicode_cjk_exact_with_strong_identity_but_same_digit_key_conflict(self):
        exact = match_observation(self.observation(),self.work)
        self.assertEqual(exact.state, 'exact')
        self.assertIn('catalog_key_not_identity_evidence', exact.reasons)
        conflict = match_observation(self.observation(title='大阪1'),self.work)
        self.assertEqual(conflict.state, 'reject')
        no_digits = dataclasses.replace(self.work,title='図書館')
        observed = self.observations(dict(self.data,title='図書館'),work=no_digits)[0]
        self.assertEqual(match_observation(observed,no_digits).state, 'exact')
        wrong = self.observations(dict(self.data,title='物語'),work=no_digits)[0]
        self.assertEqual(match_observation(wrong,no_digits).state, 'reject')

    def test_author_language_edition_publisher_conflict_despite_exact_isbn(self):
        for changes in [{'author_name':['別人']},{'language':['eng']},
                        {'key':'/books/OL2M'},{'publisher':['Other']},
                        {'isbn':['9780140328721']}]:
            with self.subTest(changes=changes):
                self.assertEqual(match_observation(self.observation(**changes),self.work).state, 'reject')

    def test_title_only_missing_identity_work_hit_and_ambiguous_editions_review(self):
        no_id = dataclasses.replace(self.work,isbns=(),provider_editions=())
        title_only = self.observations(dict(self.data,isbn=[]),work=no_id)[0]
        self.assertEqual(match_observation(title_only,no_id).state, 'review')
        self.assertEqual(match_observation(self.observation(author_name=[]),self.work).state, 'review')
        self.assertEqual(match_observation(self.observation(language=[]),self.work).state, 'review')
        self.assertEqual(match_observation(self.observation(key='/works/OL1W'),
                                          dataclasses.replace(self.work,provider_editions=())).state, 'reject')
        work = dataclasses.replace(self.work,provider_editions=())
        hit = self.observations(dict(self.data,key='/works/OL1W'),work=work)[0]
        self.assertEqual(match_observation(hit,work).state, 'review')
        ambiguity = self.observation(isbn=['9780306406157','9780140328721'])
        self.assertEqual(match_observation(ambiguity,self.work).state, 'review')

    def test_search_rank_and_unverified_translation_do_not_establish_match(self):
        translated = self.observation(title='Tokyo 1')
        self.assertEqual(match_observation(translated,self.work).state, 'reject')
        trusted = dataclasses.replace(self.work,trusted_title_aliases=('Tokyo 1',))
        trusted_observation = self.observations(dict(self.data,title='Tokyo 1'),work=trusted)[0]
        self.assertEqual(match_observation(trusted_observation,trusted).state, 'exact')

    def test_missing_remote_publisher_requires_review_without_claiming_conflict(self):
        observation = self.observation(publisher=[])
        decision = match_observation(observation, self.work)
        self.assertEqual(decision.state, 'review')
        self.assertIn('publisher_missing', decision.reasons)
        self.assertNotIn('publisher_or_edition_conflict', decision.reasons)
        self.assertIsInstance(evaluate_policy(observation, decision,
            self.policy(observation), now_ms=1000), LinkOnly)
        self.assertEqual(match_observation(self.observation(publisher=['Other']),
            self.work).state, 'reject')

    def test_isbn10_equivalence_checksum_and_unicode_normalization(self):
        self.assertEqual(match_observation(self.observation(isbn=['0-306-40615-2']),self.work).state, 'exact')
        with self.assertRaises(ValueError):
            self.observation(isbn=['9780306406158'])
        for title, remote in [('Ａ 物語','A 物語'),('도서관','도서관')]:
            work = dataclasses.replace(self.work,title=title)
            observation = self.observations(dict(self.data,title=remote),work=work)[0]
            self.assertEqual(match_observation(observation,work).state, 'exact')

    def test_general_ean_namespace_rejected_and_valid_978_979_preserved(self):
        for identifier in ('0000000000000','4006381333931','9790260000438'):
            with self.subTest(identifier=identifier):
                with self.assertRaises(ValueError):
                    dataclasses.replace(self.work,isbns=(identifier,))
                with self.assertRaises(ValueError):
                    self.observation(isbn=[identifier])
        # Synthetic valid9791 checkdigit, not a claim of registry allocation;
        # the 9790 ISMN namespace is intentionally excluded above.
        for identifier in ('9780306406157','9791000000008'):
            work = dataclasses.replace(self.work,isbns=(identifier,),provider_editions=())
            observation = self.observations(dict(self.data,isbn=[identifier]),work=work)[0]
            self.assertEqual(match_observation(observation,work).state,'exact')

    def test_stale_query_identity_cannot_reuse_match_or_permissions(self):
        observation = self.observation()
        current = dataclasses.replace(self.work,work_id='replacement-work')
        decision = match_observation(observation,current)
        self.assertEqual(decision.state, 'reject')
        self.assertIn('stale_local_identity',decision.reasons)
        self.assertIsInstance(evaluate_policy(observation,decision,self.policy(observation),now_ms=1000),LinkOnly)

    def test_permissions_are_independent_current_scoped_and_not_api_license(self):
        observation = self.observation()
        match = match_observation(observation,self.work)
        for changes in [{'publisher_officiality':'unknown'},{'display':None},{'display':False},
                        {'cache':None},{'cache':False},{'derivative':None},{'backup_allowed':False},
                        {'all_copies_deletion_required':True},{'attribution':None},
                        {'cache_until_ms':1000},{'valid_until_ms':999},{'external_id':'OL2M'},
                        {'source_url':'https://openlibrary.org/books/OL2M'},
                        {'resource_url':'https://evil.test/cover.jpg'},{'identity_digest':'b'*64}]:
            with self.subTest(changes=changes):
                result = evaluate_policy(observation,match,self.policy(observation,**changes),now_ms=1000)
                self.assertIsInstance(result,LinkOnly)
        result = evaluate_policy(observation,match,self.policy(observation),now_ms=1000)
        self.assertIsInstance(result,PermittedMetadata)
        self.assertTrue(result.cache)
        self.assertFalse(result.derivative)
        self.assertNotIn('description',dataclasses.asdict(result))

    def test_metadata_permission_does_not_authorize_cover_edition_or_stage(self):
        observation = self.observation()
        match = match_observation(observation,self.work)
        permission = evaluate_policy(observation,match,self.policy(observation),now_ms=1000)
        with self.assertRaises(ValueError):
            inspect_permitted_cover(-1,'image/png',permission,now_ms=1000)
        intent = OpenLibraryAdapter().cover_intent(observation)
        self.assertEqual(intent.url,'https://covers.openlibrary.org/b/id/17-L.jpg?default=false')
        permitted = evaluate_policy(observation,match,self.policy(observation,resource_kind='cover',resource_url=intent.url),now_ms=1000)
        self.assertIsInstance(permitted,PermittedMetadata)
        self.assertEqual(permitted.resource_kind,'cover')

    def test_strict_provider_json_schema_and_search_limits(self):
        cases = [b'{"docs":[],"docs":[]}',json.dumps({'docs':[self.data]*6}).encode(),
                 json.dumps(dict(self.data,key='/books/../OL1M')).encode(),
                 json.dumps(dict(self.data,cover_i=True)).encode(),
                 json.dumps(dict(self.data,author_name='not-array')).encode()]
        for body in cases:
            with self.subTest(body=body[:50]), self.assertRaises(ValueError):
                parse_observation('openlibrary',body,query_digest=self.work.digest)
        self.assertEqual(self.observations({'docs':[]}),())

    def test_detail_requires_remote_author_evidence_never_uses_local_query_author(self):
        detail = dict(self.data)
        detail.pop('author_name')
        detail['authors'] = [{'key':'/authors/OL1A'}]
        observation = self.observations(detail)[0]
        self.assertEqual(observation.authors,())
        self.assertEqual(match_observation(observation,self.work).state,'review')
        plan = OpenLibraryAdapter().detail_plan(self.query,self.observation())
        self.assertEqual(len(plan.requests),1)
        self.assertEqual(OpenLibraryPolicy().validate(plan.requests[0]).purpose,'detail')

    def test_google_fixture_parsing_ignores_publicdomain_preview_and_description(self):
        google = {'items':[{'id':'volume-1','volumeInfo':{'title':'東京1','authors':['著者'],
            'publisher':'出版','language':'ja','description':'PRIVATE GOOGLE PROSE',
            'industryIdentifiers':[{'type':'ISBN_13','identifier':'9780306406157'}],
            'imageLinks':{'thumbnail':'http://evil.test/cover'}},
            'accessInfo':{'publicDomain':True,'viewability':'ALL_PAGES'}}]}
        work = dataclasses.replace(self.work,provider_editions=())
        observation = self.observations(google,'googlebooks',work)[0]
        self.assertEqual(match_observation(observation,work).state,'exact')
        self.assertNotIn('PRIVATE GOOGLE PROSE',repr(observation))
        self.assertIsInstance(evaluate_policy(observation,match_observation(observation,work),None,now_ms=1000),LinkOnly)

    def cover_permission(self):
        observation = self.observation()
        request = OpenLibraryAdapter().cover_intent(observation)
        return evaluate_policy(observation,match_observation(observation,self.work),
            self.policy(observation,resource_kind='cover',resource_url=request.url),now_ms=1000)

    def inspect(self, data, mime='image/png'):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root)/'stage'
            path.write_bytes(data)
            fd = os.open(path,os.O_RDONLY)
            try:
                os.lseek(fd,1,os.SEEK_SET)
                result = inspect_permitted_cover(fd,mime,self.cover_permission(),now_ms=1000)
                self.assertEqual(os.lseek(fd,0,os.SEEK_CUR),1)
                return result
            finally:
                os.close(fd)

    def test_cover_clock_type_rejected_before_decoder(self):
        permission = self.cover_permission()
        with patch('document_library.ingest_formats.inspect_fd') as decoder:
            for now in (True, False, 1000.0, float('nan'), float('inf'), '1000', None):
                with self.subTest(now=now), self.assertRaises(ValueError):
                    inspect_permitted_cover(-1, 'image/png', permission, now_ms=now)
            decoder.assert_not_called()
            decoder.return_value = {'status': 'controlled-valid-clock'}
            self.assertEqual(inspect_permitted_cover(-1, 'image/png', permission,
                now_ms=1000), decoder.return_value)
            decoder.assert_called_once_with(-1, 'cover.png', MetadataService.COVER_LIMITS)

    def test_cover_reuses_actual_inspect_fd_and_A_limits_without_install(self):
        image = io.BytesIO()
        Image.new('RGB',(3,2),'white').save(image,format='PNG')
        report = self.inspect(image.getvalue())
        self.assertEqual(report['status'],'validated')
        self.assertEqual(report['image']['dimensions'],[[3,2]])
        self.assertEqual(MetadataService.COVER_LIMITS.pixels,16_000_000)
        self.assertEqual(MetadataService.COVER_LIMITS.frames,1)
        self.assertEqual(MetadataService.COVER_LIMITS.file_bytes,8*1024**2)
        with self.assertRaises(FormatError):
            self.inspect(image.getvalue(),'image/jpeg')

    def test_cover_html_svg_and_animated_raster_rejected(self):
        for body in [b'<html>not a raster</html>',b'<svg xmlns="http://www.w3.org/2000/svg"/>']:
            with self.assertRaises(FormatError):
                self.inspect(body)
        animation=io.BytesIO()
        a,b=Image.new('RGB',(3,2),'white'),Image.new('RGB',(3,2),'black')
        a.save(animation,format='WEBP',save_all=True,append_images=[b],duration=100,loop=0,lossless=True)
        with self.assertRaises(FormatError):
            self.inspect(animation.getvalue(),'image/webp')

    def test_cover_over_16m_pixels_rejected_before_large_decode(self):
        def chunk(kind,data):
            return struct.pack('>I',len(data))+kind+data+struct.pack('>I',zlib.crc32(kind+data)&0xffffffff)
        png=b'\x89PNG\r\n\x1a\n'+chunk(b'IHDR',struct.pack('>IIBBBBB',4001,4000,8,2,0,0,0))
        png+=chunk(b'IDAT',zlib.compress(b'\x00'))+chunk(b'IEND',b'')
        with self.assertRaisesRegex(FormatError,'image_pixel_limit'):
            self.inspect(png)


if __name__ == '__main__':
    unittest.main()
