"""Current-admin provenance, evidence, review CAS and SQL guard tests."""
import json
import unittest
import uuid
from fastapi import FastAPI
from fastapi.testclient import TestClient
from types import SimpleNamespace

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from document_library.backup import SnapshotService
from document_library.database import write_transaction
from document_library.metadata_generation import run_generation
from document_library.library_contracts import LibraryQuery
from document_library.library_queries import LibraryQueries
from document_library.metadata_service import MetadataError, MetadataService
from document_library.metadata_admin_api import install_metadata_admin_routes
from test_metadata_generation import GeneratedFixture, SyntheticAdapter, novel_source


class MetadataGeneratedReviewAcceptance(GeneratedFixture):
    def generate(self):
        option_id=self.prepare_job(source_text=novel_source())
        adapter=SyntheticAdapter(self.profile)
        from asyncio import run
        receipt=run(run_generation(self.metadata,self.lease,option_id=option_id,
            adapter=adapter,profile=self.profile,codec=self.codec,context_tokens=4096,
            output_reserve_tokens=1024))
        return receipt

    def evidence_and_decision(self,token,candidate_id):
        detail=self.metadata.generated_review_detail(token,candidate_id=candidate_id)
        self.assertEqual(detail['item_type'],'novel')
        unit=detail['review_units'][0]
        self.assertEqual(unit['summary_start'],0)
        self.assertEqual(unit['summary_end'],len(detail['summary']))
        self.assertEqual(unit['summary_sha256'],detail['summary_sha256'])
        end=min(detail['excluded_tail_range']['start'],100)
        tail=detail['excluded_tail_range']
        ranges=[dict(start=0,end=end),dict(start=tail['start'],end=min(tail['end'],tail['start']+40))]
        evidence=self.metadata.read_generated_evidence(token,candidate_id=candidate_id,ranges=ranges)
        flags=[code for code in detail['qa_codes']
               if code not in {'SEMANTIC_GROUNDING_NOT_VERIFIED','SPOILER_SEMANTICS_NOT_VERIFIED'}]
        return dict(candidate_id=candidate_id,
            expected_control_version=evidence['expected_control_version'],
            expected_head_version=evidence['expected_head_version'],
            expected_rights_version=evidence['expected_rights_version'],
            request_id=str(uuid.uuid4()),review_context_digest=evidence['review_context_digest'],
            ranges=ranges,review_units=[dict(unit_id=detail['review_units'][0]['unit_id'],range_indexes=[0])],
            grounding_disposition='grounded',spoiler_disposition='acceptable',
            flag_resolutions=[dict(code=code,resolution='accept_with_evidence',range_indexes=[0])
                              for code in flags],decision='approved',select=True)

    def test_completed_job_uses_current_reviewer_and_exact_evidence_replay(self):
        generated=self.generate();candidate=generated['candidate_id']
        old_head=self.scalar("SELECT version FROM metadata_heads WHERE work_id='w'")
        token,csrf=self.reviewer_session()
        # This checks historical binding integrity after the original admin is
        # suspended; backup validation does not need the original actor active.
        with self.engine.connect() as conn:
            SnapshotService._integrity(conn.connection.driver_connection)
        separate_service=MetadataService(self.publisher)
        detail=separate_service.generated_review_detail(token,candidate_id=candidate)
        self.assertEqual(detail['summary_sha256'], self.scalar(
            'SELECT content_sha256 FROM metadata_candidates WHERE id=:id', id=candidate))
        self.assertFalse(detail['current']['revalidatable'] is False)
        decision=self.evidence_and_decision(token,candidate)
        result=separate_service.review_generated_candidate(token,csrf,**decision)
        self.assertEqual(result['description_id'],candidate)
        self.assertEqual(result['selected'],True)
        self.assertEqual(self.scalar("SELECT description_text FROM library_metadata WHERE work_id='w'"),"가"*300)
        self.assertGreater(self.scalar("SELECT version FROM metadata_heads WHERE work_id='w'"),old_head)
        self.assertEqual(separate_service.review_generated_candidate(token,csrf,**decision),result)
        with self.engine.connect() as conn:
            control=json.loads(conn.execute(text("SELECT audit_context_json FROM metadata_candidate_control WHERE candidate_id=:id"),dict(id=candidate)).scalar_one())
            self.assertEqual(control['operation'],'generated_human_review_v1')
            self.assertTrue(control['review_units'] and control['ranges'] and control['flags'])
            self.assertNotIn('가' * 300, json.dumps(control))
            SnapshotService._integrity(conn.connection.driver_connection)

    def test_generic_publish_and_review_and_direct_sql_cannot_approve_generated(self):
        generated=self.generate();candidate=generated['candidate_id']
        before=self.scalar("SELECT version FROM metadata_heads WHERE work_id='w'")
        with self.assertRaises(MetadataError):
            self.metadata.review_candidate(self.token,self.csrf,candidate_id=candidate,
                expected_control_version=1,expected_head_version=before,request_id=str(uuid.uuid4()),
                review_status='approved',select=True)
        with self.assertRaises(IntegrityError):
            with write_transaction(self.engine) as conn:
                conn.execute(text("UPDATE metadata_candidate_control SET review_status='approved',version=version+1,request_id=:request WHERE candidate_id=:id"),
                    dict(id=candidate,request=str(uuid.uuid4())))
        # Candidate/binding identity and digest-shaped strings alone are not a
        # typed human-review receipt. This mirrors an SQL writer that attempts
        # to assert approval without server-issued unit coverage or decisions.
        binding_digest=self.scalar('SELECT binding_digest FROM metadata_generated_bindings WHERE candidate_id=:id',id=candidate)
        partial_receipt=json.dumps(dict(operation='generated_human_review_v1',candidate_id=candidate,
            binding_digest=binding_digest,review_context_digest='0'*64,decision_digest='1'*64))
        with self.assertRaises(IntegrityError):
            with write_transaction(self.engine) as conn:
                conn.execute(text("UPDATE metadata_candidate_control SET version=version+1,review_status='approved',actor_kind='admin',actor_id=:actor,request_id=:request,audit_context_json=:context WHERE candidate_id=:id"),
                    dict(actor='u',request=str(uuid.uuid4()),context=partial_receipt,id=candidate))
        self.assertEqual(self.scalar('SELECT review_status FROM metadata_candidate_control WHERE candidate_id=:id',id=candidate),'pending')
        self.assertEqual(self.scalar("SELECT version FROM metadata_heads WHERE work_id='w'"),before)
        self.assertEqual(self.scalar('SELECT count(*) FROM metadata_generated_bindings WHERE candidate_id=:id',id=candidate),1)

    def test_sql_approval_rejects_missing_ranges_and_forged_unit_or_novel_attestation(self):
        generated=self.generate();candidate=generated['candidate_id']
        binding_digest=self.scalar(
            'SELECT binding_digest FROM metadata_generated_bindings WHERE candidate_id=:id',
            id=candidate)
        detail=self.metadata.generated_review_detail(self.token,candidate_id=candidate)
        evidence=self.metadata.read_generated_evidence(self.token,candidate_id=candidate,
            ranges=[dict(start=0,end=1)])
        common=dict(operation='generated_human_review_v1',candidate_id=candidate,
            binding_digest=binding_digest,review_context_digest='0'*64,
            decision_digest='1'*64,operation_digest='2'*64,
            expected_control_version=1,
            expected_head_version=evidence['expected_head_version'],
            expected_rights_version=evidence['expected_rights_version'],
            decision='approved',select=True,semantic_review='HUMAN_ADMIN_ATTESTED',
            grounding='grounded',spoiler='not_applicable',
            review_units=[dict(unit_id='summary-v1:'+'f'*64,range_indexes=[0])],
            flags=[])
        variants=[
            dict(common,ranges=[{}]),
            dict(common,ranges=[dict(start=0,end=1,sha256='a'*64,tail=False)]),
        ]
        for audit in variants:
            with self.subTest(ranges=audit['ranges']):
                with self.assertRaises(IntegrityError):
                    with write_transaction(self.engine) as conn:
                        conn.execute(text("""UPDATE metadata_candidate_control
                            SET version=version+1,review_status='approved',actor_kind='admin',
                                actor_id='u',request_id=:request,audit_context_json=:context
                            WHERE candidate_id=:id"""),
                            dict(request=str(uuid.uuid4()),
                                 context=json.dumps(audit,separators=(',',':')),id=candidate))
                self.assertEqual(self.scalar(
                    'SELECT review_status FROM metadata_candidate_control WHERE candidate_id=:id',
                    id=candidate),'pending')
                self.assertIsNone(self.scalar(
                    "SELECT description_id FROM metadata_heads WHERE work_id='w'"))
        with self.engine.connect() as conn:
            SnapshotService._integrity(conn.connection.driver_connection)

    def test_backup_rejects_forged_approved_review_after_sql_guard_bypass(self):
        generated=self.generate();candidate=generated['candidate_id']
        binding_digest=self.scalar(
            'SELECT binding_digest FROM metadata_generated_bindings WHERE candidate_id=:id',
            id=candidate)
        forged=json.dumps(dict(operation='generated_human_review_v1',
            candidate_id=candidate,binding_digest=binding_digest,
            review_context_digest='0'*64,decision_digest='1'*64,
            operation_digest='2'*64,expected_control_version=1,
            expected_head_version=1,expected_rights_version=1,
            decision='approved',select=True,semantic_review='HUMAN_ADMIN_ATTESTED',
            grounding='grounded',spoiler='not_applicable',
            review_units=[dict(unit_id='summary-v1:'+'f'*64,range_indexes=[0])],
            ranges=[dict(start=0,end=1,sha256='a'*64,tail=False)],
            flags=[],receipt={}),separators=(',',':'))
        # Bypass only the SQL approval trigger in this disposable fixture to
        # exercise the independent backup/restore verifier on persisted state.
        with write_transaction(self.engine) as conn:
            conn.execute(text('DROP TRIGGER metadata_generated_approval_guard_update'))
            conn.execute(text("""UPDATE metadata_candidate_control SET version=version+1,
                review_status='approved',actor_kind='admin',actor_id='u',
                request_id=:request,audit_context_json=:audit WHERE candidate_id=:id"""),
                dict(request=str(uuid.uuid4()),audit=forged,id=candidate))
        with self.engine.connect() as conn:
            with self.assertRaisesRegex(ValueError,'Generated binding, payload, or review evidence differs'):
                from document_library.metadata_generated_contracts import verify_metadata_generated_in
                verify_metadata_generated_in(conn.connection.driver_connection)

    def test_generated_approval_requires_exact_server_issued_full_summary_unit(self):
        generated=self.generate();candidate=generated['candidate_id']
        decision=self.evidence_and_decision(self.token,candidate)
        decision['review_units']=[]
        with self.assertRaises(MetadataError):
            self.metadata.review_generated_candidate(self.token,self.csrf,**decision)
        decision['request_id']=str(uuid.uuid4())
        decision['review_units']=[dict(unit_id='summary-v1:'+'0'*64,range_indexes=[0])]
        with self.assertRaises(MetadataError):
            self.metadata.review_generated_candidate(self.token,self.csrf,**decision)
        self.assertEqual(self.scalar("SELECT review_status FROM metadata_candidate_control WHERE candidate_id=:id",
                                     id=candidate),'pending')
        self.assertIsNone(self.scalar("SELECT description_text FROM library_metadata WHERE work_id='w'"))

    def test_direct_qwen_publish_is_rejected_before_candidate_creation(self):
        revision,_,_=self.publish(payload=novel_source().encode(),format='txt')
        rights=self.metadata.review_rights(self.token,self.csrf,rights_id=str(uuid.uuid4()),work_id='w',
            kind='description',expected_version=0,request_id=str(uuid.uuid4()),
            decision=dict(state='allowed',officiality='not_applicable',match_state='not_applicable',
                display_allowed=True,cache_allowed=True,derivative_allowed=True,cache_until_ms=None,
                retention_policy=dict(backup_allowed=True,all_copies_deletion_required=False),evidence={}))
        job=self.metadata.enqueue(self.token,self.csrf,work_id='w',origin='QWEN_TEXT',rights_id=rights['rights_id'],
            expected_rights_version=1,expected_head_version=self.scalar("SELECT version FROM metadata_heads WHERE work_id='w'"),
            source_revision_id=revision,request_id=str(uuid.uuid4()))
        lease=self.h.jobs.claim('direct-publish-test',kind='metadata.description.publish')
        self.assertEqual(lease.job_id,job)
        before=self.scalar("SELECT version FROM metadata_heads WHERE work_id='w'")
        with self.assertRaises(MetadataError):
            self.metadata.publish_description(lease,text='가'*300,provenance={})
        self.assertEqual(self.scalar('SELECT count(*) FROM metadata_candidates WHERE producing_job_id=:job',job=job),0)
        self.assertEqual(self.scalar("SELECT version FROM metadata_heads WHERE work_id='w'"),before)

    def test_rights_tombstone_removes_auxiliary_but_preserves_immutable_binding(self):
        generated=self.generate();candidate=generated['candidate_id']
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE metadata_rights SET version=version+1,state='blocked',request_id=:request WHERE id=:id"),
                dict(request=str(uuid.uuid4()),id=self.rights))
        self.assertEqual(self.scalar('SELECT count(*) FROM metadata_payloads WHERE candidate_id=:id',id=candidate),0)
        self.assertEqual(self.scalar('SELECT count(*) FROM metadata_generated_payloads WHERE candidate_id=:id',id=candidate),0)
        self.assertEqual(self.scalar('SELECT count(*) FROM metadata_generated_bindings WHERE candidate_id=:id',id=candidate),1)
        with self.assertRaises(IntegrityError):
            with write_transaction(self.engine) as conn:
                conn.execute(text("DELETE FROM metadata_generated_bindings WHERE candidate_id=:id"),
                             dict(id=candidate))
        with self.engine.connect() as conn:
            SnapshotService._integrity(conn.connection.driver_connection)

    def test_current_source_rights_revocation_blocks_review_evidence(self):
        generated=self.generate();candidate=generated['candidate_id']
        decision=self.evidence_and_decision(self.token,candidate)
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE publication_plans SET rights_status='blocked' WHERE job_id=:job"),
                         dict(job=self.source_job))
        detail=self.metadata.generated_review_detail(self.token,candidate_id=candidate)
        self.assertFalse(detail['current']['revalidatable'])
        with self.assertRaises(MetadataError):
            self.metadata.review_generated_candidate(self.token,self.csrf,**decision)
        self.assertEqual(self.scalar("SELECT review_status FROM metadata_candidate_control WHERE candidate_id=:id",
                                     id=candidate),'pending')
        self.assertIsNone(self.scalar("SELECT description_text FROM library_metadata WHERE work_id='w'"))

    def test_generated_approval_preserves_higher_priority_official_description(self):
        official_rights=str(uuid.uuid4())
        official_decision=dict(state='allowed',officiality='verified',match_state='exact',
            display_allowed=True,cache_allowed=True,derivative_allowed=False,cache_until_ms=None,
            retention_policy=dict(backup_allowed=True,all_copies_deletion_required=False),
            evidence={'publisher':'synthetic verified source'})
        self.metadata.review_rights(self.token,self.csrf,rights_id=official_rights,work_id='w',
            kind='description',expected_version=0,request_id=str(uuid.uuid4()),decision=official_decision)
        official_job=self.metadata.enqueue(self.token,self.csrf,work_id='w',origin='PUBLISHER_OFFICIAL',
            rights_id=official_rights,expected_rights_version=1,
            expected_head_version=self.scalar("SELECT version FROM metadata_heads WHERE work_id='w'"),
            source_revision_id=None,request_id=str(uuid.uuid4()))
        official_lease=self.h.jobs.claim('synthetic-official-writer',kind='metadata.description.publish')
        self.assertEqual(official_lease.job_id,official_job)
        official=self.metadata.publish_description(official_lease,text='official description',provenance={})

        generated=self.generate();candidate=generated['candidate_id']
        decision=self.evidence_and_decision(self.token,candidate)
        receipt=self.metadata.review_generated_candidate(self.token,self.csrf,**decision)
        self.assertEqual(receipt['review_status'],'approved')
        self.assertFalse(receipt['selected'])
        self.assertEqual(receipt['description_id'],official['description_id'])
        self.assertEqual(self.scalar("SELECT description_text FROM library_metadata WHERE work_id='w'"),
                         'official description')

    def test_generated_review_rejects_stale_head_after_editorial_selection(self):
        generated=self.generate();candidate=generated['candidate_id']
        approval=self.evidence_and_decision(self.token,candidate)
        editorial_rights=str(uuid.uuid4())
        self.metadata.review_rights(self.token,self.csrf,rights_id=editorial_rights,work_id='w',
            kind='description',expected_version=0,request_id=str(uuid.uuid4()),
            decision=dict(state='allowed',officiality='not_applicable',
                match_state='not_applicable',display_allowed=True,cache_allowed=True,
                derivative_allowed=False,cache_until_ms=None,
                retention_policy=dict(backup_allowed=True,all_copies_deletion_required=False),
                evidence={'license':'synthetic editorial authority'}))
        self.metadata.edit_description(self.token,self.csrf,work_id='w',
            text='Editorial replacement after generated review opened',
            rights_id=editorial_rights,expected_rights_version=1,
            expected_head_version=self.scalar("SELECT version FROM metadata_heads WHERE work_id='w'"),
            request_id=str(uuid.uuid4()))
        with self.assertRaises(MetadataError):
            self.metadata.review_generated_candidate(self.token,self.csrf,**approval)
        self.assertEqual(self.scalar(
            'SELECT review_status FROM metadata_candidate_control WHERE candidate_id=:id',
            id=candidate),'pending')
        self.assertEqual(self.scalar(
            "SELECT description_text FROM library_metadata WHERE work_id='w'"),
            'Editorial replacement after generated review opened')

    def test_generated_review_rejects_stale_control_version(self):
        generated=self.generate();candidate=generated['candidate_id']
        original=self.evidence_and_decision(self.token,candidate)
        rejected=dict(original,request_id=str(uuid.uuid4()),decision='rejected',
                      select=False,grounding_disposition='insufficient',
                      spoiler_disposition='reject')
        result=self.metadata.review_generated_candidate(self.token,self.csrf,**rejected)
        self.assertEqual(result['review_status'],'rejected')
        with self.assertRaises(MetadataError):
            self.metadata.review_generated_candidate(self.token,self.csrf,**original)
        self.assertEqual(self.scalar(
            'SELECT version FROM metadata_candidate_control WHERE candidate_id=:id',
            id=candidate),2)
        self.assertIsNone(self.scalar("SELECT description_text FROM library_metadata WHERE work_id='w'"))

    def test_approved_generated_selection_becomes_stale_after_source_revision_changes(self):
        generated=self.generate();candidate=generated['candidate_id']
        approval=self.evidence_and_decision(self.token,candidate)
        self.metadata.review_generated_candidate(self.token,self.csrf,**approval)
        self.assertEqual(self.scalar("SELECT description_freshness FROM metadata_expected_heads_v1 WHERE work_id='w'"),'current')
        self.publish(payload=novel_source(repeats=2).encode('utf-8'),format='txt',
                     previous=self.revision)
        self.assertEqual(self.scalar(
            "SELECT description_freshness FROM metadata_expected_heads_v1 WHERE work_id='w'"),
            'stale')
        self.assertEqual(self.scalar(
            "SELECT description_id FROM metadata_heads WHERE work_id='w'"),candidate)
        stale=dict(approval,request_id=str(uuid.uuid4()))
        with self.assertRaises(MetadataError):
            self.metadata.review_generated_candidate(self.token,self.csrf,**stale)
        detail=self.metadata.generated_review_detail(self.token,candidate_id=candidate)
        self.assertFalse(detail['current']['revalidatable'])
        self.assertEqual(self.scalar(
            'SELECT review_status FROM metadata_candidate_control WHERE candidate_id=:id',
            id=candidate),'approved')

    def test_approved_generated_description_remains_subject_to_rights_expiry(self):
        deadline=self.h.clock.wall+1000
        option_id=self.prepare_job(source_text=novel_source(),cache_until_ms=deadline)
        from asyncio import run
        generated=run(run_generation(self.metadata,self.lease,option_id=option_id,
            adapter=SyntheticAdapter(self.profile),profile=self.profile,codec=self.codec,
            context_tokens=4096,output_reserve_tokens=1024))
        candidate=generated['candidate_id']
        decision=self.evidence_and_decision(self.token,candidate)
        approved=self.metadata.review_generated_candidate(self.token,self.csrf,**decision)
        self.assertTrue(approved['selected'])
        queries=LibraryQueries(self.auth,now_ms=lambda:self.h.clock.wall)
        self.assertEqual(queries.detail(self.token,'w')['description']['text'],'가'*300)
        self.assertEqual([row.work.work_id for row in queries.list(
            self.token,LibraryQuery(has_summary=True)).items],['w'])
        self.h.clock.advance(1000)
        detail=queries.detail(self.token,'w')
        self.assertEqual(detail['work']['summary_state'],'pending')
        self.assertIsNone(detail['description']['text'])
        self.assertFalse(queries.list(self.token,LibraryQuery(has_summary=True)).items)

    def test_admin_api_exposes_typed_evidence_and_review_contract(self):
        generated=self.generate();candidate=generated['candidate_id']

        class Supervisor:
            def admit(self):
                return None

        app=FastAPI()
        app.state.library=SimpleNamespace(supervisor=Supervisor())
        install_metadata_admin_routes(app,self.metadata,lambda request:(
            request.headers.get('x-test-token',''),request.headers.get('x-csrf-token','')))
        client=TestClient(app)
        headers={'x-test-token':self.token,'x-csrf-token':self.csrf}
        response=client.get(f'/api/v1/admin/metadata/candidates/{candidate}/generated-review',headers=headers)
        self.assertEqual(response.status_code,200)
        detail=response.json()
        self.assertEqual(detail['item_type'],'novel')
        self.assertEqual(detail['review_units'][0]['summary_end'],len(detail['summary']))
        tail=detail['excluded_tail_range']
        ranges=[dict(start=0,end=100),dict(start=tail['start'],end=tail['start']+40)]
        evidence_response=client.post(
            f'/api/v1/admin/metadata/candidates/{candidate}/generated-evidence',
            headers=headers,json={'ranges':ranges})
        self.assertEqual(evidence_response.status_code,200)
        evidence=evidence_response.json()
        flags=[code for code in detail['qa_codes']
               if code not in {'SEMANTIC_GROUNDING_NOT_VERIFIED','SPOILER_SEMANTICS_NOT_VERIFIED'}]
        body=dict(expected_control_version=evidence['expected_control_version'],
            expected_head_version=evidence['expected_head_version'],
            expected_rights_version=evidence['expected_rights_version'],request_id=str(uuid.uuid4()),
            review_context_digest=evidence['review_context_digest'],ranges=ranges,
            review_units=[dict(unit_id=detail['review_units'][0]['unit_id'],range_indexes=[0])],
            grounding_disposition='grounded',spoiler_disposition='acceptable',
            flag_resolutions=[dict(code=code,resolution='accept_with_evidence',range_indexes=[0])
                              for code in flags],decision='approved',select=True)
        result=client.post(f'/api/v1/admin/metadata/candidates/{candidate}/generated-review',
                           headers=headers,json=body)
        self.assertEqual(result.status_code,200)
        self.assertTrue(result.json()['selected'])


if __name__ == '__main__':
    unittest.main()
