"""Synthetic single-lease generation tests; no provider, model or network."""
import asyncio
import hashlib
import json
import unittest
import uuid
from dataclasses import dataclass, replace
from types import SimpleNamespace

from sqlalchemy import text

from document_library.auth import digest
from document_library.catalog_service import CatalogService
from document_library.database import write_transaction
from document_library.ingestion import IngestionService
from document_library.metadata_generation import run_generation, GenerationError
from document_library.metadata_requests import (
    PromptPolicy, RequestLimits, RequestProfile, render_summary_prompt,
)
from document_library.metadata_service import MetadataService
from library_helpers import LibraryFixture


SUMMARY = "가" * 300


def novel_source(*, repeats=1):
    return "\n".join(f"{number}화\n" + "하린은 도서관 기록을 확인한다. " * repeats
                     for number in range(1, 151))


class SyntheticCodec:
    def __init__(self, profile):
        self.profile_digest = profile.digest

    def render(self, source_text, policy):
        return render_summary_prompt(source_text, policy)

    def encode(self, rendered_prompt):
        payload = rendered_prompt.split('<|im_start|>user\n', 1)[1].split('<|im_end|>', 1)[0]
        value = json.loads(payload)["source_text"]
        count = max(1, (len(value) + 3) // 4 + 50)
        return (7,) * count


@dataclass(frozen=True)
class SyntheticReceipt:
    response_bytes: bytes
    response_sha256: str
    output_text: str
    request_id: str
    wire_sha256: str
    profile_digest: str
    model: str
    prompt_eval_count: int
    eval_count: int
    done_reason: str = "stop"
    terminal: bool = True
    fully_drained: bool = True


class SyntheticAdapter:
    """A test-only one-exchange wrapper; profile remains UNQUALIFIED."""
    def __init__(self, profile, *, fail=None, summary=SUMMARY):
        self.profile = profile
        self.calls = []
        self.fail = fail
        self.summary = summary

    def validate_envelope(self, envelope):
        if envelope.profile_digest != self.profile.digest:
            raise ValueError("profile mismatch")

    async def generate(self, envelope, *, request_id):
        self.calls.append((request_id, envelope.wire_sha256))
        if self.fail:
            raise self.fail
        output_text = json.dumps({"summary": self.summary, "uncertainties": [],
                                  "spoiler_risk": "low"}, ensure_ascii=False,
                                 separators=(",", ":"))
        response = json.dumps({"model": self.profile.model, "response": output_text,
            "done": True, "done_reason": "stop", "prompt_eval_count": envelope.request_tokens,
            "eval_count": 32}, ensure_ascii=False, separators=(",", ":")).encode()
        return SyntheticReceipt(response, hashlib.sha256(response).hexdigest(), output_text,
            request_id, envelope.wire_sha256, envelope.profile_digest, self.profile.model,
            envelope.request_tokens, 32)


class GeneratedFixture(LibraryFixture):
    def setUp(self):
        super().setUp()
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE users SET role='admin' WHERE id='u'"))
        self.metadata = MetadataService(self.publisher)
        ingestion = SimpleNamespace(auth=self.auth, engine=self.engine)
        ingestion._admin = lambda conn, token, csrf: IngestionService._admin(
            ingestion, conn, token, csrf)
        self.catalog = CatalogService(ingestion)
        self.revision = None
        self.rights = None
        self.lease = None
        self.profile = RequestProfile(
            model="synthetic-model", manifest_sha256="1"*64, tokenizer_sha256="2"*64,
            tokenizer_config_sha256="3"*64, model_config_sha256="4"*64,
            tokenizer_implementation="test.synthetic.v1", renderer_implementation="product.raw.v1",
            token_id_ranges=((0, 256),), special_tokens=(("<|im_start|>", 1), ("<|im_end|>", 2)),
            max_context_tokens=8192, max_output_tokens=2048)
        self.codec = SyntheticCodec(self.profile)

    def prepare_job(self, *, source_text, item_type="novel", cache_until_ms=None):
        self.revision, source_lease, _ = self.publish(payload=source_text.encode("utf-8"), format="txt")
        self.source_job = source_lease.job_id
        with self.engine.connect() as conn:
            current=conn.execute(text("SELECT item_type,publication_state FROM works WHERE id='w'")).one()
        self.catalog.classify(self.token,self.csrf,"w",str(uuid.uuid4()),item_type=item_type,
            publication_state=current.publication_state,expected_item_type=current.item_type,
            expected_publication_state=current.publication_state,
            evidence="Explicit synthetic generation test classification")
        self.rights = str(uuid.uuid4())
        self.metadata.review_rights(self.token, self.csrf, rights_id=self.rights,
            work_id="w", kind="description", expected_version=0, request_id=str(uuid.uuid4()),
            decision=dict(state="allowed", officiality="not_applicable", match_state="not_applicable",
                display_allowed=True, cache_allowed=True, derivative_allowed=True,
                cache_until_ms=cache_until_ms, retention_policy=dict(backup_allowed=True,
                    all_copies_deletion_required=False), evidence={"license": "synthetic"}))
        option_id = self.scalar("SELECT id FROM revision_options WHERE edition_revision_id=:id",
                                id=self.revision)
        job_id = self.metadata.enqueue(self.token, self.csrf, work_id="w", origin="QWEN_TEXT",
            rights_id=self.rights, expected_rights_version=1,
            expected_head_version=self.scalar("SELECT version FROM metadata_heads WHERE work_id='w'"),
            source_revision_id=self.revision, request_id=str(uuid.uuid4()))
        self.lease = self.h.jobs.claim("generated-synthetic-worker", kind="metadata.description.publish")
        self.assertEqual(self.lease.job_id, job_id)
        return option_id

    def reviewer_session(self):
        token='synthetic-current-reviewer-session'
        csrf='synthetic-current-reviewer-csrf'
        with write_transaction(self.engine) as conn:
            conn.execute(text("UPDATE users SET status='suspended',credential_version=credential_version+1,authz_version=authz_version+1 WHERE id='u'"))
            conn.execute(text("INSERT INTO users(id,username,password_hash,role,created_at) VALUES('reviewer','reviewer','disabled-synthetic','admin','test')"))
            conn.execute(text("INSERT INTO auth_sessions(token_hash,csrf_hash,user_id,credential_version,created_at,expires_at) VALUES(:token,:csrf,'reviewer',1,:now,:end)"),
                dict(token=digest(token),csrf=digest(csrf),now=self.auth.now(),end=self.auth.now()+3600))
        return token,csrf


class MetadataGenerationAcceptance(GeneratedFixture):

    def test_one_request_stores_pending_without_selecting_and_records_no_raw_body(self):
        source = novel_source()
        option_id = self.prepare_job(source_text=source)
        adapter = SyntheticAdapter(self.profile)
        before_head = self.scalar("SELECT version FROM metadata_heads WHERE work_id='w'")
        receipt = asyncio.run(run_generation(self.metadata, self.lease, option_id=option_id,
            adapter=adapter, profile=self.profile, codec=self.codec, context_tokens=4096,
            output_reserve_tokens=1024))
        self.assertEqual(len(adapter.calls), 1)
        self.assertEqual(self.h.jobs.get(self.lease.job_id)["state"], "completed")
        self.assertEqual(receipt["state"], "pending_review")
        self.assertEqual(self.scalar("SELECT version FROM metadata_heads WHERE work_id='w'"), before_head)
        self.assertIsNone(self.scalar("SELECT description_text FROM library_metadata WHERE work_id='w'"))
        self.assertEqual(self.scalar("SELECT review_status FROM metadata_candidate_control WHERE candidate_id=:id",
                                     id=receipt["candidate_id"]), "pending")
        with self.engine.connect() as conn:
            binding = json.loads(conn.execute(text("SELECT binding_json FROM metadata_generated_bindings WHERE candidate_id=:id"),
                                               dict(id=receipt["candidate_id"])).scalar_one())
            auxiliary = conn.execute(text("SELECT auxiliary_json FROM metadata_generated_payloads WHERE candidate_id=:id"),
                                     dict(id=receipt["candidate_id"])).scalar_one()
            self.assertEqual(binding["qa_status"], "review_required")
            self.assertTrue(binding["request_manifest"])
            encoded_binding=json.dumps(binding)
            for forbidden in ('"source_text":','"prompt":','"raw_response":','"body":'):
                self.assertNotIn(forbidden,encoded_binding)
            self.assertNotIn(SUMMARY, auxiliary)
            self.assertEqual(conn.execute(text("SELECT count(*) FROM job_outbox WHERE kind='metadata_review_pending'")).scalar_one(),1)

    def test_multiple_chunks_use_one_complete_synthesis_or_fail_before_sending_it(self):
        source = novel_source(repeats=2)
        option_id = self.prepare_job(source_text=source)
        adapter = SyntheticAdapter(self.profile)
        receipt = asyncio.run(run_generation(self.metadata, self.lease, option_id=option_id,
            adapter=adapter, profile=self.profile, codec=self.codec, context_tokens=4096,
            output_reserve_tokens=1024,
            request_limits=RequestLimits(max_chunk_chars=4096, max_chunks=8)))
        self.assertGreaterEqual(len(adapter.calls), 3)
        self.assertEqual(len({request_id for request_id, _ in adapter.calls}), len(adapter.calls))
        manifest = json.loads(self.scalar("SELECT binding_json FROM metadata_generated_bindings WHERE candidate_id=:id",
                                           id=receipt["candidate_id"]))["request_manifest"]
        self.assertEqual(len(manifest), len(adapter.calls))
        self.assertTrue(all(item["terminal"] and item["fully_drained"] for item in manifest))
        self.assertEqual(manifest[-1]["source_start"], 0)

    def test_synthesis_overflow_fails_before_any_synthesis_request(self):
        option_id=self.prepare_job(source_text=novel_source(repeats=8))
        adapter=SyntheticAdapter(self.profile,summary='가'*600)
        with self.assertRaisesRegex(GenerationError,'GENERATION_SYNTHESIS_BUILD_FAILED'):
            asyncio.run(run_generation(self.metadata,self.lease,option_id=option_id,
                adapter=adapter,profile=self.profile,codec=self.codec,
                context_tokens=4096,output_reserve_tokens=1024,
                request_limits=RequestLimits(max_chunk_chars=4096,max_chunks=8)))
        self.assertGreater(len(adapter.calls),1)
        self.assertLessEqual(len(adapter.calls),8)
        checkpoint=json.loads(self.scalar("SELECT checkpoint_json FROM jobs WHERE id=:id",
                                          id=self.lease.job_id))
        self.assertEqual(checkpoint['phase'],'TERMINAL_CAPTURED')
        self.assertEqual(checkpoint['completed_requests'],len(adapter.calls))
        self.assertIsNone(self.scalar("SELECT job_id FROM resource_slots WHERE name='local-qwen'"))
        self.assertEqual(self.scalar("SELECT count(*) FROM metadata_candidates WHERE producing_job_id=:job",
                                     job=self.lease.job_id),0)

    def test_uncertain_exchange_keeps_hold_and_suppresses_later_chunks(self):
        class UnknownFailure(RuntimeError):
            pass
        source = novel_source(repeats=2)
        option_id = self.prepare_job(source_text=source)
        adapter = SyntheticAdapter(self.profile, fail=UnknownFailure("transport body must not leak"))
        with self.assertRaises(GenerationError):
            asyncio.run(run_generation(self.metadata, self.lease, option_id=option_id,
                adapter=adapter, profile=self.profile, codec=self.codec, context_tokens=4096,
                output_reserve_tokens=1024,
                request_limits=RequestLimits(max_chunk_chars=4096, max_chunks=8)))
        self.assertEqual(len(adapter.calls), 1)
        self.assertEqual(self.scalar("SELECT job_id FROM resource_slots WHERE name='local-qwen'"),
                         self.lease.job_id)
        self.assertEqual(self.scalar("SELECT count(*) FROM metadata_candidates WHERE producing_job_id=:job",
                                     job=self.lease.job_id), 0)
        with self.assertRaises(GenerationError):
            asyncio.run(run_generation(self.metadata, self.lease, option_id=option_id,
                adapter=adapter, profile=self.profile, codec=self.codec, context_tokens=4096,
                output_reserve_tokens=1024,
                request_limits=RequestLimits(max_chunk_chars=4096, max_chunks=8)))
        self.assertEqual(len(adapter.calls), 1)

    def test_matching_terminal_stop_proof_releases_hold_and_rejects_candidate(self):
        class StopProof:
            def __init__(self, envelope, request_id):
                self.request_id = request_id
                self.wire_sha256 = envelope.wire_sha256
                self.profile_digest = envelope.profile_digest
                self.disposition = "completed"
                self.confirmation = "synthetic terminal output rejected"
                self.terminal = True
                self.fully_drained = True

        class TerminalFailure(RuntimeError):
            pass

        class TerminalFailureAdapter(SyntheticAdapter):
            async def generate(inner, envelope, *, request_id):
                inner.calls.append((request_id, envelope.wire_sha256))
                failure = TerminalFailure("provider response must not be retained")
                failure.stop_proof = StopProof(envelope, request_id)
                raise failure

        option_id = self.prepare_job(source_text=novel_source(repeats=2))
        adapter = TerminalFailureAdapter(self.profile)
        with self.assertRaisesRegex(GenerationError, "RUNTIME_EXCHANGE_REJECTED"):
            asyncio.run(run_generation(self.metadata, self.lease, option_id=option_id,
                adapter=adapter, profile=self.profile, codec=self.codec,
                context_tokens=4096, output_reserve_tokens=1024,
                request_limits=RequestLimits(max_chunk_chars=4096, max_chunks=8)))
        self.assertEqual(len(adapter.calls), 1)
        self.assertIsNone(self.scalar("SELECT job_id FROM resource_slots WHERE name='local-qwen'"))
        self.assertEqual(self.scalar("SELECT count(*) FROM metadata_candidates WHERE producing_job_id=:job",
                                     job=self.lease.job_id), 0)
        checkpoint = json.loads(self.scalar("SELECT checkpoint_json FROM jobs WHERE id=:id",
                                            id=self.lease.job_id))
        self.assertEqual(checkpoint["phase"], "TERMINAL_REJECTED")

    def test_stop_proof_with_mismatched_wire_retains_hold(self):
        class StopProof:
            def __init__(self, envelope, request_id):
                self.request_id = request_id
                self.wire_sha256 = "0" * 64
                self.profile_digest = envelope.profile_digest
                self.disposition = "completed"
                self.confirmation = "synthetic terminal output rejected"
                self.terminal = True
                self.fully_drained = True

        class TerminalFailure(RuntimeError):
            pass

        class MismatchedProofAdapter(SyntheticAdapter):
            async def generate(inner, envelope, *, request_id):
                inner.calls.append((request_id, envelope.wire_sha256))
                failure = TerminalFailure("must not leak")
                failure.stop_proof = StopProof(envelope, request_id)
                raise failure

        option_id = self.prepare_job(source_text=novel_source())
        adapter = MismatchedProofAdapter(self.profile)
        with self.assertRaisesRegex(GenerationError, "RUNTIME_EXCHANGE_UNCERTAIN"):
            asyncio.run(run_generation(self.metadata, self.lease, option_id=option_id,
                adapter=adapter, profile=self.profile, codec=self.codec,
                context_tokens=4096, output_reserve_tokens=1024))
        self.assertEqual(len(adapter.calls), 1)
        self.assertEqual(self.scalar("SELECT job_id FROM resource_slots WHERE name='local-qwen'"),
                         self.lease.job_id)
        self.assertEqual(self.scalar("SELECT count(*) FROM metadata_candidates WHERE producing_job_id=:job",
                                     job=self.lease.job_id), 0)

    def test_mismatched_normal_receipt_retains_hold_before_product_validation(self):
        class MismatchedReceiptAdapter(SyntheticAdapter):
            async def generate(inner, envelope, *, request_id):
                receipt=await super().generate(envelope,request_id=request_id)
                return replace(receipt,wire_sha256='f'*64)

        option_id=self.prepare_job(source_text=novel_source())
        adapter=MismatchedReceiptAdapter(self.profile)
        with self.assertRaisesRegex(GenerationError,'RUNTIME_TERMINAL_UNPROVEN'):
            asyncio.run(run_generation(self.metadata,self.lease,option_id=option_id,
                adapter=adapter,profile=self.profile,codec=self.codec,
                context_tokens=4096,output_reserve_tokens=1024))
        self.assertEqual(len(adapter.calls),1)
        self.assertEqual(self.scalar("SELECT job_id FROM resource_slots WHERE name='local-qwen'"),
                         self.lease.job_id)
        self.assertEqual(self.scalar('SELECT count(*) FROM resource_release_receipts WHERE job_id=:job',
                                     job=self.lease.job_id),0)
        self.assertEqual(self.scalar('SELECT count(*) FROM metadata_candidates WHERE producing_job_id=:job',
                                     job=self.lease.job_id),0)
        checkpoint=json.loads(self.scalar('SELECT checkpoint_json FROM jobs WHERE id=:id',
                                          id=self.lease.job_id))
        self.assertEqual(checkpoint['phase'],'UNCERTAIN_HELD')

    def test_correlated_terminal_invalid_product_releases_hold_and_rejects_candidate(self):
        class InvalidProductAdapter(SyntheticAdapter):
            async def generate(inner,envelope,*,request_id):
                receipt=await super().generate(envelope,request_id=request_id)
                output_text='not a summary JSON object'
                response=json.dumps({'model':inner.profile.model,'response':output_text,
                    'done':True,'done_reason':'stop',
                    'prompt_eval_count':envelope.request_tokens,'eval_count':32},
                    separators=(',',':')).encode()
                return replace(receipt,response_bytes=response,
                    response_sha256=hashlib.sha256(response).hexdigest(),
                    output_text=output_text)

        option_id=self.prepare_job(source_text=novel_source())
        adapter=InvalidProductAdapter(self.profile)
        with self.assertRaisesRegex(GenerationError,'RUNTIME_OUTPUT_REJECTED'):
            asyncio.run(run_generation(self.metadata,self.lease,option_id=option_id,
                adapter=adapter,profile=self.profile,codec=self.codec,
                context_tokens=4096,output_reserve_tokens=1024))
        self.assertEqual(len(adapter.calls),1)
        self.assertIsNone(self.scalar("SELECT job_id FROM resource_slots WHERE name='local-qwen'"))
        self.assertEqual(self.scalar('SELECT disposition FROM resource_release_receipts WHERE job_id=:job',
                                     job=self.lease.job_id),'completed')
        self.assertEqual(self.scalar('SELECT count(*) FROM metadata_candidates WHERE producing_job_id=:job',
                                     job=self.lease.job_id),0)
        checkpoint=json.loads(self.scalar('SELECT checkpoint_json FROM jobs WHERE id=:id',
                                          id=self.lease.job_id))
        self.assertEqual(checkpoint['phase'],'TERMINAL_REJECTED')

    def test_authority_revoked_during_admission_blocks_send_and_releases_as_not_started(self):
        class RevokingAdapter(SyntheticAdapter):
            def validate_envelope(inner,envelope):
                super().validate_envelope(envelope)
                with write_transaction(self.engine) as conn:
                    conn.execute(text("UPDATE publication_plans SET rights_status='blocked' WHERE job_id=:job"),
                                 dict(job=self.source_job))

        option_id=self.prepare_job(source_text=novel_source())
        adapter=RevokingAdapter(self.profile)
        with self.assertRaisesRegex(GenerationError,'PRE_SEND_CONTEXT_REVOKED'):
            asyncio.run(run_generation(self.metadata,self.lease,option_id=option_id,
                adapter=adapter,profile=self.profile,codec=self.codec,
                context_tokens=4096,output_reserve_tokens=1024))
        self.assertEqual(adapter.calls,[])
        self.assertIsNone(self.scalar("SELECT job_id FROM resource_slots WHERE name='local-qwen'"))
        release=self.scalar('SELECT disposition FROM resource_release_receipts WHERE job_id=:job',
                            job=self.lease.job_id)
        self.assertEqual(release,'not_started')
        self.assertEqual(self.scalar('SELECT count(*) FROM metadata_candidates WHERE producing_job_id=:job',
                                     job=self.lease.job_id),0)
        checkpoint=json.loads(self.scalar('SELECT checkpoint_json FROM jobs WHERE id=:id',
                                          id=self.lease.job_id))
        self.assertEqual(checkpoint['phase'],'NOT_STARTED')

    def test_replaced_source_lease_fails_before_runtime_admission_or_send(self):
        option_id=self.prepare_job(source_text=novel_source())
        self.publish(payload=novel_source(repeats=2).encode('utf-8'),format='txt',previous=self.revision)
        adapter=SyntheticAdapter(self.profile)
        with self.assertRaises(GenerationError):
            asyncio.run(run_generation(self.metadata,self.lease,option_id=option_id,
                adapter=adapter,profile=self.profile,codec=self.codec,
                context_tokens=4096,output_reserve_tokens=1024))
        self.assertEqual(adapter.calls,[])
        self.assertIsNone(self.scalar("SELECT job_id FROM resource_slots WHERE name='local-qwen'"))
        self.assertEqual(self.scalar("SELECT count(*) FROM metadata_candidates WHERE producing_job_id=:job",
                                     job=self.lease.job_id),0)


if __name__ == "__main__":
    unittest.main()
