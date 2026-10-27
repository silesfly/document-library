"""One explicitly claimed metadata lease through pending human review.

This module has no queue loop, model selection, transport implementation or
qualification authority. The caller injects the current runtime admission
wrapper. Request profiles and envelopes remain UNQUALIFIED; only the wrapper
may accept a particular envelope under separately reviewed evidence.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import inspect
import json
import uuid
from typing import Any, Protocol, Literal

from .jobs import ResourceHold
from .metadata_generated_contracts import (
    GeneratedRequestReceipt, GeneratedContractError, request_manifest_json,
    sha256_text,
)
from .metadata_requests import (
    DraftLeaf, DraftSet, ExactPromptCodec, PromptPolicy, RequestEnvelope,
    RequestLimits, RequestProfile, prepare_summary_requests,
    prepare_synthesis_request,
)
from .metadata_service import MetadataError, MetadataService
from .metadata_summary import (
    RuleQAResult, SummaryError, SummaryOutput, assess_summary_rules,
    parse_summary_output,
)

MAX_OUTER_RESPONSE_BYTES = 1_048_576
MAX_GENERATED_RESPONSE_BYTES = 65_536
MAX_RESOURCE_CONFIRMATION_CHARS = 500
_CHECKPOINT_SCHEMA = "metadata.generated.exchange.v1"
_QA_POLICY_ID = "metadata.summary.ruleqa.conservative.v1"


class RuntimeReceipt(Protocol):
    """Transient normal-terminal receipt supplied by the qualified wrapper."""

    response_bytes: bytes
    response_sha256: str
    output_text: str
    request_id: str
    wire_sha256: str
    profile_digest: str
    model: str
    prompt_eval_count: int
    eval_count: int
    done_reason: str
    terminal: bool
    fully_drained: bool


class RuntimeAdmission(Protocol):
    """Evidence-backed wrapper; its concrete authority lives outside pure123."""

    def validate_envelope(self, envelope: RequestEnvelope) -> None: ...

    def generate(self, envelope: RequestEnvelope, *, request_id: str) -> RuntimeReceipt: ...


class StopProof(Protocol):
    request_id: str
    wire_sha256: str
    profile_digest: str
    disposition: Literal["completed", "not_started"]
    confirmation: str
    terminal: bool
    fully_drained: bool


class GenerationError(MetadataError):
    """Stable failure code without retaining source, response, or adapter text."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


class _ReceiptFailure(ValueError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _json_digest(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True,
                         separators=(",", ":"), allow_nan=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _profile_model(profile: RequestProfile) -> str:
    if type(profile) is not RequestProfile or type(profile.model) is not str or not profile.model:
        raise GenerationError("INVALID_REQUEST_PROFILE")
    return profile.model


def _receipt_values(receipt: RuntimeReceipt, *, envelope: RequestEnvelope,
                    request_id: str, profile: RequestProfile) -> tuple[str, int, int, str]:
    """Validate receipt structure and parity; return output text and counters.

    Remote-stop confirmation is handled separately so a fully drained response
    with a bad product output can release the hold without storing a candidate.
    """
    try:
        fields = (
            receipt.response_bytes, receipt.response_sha256, receipt.output_text,
            receipt.request_id, receipt.wire_sha256, receipt.profile_digest,
            receipt.model, receipt.prompt_eval_count, receipt.eval_count,
            receipt.done_reason, receipt.terminal, receipt.fully_drained,
        )
    except Exception:
        raise _ReceiptFailure("INVALID_RUNTIME_RECEIPT") from None
    (response_bytes, response_sha256, output_text, response_request_id,
     wire_sha256, profile_digest, model, prompt_eval_count, eval_count,
     done_reason, terminal, fully_drained) = fields
    if (type(response_bytes) is not bytes or not 0 < len(response_bytes) <= MAX_OUTER_RESPONSE_BYTES
            or type(response_sha256) is not str
            or hashlib.sha256(response_bytes).hexdigest() != response_sha256):
        raise _ReceiptFailure("INVALID_RUNTIME_RESPONSE")
    if (type(response_request_id) is not str or response_request_id != request_id
            or type(wire_sha256) is not str or wire_sha256 != envelope.wire_sha256
            or type(profile_digest) is not str or profile_digest != envelope.profile_digest
            or type(model) is not str or model != _profile_model(profile)):
        raise _ReceiptFailure("RUNTIME_PARITY_MISMATCH")
    if (type(prompt_eval_count) is not int or prompt_eval_count != envelope.request_tokens
            or type(eval_count) is not int or not 0 <= eval_count <= envelope.output_reserve_tokens):
        raise _ReceiptFailure("RUNTIME_TOKEN_PARITY_MISMATCH")
    if (type(done_reason) is not str or not 1 <= len(done_reason) <= 100
            or terminal is not True or fully_drained is not True):
        raise _ReceiptFailure("RUNTIME_TERMINAL_INVALID")
    if (type(output_text) is not str or not output_text
            or len(output_text.encode("utf-8")) > MAX_GENERATED_RESPONSE_BYTES):
        raise _ReceiptFailure("RUNTIME_OUTPUT_TOO_LARGE")
    return output_text, prompt_eval_count, eval_count, done_reason


def _terminal_proof(error: BaseException, *, request_id: str,
                    envelope: RequestEnvelope) -> StopProof | None:
    """Accept only explicit per-exchange stop evidence from a typed adapter error."""
    proof = getattr(error, "stop_proof", None)
    if proof is None:
        return None
    try:
        proof_request = proof.request_id
        wire_sha256 = proof.wire_sha256
        profile_digest = proof.profile_digest
        disposition = proof.disposition
        confirmation = proof.confirmation
        terminal = proof.terminal
        drained = proof.fully_drained
    except Exception:
        return None
    if (type(proof_request) is not str or proof_request != request_id
            or type(wire_sha256) is not str or wire_sha256 != envelope.wire_sha256
            or type(profile_digest) is not str or profile_digest != envelope.profile_digest
            or type(disposition) is not str
            or disposition not in {"completed", "not_started"}
            or type(confirmation) is not str
            or not 1 <= len(confirmation.strip()) <= MAX_RESOURCE_CONFIRMATION_CHARS):
        return None
    if disposition == "completed" and (terminal is not True or drained is not True):
        return None
    if disposition == "not_started" and (terminal is not False or drained is not False):
        return None
    return proof


def _confirmation(proof: StopProof) -> str:
    # Keep durable JobService evidence short and free of provider prose/body.
    proof_digest = hashlib.sha256(proof.confirmation.strip().encode("utf-8")).hexdigest()
    return f"{proof.disposition}:{proof.request_id}:{proof_digest}"[:MAX_RESOURCE_CONFIRMATION_CHARS]


def _qa_policy_digest(forbidden_phrases: tuple[str, ...],
                      tail_only_entities: tuple[str, ...]) -> str:
    return _json_digest({
        "schema": _QA_POLICY_ID,
        "forbidden_phrases_digest": _json_digest(forbidden_phrases),
        "tail_only_entities_digest": _json_digest(tail_only_entities),
    })


def _checkpoint(job_service, lease, *, phase: str, requests: list[dict[str, Any]],
                active: dict[str, Any] | None = None) -> None:
    manifest_hash = _json_digest(requests)
    value = {
        "schema": _CHECKPOINT_SCHEMA,
        "phase": phase,
        "lease_generation": lease.generation,
        "completed_requests": len(requests),
        "completed_manifest_sha256": manifest_hash,
    }
    if active is not None:
        value["active"] = active
    job_service.checkpoint(lease, value)


def _prior_attempt(service: MetadataService, lease) -> None:
    job = service.jobs.get(lease.job_id)
    if not job:
        raise GenerationError("JOB_UNAVAILABLE")
    raw = job.get("checkpoint_json")
    if raw in (None, ""):
        return
    try:
        previous = json.loads(raw)
    except (ValueError, TypeError, RecursionError):
        raise GenerationError("PRIOR_GENERATION_STATE_UNREADABLE") from None
    if type(previous) is dict and previous.get("schema") == _CHECKPOINT_SCHEMA:
        # This runner intentionally has no crash recovery or resend path. A new
        # lease cannot infer remote stop from a prior checkpoint.
        raise GenerationError("PRIOR_GENERATION_ATTEMPT_REQUIRES_RECONCILIATION")


def _release(services: MetadataService, hold: ResourceHold, proof: StopProof) -> None:
    try:
        services.jobs.confirm_resource_stopped(
            hold, disposition=proof.disposition, confirmation=_confirmation(proof))
    except Exception:
        raise GenerationError("RESOURCE_RELEASE_CONFIRMATION_FAILED") from None


async def _read_request(
    service: MetadataService, lease, envelope: RequestEnvelope, *,
    adapter: RuntimeAdmission, profile: RequestProfile,
    request_manifest: list[dict[str, Any]], ordinal: int, pre_send_check,
) -> tuple[SummaryOutput, dict[str, Any]]:
    """One validate → acquire → intent → send → drain/release operation."""
    request_id = str(uuid.uuid4())
    # The evidence wrapper is the sole admission authority. UNQUALIFIED pure
    # profile/envelope labels are never rewritten here.
    try:
        with service.jobs.ownership(lease) as conn:
            source = service._input(conn, lease, "description")
            if source.get("origin") != "QWEN_TEXT":
                raise GenerationError("LOCAL_TEXT_GENERATION_REQUIRED")
        adapter.validate_envelope(envelope)
    except GenerationError:
        raise
    except Exception:
        raise GenerationError("RUNTIME_ADMISSION_REJECTED") from None

    try:
        hold = service.jobs.acquire_resource(lease, "local-qwen")
    except Exception:
        raise GenerationError("LOCAL_QWEN_RESOURCE_UNAVAILABLE") from None

    active = dict(request_id=request_id, ordinal=ordinal,
        wire_sha256=envelope.wire_sha256, profile_digest=envelope.profile_digest,
        source_start=envelope.source_start, source_end=envelope.source_end,
        state="SEND_ATTEMPTED")
    try:
        _checkpoint(service.jobs, lease, phase="SEND_ATTEMPTED",
                    requests=request_manifest, active=active)
    except Exception:
        proof = _LocalNotStartedProof(request_id, envelope.wire_sha256,
                                      envelope.profile_digest)
        _release(service, hold, proof)
        raise GenerationError("GENERATION_INTENT_CHECKPOINT_FAILED") from None

    # Admission may take time or mutate external state. Recheck the full
    # prepared source, option, rights, classification and lease immediately
    # before the sender can observe the envelope.
    try:
        pre_send_check()
    except Exception:
        proof = _LocalNotStartedProof(request_id, envelope.wire_sha256,
                                      envelope.profile_digest)
        try:
            _release(service, hold, proof)
        except GenerationError:
            try:
                _checkpoint(service.jobs, lease, phase="UNCERTAIN_HELD",
                            requests=request_manifest,
                            active=dict(active, state="UNCERTAIN_HELD"))
            except Exception:
                pass
            raise GenerationError("PRE_SEND_STOP_CONFIRMATION_FAILED") from None
        try:
            _checkpoint(service.jobs, lease, phase="NOT_STARTED",
                        requests=request_manifest,
                        active=dict(active, state="NOT_STARTED",
                                    stop_disposition="not_started"))
        except Exception:
            raise GenerationError("PRE_SEND_FENCE_CHECKPOINT_FAILED") from None
        raise GenerationError("PRE_SEND_CONTEXT_REVOKED") from None

    try:
        receipt = adapter.generate(envelope, request_id=request_id)
        if inspect.isawaitable(receipt):
            receipt = await receipt
    except Exception as error:
        proof = _terminal_proof(error, request_id=request_id, envelope=envelope)
        if proof is not None:
            _release(service, hold, proof)
            _checkpoint(service.jobs, lease,
                phase="TERMINAL_REJECTED" if proof.disposition == "completed" else "NOT_STARTED",
                requests=request_manifest,
                active=dict(active, state="TERMINAL_REJECTED" if proof.disposition == "completed" else "NOT_STARTED",
                            stop_disposition=proof.disposition))
            raise GenerationError("RUNTIME_EXCHANGE_REJECTED") from None
        _checkpoint(service.jobs, lease, phase="UNCERTAIN_HELD",
                    requests=request_manifest, active=dict(active, state="UNCERTAIN_HELD"))
        # Keep the resource held and do not attempt later chunks.
        raise GenerationError("RUNTIME_EXCHANGE_UNCERTAIN") from None

    # A returned normal receipt can release only after its local request,
    # wire/profile bindings and complete terminal/drain proof are established.
    try:
        if (getattr(receipt, "request_id", None) != request_id
                or getattr(receipt, "wire_sha256", None) != envelope.wire_sha256
                or getattr(receipt, "profile_digest", None) != envelope.profile_digest
                or getattr(receipt, "terminal", None) is not True
                or getattr(receipt, "fully_drained", None) is not True):
            raise _ReceiptFailure("RUNTIME_TERMINAL_UNPROVEN")
        stop_proof = _ReceiptStopProof(request_id, envelope.wire_sha256,
                                       envelope.profile_digest)
        _release(service, hold, stop_proof)
    except GenerationError:
        raise
    except Exception:
        _checkpoint(service.jobs, lease, phase="UNCERTAIN_HELD",
                    requests=request_manifest, active=dict(active, state="UNCERTAIN_HELD"))
        raise GenerationError("RUNTIME_TERMINAL_UNPROVEN") from None

    try:
        output_text, prompt_tokens, output_tokens, done_reason = _receipt_values(
            receipt, envelope=envelope, request_id=request_id, profile=profile)
        if done_reason != "stop":
            raise _ReceiptFailure("RUNTIME_OUTPUT_NOT_NORMAL")
        output = parse_summary_output(output_text,
            max_response_bytes=MAX_GENERATED_RESPONSE_BYTES)
        response_hash = hashlib.sha256(receipt.response_bytes).hexdigest()
        manifest = GeneratedRequestReceipt(
            request_id=request_id, request_sha256=envelope.wire_sha256,
            response_sha256=response_hash, request_tokens=prompt_tokens,
            output_tokens=output_tokens, profile_digest=envelope.profile_digest,
            prompt_policy_digest=envelope.prompt_policy_digest,
            source_start=envelope.source_start, source_end=envelope.source_end,
            terminal=True, fully_drained=True, finish_reason=done_reason,
        ).as_dict()
    except (SummaryError, _ReceiptFailure, GeneratedContractError, UnicodeError,
            ValueError, TypeError, AttributeError, RecursionError):
        _checkpoint(service.jobs, lease, phase="TERMINAL_REJECTED",
                    requests=request_manifest,
                    active=dict(active, state="TERMINAL_REJECTED", response_sha256=
                                getattr(receipt, "response_sha256", "")))
        raise GenerationError("RUNTIME_OUTPUT_REJECTED") from None
    request_manifest.append(manifest)
    _checkpoint(service.jobs, lease, phase="TERMINAL_CAPTURED",
                requests=request_manifest,
                active=dict(active, state="TERMINAL_CAPTURED",
                            response_sha256=manifest["response_sha256"]))
    return output, manifest


@dataclass(frozen=True)
class _ReceiptStopProof:
    request_id: str
    wire_sha256: str
    profile_digest: str
    disposition: Literal["completed"] = "completed"
    confirmation: str = "adapter_returned_terminal_drained_receipt"
    terminal: bool = True
    fully_drained: bool = True


@dataclass(frozen=True)
class _LocalNotStartedProof:
    request_id: str
    wire_sha256: str
    profile_digest: str
    disposition: Literal["not_started"] = "not_started"
    confirmation: str = "send_not_called_after_intent_checkpoint_failure"
    terminal: bool = False
    fully_drained: bool = False


async def run_generation(
    service: MetadataService,
    lease,
    *,
    option_id: str,
    adapter: RuntimeAdmission,
    profile: RequestProfile,
    codec: ExactPromptCodec,
    context_tokens: int,
    output_reserve_tokens: int,
    forbidden_phrases: tuple[str, ...] = (),
    tail_only_entities: tuple[str, ...] = (),
    request_limits: RequestLimits = RequestLimits(),
) -> dict[str, Any]:
    """Run one claimed lease and atomically leave one pending generated candidate.

    Unknown transport failure retains the local-qwen hold. A completed response
    that fails output/schema/QA is never converted to a candidate. The returned
    job result describes pending review; this function never selects a head.
    """
    if not isinstance(service, MetadataService):
        raise GenerationError("METADATA_SERVICE_REQUIRED")
    if type(option_id) is not str or not option_id:
        raise GenerationError("OPTION_ID_REQUIRED")
    if not callable(getattr(adapter, "validate_envelope", None)) or not callable(
            getattr(adapter, "generate", None)):
        raise GenerationError("RUNTIME_ADMISSION_REQUIRED")
    if type(forbidden_phrases) is not tuple or type(tail_only_entities) is not tuple:
        raise GenerationError("INVALID_RULE_POLICY")
    if any(type(value) is not str or not 1 <= len(value) <= 256
           for value in (*forbidden_phrases, *tail_only_entities)):
        raise GenerationError("INVALID_RULE_POLICY")
    _prior_attempt(service, lease)
    stage = "context"
    try:
        generation_context = service.generation_context(lease, option_id=option_id)
        stage = "source_assembly"
        pending_store = None
        with service.prepare_summary_input(lease, option_id=option_id) as prepared:
            selection = prepared.select()
            policy = PromptPolicy(document_kind=generation_context["item_type"])
            stage = "request_build"
            envelopes = prepare_summary_requests(
                selection, option_digest=generation_context["option_digest"],
                input_digest=prepared.input_digest, context_digest=prepared.context_digest,
                profile=profile, prompt_policy=policy, codec=codec,
                context_tokens=context_tokens, output_reserve_tokens=output_reserve_tokens,
                limits=request_limits)

            # Revalidate the complete accepted122 context before each exchange.
            output_rows: list[tuple[SummaryOutput, dict[str, Any]]] = []
            manifest: list[dict[str, Any]] = []
            stage = "transport"
            for ordinal, envelope in enumerate(envelopes):
                prepared._revalidate()
                item = await _read_request(service, lease, envelope, adapter=adapter,
                    profile=profile, request_manifest=manifest, ordinal=ordinal,
                    pre_send_check=prepared._revalidate)
                output_rows.append(item)

            if len(output_rows) == 1:
                final_output = output_rows[0][0]
            else:
                stage = "synthesis_build"
                leaves = tuple(DraftLeaf(
                    summary=output.summary, uncertainties=output.uncertainties,
                    spoiler_risk=output.spoiler_risk,
                    source_start=envelope.source_start, source_end=envelope.source_end,
                    response_sha256=receipt["response_sha256"],
                ) for output, receipt, envelope in zip(
                    (row[0] for row in output_rows),
                    (row[1] for row in output_rows), envelopes, strict=True))
                drafts = DraftSet(
                    source_revision_id=selection.source_revision_id,
                    source_sha256=selection.source_sha256,
                    selected_codepoints=len(selection.text),
                    option_digest=generation_context["option_digest"],
                    input_digest=prepared.input_digest,
                    context_digest=prepared.context_digest,
                    leaves=leaves)
                synthesis = prepare_synthesis_request(
                    drafts, profile=profile, prompt_policy=policy, codec=codec,
                    context_tokens=context_tokens,
                    output_reserve_tokens=output_reserve_tokens,
                    limits=request_limits)
                prepared._revalidate()
                final_output, _ = await _read_request(service, lease, synthesis,
                    adapter=adapter, profile=profile, request_manifest=manifest,
                    ordinal=len(envelopes), pre_send_check=prepared._revalidate)

            stage = "rule_qa"
            prepared._revalidate()
            qa: RuleQAResult = assess_summary_rules(
                final_output, selection=selection,
                forbidden_phrases=forbidden_phrases,
                tail_only_entities=tail_only_entities)
            if qa.decision not in {"reject", "review_required"}:
                raise GenerationError("RULE_QA_INVALID")
            if qa.semantic_grounding_status != "NOT_VERIFIED" or qa.spoiler_semantics_status != "NOT_VERIFIED":
                raise GenerationError("RULE_QA_AUTHORITY_INVALID")
            # The selected/tested flags are coded findings; no candidate can
            # inherit semantic approval from an empty/no-hit ruleset.
            auxiliary = dict(schema="metadata.generated.auxiliary.v1",
                uncertainties=list(final_output.uncertainties),
                spoiler_risk=final_output.spoiler_risk)
            request_manifest_json(tuple(_manifest_receipt(row) for row in manifest))
            prepared_receipt = prepared.receipt
            binding = dict(
                schema="metadata.generated.binding.v1",
                candidate_id=str(uuid.uuid5(uuid.UUID(lease.job_id),
                    f"metadata:{lease.generation}")),
                job_id=lease.job_id, generation=lease.generation,
                work_id=generation_context["work_id"],
                source_revision_id=generation_context["source_revision_id"],
                option_id=option_id,
                classification_event_id=generation_context["classification_event_id"],
                classification_digest=generation_context["classification_digest"],
                item_type=generation_context["item_type"],
                generation_head_version=generation_context["generation_head_version"],
                rights_id=generation_context["rights_id"],
                generation_rights_version=generation_context["generation_rights_version"],
                request_digest=generation_context["request_digest"],
                context_digest=generation_context["context_digest"],
                source_binding_digest=generation_context["source_binding_digest"],
                option_snapshot=generation_context["option_snapshot"],
                option_digest=generation_context["option_digest"],
                full_text_sha256=prepared_receipt["text_sha256"],
                full_text_codepoints=prepared_receipt["text_codepoints"],
                selected_ranges=[dict(start=span.start, end=span.end)
                                 for span in selection.selected_ranges],
                excluded_tail_range=dict(start=selection.excluded_tail_range.start,
                                         end=selection.excluded_tail_range.end),
                assembly_version=prepared_receipt["schema"],
                profile_digest=profile.digest,
                prompt_policy_digest=policy.digest,
                request_manifest=manifest,
                request_manifest_digest=_json_digest(manifest),
                final_output_sha256=sha256_text(final_output.summary),
                qa_status=qa.decision, qa_codes=list(qa.reason_codes),
                qa_policy_digest=_qa_policy_digest(forbidden_phrases, tail_only_entities),
            )
            _checkpoint(service.jobs, lease, phase="PENDING_STORAGE",
                        requests=manifest)
            prepared._revalidate()
            pending_store=(final_output,binding,auxiliary)
        # The prepared-input context performs its final lease/source fence as
        # it closes. Complete the job only after that fence has returned.
        stage = "pending_storage"
        output,binding,auxiliary=pending_store
        return service.store_generated_description(
            lease, output=output, binding=binding, auxiliary=auxiliary)
    except GenerationError:
        raise
    except (MetadataError, SummaryError, GeneratedContractError, ValueError,
            TypeError, UnicodeError, RecursionError):
        raise GenerationError("GENERATION_"+stage.upper()+"_FAILED") from None
    except Exception as error:
        safe_type=type(error).__name__
        if not safe_type.isascii() or not safe_type.isidentifier():safe_type="UNKNOWN"
        raise GenerationError("GENERATION_"+stage.upper()+"_UNEXPECTED_"+safe_type) from None


def _manifest_receipt(value: dict[str, Any]) -> GeneratedRequestReceipt:
    return GeneratedRequestReceipt(
        request_id=value["request_id"], request_sha256=value["request_sha256"],
        response_sha256=value["response_sha256"], request_tokens=value["request_tokens"],
        output_tokens=value["output_tokens"], profile_digest=value["profile_digest"],
        prompt_policy_digest=value["prompt_policy_digest"],
        source_start=value["source_start"], source_end=value["source_end"],
        terminal=value["terminal"], fully_drained=value["fully_drained"],
        finish_reason=value["finish_reason"])
