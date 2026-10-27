"""Typed durable contracts for generated metadata awaiting human review.

The binding records hashes, ranges, policy codes and request/response receipts,
never source text, a prompt, or an unbounded model response. Generated bodies
remain in the existing removable metadata payload table.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import re
from typing import Any

from .library_contracts import canonical_json

SCHEMA_VERSION = "metadata.generated.binding.v1"
AUXILIARY_SCHEMA = "metadata.generated.auxiliary.v1"
MAX_OPTION_SNAPSHOT_BYTES = 65_536
MAX_QA_CODES_BYTES = 16_384
MAX_REQUEST_MANIFEST_BYTES = 1_048_576
MAX_REQUESTS = 1_025  # bounded leaf requests plus at most one synthesis request
MAX_AUXILIARY_BYTES = 32_768
MAX_RANGES = 16
MAX_RANGE_CHARS = 2_048
MAX_REVIEW_BYTES = 4_096
_SHA = re.compile(r"^[0-9a-f]{64}$")


class GeneratedContractError(ValueError):
    """A stable contract failure that never includes source or model text."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def sha256_text(value: str) -> str:
    if type(value) is not str:
        raise GeneratedContractError("INVALID_TEXT")
    try:
        encoded = value.encode("utf-8")
    except UnicodeError:
        raise GeneratedContractError("INVALID_TEXT") from None
    return hashlib.sha256(encoded).hexdigest()


def generated_review_units(candidate_id: str, binding_digest: str,
                           summary: str) -> tuple[dict[str, Any], ...]:
    """Return the single complete, server-derived review unit for a summary.

    A unit covers every summary codepoint. The opaque ID binds that coverage to
    the candidate, immutable generated binding, and exact output hash. It is a
    transport identity, not a semantic claim parser or a human judgment.
    """
    if (type(candidate_id) is not str or not 1 <= len(candidate_id) <= 200
            or not valid_sha(binding_digest) or type(summary) is not str
            or not summary or len(summary) > 16_384):
        raise GeneratedContractError("INVALID_REVIEW_UNIT")
    summary_digest = sha256_text(summary)
    unit_id = generated_review_unit_id(
        candidate_id, binding_digest, summary_digest, len(summary))
    return (dict(unit_id=unit_id, summary_start=0, summary_end=len(summary),
                 summary_sha256=summary_digest),)


def generated_review_unit_id(candidate_id: str, binding_digest: str,
                             summary_sha256: str, summary_codepoints: int) -> str:
    """Derive the opaque whole-summary unit identity from immutable fields."""
    if (type(candidate_id) is not str or not 1 <= len(candidate_id) <= 200
            or not valid_sha(binding_digest) or not valid_sha(summary_sha256)
            or type(summary_codepoints) is not int or not 1 <= summary_codepoints <= 16_384):
        raise GeneratedContractError("INVALID_REVIEW_UNIT")
    identity = {
        "schema": "metadata.generated.review-unit.v1",
        "candidate_id": candidate_id,
        "binding_digest": binding_digest,
        "summary_sha256": summary_sha256,
        "summary_start": 0,
        "summary_end": summary_codepoints,
    }
    return "summary-v1:" + sha256_text(canonical_json(identity))


def valid_sha(value: Any) -> bool:
    return type(value) is str and _SHA.fullmatch(value) is not None


def _bounded_json(value: Any, maximum: int, *, kind: type, code: str) -> str:
    if type(value) is not kind:
        raise GeneratedContractError(code)
    try:
        result = canonical_json(value)
        size = len(result.encode("utf-8"))
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise GeneratedContractError(code) from None
    if size > maximum:
        raise GeneratedContractError(code)
    return result


def _required(value: Any, expected: type, code: str) -> Any:
    if type(value) is not expected:
        raise GeneratedContractError(code)
    return value


@dataclass(frozen=True)
class GeneratedRequestReceipt:
    request_id: str
    request_sha256: str
    response_sha256: str
    request_tokens: int
    output_tokens: int
    profile_digest: str
    prompt_policy_digest: str
    source_start: int
    source_end: int
    terminal: bool = True
    fully_drained: bool = True
    finish_reason: str = "stop"

    def as_dict(self) -> dict[str, Any]:
        value = {
            "request_id": self.request_id,
            "request_sha256": self.request_sha256,
            "response_sha256": self.response_sha256,
            "request_tokens": self.request_tokens,
            "output_tokens": self.output_tokens,
            "profile_digest": self.profile_digest,
            "prompt_policy_digest": self.prompt_policy_digest,
            "source_start": self.source_start,
            "source_end": self.source_end,
            "terminal": self.terminal,
            "fully_drained": self.fully_drained,
            "finish_reason": self.finish_reason,
        }
        validate_request_receipt(value)
        return value


def validate_request_receipt(value: Any) -> dict[str, Any]:
    if type(value) is not dict or set(value) != {
        "request_id", "request_sha256", "response_sha256", "request_tokens",
        "output_tokens", "profile_digest", "prompt_policy_digest", "source_start",
        "source_end", "terminal", "fully_drained", "finish_reason",
    }:
        raise GeneratedContractError("INVALID_REQUEST_RECEIPT")
    for name in ("request_id", "finish_reason"):
        if type(value[name]) is not str or not 1 <= len(value[name]) <= 200:
            raise GeneratedContractError("INVALID_REQUEST_RECEIPT")
    for name in ("request_sha256", "response_sha256", "profile_digest", "prompt_policy_digest"):
        if not valid_sha(value[name]):
            raise GeneratedContractError("INVALID_REQUEST_RECEIPT")
    for name in ("request_tokens", "output_tokens", "source_start", "source_end"):
        if type(value[name]) is not int or value[name] < 0:
            raise GeneratedContractError("INVALID_REQUEST_RECEIPT")
    if value["source_end"] <= value["source_start"]:
        raise GeneratedContractError("INVALID_REQUEST_RECEIPT")
    if value["terminal"] is not True or value["fully_drained"] is not True:
        raise GeneratedContractError("INCOMPLETE_REQUEST")
    if value["finish_reason"] == "length":
        raise GeneratedContractError("OUTPUT_TRUNCATED")
    return value


def request_manifest_json(receipts: tuple[GeneratedRequestReceipt, ...]) -> str:
    if type(receipts) is not tuple or not 1 <= len(receipts) <= MAX_REQUESTS:
        raise GeneratedContractError("INVALID_REQUEST_MANIFEST")
    values = [receipt.as_dict() for receipt in receipts]
    return _bounded_json(values, MAX_REQUEST_MANIFEST_BYTES, kind=list,
                         code="REQUEST_MANIFEST_TOO_LARGE")


def validate_ranges(value: Any, *, source_chars: int, allow_tail: bool = False) -> tuple[dict[str, int], ...]:
    if type(value) is not list or not 1 <= len(value) <= MAX_RANGES:
        raise GeneratedContractError("INVALID_EVIDENCE_RANGES")
    ranges = []
    for item in value:
        if type(item) is not dict or set(item) != {"start", "end"}:
            raise GeneratedContractError("INVALID_EVIDENCE_RANGES")
        start, end = item["start"], item["end"]
        if (type(start) is not int or type(end) is not int or start < 0
                or end <= start or end > source_chars or end - start > MAX_RANGE_CHARS):
            raise GeneratedContractError("INVALID_EVIDENCE_RANGES")
        ranges.append({"start": start, "end": end})
    return tuple(ranges)


def validate_auxiliary(value: Any, *, content_sha256: str) -> dict[str, Any]:
    if type(value) is not dict or set(value) != {"schema", "uncertainties", "spoiler_risk"}:
        raise GeneratedContractError("INVALID_AUXILIARY")
    if type(value["schema"]) is not str or value["schema"] != AUXILIARY_SCHEMA:
        raise GeneratedContractError("INVALID_AUXILIARY")
    if not valid_sha(content_sha256):
        raise GeneratedContractError("INVALID_AUXILIARY")
    uncertainties = value["uncertainties"]
    if (type(uncertainties) is not list or len(uncertainties) > 16
            or any(type(item) is not str or not 1 <= len(item) <= 256 for item in uncertainties)):
        raise GeneratedContractError("INVALID_AUXILIARY")
    if (type(value["spoiler_risk"]) is not str
            or value["spoiler_risk"] not in {"low", "medium", "high"}):
        raise GeneratedContractError("INVALID_AUXILIARY")
    return value


def canonical_binding(value: dict[str, Any]) -> tuple[str, str]:
    """Return a bounded canonical binding and its immutable digest."""
    required = {
        "schema", "candidate_id", "job_id", "generation", "work_id",
        "source_revision_id", "option_id", "classification_event_id",
        "classification_digest", "item_type", "generation_head_version",
        "rights_id", "generation_rights_version", "request_digest", "context_digest",
        "source_binding_digest", "option_snapshot", "option_digest",
        "full_text_sha256", "full_text_codepoints", "selected_ranges",
        "excluded_tail_range", "assembly_version", "profile_digest",
        "prompt_policy_digest", "request_manifest", "request_manifest_digest",
        "final_output_sha256", "qa_status", "qa_codes", "qa_policy_digest",
        "created_at_ms",
    }
    if type(value) is not dict or set(value) != required:
        raise GeneratedContractError("INVALID_BINDING")
    if type(value["schema"]) is not str or value["schema"] != SCHEMA_VERSION:
        raise GeneratedContractError("INVALID_BINDING")
    for name in ("candidate_id", "job_id", "work_id", "source_revision_id", "option_id",
                 "classification_event_id", "rights_id"):
        if type(value[name]) is not str or not 1 <= len(value[name]) <= 200:
            raise GeneratedContractError("INVALID_BINDING")
    for name in ("classification_digest", "request_digest", "context_digest", "source_binding_digest",
                 "option_digest", "full_text_sha256", "profile_digest",
                 "prompt_policy_digest", "request_manifest_digest", "final_output_sha256",
                 "qa_policy_digest"):
        if not valid_sha(value[name]):
            raise GeneratedContractError("INVALID_BINDING")
    for name in ("generation", "generation_head_version", "generation_rights_version",
                 "full_text_codepoints", "created_at_ms"):
        if type(value[name]) is not int or value[name] < (0 if name == "generation_head_version" else 1):
            raise GeneratedContractError("INVALID_BINDING")
    if type(value["item_type"]) is not str or value["item_type"] not in {"novel", "document"}:
        raise GeneratedContractError("INVALID_BINDING")
    if (type(value["qa_status"]) is not str
            or value["qa_status"] not in {"reject", "review_required"}):
        raise GeneratedContractError("INVALID_BINDING")
    option_json = _bounded_json(value["option_snapshot"], MAX_OPTION_SNAPSHOT_BYTES,
                                kind=dict, code="INVALID_OPTION_SNAPSHOT")
    option=value["option_snapshot"]
    if (set(option)!={"schema","option_id","format","delivery_kind","coverage",
                     "coverage_confidence","members"}
            or option.get("schema")!="metadata.generated.option.v1"
            or option.get("option_id")!=value["option_id"]
            or type(option.get("format")) is not str
            or type(option.get("delivery_kind")) is not str
            or option.get("delivery_kind") not in {"single_file","bundle"}
            or type(option.get("coverage")) is not dict
            or type(option.get("coverage_confidence")) is not str
            or type(option.get("members")) is not list
            or not 1<=len(option["members"])<=200):
        raise GeneratedContractError("INVALID_OPTION_SNAPSHOT")
    member_keys={"ordinal","revision_id","asset_position","object_sha","byte_size",
                 "file_format","asset_role","local_relpath","state"}
    for ordinal,member in enumerate(option["members"]):
        if (type(member) is not dict or set(member)!=member_keys
                or member["ordinal"]!=ordinal or member["revision_id"]!=value["source_revision_id"]
                or type(member["asset_position"]) is not int or member["asset_position"]<0
                or not valid_sha(member["object_sha"])
                or type(member["byte_size"]) is not int or member["byte_size"]<0
                or type(member["file_format"]) is not str or member["asset_role"]!="content"
                or member["local_relpath"]!=f"objects/{member['object_sha'][:2]}/{member['object_sha'][2:4]}/{member['object_sha']}"
                or member["state"]!="LIVE"):
            raise GeneratedContractError("INVALID_OPTION_SNAPSHOT")
    if hashlib.sha256(option_json.encode("utf-8")).hexdigest()!=value["option_digest"]:
        raise GeneratedContractError("INVALID_OPTION_SNAPSHOT")
    codes_json = _bounded_json(value["qa_codes"], MAX_QA_CODES_BYTES,
                               kind=list, code="INVALID_QA_CODES")
    if (any(type(code) is not str or not re.fullmatch(r"[A-Z0-9_]{1,128}",code)
            for code in value["qa_codes"])
            or len(set(value["qa_codes"]))!=len(value["qa_codes"])):
        raise GeneratedContractError("INVALID_QA_CODES")
    if type(value["selected_ranges"]) is not list or not value["selected_ranges"]:
        raise GeneratedContractError("INVALID_BINDING")
    previous_end=0
    for item in value["selected_ranges"]:
        if (type(item) is not dict or set(item) != {"start", "end"}
                or type(item["start"]) is not int or type(item["end"]) is not int
                or item["start"] != previous_end or item["end"] <= item["start"]
                or item["end"] > value["full_text_codepoints"]):
            raise GeneratedContractError("INVALID_BINDING")
        previous_end=item["end"]
    tail = value["excluded_tail_range"]
    if (type(tail) is not dict or set(tail) != {"start", "end"}
            or type(tail["start"]) is not int or type(tail["end"]) is not int
            or tail["start"] < 0 or tail["end"] < tail["start"]):
        raise GeneratedContractError("INVALID_BINDING")
    if (tail["end"]!=value["full_text_codepoints"]
            or value["selected_ranges"][-1]["end"]!=tail["start"]):
        raise GeneratedContractError("INVALID_BINDING")
    manifest_json = _bounded_json(value["request_manifest"], MAX_REQUEST_MANIFEST_BYTES,
                                  kind=list, code="REQUEST_MANIFEST_TOO_LARGE")
    if not 1 <= len(value["request_manifest"]) <= MAX_REQUESTS:
        raise GeneratedContractError("INVALID_REQUEST_MANIFEST")
    if hashlib.sha256(manifest_json.encode("utf-8")).hexdigest()!=value["request_manifest_digest"]:
        raise GeneratedContractError("INVALID_REQUEST_MANIFEST")
    for receipt in value["request_manifest"]:
        validate_request_receipt(receipt)
        if (receipt["profile_digest"]!=value["profile_digest"]
                or receipt["prompt_policy_digest"]!=value["prompt_policy_digest"]
                or receipt["source_end"]>tail["start"] or receipt["finish_reason"]!="stop"):
            raise GeneratedContractError("INVALID_REQUEST_MANIFEST")
    if set(option) & {"source_text", "prompt", "raw_response", "body"}:
        raise GeneratedContractError("RAW_SOURCE_FORBIDDEN")
    binding_json = _bounded_json(value, 2_000_000, kind=dict, code="INVALID_BINDING")
    # Check the subdocument serializers used for explicit schema byte limits.
    if len(codes_json.encode()) > MAX_QA_CODES_BYTES or len(manifest_json.encode()) > MAX_REQUEST_MANIFEST_BYTES:
        raise GeneratedContractError("INVALID_BINDING")
    return binding_json, hashlib.sha256(binding_json.encode("utf-8")).hexdigest()


def review_context_digest(value: dict[str, Any]) -> str:
    if type(value) is not dict:
        raise GeneratedContractError("INVALID_REVIEW_CONTEXT")
    return hashlib.sha256(_bounded_json(value, MAX_REVIEW_BYTES, kind=dict,
                                        code="INVALID_REVIEW_CONTEXT").encode()).hexdigest()


def verify_metadata_generated_in(db: Any) -> None:
    """Verify generated provenance structurally for backups and restore.

    This intentionally checks recorded identity and hashes only. It does not
    require the historical requester or reviewer to remain an active admin,
    nor does it turn restored evidence into current source/rights authority.
    """
    try:
        rows=db.execute("""SELECT b.candidate_id,b.schema_version,b.binding_json,b.binding_digest,
              b.review_unit_id,b.summary_codepoints,b.created_at_ms,
              c.work_id,c.kind,c.origin,c.source_revision_id,c.rights_id,
              c.rights_version_at_creation,c.content_sha256,c.producing_job_id,c.generation,
              i.request_digest,i.source_binding_json,j.kind,j.state,j.result_json,
              k.review_status,k.audit_context_json,k.version
            FROM metadata_generated_bindings b
            JOIN metadata_candidates c ON c.id=b.candidate_id
            JOIN metadata_job_inputs i ON i.job_id=c.producing_job_id
            JOIN jobs j ON j.id=c.producing_job_id
            JOIN metadata_candidate_control k ON k.candidate_id=c.id
            ORDER BY b.candidate_id""").fetchall()
    except Exception as exc:
        # Avoid preserving a provider/driver error that could contain row data.
        raise ValueError("Generated binding schema or rows are unavailable") from None
    for row in rows:
        (candidate_id,schema_version,binding_json,binding_digest,review_unit_id,
         summary_codepoints,created_at_ms,
         work_id,kind,origin,source_revision_id,rights_id,rights_version,content_sha256,
         job_id,generation,request_digest,source_binding_json,job_kind,job_state,
         result_json,review_status,audit_context_json,current_control_version)=row
        try:
            binding=json.loads(binding_json)
            canonical,computed_digest=canonical_binding(binding)
            if (schema_version!=SCHEMA_VERSION or canonical!=binding_json
                    or computed_digest!=binding_digest or binding.get("candidate_id")!=candidate_id
                    or binding.get("work_id")!=work_id or binding.get("source_revision_id")!=source_revision_id
                    or binding.get("rights_id")!=rights_id
                    or binding.get("generation_rights_version")!=rights_version
                    or binding.get("job_id")!=job_id or binding.get("generation")!=generation
                    or binding.get("request_digest")!=request_digest
                    or binding.get("created_at_ms")!=created_at_ms
                    or kind!="description" or origin!="QWEN_TEXT"
                    or job_kind!="metadata.description.publish" or job_state!="completed"
                    or not valid_sha(content_sha256)
                    or binding.get("final_output_sha256")!=content_sha256
                    or type(summary_codepoints) is not int or not 1<=summary_codepoints<=16_384
                    or generated_review_unit_id(candidate_id,binding_digest,content_sha256,
                        summary_codepoints)!=review_unit_id):
                raise ValueError
            source_binding=json.loads(source_binding_json)
            if (canonical_json(source_binding)!=source_binding_json
                    or hashlib.sha256(source_binding_json.encode("utf-8")).hexdigest()
                       !=binding["source_binding_digest"]):
                raise ValueError
            result=json.loads(result_json)
            if result.get("candidate_id")!=candidate_id or result.get("state")!="pending_review":
                raise ValueError
            manifest_json=_bounded_json(binding["request_manifest"],MAX_REQUEST_MANIFEST_BYTES,
                                        kind=list,code="INVALID_REQUEST_MANIFEST")
            if hashlib.sha256(manifest_json.encode("utf-8")).hexdigest()!=binding["request_manifest_digest"]:
                raise ValueError
            auxiliary_row=db.execute("SELECT content_sha256,auxiliary_json,auxiliary_digest FROM metadata_generated_payloads WHERE candidate_id=?",
                                     (candidate_id,)).fetchone()
            payload_row=db.execute("SELECT text_value,text_sha256 FROM metadata_payloads WHERE candidate_id=?",
                                   (candidate_id,)).fetchone()
            tombstone=db.execute("SELECT 1 FROM metadata_retention_tombstones WHERE candidate_id=?",
                                 (candidate_id,)).fetchone()
            if tombstone:
                if auxiliary_row is not None or payload_row is not None:
                    raise ValueError
            elif payload_row is None or auxiliary_row is None:
                raise ValueError
            else:
                text_value,text_sha=payload_row
                auxiliary_sha,auxiliary_json,auxiliary_digest=auxiliary_row
                auxiliary=json.loads(auxiliary_json)
                validate_auxiliary(auxiliary,content_sha256=content_sha256)
                if (text_sha!=content_sha256 or auxiliary_sha!=content_sha256
                        or sha256_text(text_value)!=content_sha256
                        or len(text_value)!=summary_codepoints
                        or canonical_json(auxiliary)!=auxiliary_json
                        or hashlib.sha256(auxiliary_json.encode("utf-8")).hexdigest()!=auxiliary_digest):
                    raise ValueError
            if review_status=="approved":
                context=json.loads(audit_context_json)
                expected_keys={"operation","candidate_id","binding_digest",
                    "review_context_digest","decision_digest","operation_digest",
                    "expected_control_version","expected_head_version","expected_rights_version",
                    "decision","select","semantic_review","grounding","spoiler",
                    "review_units","ranges","flags","receipt"}
                if (type(context) is not dict or set(context)!=expected_keys
                        or context.get("operation")!="generated_human_review_v1"
                        or context.get("candidate_id")!=candidate_id
                        or context.get("binding_digest")!=binding_digest
                        or not valid_sha(context.get("review_context_digest"))
                        or not valid_sha(context.get("decision_digest"))
                        or not valid_sha(context.get("operation_digest"))
                        or context.get("semantic_review")!="HUMAN_ADMIN_ATTESTED"
                        or context.get("decision")!="approved"
                        or context.get("grounding")!="grounded"
                        or type(context.get("select")) is not bool
                        or type(context.get("expected_control_version")) is not int
                        or context.get("expected_control_version")<1
                        or context.get("expected_control_version")+1!=current_control_version
                        or type(context.get("expected_head_version")) is not int
                        or context.get("expected_head_version")<0
                        or type(context.get("expected_rights_version")) is not int
                        or context.get("expected_rights_version")<1
                        or context.get("spoiler") not in {"acceptable","not_applicable"}
                        or type(context.get("receipt")) is not dict):
                    raise ValueError
                units=context.get("review_units")
                ranges=context.get("ranges")
                if (type(units) is not list or len(units)!=1
                        or type(units[0]) is not dict
                        or set(units[0])!={"unit_id","range_indexes"}
                        or units[0]["unit_id"]!=review_unit_id
                        or type(units[0]["range_indexes"]) is not list
                        or not units[0]["range_indexes"]
                        or any(type(index) is not int or not 0<=index<len(ranges or [])
                               for index in units[0]["range_indexes"])
                        or len(set(units[0]["range_indexes"]))!=len(units[0]["range_indexes"])
                        or type(ranges) is not list or not 1<=len(ranges)<=MAX_RANGES
                        or type(context.get("flags")) is not list):
                    raise ValueError
                if any(ranges[index]["tail"] for index in units[0]["range_indexes"]):
                    raise ValueError
                required_flags=set(binding["qa_codes"])-{
                    "SEMANTIC_GROUNDING_NOT_VERIFIED","SPOILER_SEMANTICS_NOT_VERIFIED"}
                flag_codes=set()
                for flag in context["flags"]:
                    if (type(flag) is not dict
                            or set(flag)!={"code","resolution","range_indexes"}
                            or type(flag["code"]) is not str
                            or flag["code"] not in required_flags
                            or flag["code"] in flag_codes
                            or flag["resolution"]!="accept_with_evidence"
                            or type(flag["range_indexes"]) is not list
                            or not flag["range_indexes"]
                            or any(type(index) is not int or not 0<=index<len(ranges)
                                   for index in flag["range_indexes"])
                            or len(set(flag["range_indexes"]))!=len(flag["range_indexes"])):
                        raise ValueError
                    flag_codes.add(flag["code"])
                if flag_codes!=required_flags:
                    raise ValueError
                tail=binding["excluded_tail_range"]
                for item in ranges:
                    if (type(item) is not dict or set(item)!={"start","end","sha256","tail"}
                            or type(item["start"]) is not int or type(item["end"]) is not int
                            or item["start"]<0 or item["end"]<=item["start"]
                            or item["end"]>binding["full_text_codepoints"]
                            or item["end"]-item["start"]>MAX_RANGE_CHARS
                            or not valid_sha(item["sha256"]) or type(item["tail"]) is not bool
                            or item["tail"]!=(item["end"]>tail["start"])
                            or (item["tail"] and not (item["start"]>=tail["start"]
                                and item["end"]<=tail["end"]))
                            or (not item["tail"] and item["end"]>tail["start"])):
                        raise ValueError
                if binding["item_type"]=="novel":
                    if context["spoiler"]!="acceptable" or not any(item["tail"] for item in ranges):
                        raise ValueError
                elif binding["item_type"]!="document":
                    raise ValueError
                range_digests=[dict(start=item["start"],end=item["end"],sha256=item["sha256"])
                               for item in ranges]
                review_context=dict(schema="metadata.generated.review-context.v1",
                    candidate_id=candidate_id,binding_digest=binding_digest,
                    summary_sha256=content_sha256,source_revision_id=source_revision_id,
                    source_sha256=binding["full_text_sha256"],option_digest=binding["option_digest"],
                    classification_digest=binding["classification_digest"],rights_id=rights_id,
                    rights_version=context["expected_rights_version"],
                    head_version=context["expected_head_version"],
                    control_version=context["expected_control_version"],ranges=range_digests)
                if review_context_digest(review_context)!=context["review_context_digest"]:
                    raise ValueError
                decision_record=dict(schema="metadata.generated.decision.v1",
                    candidate_id=candidate_id,binding_digest=binding_digest,
                    review_context_digest=context["review_context_digest"],
                    expected_control_version=context["expected_control_version"],
                    expected_head_version=context["expected_head_version"],
                    expected_rights_version=context["expected_rights_version"],
                    grounding=context["grounding"],spoiler=context["spoiler"],
                    review_units=units,ranges=ranges,flags=context["flags"],
                    decision=context["decision"],select=context["select"])
                operation_value=dict(candidate_id=candidate_id,
                    expected_control_version=context["expected_control_version"],
                    expected_head_version=context["expected_head_version"],
                    expected_rights_version=context["expected_rights_version"],
                    review_context_digest=context["review_context_digest"],ranges=ranges,
                    review_units=units,grounding=context["grounding"],
                    spoiler=context["spoiler"],flags=context["flags"],
                    decision=context["decision"],select=context["select"])
                if (hashlib.sha256(_bounded_json(decision_record,MAX_REVIEW_BYTES,
                        kind=dict,code="INVALID_REVIEW_CONTEXT").encode()).hexdigest()
                        !=context["decision_digest"]
                        or hashlib.sha256(canonical_json(operation_value).encode()).hexdigest()
                        !=context["operation_digest"]):
                    raise ValueError
        except (KeyError,TypeError,ValueError,UnicodeError,RecursionError,
                GeneratedContractError):
            raise ValueError("Generated binding, payload, or review evidence differs") from None
    if db.execute("""SELECT 1 FROM metadata_generated_payloads a
          LEFT JOIN metadata_generated_bindings b ON b.candidate_id=a.candidate_id
          LEFT JOIN metadata_payloads p ON p.candidate_id=a.candidate_id
          LEFT JOIN metadata_retention_tombstones t ON t.candidate_id=a.candidate_id
          WHERE b.candidate_id IS NULL OR p.candidate_id IS NULL OR t.candidate_id IS NOT NULL LIMIT 1""").fetchone():
        raise ValueError("Generated auxiliary payload lacks a removable bound body")
