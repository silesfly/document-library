"""Pure, bounded summary preparation; no extraction, models or publication.

Source coordinates are unnormalized Python Unicode code points. The mandatory
complete-source attestation, expected length and digest bind a trusted caller's
manifest; they cannot detect a caller falsely attesting that an excerpt is whole.
A protected, complete extractor and an authoritative classification are separate
integration requirements. SelectedInput is a transient value, never an authority
proof or a public API receipt.

Hard caps below are local resource limits, not model token/context estimates.
Semantic grounding and spoiler safety remain NOT_VERIFIED by these rules.
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from hashlib import sha256
import json
from typing import Literal
import unicodedata

MAX_SOURCE_CHARS = 16_000_000
MAX_REVISION_ID_CHARS = 256
MAX_CONTEXT_TOKENS = 16_000_000
MAX_CHUNK_CHARS = 1_000_000
MAX_CHUNKS = 4_096
MAX_COUNTER_CALLS = 100_000
MAX_RESPONSE_BYTES = 65_536
MAX_UNCERTAINTIES = 16
MAX_UNCERTAINTY_CHARS = 256
MAX_RULES = 64
MAX_RULE_CHARS = 256


class SummaryError(ValueError):
    """A stable reason code; never retains source, response or callback errors."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class SourceRange:
    start: int
    end: int


@dataclass(frozen=True)
class ChapterSpan:
    ordinal: int
    start: int
    end: int


@dataclass(frozen=True)
class SelectedInput:
    source_revision_id: str
    source_sha256: str
    source_char_count: int
    selection_mode: str
    selected_ranges: tuple[SourceRange, ...]
    excluded_tail_range: SourceRange
    text: str = field(repr=False)


@dataclass(frozen=True)
class SummaryChunk:
    source_revision_id: str
    source_sha256: str
    start: int
    end: int
    request_tokens: int
    text: str = field(repr=False)


@dataclass(frozen=True)
class SummaryOutput:
    summary: str = field(repr=False)
    uncertainties: tuple[str, ...] = field(repr=False)
    spoiler_risk: Literal["low", "medium", "high"]


@dataclass(frozen=True)
class RuleQAResult:
    decision: Literal["reject", "review_required"]
    reason_codes: tuple[str, ...]
    output_schema_checked: bool
    selected_input_checked: bool
    forbidden_phrases_checked: bool
    tail_only_entities_checked: bool
    semantic_grounding_status: Literal["NOT_VERIFIED"] = "NOT_VERIFIED"
    spoiler_semantics_status: Literal["NOT_VERIFIED"] = "NOT_VERIFIED"


def _require(condition: bool, code: str) -> None:
    if not condition:
        raise SummaryError(code)


def _positive_bound(value: int, cap: int, code: str) -> int:
    _require(type(value) is int and 0 < value <= cap, code)
    return value


def _utf8(value: str, code: str) -> bytes:
    _require(type(value) is str, code)
    encoded = None
    try:
        encoded = value.encode("utf-8")
    except UnicodeError:
        pass
    # Raise outside the handler: do not retain UnicodeEncodeError's raw input.
    _require(encoded is not None, code)
    return encoded


def _is_digest(value: str) -> bool:
    return (type(value) is str and len(value) == 64
            and all(char in "0123456789abcdef" for char in value))


def _validate_revision_id(value: str) -> None:
    _require(type(value) is str and 0 < len(value) <= MAX_REVISION_ID_CHARS
             and bool(value.strip()), "INVALID_SOURCE_REVISION")
    _utf8(value, "INVALID_SOURCE_REVISION")


def select_summary_input(text: str, *, source_revision_id: str, document_kind: Literal["novel", "document"], chapters: tuple[ChapterSpan, ...] | None, max_source_chars: int, source_complete: bool, expected_source_chars: int, expected_source_sha256: str) -> SelectedInput:
    max_source_chars = _positive_bound(max_source_chars, MAX_SOURCE_CHARS, "INVALID_SOURCE_BOUND")

    _require(type(text) is str and len(text) > 0, "INVALID_SOURCE_TEXT")
    _require(len(text) <= max_source_chars, "SOURCE_TOO_LARGE")
    _require(source_complete is True, "SOURCE_NOT_COMPLETE")
    _require(
        type(expected_source_chars) is int
        and expected_source_chars > 0
        and expected_source_chars == len(text),
        "SOURCE_LENGTH_MISMATCH",
    )
    _require(_is_digest(expected_source_sha256), "INVALID_SOURCE_DIGEST")

    source_bytes = _utf8(text, "INVALID_SOURCE_UNICODE")
    _require(sha256(source_bytes).hexdigest() == expected_source_sha256, "SOURCE_DIGEST_MISMATCH")
    _validate_revision_id(source_revision_id)
    _require(type(document_kind) is str and document_kind in ("novel", "document"), "INVALID_DOCUMENT_KIND")

    chapters_are_valid = chapters is not None
    if chapters_are_valid:
        chapters_are_valid = type(chapters) is tuple and len(chapters) > 0
        if chapters_are_valid:
            for index, chapter in enumerate(chapters):
                if type(chapter) is not ChapterSpan:
                    chapters_are_valid = False
                    break
                if type(chapter.ordinal) is not int or chapter.ordinal < 1:
                    chapters_are_valid = False
                    break
                if chapter.ordinal != chapters[0].ordinal + index:
                    chapters_are_valid = False
                    break
                if type(chapter.start) is not int or type(chapter.end) is not int:
                    chapters_are_valid = False
                    break
                if chapter.start < 0 or chapter.end <= chapter.start:
                    chapters_are_valid = False
                    break
                if chapter.end > len(text):
                    chapters_are_valid = False
                    break
                if index == 0:
                    if chapter.start != 0:
                        chapters_are_valid = False
                        break
                else:
                    if chapter.start != chapters[index - 1].end:
                        chapters_are_valid = False
                        break
            if chapters_are_valid and chapters[-1].end != len(text):
                chapters_are_valid = False
    _require(chapters is None or chapters_are_valid, "INVALID_CHAPTERS")

    if document_kind == "document":
        selected_end = len(text)
        selected_ranges = (SourceRange(0, selected_end),)
        excluded_tail_range = SourceRange(selected_end, selected_end)
        selection_mode = "document"
    elif chapters is not None:
        retained_count = (3 * len(chapters)) // 4
        _require(retained_count > 0, "INSUFFICIENT_INPUT")
        selected_end = chapters[retained_count - 1].end
        selected_ranges = tuple(
            SourceRange(chapter.start, chapter.end)
            for chapter in chapters[:retained_count]
        )
        excluded_tail_range = SourceRange(selected_end, len(text))
        selection_mode = "novel_chapters"
    else:
        selected_end = (3 * len(text)) // 4
        _require(selected_end > 0, "INSUFFICIENT_INPUT")
        selected_ranges = (SourceRange(0, selected_end),)
        excluded_tail_range = SourceRange(selected_end, len(text))
        selection_mode = "novel_length"

    return SelectedInput(
        source_revision_id=source_revision_id,
        source_sha256=expected_source_sha256,
        source_char_count=len(text),
        selection_mode=selection_mode,
        selected_ranges=selected_ranges,
        excluded_tail_range=excluded_tail_range,
        text=text[:selected_end],
    )


def _validate_selection(selection: SelectedInput) -> None:
    """Check transient shape; cannot authenticate the original whole source."""
    _require(type(selection) is SelectedInput, "INVALID_SELECTION")
    _validate_revision_id(selection.source_revision_id)
    _require(_is_digest(selection.source_sha256), "INVALID_SELECTION")
    _positive_bound(selection.source_char_count, MAX_SOURCE_CHARS, "INVALID_SELECTION")
    _require(type(selection.selection_mode) is str and selection.selection_mode
             in ("document", "novel_length", "novel_chapters"), "INVALID_SELECTION")
    _require(type(selection.text) is str and 0 < len(selection.text)
             <= selection.source_char_count, "INVALID_SELECTION")
    _utf8(selection.text, "INVALID_SELECTION")
    _require(type(selection.selected_ranges) is tuple
             and 0 < len(selection.selected_ranges) <= selection.source_char_count,
             "INVALID_SELECTION")
    end = 0
    for span in selection.selected_ranges:
        _require(type(span) is SourceRange and type(span.start) is int
                 and type(span.end) is int and span.start == end
                 and end < span.end <= selection.source_char_count, "INVALID_SELECTION")
        end = span.end
    tail = selection.excluded_tail_range
    _require(end == len(selection.text) and type(tail) is SourceRange
             and type(tail.start) is int and type(tail.end) is int
             and tail.start == end and tail.end == selection.source_char_count,
             "INVALID_SELECTION")
    if selection.selection_mode == "document":
        _require(end == selection.source_char_count, "INVALID_SELECTION")
    elif selection.selection_mode == "novel_length":
        _require(end == 3 * selection.source_char_count // 4, "INVALID_SELECTION")
    else:
        _require(end < selection.source_char_count, "INVALID_SELECTION")


def chunk_summary_input(
    selection: SelectedInput, *, count_request_tokens: Callable[[str], int],
    context_tokens: int, output_reserve_tokens: int, max_chunk_chars: int,
    max_chunks: int, max_counter_calls: int,
) -> tuple[SummaryChunk, ...]:
    """Partition selected text with an exact trusted request-token counter.

    Descending candidate search assumes no monotonicity. All calls, including a
    final recount of every accepted slice, share one hard call cap. Any error
    aborts without returning partial chunks. The adapter owns counter execution
    isolation and real request/tokenizer provenance; no tokenizer is loaded here.
    """
    _validate_selection(selection)
    _positive_bound(context_tokens, MAX_CONTEXT_TOKENS, "INVALID_CONTEXT_BOUND")
    _require(type(output_reserve_tokens) is int
             and 0 <= output_reserve_tokens < context_tokens, "INVALID_OUTPUT_RESERVE")
    _positive_bound(max_chunk_chars, MAX_CHUNK_CHARS, "INVALID_CHUNK_BOUND")
    _positive_bound(max_chunks, MAX_CHUNKS, "INVALID_CHUNK_COUNT_BOUND")
    _positive_bound(max_counter_calls, MAX_COUNTER_CALLS, "INVALID_COUNTER_CALL_BOUND")
    _require(callable(count_request_tokens), "INVALID_COUNTER")
    calls = 0

    def count(text: str) -> int:
        nonlocal calls
        _require(calls < max_counter_calls, "COUNTER_CALL_LIMIT")
        calls += 1
        failed = False
        tokens = None
        try:
            tokens = count_request_tokens(text)
        except Exception:
            failed = True
        # Raise after leaving the handler; never retain callback exception text.
        _require(not failed, "COUNTER_FAILED")
        _require(type(tokens) is int and tokens >= 0, "INVALID_COUNTER_VALUE")
        return tokens

    chunks = []
    start = 0
    while start < len(selection.text):
        _require(len(chunks) < max_chunks, "CHUNK_COUNT_LIMIT")
        chosen = None
        for length in range(min(max_chunk_chars, len(selection.text) - start), 0, -1):
            end = start + length
            text = selection.text[start:end]
            tokens = count(text)
            if tokens + output_reserve_tokens <= context_tokens:
                chosen = SummaryChunk(selection.source_revision_id, selection.source_sha256,
                                      start, end, tokens, text)
                break
        _require(chosen is not None, "TOKEN_BUDGET_EXCEEDED")
        chunks.append(chosen)
        start = chosen.end
    for chunk in chunks:
        final_tokens = count(chunk.text)
        _require(final_tokens == chunk.request_tokens, "COUNTER_UNSTABLE")
        _require(final_tokens + output_reserve_tokens <= context_tokens,
                 "TOKEN_BUDGET_EXCEEDED")
    return tuple(chunks)


def _validate_output(output: SummaryOutput) -> None:
    _require(type(output) is SummaryOutput, "INVALID_OUTPUT")
    _require(type(output.summary) is str and 300 <= len(output.summary) <= 600
             and output.summary == output.summary.strip(), "INVALID_SUMMARY")
    _utf8(output.summary, "INVALID_OUTPUT_UNICODE")
    _require(type(output.uncertainties) is tuple
             and len(output.uncertainties) <= MAX_UNCERTAINTIES, "INVALID_UNCERTAINTIES")
    for item in output.uncertainties:
        _require(type(item) is str and 0 < len(item) <= MAX_UNCERTAINTY_CHARS
                 and item == item.strip(), "INVALID_UNCERTAINTIES")
        _utf8(item, "INVALID_OUTPUT_UNICODE")
    _require(type(output.spoiler_risk) is str
             and output.spoiler_risk in ("low", "medium", "high"), "INVALID_SPOILER_RISK")


def parse_summary_output(raw_json: str, *, max_response_bytes: int) -> SummaryOutput:
    """Accept exactly the example schema, without repair or markdown extraction."""
    _positive_bound(max_response_bytes, MAX_RESPONSE_BYTES, "INVALID_RESPONSE_BOUND")
    _require(type(raw_json) is str, "INVALID_RESPONSE_TYPE")
    _require(len(raw_json) <= max_response_bytes, "RESPONSE_TOO_LARGE")
    _require(len(_utf8(raw_json, "INVALID_RESPONSE_UNICODE")) <= max_response_bytes,
             "RESPONSE_TOO_LARGE")

    def unique_object(pairs: list[tuple[str, object]]) -> dict:
        result = {}
        for key, item in pairs:
            _require(key not in result, "DUPLICATE_JSON_KEY")
            result[key] = item
        return result

    def invalid_constant(value: str) -> None:
        raise SummaryError("NONFINITE_JSON")

    error = None
    value = None
    try:
        value = json.loads(raw_json, object_pairs_hook=unique_object,
                           parse_constant=invalid_constant)
    except SummaryError as exc:
        error = exc.code
    except (ValueError, TypeError, RecursionError):
        error = "INVALID_JSON"
    # Do not retain JSONDecodeError.doc or any raw parser exception context.
    _require(error is None, error)
    _require(type(value) is dict and set(value) == {"summary", "uncertainties", "spoiler_risk"},
             "INVALID_OUTPUT_SCHEMA")
    _require(type(value["summary"]) is str, "INVALID_SUMMARY")
    _require(type(value["uncertainties"]) is list
             and len(value["uncertainties"]) <= MAX_UNCERTAINTIES, "INVALID_UNCERTAINTIES")
    _require(all(type(item) is str for item in value["uncertainties"]),
             "INVALID_UNCERTAINTIES")
    output = SummaryOutput(value["summary"].strip(),
                           tuple(item.strip() for item in value["uncertainties"]),
                           value["spoiler_risk"])
    _validate_output(output)
    return output


def _normalized_rules(values: tuple[str, ...]) -> tuple[str, ...]:
    _require(type(values) is tuple and len(values) <= MAX_RULES, "INVALID_RULES")
    normalized = []
    for value in values:
        _require(type(value) is str and 0 < len(value) <= MAX_RULE_CHARS, "INVALID_RULES")
        _utf8(value, "INVALID_RULES")
        item = unicodedata.normalize("NFKC", value).casefold().strip()
        _require(bool(item), "INVALID_RULES")
        normalized.append(item)
    return tuple(normalized)


def assess_summary_rules(
    output: SummaryOutput, *, selection: SelectedInput,
    forbidden_phrases: tuple[str, ...], tail_only_entities: tuple[str, ...],
) -> RuleQAResult:
    """Conservative local evidence only; never produces publishable/passed=True.

    Tail-only evidence must be supplied by a trusted independent process; this
    function cannot establish that an entity is exclusive to the excluded tail.
    It checks both summary and uncertainty text. Missing rules are unavailable,
    and no-hit results still require review of grounding and spoiler semantics.
    Do not serialize this value directly into today's publication provenance.
    """
    _validate_selection(selection)
    _validate_output(output)
    phrases = _normalized_rules(forbidden_phrases)
    entities = _normalized_rules(tail_only_entities)
    texts = tuple(unicodedata.normalize("NFKC", item).casefold()
                  for item in (output.summary, *output.uncertainties))
    reasons = []
    reject = False
    if output.spoiler_risk == "high":
        reasons.append("HIGH_SPOILER_RISK")
        reject = True
    if any(rule in text for rule in phrases for text in texts):
        reasons.append("FORBIDDEN_PHRASE")
        reject = True
    if any(rule in text for rule in entities for text in texts):
        reasons.append("TAIL_ONLY_ENTITY")
        reject = True
    if output.spoiler_risk == "medium":
        reasons.append("MEDIUM_SPOILER_RISK")
    if output.uncertainties:
        reasons.append("UNCERTAINTIES")
    if not phrases:
        reasons.append("FORBIDDEN_PHRASES_UNAVAILABLE")
    if not entities:
        reasons.append("TAIL_ENTITIES_UNAVAILABLE")
    reasons.extend(("SEMANTIC_GROUNDING_NOT_VERIFIED", "SPOILER_SEMANTICS_NOT_VERIFIED"))
    return RuleQAResult("reject" if reject else "review_required", tuple(reasons),
                        True, True, bool(phrases), bool(entities))
