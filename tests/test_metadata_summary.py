"""Synthetic contract tests for the pure G4 preparation kernel only."""
from dataclasses import FrozenInstanceError, replace
from hashlib import sha256
import json
import unittest

from document_library.metadata_summary import (
    ChapterSpan, MAX_CHUNK_CHARS, MAX_CHUNKS, MAX_CONTEXT_TOKENS,
    MAX_COUNTER_CALLS, MAX_RESPONSE_BYTES, MAX_RULES, MAX_SOURCE_CHARS,
    RuleQAResult, SelectedInput, SourceRange, SummaryError, SummaryOutput,
    assess_summary_rules, chunk_summary_input, parse_summary_output,
    select_summary_input,
)


class SummaryTestCase(unittest.TestCase):
    def select(self, text="abcdefgh", **changes):
        options = dict(source_revision_id="revision-1", document_kind="novel",
                       chapters=None, max_source_chars=100_000, source_complete=True,
                       expected_source_chars=len(text),
                       expected_source_sha256=sha256(text.encode("utf-8")).hexdigest())
        options.update(changes)
        return select_summary_input(text, **options)

    def assert_code(self, code, action):
        with self.assertRaises(SummaryError) as caught:
            action()
        self.assertEqual(caught.exception.code, code)
        self.assertEqual(str(caught.exception), code)
        self.assertIsNone(caught.exception.__context__)
        self.assertIsNone(caught.exception.__cause__)

    def output(self, **changes):
        value = dict(summary="가" * 300, uncertainties=[], spoiler_risk="low")
        value.update(changes)
        return parse_summary_output(json.dumps(value, ensure_ascii=False),
                                    max_response_bytes=MAX_RESPONSE_BYTES)


class SourceSelectionTests(SummaryTestCase):
    def test_length_cutoff_exact_codepoint_partition(self):
        for length in range(1, 22):
            text = ("甲😀e\u0301Z" * 5)[:length]
            with self.subTest(length=length):
                if length == 1:
                    self.assert_code("INSUFFICIENT_INPUT", lambda: self.select(text))
                    continue
                selected = self.select(text)
                boundary = 3 * length // 4
                self.assertEqual(selected.text, text[:boundary])
                self.assertEqual(selected.selected_ranges, (SourceRange(0, boundary),))
                self.assertEqual(selected.excluded_tail_range, SourceRange(boundary, length))
                self.assertEqual(selected.text + text[boundary:], text)
                self.assertEqual(selected.source_char_count, length)
                self.assertEqual(selected.source_sha256, sha256(text.encode()).hexdigest())
                self.assertEqual(selected.selection_mode, "novel_length")

    def test_tail_sentinel_excluded_and_document_end_retained(self):
        text = "前文😀" * 12 + "TAIL"
        novel = self.select(text)
        self.assertNotIn("TAIL", novel.text)
        document = self.select(text, document_kind="document")
        self.assertEqual(document.text, text)
        self.assertTrue(document.text.endswith("TAIL"))
        self.assertEqual(document.excluded_tail_range, SourceRange(len(text), len(text)))
        self.assertEqual(document.selected_ranges, (SourceRange(0, len(text)),))
        self.assertEqual(self.select("😀", document_kind="document").text, "😀")

    def test_chapter_counts_one_to_nine_varying_lengths_and_nonzero_ordinals(self):
        for count in range(1, 10):
            parts = ["😀章" * (index + 1) for index in range(count)]
            chapters = []
            offset = 0
            for index, part in enumerate(parts):
                chapters.append(ChapterSpan(index + 11, offset, offset + len(part)))
                offset += len(part)
            text = "".join(parts)
            original = tuple(chapters)
            with self.subTest(count=count):
                if count == 1:
                    self.assert_code("INSUFFICIENT_INPUT",
                                     lambda: self.select(text, chapters=original))
                else:
                    selected = self.select(text, chapters=original)
                    retained = 3 * count // 4
                    end = sum(map(len, parts[:retained]))
                    self.assertEqual(selected.text, "".join(parts[:retained]))
                    self.assertEqual(selected.selected_ranges,
                                     tuple(SourceRange(c.start, c.end) for c in original[:retained]))
                    self.assertEqual(selected.excluded_tail_range, SourceRange(end, len(text)))
                    self.assertEqual(selected.selection_mode, "novel_chapters")
                self.assertEqual(tuple(chapters), original)
                self.assertEqual(self.select(text, chapters=original, document_kind="document").text,
                                 text)

    def test_whole_large_chapter_and_repeated_source_offsets(self):
        text = "same" * 4
        spans = tuple(ChapterSpan(i + 1, 4 * i, 4 * (i + 1)) for i in range(4))
        selected = self.select(text, chapters=spans)
        self.assertEqual(selected.selected_ranges, (SourceRange(0, 4), SourceRange(4, 8),
                                                    SourceRange(8, 12)))
        self.assertEqual(selected.text, "same" * 3)
        unequal = (ChapterSpan(1, 0, 91), ChapterSpan(2, 91, 94),
                   ChapterSpan(3, 94, 97), ChapterSpan(4, 97, 100))
        selected = self.select("a" * 100, chapters=unequal)
        self.assertEqual(len(selected.text), 97)  # chapter policy, not char fallback

    def test_invalid_chapter_evidence_never_silently_falls_back(self):
        invalid = [(), [], (object(),), (ChapterSpan(1, 0, 8), ChapterSpan(2, 7, 16)),
                   (ChapterSpan(1, 0, 7), ChapterSpan(2, 8, 16)),
                   (ChapterSpan(1, 1, 8), ChapterSpan(2, 8, 16)),
                   (ChapterSpan(1, 0, 8), ChapterSpan(2, 8, 15)),
                   (ChapterSpan(1, 0, 8), ChapterSpan(2, 8, 17)),
                   (ChapterSpan(1, 0, 8), ChapterSpan(1, 8, 16)),
                   (ChapterSpan(1, 0, 8), ChapterSpan(3, 8, 16)),
                   (ChapterSpan(2, 8, 16), ChapterSpan(1, 0, 8)),
                   (ChapterSpan(True, 0, 8), ChapterSpan(2, 8, 16)),
                   (ChapterSpan(0, 0, 8), ChapterSpan(1, 8, 16)),
                   (ChapterSpan(1, False, 8), ChapterSpan(2, 8, 16)),
                   (ChapterSpan(1, 0, 8), ChapterSpan(2, 8, True)),
                   (ChapterSpan(1, 0.0, 8), ChapterSpan(2, 8, 16)),
                   (ChapterSpan(1, 0, 0), ChapterSpan(2, 0, 16))]
        for spans in invalid:
            for kind in ("novel", "document"):
                with self.subTest(spans=spans, kind=kind):
                    self.assert_code("INVALID_CHAPTERS", lambda: self.select(
                        "x" * 16, chapters=spans, document_kind=kind))

    def test_complete_source_binding_is_required(self):
        for attestation in (False, 1, "yes", None):
            self.assert_code("SOURCE_NOT_COMPLETE",
                             lambda: self.select(source_complete=attestation))
        for count in (True, 0, -1, 7, 9, 8.0):
            self.assert_code("SOURCE_LENGTH_MISMATCH",
                             lambda: self.select(expected_source_chars=count))
        self.assert_code("SOURCE_DIGEST_MISMATCH",
                         lambda: self.select(expected_source_sha256="0" * 64))
        for digest in ("x" * 64, "A" * 64, "0" * 63, 42, None):
            self.assert_code("INVALID_SOURCE_DIGEST",
                             lambda: self.select(expected_source_sha256=digest))
        full = "original source with ending"
        excerpt = full[:8]
        self.assert_code("SOURCE_LENGTH_MISMATCH", lambda: self.select(
            excerpt, expected_source_chars=len(full),
            expected_source_sha256=sha256(full.encode()).hexdigest()))
        # A falsely attested excerpt cannot be detected without an independent manifest.
        self.assertEqual(self.select(excerpt).source_char_count, len(excerpt))

    def test_revision_and_unnormalized_string_bindings(self):
        first = self.select("e\u0301😀ABCD")
        other_revision = self.select("e\u0301😀ABCD", source_revision_id="revision-2")
        normalized = self.select("é😀ABCD")
        self.assertEqual(first.source_sha256, other_revision.source_sha256)
        self.assertNotEqual(first.source_revision_id, other_revision.source_revision_id)
        self.assertNotEqual(first.source_sha256, normalized.source_sha256)
        self.assertEqual(first.text, "e\u0301😀AB")
        with self.assertRaises(FrozenInstanceError):
            first.text = "changed"

    def test_source_bounds_types_and_invalid_surrogates(self):
        for bound in (True, 0, -1, 1.5, MAX_SOURCE_CHARS + 1):
            self.assert_code("INVALID_SOURCE_BOUND", lambda: self.select(max_source_chars=bound))
        self.assert_code("SOURCE_TOO_LARGE", lambda: self.select(max_source_chars=7))
        self.assert_code("INVALID_SOURCE_TEXT", lambda: self.select(""))
        self.assert_code("INVALID_SOURCE_TEXT", lambda: select_summary_input(
            b"abcdefgh", source_revision_id="r", document_kind="novel", chapters=None,
            max_source_chars=20, source_complete=True, expected_source_chars=8,
            expected_source_sha256="0" * 64))
        for kind in ("comic", True, None):
            self.assert_code("INVALID_DOCUMENT_KIND", lambda: self.select(document_kind=kind))
        for revision in ("", "  ", "r" * 257, True, "\ud800"):
            self.assert_code("INVALID_SOURCE_REVISION",
                             lambda: self.select(source_revision_id=revision))
        for text in ("abcd\ud800", "abcd\udfff", "\ud83d\ude00abcd"):
            self.assert_code("INVALID_SOURCE_UNICODE", lambda: select_summary_input(
                text, source_revision_id="r", document_kind="novel", chapters=None,
                max_source_chars=20, source_complete=True, expected_source_chars=len(text),
                expected_source_sha256="0" * 64))


class ChunkTests(SummaryTestCase):
    def chunks(self, selection=None, **changes):
        if selection is None:
            selection = self.select("abcdefgh", document_kind="document")
        options = dict(count_request_tokens=lambda text: len(text) + 2,
                       context_tokens=7, output_reserve_tokens=2,
                       max_chunk_chars=8, max_chunks=20, max_counter_calls=100)
        options.update(changes)
        return chunk_summary_input(selection, **options)

    def test_exact_overhead_reserve_and_final_recount(self):
        selection = self.select("甲😀乙\u0301abcdTAIL")
        seen = []
        def counter(text):
            seen.append(text)
            self.assertNotIn("TAIL", text)
            return 2 + len(text)
        chunks = self.chunks(selection, count_request_tokens=counter)
        self.assertEqual("".join(c.text for c in chunks), selection.text)
        self.assertEqual(chunks[0].start, 0)
        self.assertEqual(chunks[-1].end, selection.excluded_tail_range.start)
        self.assertTrue(all(a.end == b.start for a, b in zip(chunks, chunks[1:])))
        for chunk in chunks:
            self.assertEqual(chunk.text, selection.text[chunk.start:chunk.end])
            self.assertEqual(chunk.source_revision_id, selection.source_revision_id)
            self.assertEqual(chunk.source_sha256, selection.source_sha256)
            self.assertEqual(chunk.request_tokens, len(chunk.text) + 2)
            self.assertLessEqual(chunk.request_tokens + 2, 7)
        self.assertEqual(seen[-len(chunks):], [c.text for c in chunks])
        self.assertLessEqual(len(seen), 100)

    def test_nonmonotonic_counter_uses_exact_candidates(self):
        seen = []
        def counter(text):
            seen.append(text)
            return {1: 9, 2: 1, 3: 9, 4: 1}[len(text)]
        chunks = self.chunks(self.select("abcd", document_kind="document"),
                             count_request_tokens=counter, context_tokens=2,
                             output_reserve_tokens=1, max_chunk_chars=4,
                             max_counter_calls=2)
        self.assertEqual([c.text for c in chunks], ["abcd"])
        self.assertEqual(seen, ["abcd", "abcd"])

    def test_search_does_not_skip_fit_between_nonmonotone_failures(self):
        seen = []
        def counter(text):
            seen.append(text)
            return 99 if len(text) in (1, 3, 4) else 1
        chunks = self.chunks(self.select("abcd", document_kind="document"),
                             count_request_tokens=counter, context_tokens=2,
                             output_reserve_tokens=1, max_chunk_chars=4)
        self.assertEqual([c.text for c in chunks], ["ab", "cd"])
        self.assertEqual(seen, ["abcd", "abc", "ab", "cd", "ab", "cd"])

    def test_single_character_over_budget_and_empty_progress_forbidden(self):
        seen = []
        def counter(text):
            seen.append(text)
            return 100
        self.assert_code("TOKEN_BUDGET_EXCEEDED", lambda: self.chunks(
            self.select("😀", document_kind="document"), count_request_tokens=counter))
        self.assertEqual(seen, ["😀"])

    def test_counter_return_types_exceptions_and_unavailable(self):
        for value in (True, False, -1, 1.0, float("nan"), float("inf"), "2", None):
            self.assert_code("INVALID_COUNTER_VALUE", lambda: self.chunks(
                count_request_tokens=lambda text: value))
        def failing(text):
            raise ValueError("PRIVATE_BODY=" + text)
        self.assert_code("COUNTER_FAILED", lambda: self.chunks(count_request_tokens=failing))
        self.assert_code("INVALID_COUNTER", lambda: self.chunks(count_request_tokens=None))

    def test_final_recount_must_be_stable_even_if_still_in_budget(self):
        for changed in (1, 100):
            calls = []
            def counter(text):
                calls.append(text)
                return 0 if len(calls) == 1 else changed
            self.assert_code("COUNTER_UNSTABLE", lambda: self.chunks(
                count_request_tokens=counter, context_tokens=10, output_reserve_tokens=0))
            self.assertEqual(len(calls), 2)

    def test_global_call_cap_includes_final_pass_and_no_partial_return(self):
        for cap in (1, 2, 3):
            seen = []
            def counter(text):
                seen.append(text)
                return 1
            self.assert_code("COUNTER_CALL_LIMIT", lambda: self.chunks(
                count_request_tokens=counter, max_chunk_chars=4, max_counter_calls=cap))
            self.assertEqual(len(seen), cap)
        seen = []
        chunks = self.chunks(count_request_tokens=lambda text: seen.append(text) or 1,
                             max_chunk_chars=4, max_counter_calls=4)
        self.assertEqual(len(chunks), 2)
        self.assertEqual(len(seen), 4)

    def test_search_call_exhaustion_and_chunk_count_bound(self):
        seen = []
        self.assert_code("COUNTER_CALL_LIMIT", lambda: self.chunks(
            count_request_tokens=lambda text: seen.append(text) or 100,
            max_counter_calls=3))
        self.assertEqual(len(seen), 3)
        seen = []
        self.assert_code("CHUNK_COUNT_LIMIT", lambda: self.chunks(
            count_request_tokens=lambda text: seen.append(text) or 1,
            max_chunk_chars=4, max_chunks=1))
        self.assertEqual(seen, ["abcd"])

    def test_chunk_configuration_rejects_booleans_and_hard_cap_overruns(self):
        bounds = {"context_tokens": MAX_CONTEXT_TOKENS, "max_chunk_chars": MAX_CHUNK_CHARS,
                  "max_chunks": MAX_CHUNKS, "max_counter_calls": MAX_COUNTER_CALLS}
        errors = {"context_tokens": "INVALID_CONTEXT_BOUND", "max_chunk_chars": "INVALID_CHUNK_BOUND",
                  "max_chunks": "INVALID_CHUNK_COUNT_BOUND", "max_counter_calls": "INVALID_COUNTER_CALL_BOUND"}
        for key, cap in bounds.items():
            for value in (True, 0, -1, 1.0, cap + 1):
                with self.subTest(key=key, value=value):
                    self.assert_code(errors[key], lambda: self.chunks(**{key: value}))
        for reserve in (True, -1, 7, 8, 1.0):
            self.assert_code("INVALID_OUTPUT_RESERVE", lambda: self.chunks(output_reserve_tokens=reserve))

    def test_invalid_selected_shape_never_reaches_counter(self):
        selection = self.select()
        invalid = [None, replace(selection, text=""), replace(selection, source_char_count=True),
                   replace(selection, selected_ranges=(SourceRange(1, 6),)),
                   replace(selection, selected_ranges=(SourceRange(False, 6),)),
                   replace(selection, selected_ranges=(SourceRange(0, 3), SourceRange(2, 6))),
                   replace(selection, excluded_tail_range=SourceRange(5, 8)),
                   replace(selection, text="a" * 5), replace(selection, source_sha256="bad"),
                   replace(selection, selection_mode="document")]
        for item in invalid:
            seen = []
            with self.subTest(item=item):
                self.assert_code("INVALID_SELECTION", lambda: chunk_summary_input(
                    item, count_request_tokens=lambda text: seen.append(text) or 0,
                    context_tokens=5, output_reserve_tokens=1, max_chunk_chars=5,
                    max_chunks=10, max_counter_calls=100))
                self.assertEqual(seen, [])


class OutputParserTests(SummaryTestCase):
    def test_summary_lengths_trimmed_codepoints_and_surrogate_pair_json(self):
        for length in (299, 300, 600, 601):
            if 300 <= length <= 600:
                self.assertEqual(len(self.output(summary="😀" * length).summary), length)
            else:
                self.assert_code("INVALID_SUMMARY", lambda: self.output(summary="😀" * length))
        output = self.output(summary="  " + "甲" * 300 + " ", uncertainties=["  不明  "])
        self.assertEqual(output.summary, "甲" * 300)
        self.assertEqual(output.uncertainties, ("不明",))
        raw = json.dumps(dict(summary="😀" * 300, uncertainties=[], spoiler_risk="low"))
        self.assertEqual(parse_summary_output(raw, max_response_bytes=len(raw)).summary, "😀" * 300)

    def test_duplicate_unknown_missing_nonobject_and_malformed_json(self):
        summary = json.dumps("a" * 300)
        cases = [("DUPLICATE_JSON_KEY", '{"summary":' + summary + ',"summary":' + summary +
                  ',"uncertainties":[],"spoiler_risk":"low"}'),
                 ("DUPLICATE_JSON_KEY", '{"summary":' + summary +
                  ',"uncertainties":[{"x":1,"x":2}],"spoiler_risk":"low"}'),
                 ("INVALID_OUTPUT_SCHEMA", "[]"), ("INVALID_OUTPUT_SCHEMA", "null"),
                 ("INVALID_OUTPUT_SCHEMA", "42"), ("INVALID_OUTPUT_SCHEMA", "{}"),
                 ("INVALID_JSON", "```json\n{}\n```"), ("INVALID_JSON", '{"PRIVATE_BODY":'),
                 ("INVALID_JSON", "{} {}")]
        for code, raw in cases:
            self.assert_code(code, lambda: parse_summary_output(raw, max_response_bytes=MAX_RESPONSE_BYTES))
        self.assert_code("INVALID_OUTPUT_SCHEMA", lambda: self.output(extra="private"))
        raw = json.dumps(dict(summary="a" * 300, uncertainties=[]))
        self.assert_code("INVALID_OUTPUT_SCHEMA", lambda: parse_summary_output(raw,
                         max_response_bytes=MAX_RESPONSE_BYTES))
        # Deep non-object input may hit decoder depth limits or reach schema
        # rejection, depending on Python's decoder; neither path leaks an error.
        with self.assertRaises(SummaryError) as caught:
            parse_summary_output("[" * 1500 + "]" * 1500,
                                 max_response_bytes=MAX_RESPONSE_BYTES)
        self.assertIn(caught.exception.code, ("INVALID_JSON", "INVALID_OUTPUT_SCHEMA"))
        self.assertIsNone(caught.exception.__context__)

    def test_nonfinite_constants_and_field_types(self):
        for constant in ("NaN", "Infinity", "-Infinity"):
            raw = '{"summary":' + constant + ',"uncertainties":[],"spoiler_risk":"low"}'
            self.assert_code("NONFINITE_JSON", lambda: parse_summary_output(raw,
                             max_response_bytes=MAX_RESPONSE_BYTES))
        for summary in (True, 300, None, ["a" * 300], " " * 300):
            self.assert_code("INVALID_SUMMARY", lambda: self.output(summary=summary))
        for risk in (True, 0, None, "LOW", "unknown", ["low"]):
            self.assert_code("INVALID_SPOILER_RISK", lambda: self.output(spoiler_risk=risk))
        for items in (True, {}, "text", [False], [None], [1], [""], [" "], ["a" * 257], ["a"] * 17):
            self.assert_code("INVALID_UNCERTAINTIES", lambda: self.output(uncertainties=items))
        self.assertEqual(len(self.output(uncertainties=["a" * 256] * 16).uncertainties), 16)

    def test_encoded_byte_cap_exact_and_hard_bounds(self):
        raw = json.dumps(dict(summary="😀" * 300, uncertainties=[], spoiler_risk="low"),
                         ensure_ascii=False)
        byte_length = len(raw.encode())
        self.assertGreater(byte_length, len(raw))
        self.assertEqual(len(parse_summary_output(raw, max_response_bytes=byte_length).summary), 300)
        self.assert_code("RESPONSE_TOO_LARGE", lambda: parse_summary_output(
            raw, max_response_bytes=byte_length - 1))
        self.assert_code("RESPONSE_TOO_LARGE", lambda: parse_summary_output(
            "x" * (MAX_RESPONSE_BYTES + 1), max_response_bytes=MAX_RESPONSE_BYTES))
        for bound in (True, 0, -1, 1.0, MAX_RESPONSE_BYTES + 1):
            self.assert_code("INVALID_RESPONSE_BOUND", lambda: parse_summary_output(raw,
                             max_response_bytes=bound))
        self.assert_code("INVALID_RESPONSE_TYPE", lambda: parse_summary_output(b"{}",
                         max_response_bytes=100))

    def test_invalid_unicode_in_raw_and_decoded_fields_never_repaired(self):
        self.assert_code("INVALID_RESPONSE_UNICODE", lambda: parse_summary_output(
            "{\ud800}", max_response_bytes=100))
        for value in ("\ud800" + "a" * 299, "\udfff" + "a" * 299):
            raw = json.dumps(dict(summary=value, uncertainties=[], spoiler_risk="low"),
                             ensure_ascii=True)
            self.assert_code("INVALID_OUTPUT_UNICODE", lambda: parse_summary_output(
                raw, max_response_bytes=MAX_RESPONSE_BYTES))
        raw = json.dumps(dict(summary="a" * 300, uncertainties=["\ud800"],
                              spoiler_risk="low"), ensure_ascii=True)
        self.assert_code("INVALID_OUTPUT_UNICODE", lambda: parse_summary_output(
            raw, max_response_bytes=MAX_RESPONSE_BYTES))


class RuleQATests(SummaryTestCase):
    def assess(self, output=None, **changes):
        options = dict(selection=self.select(), forbidden_phrases=(), tail_only_entities=())
        options.update(changes)
        return assess_summary_rules(self.output() if output is None else output, **options)

    def test_low_risk_no_hits_never_becomes_publishable_pass(self):
        for phrases, entities in (((), ()), (("forbidden",), ("tailperson",))):
            result = self.assess(forbidden_phrases=phrases, tail_only_entities=entities)
            self.assertEqual(result.decision, "review_required")
            self.assertEqual(result.semantic_grounding_status, "NOT_VERIFIED")
            self.assertEqual(result.spoiler_semantics_status, "NOT_VERIFIED")
            self.assertIn("SEMANTIC_GROUNDING_NOT_VERIFIED", result.reason_codes)
            self.assertIn("SPOILER_SEMANTICS_NOT_VERIFIED", result.reason_codes)
            self.assertFalse(hasattr(result, "passed"))
            self.assertEqual(result.forbidden_phrases_checked, bool(phrases))
            self.assertEqual(result.tail_only_entities_checked, bool(entities))
            self.assertTrue(result.output_schema_checked)
            self.assertTrue(result.selected_input_checked)
        self.assertIn("FORBIDDEN_PHRASES_UNAVAILABLE", self.assess().reason_codes)
        self.assertIn("TAIL_ENTITIES_UNAVAILABLE", self.assess().reason_codes)

    def test_high_rejects_medium_and_uncertainties_require_review(self):
        result = self.assess(self.output(spoiler_risk="high"))
        self.assertEqual(result.decision, "reject")
        self.assertIn("HIGH_SPOILER_RISK", result.reason_codes)
        for changes, reason in ((dict(spoiler_risk="medium"), "MEDIUM_SPOILER_RISK"),
                                (dict(uncertainties=["missing evidence"]), "UNCERTAINTIES")):
            result = self.assess(self.output(**changes))
            self.assertEqual(result.decision, "review_required")
            self.assertIn(reason, result.reason_codes)

    def test_nfkc_casefold_forbidden_and_tail_only_hits_are_stable(self):
        output = self.output(summary="ＦＩＮＡＬ Straße 破局😀" + "甲" * 300)
        result = self.assess(output, forbidden_phrases=("final", "STRASSE"),
                             tail_only_entities=("破局😀",))
        self.assertEqual(result.decision, "reject")
        self.assertEqual(result.reason_codes.count("FORBIDDEN_PHRASE"), 1)
        self.assertEqual(result.reason_codes.count("TAIL_ONLY_ENTITY"), 1)
        self.assertLessEqual(len(result.reason_codes), 9)
        # The selected source remains exact; comparison normalization changes no offsets.
        selected = self.select("e\u0301😀ABCD")
        self.assertEqual(selected.selected_ranges, (SourceRange(0, 5),))

    def test_rule_checks_include_uncertainties_without_cross_field_phrases(self):
        output = self.output(uncertainties=["ＦＩＮＡＬ SecretEnding"])
        result = self.assess(output, forbidden_phrases=("final",),
                             tail_only_entities=("secretending",))
        self.assertEqual(result.decision, "reject")
        self.assertIn("FORBIDDEN_PHRASE", result.reason_codes)
        separate = self.output(summary="甲" * 298 + "ab", uncertainties=["cd"])
        self.assertEqual(self.assess(separate, forbidden_phrases=("abcd",)).decision,
                         "review_required")

    def test_qa_evidence_is_bounded_and_invalid_output_cannot_bypass_parser(self):
        for rules in ([], "term", (True,), ("",), ("   ",), ("x" * 257,),
                      ("x",) * (MAX_RULES + 1), ("\ud800",)):
            for key in ("forbidden_phrases", "tail_only_entities"):
                self.assert_code("INVALID_RULES", lambda: self.assess(**{key: rules}))
        result = self.assess(forbidden_phrases=("absent",) * MAX_RULES)
        self.assertEqual(result.decision, "review_required")
        self.assert_code("INVALID_SUMMARY", lambda: self.assess(
            SummaryOutput("short", (), "low")))
        self.assert_code("INVALID_UNCERTAINTIES", lambda: self.assess(
            SummaryOutput("a" * 300, [], "low")))
        self.assert_code("INVALID_SELECTION", lambda: self.assess(selection=None))

    def test_raw_body_absent_from_value_repr_and_error_details(self):
        marker = "PRIVATE_BODY_0123456789"
        selection = self.select(marker * 20, document_kind="document")
        chunks = chunk_summary_input(selection, count_request_tokens=lambda text: len(text),
            context_tokens=1000, output_reserve_tokens=1, max_chunk_chars=1000,
            max_chunks=10, max_counter_calls=10)
        output = self.output(summary=marker * 15, uncertainties=[marker])
        result = self.assess(output, forbidden_phrases=(marker,))
        for value in (selection, *chunks, output, result):
            self.assertNotIn(marker, repr(value))
        self.assert_code("INVALID_JSON", lambda: parse_summary_output(
            marker, max_response_bytes=1000))
        def error_counter(text):
            raise RuntimeError(marker + text)
        self.assert_code("COUNTER_FAILED", lambda: chunk_summary_input(selection,
            count_request_tokens=error_counter, context_tokens=1000, output_reserve_tokens=1,
            max_chunk_chars=1000, max_chunks=10, max_counter_calls=10))


if __name__ == "__main__":
    unittest.main()
