import json
import unittest

from document_library import metadata_runtime


class OptionalCachedCountTests(unittest.TestCase):
    def _body(self, model, cached=None):
        payload = {
            "model": model,
            "response": "{}",
            "done": True,
            "done_reason": "stop",
            "prompt_eval_count": 4,
            "eval_count": 2,
        }
        if cached is not None:
            payload["prompt_eval_cached_count"] = cached
        return json.dumps(payload).encode("utf-8")

    def test_absent_zero_and_total_cached_counts_are_accepted(self):
        for cached in (None, 0, 4):
            parsed = metadata_runtime._decode_provider_terminal(
                self._body("m", cached), "m"
            )
            self.assertEqual(parsed["prompt_eval_count"], 4)

    def test_invalid_cached_counts_are_rejected(self):
        for cached in (True, False, -1, 5):
            with self.assertRaises(metadata_runtime._PayloadError):
                metadata_runtime._decode_provider_terminal(
                    self._body("m", cached), "m"
                )


if __name__ == "__main__":
    unittest.main()
import hashlib
import test_metadata_runtime as fixtures


class TransportFailureEventTests(unittest.TestCase):
    def setUp(self):
        self.case = fixtures.RuntimeTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)

    def _failed(self, reply):
        with fixtures.LoopbackServer(reply) as server:
            with self.assertRaises(metadata_runtime.RuntimeFailure) as caught:
                self.case.generate(self.case.adapter(server))
        self.assertEqual(len(server.requests), 1)
        path = next(self.case.events.glob("*.failed.json"))
        return caught.exception, json.loads(path.read_text()), path.read_text()

    def test_unknown_key_is_rejected_without_leaking_body(self):
        key, sentinel = "x_synthetic_unknown_key", "RAW-SENTINEL-9f3c"
        payload = self.case.provider()
        payload[key] = sentinel
        body = fixtures.encoded(payload)
        error, record, raw = self._failed(self.case.http(payload=body))
        self.assertEqual(error.code, "INVALID_PROVIDER_RESPONSE")
        self.assertEqual(error.diagnostic["stage"], "keys")
        self.assertEqual(error.diagnostic["reason"], "unknown_key")
        self.assertIs(error.diagnostic["body_complete"], True)
        self.assertEqual(error.diagnostic["response_sha256"], hashlib.sha256(body).hexdigest())
        self.assertEqual(error.diagnostic["response_bytes_len"], len(body))
        self.assertIsNone(error.stop_proof)
        self.assertEqual(record["state"], "FAILED")
        self.assertEqual(record["diagnostic"], error.diagnostic)
        self.assertNotIn(sentinel, str(error))
        self.assertNotIn(key, raw)
        self.assertNotIn(sentinel, raw)

    def test_text_plain_content_type_is_rejected_before_body(self):
        reply = self.case.http().replace(
            b"Content-Type: application/json", b"Content-Type: text/plain"
        )
        error, _, _ = self._failed(reply)
        self.assertEqual(error.code, "INVALID_PROVIDER_RESPONSE")
        self.assertEqual(error.diagnostic["stage"], "content_type")
        self.assertEqual(error.diagnostic["reason"], "invalid_content_type")
        self.assertIs(error.diagnostic["body_complete"], False)
        self.assertIsNone(error.diagnostic["response_sha256"])
        self.assertIsNone(error.diagnostic["response_bytes_len"])
        self.assertIsNone(error.stop_proof)

    def test_token_parity_mismatch_reports_completed_stop_proof(self):
        body = fixtures.encoded(self.case.provider(prompt_eval_count=5))
        error, _, _ = self._failed(self.case.http(payload=body))
        self.assertEqual(error.code, "TOKEN_PARITY_MISMATCH")
        proof = error.stop_proof
        self.assertIsNotNone(proof)
        self.assertIs(proof.terminal, True)
        self.assertIs(proof.fully_drained, True)
        self.assertEqual(proof.disposition, "completed")
