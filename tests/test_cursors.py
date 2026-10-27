"""Codec boundaries independent of library SQL and preview authorization."""
import base64
import unittest

from document_library.cursors import encode_cursor, decode_cursor, CursorError


def wire(raw):
    return base64.urlsafe_b64encode(raw.encode()).rstrip(b'=').decode()


class CursorAcceptance(unittest.TestCase):
    def test_canonical_roundtrip_returns_only_payload_and_kind_is_separate(self):
        payload=dict(query='f'*64,last=dict(null_rank=0,value=4.25,work_id='한글'))
        encoded=encode_cursor('library',payload)
        self.assertEqual(decode_cursor(encoded,expected_kind='library'),payload)
        self.assertEqual(encoded,encode_cursor('library',dict(reversed(list(payload.items())))))
        with self.assertRaises(CursorError):decode_cursor(encoded,expected_kind='preview')
        self.assertNotIn('=',encoded)

    def test_preview_namespace_is_state_without_credentials_or_byte_offset(self):
        payload=dict(revision_id='r',asset_position=0,source_sha256='a'*64,bundle_sha256='b'*64,ordinal=3)
        token=encode_cursor('preview',payload)
        self.assertEqual(decode_cursor(token,expected_kind='preview'),payload)
        # A different structurally valid ordinal is permitted state. Source
        # authorization and actual index bounds belong to the preview service.
        payload['ordinal']=4
        self.assertEqual(decode_cursor(encode_cursor('preview',payload),expected_kind='preview')['ordinal'],4)

    def test_rejects_duplicate_keys_noncanonical_or_wrong_envelope(self):
        for text in ('{"v":1,"v":1,"kind":"library","payload":{}}',
                     '{"v":true,"kind":"library","payload":{}}',
                     '{"v":2,"kind":"library","payload":{}}',
                     '{"kind":"library", "payload":{}, "v":1}',
                     '{"kind":"library","payload":[],"v":1}',
                     '{"kind":"library","payload":{},"v":1,"extra":0}', 'null'):
            with self.subTest(text=text),self.assertRaises(CursorError):
                decode_cursor(wire(text),expected_kind='library')

    def test_rejects_bad_base64_utf8_size_and_pad_bits(self):
        token=encode_cursor('library',{})
        for value in ('',token+'=',token+'!', 'a','x'*4097,base64.urlsafe_b64encode(b'\xff').rstrip(b'=').decode()):
            with self.subTest(value=value[:20]),self.assertRaises(CursorError):
                decode_cursor(value,expected_kind='library')

    def test_nested_node_integer_and_nonfinite_limits_are_real(self):
        deep={}
        for _ in range(10):deep={'x':deep}
        for payload in (deep,{'values':list(range(129))},{'x':float('nan')},{'x':float('inf')},
                        {'x':2**63},{'x':'x'*2049},{'x':'\ud800'}):
            with self.subTest(type=type(payload)),self.assertRaises(CursorError):encode_cursor('library',payload)
        for number in ('NaN','Infinity','1e999'):
            with self.assertRaises(CursorError):
                decode_cursor(wire('{"kind":"library","payload":{"x":'+number+'},"v":1}'),expected_kind='library')

    def test_encoder_requires_dict_and_bounded_namespace(self):
        for kind,payload in (('bad kind',{}),('A',{}),('library',[]),('x'*33,{})):
            with self.assertRaises(CursorError):encode_cursor(kind,payload)


if __name__=='__main__':unittest.main()
