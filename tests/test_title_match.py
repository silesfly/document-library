"""Title identity policy, independent of file or work merge authorization."""

import unittest
import unicodedata

from document_library.catalog import title_match_key, titles_match


class TitleMatchAcceptance(unittest.TestCase):
    def test_separator_variants_share_one_key(self):
        for title in (
            "나의 소설 01", "나의_소설_01", "나의.소설.01", "나의소설01",
            "나의-소설(01)", "나의 / 소설 [01]", "나의\t소설\n01",
            "나의\u00a0소설\u300001", "나의\u200b소설✨01",
        ):
            with self.subTest(title=title):
                self.assertEqual(title_match_key(title), "나의소설01")
                self.assertTrue(titles_match(title, "나의소설01"))

    def test_case_width_and_decomposed_korean_are_normalized(self):
        self.assertTrue(titles_match("ＡＢＣ_소설_０１", "abc.소설.01"))
        self.assertTrue(titles_match(unicodedata.normalize("NFD", "한글 01"), "한글01"))
        self.assertEqual(title_match_key("ㄱ_ㄴ"), "ᄀᄂ")
        self.assertTrue(titles_match("ᄀ_ᅡ", "가"))
        self.assertEqual(title_match_key(title_match_key("ᄀ_ᅡ")), "가")

    def test_other_scripts_and_symbols_are_excluded(self):
        self.assertEqual(title_match_key("ABC漢字かなΩЖ١२한글_09"), "abc한글09")
        self.assertEqual(title_match_key("\u115f\u1160\u3164"), "")

    def test_empty_keys_do_not_create_duplicate_titles(self):
        for left, right in (("", ""), ("...", "___"), ("漢字", "かな"), (" ", "소설")):
            with self.subTest(left=left, right=right):
                self.assertFalse(titles_match(left, right))

    def test_digits_and_letters_remain_significant(self):
        for left, right in (("소설01", "소설02"), ("소설01", "소설1"),
                            ("소설A", "소설B"), ("소설가", "소설나")):
            with self.subTest(left=left, right=right):
                self.assertFalse(titles_match(left, right))

    def test_original_display_and_key_idempotence(self):
        title = "Ａ_B.한 글-01"
        key = title_match_key(title)
        self.assertEqual(title, "Ａ_B.한 글-01")
        self.assertEqual(key, "ab한글01")
        self.assertEqual(title_match_key(key), key)


if __name__ == "__main__":
    unittest.main()
