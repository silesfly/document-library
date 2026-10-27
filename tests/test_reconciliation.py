"""Independent decision assertions using actual parser reports from synthetic bytes."""
import unittest

from document_library.ingest_formats import inspect_bytes
from document_library.reconciliation import ReconciliationRequired, reconcile
from test_ingest_formats import archive_bytes, image_bytes


def chapters(first, last, *, changed=None, newline='\n'):
    return ''.join(f'{number}화{newline}{"changed" if number == changed else "body " + str(number)}{newline}'
                   for number in range(first, last + 1)).encode()


def parsed(first, last, **kwargs):
    return inspect_bytes(chapters(first, last, **kwargs), 'Synthetic.txt')


def old_option(report, ident='old', positions=None):
    return dict(id=ident, format=report['format'], delivery_kind='single_file', positions=positions or [0],
                coverage=report['metadata']['coverage'])


class ReconciliationDecisions(unittest.TestCase):
    def assert_review(self, reason, fresh, options, reports):
        with self.assertRaises(ReconciliationRequired) as caught:
            reconcile(fresh, options, reports)
        self.assertEqual(caught.exception.reason, reason)

    def test_all_150_overlap_hashes_must_match_for_160_replacement(self):
        old = parsed(1, 150)
        result = reconcile(parsed(1, 160), [old_option(old)], {0: old})
        self.assertEqual(result['outcome'], 'replace_superset')
        self.assertEqual(result['replaces'], ['old'])
        self.assertEqual(result['coverage']['intervals'], [[1, 160]])
        self.assert_review('overlap_conflict', parsed(1, 160, changed=77), [old_option(old)], {0: old})

    def test_151_and_152_flatten_existing_ordered_bundle(self):
        first, second = parsed(1, 150), parsed(151, 151)
        combined = reconcile(second, [old_option(first)], {0: first})
        self.assertEqual(combined['positions'], [0, 'new'])
        bundle = dict(old_option(first), delivery_kind='bundle', positions=[0, 1], coverage=combined['coverage'])
        result = reconcile(parsed(152, 152), [bundle], {0: first, 1: second})
        self.assertEqual(result['positions'], [0, 1, 'new'])
        self.assertEqual(result['coverage']['intervals'], [[1, 152]])

    def test_smaller_gap_and_partial_overlap_are_reviewed(self):
        old = parsed(1, 150)
        option = old_option(old)
        self.assert_review('smaller_coverage', parsed(1, 145), [option], {0: old})
        self.assert_review('partial_overlap', parsed(100, 160), [option], {0: old})
        self.assert_review('noncontiguous_or_overlapping_fragments', parsed(152, 152), [option], {0: old})
        gapped = inspect_bytes(chapters(1, 69) + chapters(71, 160), 'Synthetic.txt')
        self.assert_review('unverified_overlap', gapped, [option], {0: old})
        first = parsed(1, 50)
        self.assert_review('noncontiguous_or_overlapping_fragments', parsed(100, 150), [old_option(first)], {0: first})

    def test_filename_inference_is_not_overlap_authority(self):
        fresh = inspect_bytes(b'unknown contents', 'Book 1-160화.txt')
        old = parsed(1, 150)
        self.assert_review('unverified_overlap', fresh, [old_option(old)], {0: old})
        self.assertEqual(fresh['metadata']['coverage']['confidence'], 'inferred')

    def test_same_hash_fragment_duplicate_keeps_old_bundle(self):
        first, second = parsed(1, 150), parsed(151, 151)
        coverage = dict(unit='episode', intervals=[[1, 151]], gaps=[], confidence='verified')
        option = dict(old_option(first), delivery_kind='bundle', positions=[0, 1], coverage=coverage)
        self.assertEqual(reconcile(second, [option], {0: first, 1: second})['outcome'], 'exact_duplicate')

    def test_raw_sha_different_normalized_body_equal_preserves_both_for_review(self):
        old, fresh = parsed(1, 2), parsed(1, 2, newline='\r\n')
        self.assertNotEqual(old['sha256'], fresh['sha256'])
        self.assertEqual(old['metadata']['episode_hashes'], fresh['metadata']['episode_hashes'])
        self.assert_review('same_content_different_original', fresh, [old_option(old)], {0: old})

    def test_all_current_options_are_compared_before_exact_duplicate(self):
        old, conflict = parsed(1, 2), parsed(1, 2, changed=2)
        options = [old_option(old), old_option(conflict, 'parallel', [1])]
        self.assert_review('overlap_conflict', old, options, {0: old, 1: conflict})
        self.assert_review('overlap_conflict', parsed(1, 3), options, {0: old, 1: conflict})

    def test_multiple_verified_superset_targets_are_all_replaced(self):
        first, second = parsed(1, 2), parsed(1, 3)
        result = reconcile(parsed(1, 4), [old_option(first), old_option(second, 'second', [1])], {0: first, 1: second})
        self.assertEqual(result['replaces'], ['old', 'second'])

    def test_other_format_never_replaces_old_options(self):
        old = parsed(1, 2)
        fresh = inspect_bytes('<h1>1화</h1><p>body</p>'.encode(), 'Book.html')
        result = reconcile(fresh, [old_option(old)], {})
        self.assertEqual(result['outcome'], 'add_format')
        self.assertEqual(result['replaces'], [])

    def test_volume_and_episode_units_cannot_be_composed(self):
        old = parsed(1, 2)
        fresh = inspect_bytes('3권\nbody\n'.encode(), 'Book.txt')
        self.assert_review('coverage_unit_mismatch', fresh, [old_option(old)], {0: old})

    def test_image_container_logical_duplicate_uses_ordered_original_page_sha(self):
        red, blue = image_bytes(size=(3, 2)), image_bytes(size=(4, 2))
        old = inspect_bytes(archive_bytes([('1.png', red), ('2.png', blue)]), 'Book.cbz')
        renamed = inspect_bytes(archive_bytes([('002.png', blue), ('001.png', red)]), 'Book.cbz')
        reordered = inspect_bytes(archive_bytes([('001.png', blue), ('002.png', red)]), 'Book.cbz')
        other_format = inspect_bytes(archive_bytes([('1.png', red), ('2.png', blue)]), 'Book.zip')
        self.assertEqual(reconcile(other_format, [old_option(old)], {})['outcome'], 'add_format')
        self.assertNotEqual(old['sha256'], renamed['sha256'])
        self.assert_review('logical_image_duplicate', renamed, [old_option(old)], {0: old})
        self.assert_review('image_overlap_unverified', reordered, [old_option(old)], {0: old})


if __name__ == '__main__':
    unittest.main()
