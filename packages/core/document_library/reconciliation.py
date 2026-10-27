"""Conservative option decisions inside one explicitly selected edition.

Reports come from bounded inspection of actual originals, never preview caches.
This module neither infers edition identity nor grants publication authority.
"""
import hashlib
import json


class ReconciliationRequired(ValueError):
    def __init__(self, reason):
        self.reason = reason
        super().__init__(reason)


def report_digest(value):
    encoded = json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)
    return hashlib.sha256(encoded.encode()).hexdigest()


def _episodes(report):
    metadata = report['metadata']
    coverage = metadata['coverage']
    hashes = metadata['episode_hashes']
    if (report['status'] != 'validated' or metadata.get('review_reason')
            or coverage['confidence'] != 'verified' or coverage['unit'] not in {'episode', 'volume'}
            or coverage['gaps'] or len(coverage['intervals']) != 1 or not hashes):
        raise ReconciliationRequired('unverified_overlap')
    numbers = sorted(int(key) for key in hashes)
    start, end = coverage['intervals'][0]
    if len(numbers) != end - start + 1 or numbers[0] != start or numbers[-1] != end:
        raise ReconciliationRequired('unverified_overlap')
    return coverage['unit'], {number: hashes[str(number)] for number in numbers}


def _option_episodes(option, reports):
    combined, unit, end = {}, None, None
    for position in option['positions']:
        current_unit, hashes = _episodes(reports[position])
        start = min(hashes)
        if (unit is not None and current_unit != unit) or (end is not None and start != end + 1):
            raise ReconciliationRequired('existing_bundle_not_contiguous')
        combined.update(hashes)
        unit, end = current_unit, max(hashes)
    coverage = dict(unit=unit, intervals=[[min(combined), max(combined)]], gaps=[], confidence='verified')
    if coverage != option['coverage']:
        raise ReconciliationRequired('existing_coverage_not_proven')
    return unit, combined


def reconcile(new, options, reports):
    """Return one bounded decision, or a typed human-review reason.

    Positions refer to the caller's old asset snapshot; 'new' is the new raw
    original. Superset comparisons include EVERY current same-format option.
    """
    same = [option for option in options if option['format'] == new['format']]
    base = dict(positions=['new'], coverage=new['metadata']['coverage'], replaces=[])
    if not same:
        return dict(base, outcome='add_format' if options else 'initial')
    positions = {p for option in same for p in option['positions']}
    exact = [p for p in positions if reports[p]['sha256'] == new['sha256']]
    if new['representation'] == 'imagepack':
        pages = [page['sha256'] for page in new.get('pages', [])]
        if exact and all(reports[p]['sha256'] == new['sha256'] for p in positions):
            return dict(base, outcome='exact_duplicate')
        if pages and any(pages == [page['sha256'] for page in reports[p].get('pages', [])] for p in positions):
            raise ReconciliationRequired('logical_image_duplicate')
        raise ReconciliationRequired('image_overlap_unverified')
    if exact and all(reports[p]['sha256'] == new['sha256'] for p in positions):
        # Identical bytes can also have unknown/gapped coverage. No option is
        # created or replaced, so inferred metadata cannot enlarge coverage.
        return dict(base, outcome='exact_duplicate')
    unit, fresh = _episodes(new)
    old = []
    for option in same:
        old_unit, hashes = _option_episodes(option, reports)
        if old_unit != unit:
            raise ReconciliationRequired('coverage_unit_mismatch')
        if any(fresh[n] != hashes[n] for n in fresh.keys() & hashes.keys()):
            raise ReconciliationRequired('overlap_conflict')
        old.append((option, hashes))
    # Matching one fragment cannot conceal conflicting parallel old options.
    for index, (_option, left) in enumerate(old):
        for _other, right in old[index + 1:]:
            if any(left[n] != right[n] for n in left.keys() & right.keys()):
                raise ReconciliationRequired('existing_options_conflict')
    if exact:
        return dict(base, outcome='exact_duplicate')
    old_numbers = set().union(*(set(hashes) for _option, hashes in old))
    if old_numbers <= fresh.keys():
        if old_numbers == fresh.keys():
            raise ReconciliationRequired('same_content_different_original')
        return dict(base, outcome='replace_superset', replaces=[o['id'] for o, _h in old])
    if fresh.keys() <= old_numbers:
        raise ReconciliationRequired('smaller_coverage')
    if fresh.keys() & old_numbers:
        raise ReconciliationRequired('partial_overlap')
    # Raw fragments must be disjoint and ordered; do not silently slice overlap.
    fragments = []
    seen = set()
    for option, _hashes in old:
        for position in option['positions']:
            if position not in seen:
                seen.add(position)
                _unit, hashes = _episodes(reports[position])
                fragments.append((min(hashes), max(hashes), position))
    fragments.append((min(fresh), max(fresh), 'new'))
    fragments.sort(key=lambda member: member[0])
    if any(right[0] != left[1] + 1 for left, right in zip(fragments, fragments[1:])):
        raise ReconciliationRequired('noncontiguous_or_overlapping_fragments')
    return dict(outcome='append_bundle', positions=[member[2] for member in fragments],
                coverage=dict(unit=unit, intervals=[[fragments[0][0], fragments[-1][1]]], gaps=[], confidence='verified'),
                replaces=[option['id'] for option, _hashes in old])
