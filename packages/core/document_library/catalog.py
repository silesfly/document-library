"""Category filtering over an explicit, caller-authorized edition set.

This is a query primitive, not an authentication or rights policy. G0-B must
compute the current allowed edition set before exposing any HTTP endpoint.
"""

from collections.abc import Collection
import json
import re
import unicodedata

from sqlalchemy import Connection, text


CATEGORIES = frozenset({"novel_text", "novel_scan", "webtoon", "comic", "document", "unknown"})

_TITLE_NON_MATCHING = re.compile(
    r"[^0-9a-z\u1100-\u115e\u1161-\u11ff\u3131-\u318e"
    r"\ua960-\ua97c\uac00-\ud7a3\ud7b0-\ud7c6\ud7cb-\ud7fb]+"
)


def title_match_key(title: str) -> str:
    """Compare parsed titles using digits, English and Hangul only.

    Preserve the original separately for display. File extensions, release
    tags and episode ranges belong to the ingestion parser, not this function.
    An empty key is unmatchable; key equality alone never authorizes work merges.
    """
    normalized = unicodedata.normalize("NFKC", title).casefold()
    return unicodedata.normalize("NFC", _TITLE_NON_MATCHING.sub("", normalized))


def titles_match(left: str, right: str) -> bool:
    """Return title equality without equating two empty comparison keys."""
    key = title_match_key(left)
    return bool(key) and key == title_match_key(right)


def list_works(
    connection: Connection, *, visible_edition_ids: Collection[str],
    categories: Collection[str] = (), limit: int = 50, offset: int = 0,
) -> list[dict]:
    selected = set(categories)
    if not selected <= CATEGORIES:
        raise ValueError("Unknown category")
    if type(limit) is not int or type(offset) is not int or not 1 <= limit <= 100 or offset < 0:
        raise ValueError("Invalid pagination")
    if any(not isinstance(value, str) for value in visible_edition_ids):
        raise ValueError("Edition identifiers must be strings")
    permitted = sorted(set(visible_edition_ids))
    query = text("""
        SELECT w.id, w.item_type, w.title_display, w.author
        FROM works AS w
        WHERE EXISTS (
            SELECT 1 FROM work_editions AS e
            JOIN edition_revisions AS r ON r.edition_id=e.id AND r.is_active=1
            WHERE e.work_id=w.id
              AND e.id IN (SELECT value FROM json_each(:permitted))
              AND (
                :all_categories
                OR (w.item_type='novel' AND :novel_text AND e.representation='text')
                OR (w.item_type='novel' AND :novel_scan AND e.source_form='scan')
                OR (w.item_type='webtoon' AND :webtoon)
                OR (w.item_type='comic' AND :comic)
                OR (w.item_type='document' AND :document)
                OR (w.item_type='unknown' AND :unknown)
              )
        )
        ORDER BY w.title_match_key, w.id
        LIMIT :limit OFFSET :offset
    """)
    parameters = {category: category in selected for category in CATEGORIES}
    parameters.update(
        all_categories=not selected, permitted=json.dumps(permitted),
        limit=limit, offset=offset,
    )
    return [dict(row) for row in connection.execute(query, parameters).mappings()]
