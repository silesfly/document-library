"""G0-A acceptance using only disposable SQLite files and synthetic identities."""

from pathlib import Path
import tempfile
import unittest

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from document_library.catalog import list_works
from document_library.database import database_engine, migrate


ROOT = Path(__file__).resolve().parents[1]
STAMP = "2026-10-22T00:00:00Z"


class CatalogAcceptance(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="document-library-g0a-")
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "synthetic.sqlite3"
        self.engine = database_engine(self.path)
        self.addCleanup(self.engine.dispose)
        migrate(self.engine, ROOT / "migrations")
        self.visible = []
        with self.engine.begin() as conn:
            self.insert(conn, "users", id="reader", username="synthetic-reader",
                        password_hash="NOT_AN_AUTHENTICATION_FIXTURE", created_at=STAMP)
            for work, kind in (("novel", "novel"), ("web", "webtoon"), ("comic", "comic"),
                               ("doc", "document"), ("unknown", "unknown"),
                               ("scan-only", "novel"), ("private", "novel"), ("draft", "novel")):
                self.insert(conn, "works", id=work, item_type=kind, title_display=work,
                            title_match_key=work, created_at=STAMP, updated_at=STAMP)
            editions = (
                ("text", "novel", "text", "born_digital", 1),
                ("scan", "novel", "imagepack", "scan", 1),
                ("ocr", "scan-only", "imagepack", "scan", 1),
                ("web-e", "web", "imagepack", "born_digital", 1),
                ("comic-e", "comic", "imagepack", "scan", 1),
                ("doc-e", "doc", "document", "born_digital", 1),
                ("unknown-e", "unknown", "document", "unknown", 1),
                ("private-e", "private", "text", "born_digital", 1),
                ("draft-e", "draft", "text", "born_digital", 0),
            )
            for edition, work, representation, source, active in editions:
                self.insert(conn, "work_editions", id=edition, work_id=work,
                            edition_key=edition, representation=representation,
                            source_form=source, edition_label=edition, created_at=STAMP)
                self.insert(conn, "edition_revisions", id=edition + "-r1", edition_id=edition,
                            revision_no=1, is_active=active, created_at=STAMP)
                if edition != "private-e":
                    self.visible.append(edition)
            self.insert(conn, "objects", sha256="a" * 64, byte_size=12,
                        detected_mime="text/plain", local_relpath="aa/aa/" + "a" * 64,
                        created_at=STAMP)
            for rev, role in (("text-r1", "content"), ("scan-r1", "content"), ("ocr-r1", "ocr")):
                self.insert(conn, "revision_assets", edition_revision_id=rev, asset_position=0,
                            object_sha="a" * 64, file_format="txt", asset_role=role)

    @staticmethod
    def insert(conn, table, **values):
        columns = ",".join(values)
        placeholders = ",".join(":" + key for key in values)
        conn.execute(text(f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"), values)

    def ids(self, *categories, visible=None, **page):
        with self.engine.connect() as conn:
            return [row["id"] for row in list_works(
                conn, visible_edition_ids=self.visible if visible is None else visible,
                categories=categories, **page,
            )]

    def reject(self, sql, parameters=None):
        with self.assertRaises(IntegrityError):
            with self.engine.begin() as conn:
                conn.execute(text(sql), parameters or {})

    def test_upgrade_is_repeatable_and_connection_invariants_hold(self):
        migrate(self.engine, ROOT / "migrations")
        with self.engine.connect() as conn:
            self.assertEqual(conn.exec_driver_sql("PRAGMA journal_mode").scalar(), "wal")
            self.assertEqual(conn.exec_driver_sql("PRAGMA foreign_keys").scalar(), 1)
            self.assertEqual(conn.exec_driver_sql("PRAGMA recursive_triggers").scalar(), 1)
            self.assertEqual(conn.exec_driver_sql("PRAGMA integrity_check").scalar(), "ok")
            self.assertEqual(conn.exec_driver_sql("PRAGMA foreign_key_check").all(), [])
            self.assertEqual(conn.exec_driver_sql("SELECT count(*) FROM works").scalar(), 8)
            self.assertEqual(conn.exec_driver_sql("SELECT version_num FROM alembic_version").scalar(), "0018_generated_review")

    def test_text_and_scan_are_simultaneously_active_without_duplicate_cards(self):
        self.assertEqual(self.ids("novel_text"), ["novel"])
        self.assertEqual(self.ids("novel_scan"), ["novel", "scan-only"])
        self.assertEqual(self.ids("novel_text", "novel_scan"), ["novel", "scan-only"])
        self.assertEqual(self.ids("novel_text", "novel_scan", limit=1, offset=1), ["scan-only"])

    def test_access_scope_and_active_revision_apply_to_every_category(self):
        self.assertEqual(self.ids(visible=[]), [])
        self.assertEqual(self.ids("novel_text", visible=["scan"]), [])
        self.assertEqual(self.ids("webtoon", visible=["text"]), [])
        self.assertEqual(self.ids(), ["comic", "doc", "novel", "scan-only", "unknown", "web"])
        self.assertEqual(self.ids("webtoon", "comic"), ["comic", "web"])
        self.assertEqual(self.ids("document", "unknown"), ["doc", "unknown"])

    def test_ocr_asset_does_not_create_a_text_edition(self):
        self.assertEqual(self.ids("novel_text", visible=["ocr"]), [])
        self.assertEqual(self.ids("novel_scan", visible=["ocr"]), ["scan-only"])

    def test_adaptation_has_its_own_rating_and_novel_editions_share_one(self):
        with self.engine.begin() as conn:
            self.insert(conn, "work_relations", work_id="web", related_work_id="novel", type="adaptation_of")
            for work, score in (("novel", 9), ("web", 5)):
                self.insert(conn, "ratings", user_id="reader", work_id=work,
                            rating_half_steps=score, updated_at=STAMP)
            scores = conn.exec_driver_sql("SELECT work_id,rating_half_steps FROM ratings ORDER BY work_id").all()
            self.assertEqual(scores, [("novel", 9), ("web", 5)])
        self.reject("INSERT INTO ratings VALUES ('reader','novel',8,'synthetic')")
        self.reject("UPDATE ratings SET rating_half_steps=1.5 WHERE work_id='novel'")

    def test_invalid_taxonomy_parent_and_duplicate_active_revision_are_rejected(self):
        statements = (
            "UPDATE works SET item_type='imagepack' WHERE id='novel'",
            "UPDATE works SET id=NULL WHERE id='novel'",
            "UPDATE work_editions SET source_form='ocr' WHERE id='scan'",
            "UPDATE work_editions SET representation='webtoon' WHERE id='scan'",
            "UPDATE work_editions SET work_id='missing' WHERE id='text'",
            "INSERT INTO edition_revisions VALUES ('collision','text',2,1,'synthetic')",
            "UPDATE objects SET sha256=printf('%064d',0)||'x'",
            "INSERT INTO work_relations VALUES ('novel','novel','adaptation_of')",
        )
        for sql in statements:
            with self.subTest(sql=sql):
                self.reject(sql)

    def test_option_asset_cannot_cross_revision_and_bundle_order_is_explicit(self):
        with self.engine.begin() as conn:
            self.insert(conn, "revision_options", id="bundle", edition_revision_id="scan-r1",
                        format="imagepack", delivery_kind="bundle", coverage_json='{"unit":"unknown"}',
                        coverage_confidence="unknown")
            self.insert(conn, "revision_assets", edition_revision_id="scan-r1", asset_position=1,
                        object_sha="a" * 64, file_format="jpg")
            for ordinal, position in ((0, 1), (1, 0)):
                self.insert(conn, "option_assets", option_id="bundle", edition_revision_id="scan-r1",
                            ordinal=ordinal, asset_position=position)
            rows = conn.exec_driver_sql("SELECT asset_position FROM option_assets ORDER BY ordinal").scalars().all()
            self.assertEqual(rows, [1, 0])
        self.reject("INSERT INTO option_assets VALUES ('bundle','text-r1',0,2)")
        self.reject("INSERT INTO option_assets VALUES ('bundle','scan-r1',99,2)")
        self.reject("UPDATE revision_options SET coverage_json='[]'")

    def test_fk_failure_rolls_back_the_whole_write_transaction(self):
        with self.assertRaises(IntegrityError):
            with self.engine.begin() as conn:
                conn.exec_driver_sql("UPDATE works SET title_display='uncommitted' WHERE id='novel'")
                conn.exec_driver_sql("INSERT INTO ratings VALUES ('missing','novel',9,'synthetic')")
        with self.engine.connect() as conn:
            self.assertEqual(conn.exec_driver_sql("SELECT title_display FROM works WHERE id='novel'").scalar(), "novel")

    def test_history_cannot_attribute_another_works_edition(self):
        self.reject("""INSERT INTO classification_history
            VALUES ('audit','web','text','reader','synthetic','{}','{}','synthetic')""")

    def test_invalid_filters_and_pagination_do_not_become_sql(self):
        for categories, page in ((["novel_text' OR 1=1"], {}), ([], {"limit": 0}), ([], {"offset": -1})):
            with self.subTest(categories=categories, page=page), self.assertRaises(ValueError):
                self.ids(*categories, **page)


if __name__ == "__main__":
    unittest.main()
