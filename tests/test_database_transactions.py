"""Commit-before-DBAPI faults must release a rolled-back reusable connection.

These checks do not claim rollback after an already durable, ambiguous commit.
"""
from contextlib import closing
from pathlib import Path
import sqlite3
import tempfile
import unittest

from sqlalchemy import event

from document_library.database import database_engine, write_transaction


class DatabaseTransactionAcceptance(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='g3-database-transaction-')
        self.path = Path(self.temp.name)/'test.sqlite3'
        self.engine = database_engine(self.path)
        self.addCleanup(self.cleanup)
        with self.engine.begin() as conn:
            conn.exec_driver_sql('CREATE TABLE synthetic_mutation(value INTEGER) STRICT')
            conn.exec_driver_sql('CREATE TABLE synthetic_quota(value INTEGER) STRICT')

    def cleanup(self):
        self.engine.dispose()
        self.temp.cleanup()
        self.assertFalse(self.path.parent.exists())

    def write_both(self, conn):
        conn.exec_driver_sql('INSERT INTO synthetic_mutation VALUES(1)')
        conn.exec_driver_sql('INSERT INTO synthetic_quota VALUES(1)')

    def counts(self):
        with self.engine.connect() as conn:
            reused = tuple(conn.exec_driver_sql(f'SELECT count(*) FROM {table}').scalar_one()
                           for table in ('synthetic_mutation', 'synthetic_quota'))
        with closing(sqlite3.connect(self.path, autocommit=True)) as independent:
            external = tuple(independent.execute(f'SELECT count(*) FROM {table}').fetchone()[0]
                             for table in ('synthetic_mutation', 'synthetic_quota'))
        return reused, external

    def test_precommit_event_failure_preserves_exception_rolls_back_and_never_returns(self):
        failure = RuntimeError('synthetic before-DBAPI commit failure')
        checkouts = []
        def checkout(dbapi, record, proxy):
            checkouts.append(id(dbapi))
        def fail(conn):
            raise failure
        event.listen(self.engine, 'checkout', checkout)
        event.listen(self.engine, 'commit', fail)
        returned = []
        def operation():
            with write_transaction(self.engine) as conn:
                self.write_both(conn)
            returned.append(True)
        try:
            with self.assertRaises(RuntimeError) as caught:
                operation()
        finally:
            event.remove(self.engine, 'commit', fail)
        self.assertIs(caught.exception, failure)
        self.assertEqual(returned, [])
        self.assertEqual(self.counts(), ((0, 0), (0, 0)))
        self.assertEqual(len(checkouts), 2)
        self.assertEqual(checkouts[0], checkouts[1])
        event.remove(self.engine, 'checkout', checkout)
        with write_transaction(self.engine) as conn:
            self.write_both(conn)
        self.assertEqual(self.counts(), ((1, 1), (1, 1)))

    def test_body_failure_rolls_back_business_and_quota_control(self):
        failure = RuntimeError('synthetic body failure')
        with self.assertRaises(RuntimeError) as caught:
            with write_transaction(self.engine) as conn:
                self.write_both(conn)
                raise failure
        self.assertIs(caught.exception, failure)
        self.assertEqual(self.counts(), ((0, 0), (0, 0)))

    def test_success_commits_business_and_quota_control(self):
        with write_transaction(self.engine) as conn:
            self.write_both(conn)
        self.assertEqual(self.counts(), ((1, 1), (1, 1)))
