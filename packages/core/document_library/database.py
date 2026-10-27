"""Explicit SQLite connections and versioned migrations for the core schema."""

from pathlib import Path
from contextlib import contextmanager
import sqlite3

from alembic import command
from alembic.config import Config
from sqlalchemy import Engine, URL, create_engine, event
from sqlalchemy.engine.util import TransactionalContext

from .library_contracts import register_sqlite_functions


def database_engine(path: Path, *, must_exist: bool = False) -> Engine:
    """Caller owns path authorization; production volume gating belongs to G0-D."""
    if type(must_exist) is not bool:
        raise ValueError("must_exist must be a bool")
    if not path.is_absolute():
        raise ValueError("Database path must be absolute")
    url = URL.create("sqlite+pysqlite", database=str(path))
    engine_options = {}
    if must_exist:
        # Keep the public path URL while preserving the dialect's file/thread
        # options. Only the DBAPI filename becomes an encoded, existing-only URI.
        _arguments, options = url.get_dialect()().create_connect_args(url)
        options = dict(options, autocommit=False, uri=True)
        uri = path.as_uri() + "?mode=rw"
        engine_options["creator"] = lambda: sqlite3.connect(uri, **options)
    engine = create_engine(url, connect_args={"autocommit": False}, **engine_options)

    @event.listens_for(engine, "connect")
    def configure(connection, _record):
        register_sqlite_functions(connection)
        connection.autocommit = True
        cursor = connection.cursor()
        try:
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.execute("PRAGMA busy_timeout=5000")
            if cursor.execute("PRAGMA journal_mode=WAL").fetchone()[0] != "wal":
                raise RuntimeError("WAL could not be enabled")
        finally:
            cursor.close()
            connection.autocommit = False

    return engine


class MigrationCleanupError(RuntimeError):
    """Explicit migration failure provenance and its still-owned checkout.

    Only cleanup failures use this type. Ordinary operation failures keep their
    original exception identity. Phase/error pairs preserve every cleanup cause.
    """

    def __init__(self, *, connection, primary_error, cleanup_errors):
        super().__init__("MIGRATION_CLEANUP_FAILED")
        self.connection, self.primary_error = connection, primary_error
        self.cleanup_errors = tuple(cleanup_errors)
        self.cleanup_error = self.cleanup_errors[0][1]
        self.rollback_error = next((error for phase, error in self.cleanup_errors
                                    if phase == "rollback"), None)
        self.close_error = next((error for phase, error in self.cleanup_errors
                                 if phase == "close"), None)
        self.transaction_close_error = next((error for phase, error in self.cleanup_errors
                                             if phase == "transaction_close"), None)


class _MigrationContext(TransactionalContext):
    """Record operation boundaries while reusing installed SQLAlchemy semantics.

    This bounded adapter uses SQLAlchemy 2.1.6 private TransactionalContext and
    transaction state predicates, following Engine.begin's RootTransaction
    protocol, including early-end guards and rollback after failed commit.
    No SQL transaction commands or private context-restoration logic are copied.
    """

    def __init__(self, connection, transaction):
        self.connection, self.transaction = connection, transaction
        self.primary_error = self.primary_traceback = None
        self.cleanup_errors = []

    def _get_subject(self):
        return self.connection

    def _transaction_is_active(self):
        return self.transaction._transaction_is_active()

    def _transaction_is_closed(self):
        return self.transaction._transaction_is_closed()

    def _rollback_can_be_called(self):
        return self.transaction._rollback_can_be_called()

    def commit(self):
        try:
            self.transaction.commit()
        except BaseException as error:
            self.primary_error, self.primary_traceback = error, error.__traceback__
            raise

    def rollback(self):
        try:
            self.transaction.rollback()
        except BaseException as error:
            self.cleanup_errors.append(("rollback", error))
            raise

    def close(self):
        try:
            self.transaction.close()
        except BaseException as error:
            self.cleanup_errors.append(("transaction_close", error))
            raise


def migrate(engine: Engine, migrations: Path) -> None:
    """Upgrade once; distinguish cleanup failures without changing ordinary errors.

    Cleanup failure deliberately raises MigrationCleanupError instead of the
    incidental raw cleanup exception historically exposed by context managers.
    """
    config = Config()
    config.set_main_option("script_location", str(migrations.resolve()))
    connection = engine.connect()
    context = primary = traceback = None
    cleanup = []
    try:
        transaction = connection.begin()
        context = _MigrationContext(connection, transaction)
        with context:
            try:
                config.attributes["connection"] = connection
                command.upgrade(config, "head")
            except BaseException as error:
                context.primary_error = error
                context.primary_traceback = error.__traceback__
                raise
    except BaseException as error:
        if context is None:
            primary, traceback = error, error.__traceback__
        else:
            primary, traceback = context.primary_error, context.primary_traceback
            cleanup.extend(context.cleanup_errors)
            if primary is None and not cleanup:
                primary, traceback = error, error.__traceback__
            elif error is not primary and not any(error is e for _phase, e in cleanup):
                cleanup.append(("transaction_context", error))
    finally:
        try:
            connection.close()
        except BaseException as error:
            cleanup.append(("close", error))
    if cleanup:
        raise MigrationCleanupError(connection=connection, primary_error=primary,
                                    cleanup_errors=cleanup) from (
                                        primary if primary is not None else cleanup[0][1])
    if primary is not None:
        raise primary.with_traceback(traceback)


@contextmanager
def write_transaction(engine: Engine):
    """Acquire the SQLite writer before reading authority; never reuse a caller txn.

    Python autocommit=False opens a deferred transaction on checkout. End that
    empty transaction using SQL, then begin IMMEDIATE before any application read.
    DBAPI commit/rollback subsequently restores its normal deferred mode.
    """
    with engine.connect() as connection:
        connection.exec_driver_sql("ROLLBACK")
        connection.exec_driver_sql("BEGIN IMMEDIATE")
        try:
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
