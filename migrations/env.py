"""Migrations require an explicitly supplied connection; no implicit live URL."""

from alembic import context

connection = context.config.attributes.get("connection")
if connection is None:
    raise RuntimeError("An explicitly authorized connection is required")
context.configure(connection=connection, transactional_ddl=True)
with context.begin_transaction():
    context.run_migrations()
