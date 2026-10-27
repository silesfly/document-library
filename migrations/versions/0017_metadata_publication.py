"""G4 metadata authority, removable payloads and typed cover publication."""
from alembic import op
from document_library.metadata_contracts import STATEMENTS_V1

revision = '0017_metadata_publication'
down_revision = '0016_member_features'
branch_labels = None
depends_on = None


def upgrade():
    for statement in STATEMENTS_V1:
        op.execute(statement)


def downgrade():
    raise RuntimeError('Destructive downgrade is not enabled')
