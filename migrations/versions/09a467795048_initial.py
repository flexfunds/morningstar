"""initial

Revision ID: 09a467795048
Revises: 
Create Date: 2025-03-11 16:51:57.197130

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '09a467795048'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # Use batch operations for SQLite
    with op.batch_alter_table('nav_entries') as batch_op:
        batch_op.create_foreign_key(
            'fk_nav_entries_series', 'series', ['isin'], ['isin'])


def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table('nav_entries') as batch_op:
        batch_op.drop_constraint('fk_nav_entries_series', type_='foreignkey')
