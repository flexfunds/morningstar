"""Add unique constraint to nav_entries

Revision ID: add_nav_unique_constraint
Revises: 09a467795048
Create Date: 2025-03-19 10:00:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from alembic.operations import ops

# revision identifiers, used by Alembic.
revision: str = 'add_nav_unique_constraint'
down_revision: Union[str, None] = '09a467795048'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add unique constraint to nav_entries table."""
    # First remove any duplicate entries
    op.execute("""
        DELETE FROM nav_entries
        WHERE id IN (
            SELECT id
            FROM (
                SELECT id,
                    ROW_NUMBER() OVER (
                        PARTITION BY isin, nav_date
                        ORDER BY created_at DESC
                    ) as rnum
                FROM nav_entries
            ) t
            WHERE t.rnum > 1
        )
    """)

    # Use batch operations for SQLite
    with op.batch_alter_table('nav_entries') as batch_op:
        batch_op.create_unique_constraint(
            'uix_nav_entry_isin_date', ['isin', 'nav_date'])


def downgrade() -> None:
    """Remove unique constraint from nav_entries table."""
    with op.batch_alter_table('nav_entries') as batch_op:
        batch_op.drop_constraint('uix_nav_entry_isin_date', type_='unique')
