"""baseline

Revision ID: 8105e75ad5b5
Revises: d8571cb3fb8f
Create Date: 2026-01-19 20:40:10.541733

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8105e75ad5b5'
down_revision: Union[str, None] = 'd8571cb3fb8f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
