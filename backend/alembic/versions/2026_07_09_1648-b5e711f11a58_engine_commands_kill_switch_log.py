"""engine commands kill-switch log

Phase 2c: append-only operator command log (``engine_commands``) — the
kill-switch source of truth. Derived state = latest row by ``seq``, which is
``GENERATED ALWAYS`` identity because writer clocks (laptop CLI vs server API)
can skew and must not order state. The partial index serves the engine's
boot/periodic sweep for not-yet-picked-up commands; the unique ``seq`` index
also serves the latest-state ``ORDER BY seq DESC`` query.

Revision ID: b5e711f11a58
Revises: a7c31d90f2e4
Create Date: 2026-07-09 16:48:18.650017

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b5e711f11a58"
down_revision: str | Sequence[str] | None = "a7c31d90f2e4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "engine_commands",
        sa.Column("seq", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column("action", sa.String(length=16), nullable=False),
        sa.Column("scope", sa.String(length=32), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("actor", sa.String(length=120), nullable=False),
        sa.Column(
            "issued_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("applied_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("result", sa.Text(), nullable=True),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_engine_commands_unapplied_seq",
        "engine_commands",
        ["seq"],
        unique=False,
        postgresql_where=sa.text("applied_at IS NULL"),
    )
    op.create_index("uq_engine_commands_seq", "engine_commands", ["seq"], unique=True)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("uq_engine_commands_seq", table_name="engine_commands")
    op.drop_index(
        "ix_engine_commands_unapplied_seq",
        table_name="engine_commands",
        postgresql_where=sa.text("applied_at IS NULL"),
    )
    op.drop_table("engine_commands")
