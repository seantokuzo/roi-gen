"""lot_closes ledger + lots.strategy_id index

Phase 2b: per-close realized-P&L ledger (``lot_closes``) so partial-close P&L
is visible to same-day risk queries (the strategy daily-loss breaker reads
``SUM(realized_pnl) WHERE closed_at >= <ET day start>``), and the missing
index on ``lots.strategy_id`` for per-strategy risk-state queries (issue #4).

Revision ID: a7c31d90f2e4
Revises: 5e48fb608876
Create Date: 2026-07-05 12:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a7c31d90f2e4"
down_revision: str | Sequence[str] | None = "5e48fb608876"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "lot_closes",
        sa.Column("lot_id", sa.Uuid(), nullable=False),
        sa.Column("order_id", sa.Uuid(), nullable=True),
        sa.Column("fill_id", sa.Uuid(), nullable=True),
        sa.Column("portfolio_id", sa.Uuid(), nullable=False),
        sa.Column("strategy_id", sa.Uuid(), nullable=True),
        sa.Column("symbol", sa.String(length=16), nullable=False),
        sa.Column("qty", sa.Numeric(precision=18, scale=9), nullable=False),
        sa.Column("realized_pnl", sa.Numeric(precision=18, scale=6), nullable=False),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=False),
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
        sa.ForeignKeyConstraint(["lot_id"], ["lots.id"]),
        sa.ForeignKeyConstraint(["order_id"], ["orders.id"]),
        sa.ForeignKeyConstraint(["fill_id"], ["fills.id"]),
        sa.ForeignKeyConstraint(["portfolio_id"], ["portfolios.id"]),
        sa.ForeignKeyConstraint(["strategy_id"], ["strategies.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_lot_closes_lot_id"), "lot_closes", ["lot_id"], unique=False)
    op.create_index(
        "ix_lot_closes_portfolio_strategy_closed_at",
        "lot_closes",
        ["portfolio_id", "strategy_id", "closed_at"],
        unique=False,
    )
    # Issue #4: per-strategy risk-state queries (open qty, day P&L) seq-scanned lots.
    op.create_index(op.f("ix_lots_strategy_id"), "lots", ["strategy_id"], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(op.f("ix_lots_strategy_id"), table_name="lots")
    op.drop_index("ix_lot_closes_portfolio_strategy_closed_at", table_name="lot_closes")
    op.drop_index(op.f("ix_lot_closes_lot_id"), table_name="lot_closes")
    op.drop_table("lot_closes")
