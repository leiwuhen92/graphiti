"""SurrealDB schema bootstrap for Graphiti's temporal graph records."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from graphiti_core.driver.surrealdb_driver import SurrealDBDriver


TABLES = (
    "entity_node",
    "episodic_node",
    "community_node",
    "saga_node",
    "entity_edge",
    "episodic_edge",
    "community_edge",
    "has_episode_edge",
    "next_episode_edge",
)


async def ensure_schema(driver: SurrealDBDriver, delete_existing: bool = False) -> None:
    """Create schemaless record tables and lookup indexes.

    The first implementation intentionally avoids a fixed vector dimension:
    Graphiti embedding dimensions are selected by the configured embedder. Vector
    indexes must be added once that dimension is known.
    """
    if delete_existing:
        for table in TABLES:
            await driver.execute_query(f"REMOVE TABLE IF EXISTS {table}")

    statements: list[str] = []
    for table in TABLES:
        statements.append(f"DEFINE TABLE IF NOT EXISTS {table} SCHEMALESS")
        statements.append(
            f"DEFINE INDEX IF NOT EXISTS {table}_uuid ON TABLE {table} FIELDS uuid UNIQUE"
        )
        statements.append(
            f"DEFINE INDEX IF NOT EXISTS {table}_group_id ON TABLE {table} FIELDS group_id"
        )

    await driver.execute_query(";".join(statements))
