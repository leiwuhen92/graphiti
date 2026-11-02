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

INDEXES = tuple(
    (table, f"{table}_uuid", "uuid", True) for table in TABLES
) + tuple(
    (table, f"{table}_group_id", "group_id", False)
    for table in TABLES
    if table.endswith("_node") or table == "entity_edge"
)


async def ensure_schema(driver: SurrealDBDriver, delete_existing: bool = False) -> None:
    """Create schemaless record tables and deterministic lookup indexes.

    Vector indexes are intentionally not declared here: their dimension must
    match the configured embedder and cannot safely be guessed by a driver.
    """
    if delete_existing:
        for table, index_name, _, _ in INDEXES:
            await driver.execute_query(
                f"REMOVE INDEX IF EXISTS {index_name} ON TABLE {table}"
            )

    statements: list[str] = []
    for table in TABLES:
        statements.append(f"DEFINE TABLE IF NOT EXISTS {table} SCHEMALESS")
    for table, index_name, field, unique in INDEXES:
        unique_clause = " UNIQUE" if unique else ""
        statements.append(
            f"DEFINE INDEX IF NOT EXISTS {index_name} ON TABLE {table} "
            f"FIELDS {field}{unique_clause}"
        )

    await driver.execute_query(";".join(statements))
