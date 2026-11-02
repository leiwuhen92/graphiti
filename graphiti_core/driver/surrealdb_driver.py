"""SurrealDB connection layer for Graphiti; queries passed here must be SurrealQL."""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from typing import Any

from graphiti_core.driver.driver import GraphDriver, GraphDriverSession, GraphProvider
from graphiti_core.driver.operations.community_node_ops import CommunityNodeOperations
from graphiti_core.driver.operations.entity_node_ops import EntityNodeOperations
from graphiti_core.driver.operations.entity_edge_ops import EntityEdgeOperations
from graphiti_core.driver.operations.episodic_edge_ops import EpisodicEdgeOperations
from graphiti_core.driver.operations.community_edge_ops import CommunityEdgeOperations
from graphiti_core.driver.operations.has_episode_edge_ops import HasEpisodeEdgeOperations
from graphiti_core.driver.operations.next_episode_edge_ops import NextEpisodeEdgeOperations
from graphiti_core.driver.surrealdb.operations.edge_ops import (
    SurrealDBEntityEdgeOperations, SurrealDBEpisodicEdgeOperations,
    SurrealDBCommunityEdgeOperations, SurrealDBHasEpisodeEdgeOperations,
    SurrealDBNextEpisodeEdgeOperations,
)
from graphiti_core.driver.operations.episode_node_ops import EpisodeNodeOperations
from graphiti_core.driver.operations.saga_node_ops import SagaNodeOperations
from graphiti_core.driver.surrealdb.graph_operations import SurrealDBGraphOperationsInterface
from graphiti_core.driver.surrealdb.search_interface import SurrealDBSearchInterface
from graphiti_core.driver.surrealdb.operations.node_ops import (
    SurrealDBCommunityNodeOperations,
    SurrealDBEntityNodeOperations,
    SurrealDBEpisodeNodeOperations,
    SurrealDBSagaNodeOperations,
)


def _load_surreal_factory():
    try:
        from surrealdb import AsyncSurreal
    except ImportError:
        raise ImportError(
            "The SurrealDB driver requires the optional dependency. "
            "Install it with: pip install graphiti-core[surrealdb]"
        ) from None
    return AsyncSurreal


class SurrealDBSession(GraphDriverSession):
    """Borrowed session adapter; closing it does not close the shared client."""

    provider = GraphProvider.SURREALDB

    def __init__(self, driver: SurrealDBDriver):
        self._driver = driver
        self._closed = False

    async def run(self, query: str, **kwargs: Any) -> Any:
        if self._closed:
            raise RuntimeError("SurrealDB session is closed")
        params = kwargs.pop("params", kwargs)
        return await self._driver.execute_query(query, params=params, **kwargs)

    async def execute_write(self, func, *args, **kwargs):
        return await func(self, *args, **kwargs)

    async def close(self) -> None:
        self._closed = True

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()


class SurrealDBDriver(GraphDriver):
    """Graphiti driver backed by SurrealDB and the asynchronous Python SDK."""

    provider = GraphProvider.SURREALDB
    fulltext_syntax = ""

    def __init__(
        self,
        uri: str,
        user: str | None,
        password: str | None,
        namespace: str = "graphiti",
        database: str = "graphiti",
    ):
        super().__init__()
        self.uri = uri
        self.user = user or ""
        self.password = password or ""
        self.namespace = namespace
        self._database = database
        self.client = _load_surreal_factory()(uri)
        self._connect_lock = asyncio.Lock()
        self._connected = False
        self._closed = False
        self._entity_node_ops = SurrealDBEntityNodeOperations()
        self._episode_node_ops = SurrealDBEpisodeNodeOperations()
        self._community_node_ops = SurrealDBCommunityNodeOperations()
        self._saga_node_ops = SurrealDBSagaNodeOperations()
        self._entity_edge_ops = SurrealDBEntityEdgeOperations()
        self._episodic_edge_ops = SurrealDBEpisodicEdgeOperations()
        self._community_edge_ops = SurrealDBCommunityEdgeOperations()
        self._has_episode_edge_ops = SurrealDBHasEpisodeEdgeOperations()
        self._next_episode_edge_ops = SurrealDBNextEpisodeEdgeOperations()
        self.graph_operations_interface = SurrealDBGraphOperationsInterface()
        self.search_interface = SurrealDBSearchInterface()

    @property
    def entity_node_ops(self) -> EntityNodeOperations:
        return self._entity_node_ops

    @property
    def episode_node_ops(self) -> EpisodeNodeOperations:
        return self._episode_node_ops

    @property
    def community_node_ops(self) -> CommunityNodeOperations:
        return self._community_node_ops

    @property
    def saga_node_ops(self) -> SagaNodeOperations:
        return self._saga_node_ops

    @property
    def entity_edge_ops(self) -> EntityEdgeOperations:
        return self._entity_edge_ops

    @property
    def episodic_edge_ops(self) -> EpisodicEdgeOperations:
        return self._episodic_edge_ops

    @property
    def community_edge_ops(self) -> CommunityEdgeOperations:
        return self._community_edge_ops

    @property
    def has_episode_edge_ops(self) -> HasEpisodeEdgeOperations:
        return self._has_episode_edge_ops

    @property
    def next_episode_edge_ops(self) -> NextEpisodeEdgeOperations:
        return self._next_episode_edge_ops

    async def _ensure_connected(self) -> None:
        if self._closed:
            raise RuntimeError("SurrealDB driver is closed")
        if self._connected:
            return
        async with self._connect_lock:
            if self._connected:
                return
            await self.client.connect()
            if self.user or self.password:
                await self.client.signin(
                    {"username": self.user, "password": self.password}
                )
            await self.client.use(self.namespace, self._database)
            self._connected = True

    async def execute_query(self, cypher_query_: str, **kwargs: Any) -> tuple[list, None, list]:
        """Execute SurrealQL and normalize results to Graphiti query tuple shape.

        The inherited parameter name is retained for QueryExecutor compatibility;
        this method does not translate or execute Cypher.
        """
        await self._ensure_connected()
        params = kwargs.pop("params", None) or {}
        kwargs.pop("routing_", None)
        kwargs.pop("database_", None)
        if kwargs:
            unexpected = ", ".join(sorted(kwargs))
            raise TypeError(f"Unsupported SurrealDB query options: {unexpected}")
        raw = await self.client.query_raw(cypher_query_, params)
        records: list[dict[str, Any]] = []
        if isinstance(raw, dict) and "result" in raw:
            statements = raw["result"]
            for statement in statements:
                if statement.get("status") == "ERR":
                    raise RuntimeError(str(statement.get("result", "SurrealQL query failed")))
                result = statement.get("result")
                if isinstance(result, list):
                    records.extend(item for item in result if isinstance(item, dict))
                elif isinstance(result, dict):
                    records.append(result)
        elif isinstance(raw, list):
            for statement in raw:
                result = statement.get("result") if isinstance(statement, dict) else statement
                if isinstance(result, list):
                    records.extend(item for item in result if isinstance(item, dict))
                elif isinstance(result, dict):
                    records.append(result)
        elif isinstance(raw, dict):
            records.append(raw)
        return records, None, list(records[0].keys()) if records else []

    def session(self, database: str | None = None) -> SurrealDBSession:
        if database is not None and database != self._database:
            raise ValueError(
                "Use driver.with_database() for a different SurrealDB database; "
                "a borrowed session cannot change the shared connection context."
            )
        return SurrealDBSession(self)

    async def close(self) -> None:
        if not self._closed:
            self._closed = True
            if self._connected:
                await self.client.close()
                self._connected = False

    def delete_all_indexes(self) -> Coroutine:
        async def _delete() -> None:
            await self._ensure_connected()
            raise NotImplementedError(
                "Index deletion is schema-managed; use build_indices_and_constraints(delete_existing=True)."
            )
        return _delete()

    async def build_indices_and_constraints(self, delete_existing: bool = False):
        await self._ensure_connected()
        from graphiti_core.driver.surrealdb.schema import ensure_schema
        await ensure_schema(self, delete_existing=delete_existing)

    def with_database(self, database: str) -> SurrealDBDriver:
        return SurrealDBDriver(self.uri, self.user, self.password, self.namespace, database)

    def clone(self, database: str) -> SurrealDBDriver:
        return self.with_database(database)

    def build_fulltext_query(
        self, query: str, group_ids: list[str] | None = None, max_query_length: int = 128
    ) -> str:
        return query[:max_query_length]
