"""SurrealDB node operations shared by Graphiti node types."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from graphiti_core.driver.query_executor import QueryExecutor, Transaction
from graphiti_core.errors import NodeNotFoundError
from graphiti_core.nodes import CommunityNode, EntityNode, EpisodeType, EpisodicNode, SagaNode


async def _execute(
    executor: QueryExecutor, query: str, params: dict[str, Any], tx: Transaction | None = None
) -> list[dict[str, Any]]:
    result = await (tx.run(query, **params) if tx else executor.execute_query(query, params=params))
    if isinstance(result, tuple):
        return result[0]
    if isinstance(result, list):
        return result
    return []


class _NodeOps:
    table: str
    model: type

    def _data(self, node: Any) -> dict[str, Any]:
        data = node.model_dump(mode="python")
        if isinstance(node, EntityNode):
            data["attributes"] = node.attributes or {}
            data["labels"] = list(set(node.labels + ["Entity"]))
        if isinstance(node, EpisodicNode):
            data["source"] = node.source.value
        return data

    async def save(self, executor: QueryExecutor, node: Any, tx: Transaction | None = None) -> None:
        await _execute(
            executor,
            f"UPSERT type::thing('{self.table}', $uuid) CONTENT $data",
            {"uuid": node.uuid, "data": self._data(node)},
            tx,
        )

    async def save_bulk(
        self, executor: QueryExecutor, nodes: list[Any], tx: Transaction | None = None,
        batch_size: int = 100
    ) -> None:
        for offset in range(0, len(nodes), batch_size):
            for node in nodes[offset : offset + batch_size]:
                await self.save(executor, node, tx)

    async def delete(self, executor: QueryExecutor, node: Any, tx: Transaction | None = None) -> None:
        await _execute(
            executor, f"DELETE FROM {self.table} WHERE uuid = $uuid", {"uuid": node.uuid}, tx
        )

    async def delete_by_group_id(
        self, executor: QueryExecutor, group_id: str, tx: Transaction | None = None,
        batch_size: int = 100
    ) -> None:
        await _execute(
            executor, f"DELETE FROM {self.table} WHERE group_id = $group_id",
            {"group_id": group_id}, tx
        )

    async def delete_by_uuids(
        self, executor: QueryExecutor, uuids: list[str], tx: Transaction | None = None,
        batch_size: int = 100
    ) -> None:
        await _execute(
            executor, f"DELETE FROM {self.table} WHERE uuid IN $uuids", {"uuids": uuids}, tx
        )

    async def get_by_uuid(self, executor: QueryExecutor, uuid: str):
        records = await _execute(
            executor, f"SELECT * FROM {self.table} WHERE uuid = $uuid", {"uuid": uuid}
        )
        if not records:
            raise NodeNotFoundError(uuid)
        return self.model.model_validate(records[0])

    async def get_by_uuids(self, executor: QueryExecutor, uuids: list[str]) -> list[Any]:
        records = await _execute(
            executor, f"SELECT * FROM {self.table} WHERE uuid IN $uuids", {"uuids": uuids}
        )
        return [self.model.model_validate(row) for row in records]

    async def get_by_group_ids(
        self, executor: QueryExecutor, group_ids: list[str], limit: int | None = None,
        uuid_cursor: str | None = None
    ) -> list[Any]:
        query = f"SELECT * FROM {self.table} WHERE group_id IN $group_ids"
        params: dict[str, Any] = {"group_ids": group_ids}
        if uuid_cursor is not None:
            query += " AND uuid < $uuid_cursor"
            params["uuid_cursor"] = uuid_cursor
        query += " ORDER BY uuid DESC"
        if limit is not None:
            query += " LIMIT $limit"
            params["limit"] = limit
        records = await _execute(executor, query, params)
        return [self.model.model_validate(row) for row in records]


class SurrealDBEntityNodeOperations(_NodeOps):
    table = "entity_node"
    model = EntityNode

    async def load_embeddings(self, executor: QueryExecutor, node: EntityNode) -> None:
        record = await self.get_by_uuid(executor, node.uuid)
        node.name_embedding = record.name_embedding

    async def load_embeddings_bulk(
        self, executor: QueryExecutor, nodes: list[EntityNode], batch_size: int = 100
    ) -> None:
        loaded = {n.uuid: n for n in await self.get_by_uuids(executor, [n.uuid for n in nodes])}
        for node in nodes:
            if node.uuid in loaded:
                node.name_embedding = loaded[node.uuid].name_embedding


class SurrealDBEpisodeNodeOperations(_NodeOps):
    table = "episodic_node"
    model = EpisodicNode

    async def get_by_entity_node_uuid(
        self, executor: QueryExecutor, entity_node_uuid: str
    ) -> list[EpisodicNode]:
        records = await _execute(
            executor,
            "SELECT * FROM episodic_node WHERE $entity_uuid IN entity_edges",
            {"entity_uuid": entity_node_uuid},
        )
        return [EpisodicNode.model_validate(row) for row in records]

    async def retrieve_episodes(
        self, executor: QueryExecutor, reference_time: datetime, last_n: int = 3,
        group_ids: list[str] | None = None, source: str | None = None,
        saga: str | None = None
    ) -> list[EpisodicNode]:
        query = "SELECT * FROM episodic_node WHERE valid_at <= $reference_time"
        params: dict[str, Any] = {"reference_time": reference_time, "last_n": last_n}
        if group_ids is not None:
            query += " AND group_id IN $group_ids"
            params["group_ids"] = group_ids
        if source is not None:
            query += " AND source = $source"
            params["source"] = source
        if saga is not None:
            query += " AND saga = $saga"
            params["saga"] = saga
        query += " ORDER BY valid_at DESC LIMIT $last_n"
        records = await _execute(executor, query, params)
        return [EpisodicNode.model_validate(row) for row in records]


class SurrealDBCommunityNodeOperations(_NodeOps):
    table = "community_node"
    model = CommunityNode

    async def load_name_embedding(self, executor: QueryExecutor, node: CommunityNode) -> None:
        record = await self.get_by_uuid(executor, node.uuid)
        node.name_embedding = record.name_embedding


class SurrealDBSagaNodeOperations(_NodeOps):
    table = "saga_node"
    model = SagaNode
