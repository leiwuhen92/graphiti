from __future__ import annotations
from typing import Any
from graphiti_core.driver.query_executor import QueryExecutor, Transaction
from graphiti_core.edges import EntityEdge, EpisodicEdge, CommunityEdge, HasEpisodeEdge, NextEpisodeEdge
from graphiti_core.errors import EdgeNotFoundError

async def _run(executor: QueryExecutor, query: str, params: dict[str, Any], tx: Transaction | None):
    result = await (tx.run(query, **params) if tx else executor.execute_query(query, params=params))
    return result[0] if isinstance(result, tuple) else result if isinstance(result, list) else []

class _Ops:
    table: str
    model: type
    async def save(self, executor, edge, tx=None):
        await _run(executor, f"UPSERT type::thing('{self.table}', $uuid) CONTENT $data",
                   {"uuid": edge.uuid, "data": edge.model_dump(mode="python")}, tx)
    async def save_bulk(self, executor, edges, tx=None, batch_size=100):
        for e in edges: await self.save(executor, e, tx)
    async def delete(self, executor, edge, tx=None):
        await _run(executor, f"DELETE FROM {self.table} WHERE uuid = $uuid", {"uuid": edge.uuid}, tx)
    async def delete_by_uuids(self, executor, uuids, tx=None):
        await _run(executor, f"DELETE FROM {self.table} WHERE uuid IN $uuids", {"uuids": uuids}, tx)
    async def get_by_uuid(self, executor, uuid):
        rows = await _run(executor, f"SELECT * FROM {self.table} WHERE uuid = $uuid", {"uuid": uuid}, None)
        if not rows: raise EdgeNotFoundError(uuid)
        return self.model.model_validate(rows[0])
    async def get_by_uuids(self, executor, uuids):
        rows = await _run(executor, f"SELECT * FROM {self.table} WHERE uuid IN $uuids", {"uuids": uuids}, None)
        return [self.model.model_validate(r) for r in rows]
    async def get_by_group_ids(self, executor, group_ids, limit=None, uuid_cursor=None):
        q = f"SELECT * FROM {self.table} WHERE group_id IN $group_ids"
        p = {"group_ids": group_ids}
        if uuid_cursor is not None: q += " AND uuid < $cursor"; p["cursor"] = uuid_cursor
        q += " ORDER BY uuid DESC"
        if limit is not None: q += " LIMIT $limit"; p["limit"] = limit
        rows = await _run(executor, q, p, None)
        return [self.model.model_validate(r) for r in rows]

class SurrealDBEntityEdgeOperations(_Ops):
    table, model = "entity_edge", EntityEdge
    async def get_between_nodes(self, executor, source_node_uuid, target_node_uuid):
        rows = await _run(executor, "SELECT * FROM entity_edge WHERE source_node_uuid = $s AND target_node_uuid = $t",
                          {"s": source_node_uuid, "t": target_node_uuid}, None)
        return [EntityEdge.model_validate(r) for r in rows]
    async def get_by_node_uuid(self, executor, node_uuid):
        rows = await _run(executor, "SELECT * FROM entity_edge WHERE source_node_uuid = $u OR target_node_uuid = $u",
                          {"u": node_uuid}, None)
        return [EntityEdge.model_validate(r) for r in rows]
    async def load_embeddings(self, executor, edge):
        edge.fact_embedding = (await self.get_by_uuid(executor, edge.uuid)).fact_embedding
    async def load_embeddings_bulk(self, executor, edges, batch_size=100):
        found = {e.uuid: e for e in await self.get_by_uuids(executor, [e.uuid for e in edges])}
        for e in edges:
            if e.uuid in found: e.fact_embedding = found[e.uuid].fact_embedding

class SurrealDBEpisodicEdgeOperations(_Ops):
    table, model = "episodic_edge", EpisodicEdge
class SurrealDBCommunityEdgeOperations(_Ops):
    table, model = "community_edge", CommunityEdge
class SurrealDBHasEpisodeEdgeOperations(_Ops):
    table, model = "has_episode_edge", HasEpisodeEdge
class SurrealDBNextEpisodeEdgeOperations(_Ops):
    table, model = "next_episode_edge", NextEpisodeEdge
