"""Legacy GraphOperationsInterface bridge to native SurrealDB operations.

Graphiti's node/edge model methods still dispatch through this interface in
0.31.x. This bridge is required to prevent their Cypher fallback branches.
"""

from __future__ import annotations

from typing import Any

from graphiti_core.driver.graph_operations.graph_operations import GraphOperationsInterface
from graphiti_core.nodes import EntityNode, EpisodicNode, CommunityNode, SagaNode
from graphiti_core.edges import EntityEdge, EpisodicEdge, CommunityEdge, HasEpisodeEdge, NextEpisodeEdge


class SurrealDBGraphOperationsInterface(GraphOperationsInterface):


    async def node_get_by_group_ids(self, _cls: Any, driver: Any, group_ids: list[str], limit: int | None = None, uuid_cursor: str | None = None):
        return await driver.entity_node_ops.get_by_group_ids(driver, group_ids, limit, uuid_cursor)

    async def community_node_get_by_uuids(self, _cls: Any, driver: Any, uuids: list[str]):
        return await driver.community_node_ops.get_by_uuids(driver, uuids)

    async def community_node_get_by_group_ids(self, _cls: Any, driver: Any, group_ids: list[str], limit: int | None = None, uuid_cursor: str | None = None):
        return await driver.community_node_ops.get_by_group_ids(driver, group_ids, limit, uuid_cursor)

    async def saga_node_get_by_uuid(self, _cls: Any, driver: Any, uuid: str):
        return await driver.saga_node_ops.get_by_uuid(driver, uuid)

    async def saga_node_get_by_uuids(self, _cls: Any, driver: Any, uuids: list[str]):
        return await driver.saga_node_ops.get_by_uuids(driver, uuids)

    async def saga_node_get_by_group_ids(self, _cls: Any, driver: Any, group_ids: list[str], limit: int | None = None, uuid_cursor: str | None = None):
        return await driver.saga_node_ops.get_by_group_ids(driver, group_ids, limit, uuid_cursor)

    async def episodic_edge_get_by_group_ids(self, _cls: Any, driver: Any, group_ids: list[str], limit: int | None = None, uuid_cursor: str | None = None):
        return await driver.episodic_edge_ops.get_by_group_ids(driver, group_ids, limit, uuid_cursor)

    async def community_edge_get_by_uuid(self, _cls: Any, driver: Any, uuid: str):
        return await driver.community_edge_ops.get_by_uuid(driver, uuid)

    async def community_edge_get_by_uuids(self, _cls: Any, driver: Any, uuids: list[str]):
        return await driver.community_edge_ops.get_by_uuids(driver, uuids)

    async def community_edge_get_by_group_ids(self, _cls: Any, driver: Any, group_ids: list[str], limit: int | None = None, uuid_cursor: str | None = None):
        return await driver.community_edge_ops.get_by_group_ids(driver, group_ids, limit, uuid_cursor)

    async def community_edge_delete(self, edge: CommunityEdge, driver: Any):
        return await driver.community_edge_ops.delete(driver, edge)

    async def community_edge_delete_by_uuids(self, _cls: Any, driver: Any, uuids: list[str]):
        return await driver.community_edge_ops.delete_by_uuids(driver, uuids)

    async def has_episode_edge_delete(self, edge: HasEpisodeEdge, driver: Any):
        return await driver.has_episode_edge_ops.delete(driver, edge)

    async def has_episode_edge_save_bulk(self, _cls: Any, driver: Any, transaction: Any, edges: list[Any], batch_size: int = 100):
        await driver.has_episode_edge_ops.save_bulk(driver, edges, transaction, batch_size)

    async def next_episode_edge_delete(self, edge: NextEpisodeEdge, driver: Any):
        return await driver.next_episode_edge_ops.delete(driver, edge)

    async def next_episode_edge_save_bulk(self, _cls: Any, driver: Any, transaction: Any, edges: list[Any], batch_size: int = 100):
        await driver.next_episode_edge_ops.save_bulk(driver, edges, transaction, batch_size)

    async def node_get_by_names(self, _cls: Any, driver: Any, names: list[str], group_ids: list[str]):
        records, _, _ = await driver.execute_query(
            "SELECT * FROM entity_node WHERE name IN $names AND group_id IN $groups",
            params={"names": names, "groups": group_ids},
        )
        return [EntityNode.model_validate(row) for row in records]

    async def node_get_by_attribute_values(self, _cls: Any, driver: Any, group_id: str, attributes: dict[str, Any], node_labels: list[str] | None = None, limit: int = 100):
        nodes = await driver.entity_node_ops.get_by_group_ids(driver, [group_id], limit)
        return [node for node in nodes if all((node.attributes or {}).get(k) == v for k, v in attributes.items()) and (node_labels is None or set(node_labels).issubset(set(node.labels)))]

    async def episodic_node_get_by_entity_node_uuid(self, _cls: Any, driver: Any, entity_node_uuid: str):
        return await driver.episode_node_ops.get_by_entity_node_uuid(driver, entity_node_uuid)

    async def get_mentioned_nodes(self, driver: Any, episodes: list[Any]):
        episode_ids = [episode.uuid for episode in episodes]
        if not episode_ids:
            return []
        records, _, _ = await driver.execute_query(
            "SELECT target_node_uuid FROM episodic_edge WHERE source_node_uuid IN $episode_ids",
            params={"episode_ids": episode_ids},
        )
        uuids = list({row["target_node_uuid"] for row in records if row.get("target_node_uuid")})
        return await driver.entity_node_ops.get_by_uuids(driver, uuids)

    async def episodic_node_save_bulk(self, _cls: Any, driver: Any, transaction: Any, nodes: list[Any], batch_size: int = 100):
        await driver.episode_node_ops.save_bulk(driver, nodes, transaction, batch_size)

    async def episodic_edge_save_bulk(self, _cls: Any, driver: Any, transaction: Any, episodic_edges: list[Any], batch_size: int = 100):
        await driver.episodic_edge_ops.save_bulk(driver, episodic_edges, transaction, batch_size)

    async def edge_save_bulk(self, _cls: Any, driver: Any, transaction: Any, edges: list[Any], batch_size: int = 100):
        await driver.entity_edge_ops.save_bulk(driver, edges, transaction, batch_size)

    async def node_save(self, node: Any, driver: Any) -> None:
        if isinstance(node, EntityNode): return await driver.entity_node_ops.save(driver, node)
        if isinstance(node, CommunityNode): return await driver.community_node_ops.save(driver, node)
        if isinstance(node, SagaNode): return await driver.saga_node_ops.save(driver, node)
        raise TypeError(f"Unsupported node type: {type(node).__name__}")

    async def node_delete(self, node: Any, driver: Any) -> None:
        if isinstance(node, EntityNode): return await driver.entity_node_ops.delete(driver, node)
        if isinstance(node, CommunityNode): return await driver.community_node_ops.delete(driver, node)
        if isinstance(node, SagaNode): return await driver.saga_node_ops.delete(driver, node)
        raise TypeError(f"Unsupported node type: {type(node).__name__}")

    async def node_get_by_uuid(self, _cls: Any, driver: Any, uuid: str) -> Any:
        return await driver.entity_node_ops.get_by_uuid(driver, uuid)

    async def node_get_by_uuids(self, _cls: Any, driver: Any, uuids: list[str], group_id: str | None = None):
        return await driver.entity_node_ops.get_by_uuids(driver, uuids)

    async def node_save_bulk(self, _cls: Any, driver: Any, transaction: Any, nodes: list[Any], batch_size: int = 100):
        await driver.entity_node_ops.save_bulk(driver, nodes, transaction, batch_size)

    async def episodic_node_save(self, node: EpisodicNode, driver: Any) -> None:
        await driver.episode_node_ops.save(driver, node)

    async def episodic_node_delete(self, node: EpisodicNode, driver: Any) -> None:
        await driver.episode_node_ops.delete(driver, node)

    async def episodic_node_get_by_uuid(self, _cls: Any, driver: Any, uuid: str):
        return await driver.episode_node_ops.get_by_uuid(driver, uuid)

    async def episodic_node_get_by_uuids(self, _cls: Any, driver: Any, uuids: list[str]):
        return await driver.episode_node_ops.get_by_uuids(driver, uuids)

    async def episodic_node_get_by_group_ids(self, _cls: Any, driver: Any, group_ids: list[str], limit: int | None = None, uuid_cursor: str | None = None):
        return await driver.episode_node_ops.get_by_group_ids(driver, group_ids, limit, uuid_cursor)

    async def retrieve_episodes(self, driver: Any, reference_time: Any, last_n: int = 3, group_ids: list[str] | None = None, source: str | None = None, saga: str | None = None):
        return await driver.episode_node_ops.retrieve_episodes(driver, reference_time, last_n, group_ids, source, saga)

    async def community_node_save(self, node: CommunityNode, driver: Any):
        return await driver.community_node_ops.save(driver, node)

    async def community_node_get_by_uuid(self, _cls: Any, driver: Any, uuid: str):
        return await driver.community_node_ops.get_by_uuid(driver, uuid)

    async def saga_node_save(self, node: SagaNode, driver: Any):
        return await driver.saga_node_ops.save(driver, node)

    async def saga_node_delete(self, node: SagaNode, driver: Any):
        return await driver.saga_node_ops.delete(driver, node)

    async def edge_save(self, edge: EntityEdge, driver: Any):
        return await driver.entity_edge_ops.save(driver, edge)

    async def edge_delete(self, edge: Any, driver: Any):
        return await driver.entity_edge_ops.delete(driver, edge)

    async def edge_get_by_uuid(self, _cls: Any, driver: Any, uuid: str):
        return await driver.entity_edge_ops.get_by_uuid(driver, uuid)

    async def edge_get_by_uuids(self, _cls: Any, driver: Any, uuids: list[str]):
        return await driver.entity_edge_ops.get_by_uuids(driver, uuids)

    async def edge_get_between_nodes(self, _cls: Any, driver: Any, source_node_uuid: str, target_node_uuid: str):
        return await driver.entity_edge_ops.get_between_nodes(driver, source_node_uuid, target_node_uuid)

    async def edge_get_by_node_uuid(self, _cls: Any, driver: Any, node_uuid: str):
        return await driver.entity_edge_ops.get_by_node_uuid(driver, node_uuid)

    async def episodic_edge_save(self, edge: EpisodicEdge, driver: Any):
        return await driver.episodic_edge_ops.save(driver, edge)

    async def episodic_edge_get_by_uuid(self, _cls: Any, driver: Any, uuid: str):
        return await driver.episodic_edge_ops.get_by_uuid(driver, uuid)

    async def episodic_edge_get_by_uuids(self, _cls: Any, driver: Any, uuids: list[str]):
        return await driver.episodic_edge_ops.get_by_uuids(driver, uuids)

    async def community_edge_save(self, edge: CommunityEdge, driver: Any):
        return await driver.community_edge_ops.save(driver, edge)

    async def has_episode_edge_save(self, edge: HasEpisodeEdge, driver: Any):
        return await driver.has_episode_edge_ops.save(driver, edge)

    async def next_episode_edge_save(self, edge: NextEpisodeEdge, driver: Any):
        return await driver.next_episode_edge_ops.save(driver, edge)
