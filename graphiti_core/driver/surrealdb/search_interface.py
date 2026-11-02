"""SurrealDB search adapter used by Graphiti's legacy search dispatch."""

from __future__ import annotations

import math
from typing import Any

from graphiti_core.driver.search_interface.search_interface import SearchInterface
from graphiti_core.nodes import CommunityNode, EntityNode, EpisodicNode
from graphiti_core.edges import EntityEdge


def _cosine(a: list[float] | None, b: list[float]) -> float:
    if not a or len(a) != len(b):
        return 0.0
    denom = math.sqrt(sum(x * x for x in a)) * math.sqrt(sum(x * x for x in b))
    return sum(x * y for x, y in zip(a, b, strict=True)) / denom if denom else 0.0


async def _rows(driver: Any, table: str, group_ids: list[str] | None = None):
    query = f"SELECT * FROM {table}"
    params = {}
    if group_ids is not None:
        query += " WHERE group_id IN $group_ids"
        params["group_ids"] = group_ids
    records, _, _ = await driver.execute_query(query, params=params)
    return records


class SurrealDBSearchInterface(SearchInterface):
    async def node_similarity_search(
        self, driver, search_vector, search_filter, group_ids=None, limit=100, min_score=0.7
    ):
        rows = await _rows(driver, "entity_node", group_ids)
        candidates = []
        for row in rows:
            score = _cosine(row.get("name_embedding"), search_vector)
            if score > min_score:
                candidates.append((score, EntityNode.model_validate(row)))
        candidates.sort(key=lambda item: item[0], reverse=True)
        return [node for _, node in candidates[:limit]]

    async def node_fulltext_search(self, driver, query, search_filter, group_ids=None, limit=100):
        rows = await _rows(driver, "entity_node", group_ids)
        terms = [term.casefold() for term in query.split() if term]
        matches = []
        for row in rows:
            text = f"{row.get('name', '')} {row.get('summary', '')}".casefold()
            score = sum(text.count(term) for term in terms)
            if score:
                matches.append((score, EntityNode.model_validate(row)))
        matches.sort(key=lambda item: item[0], reverse=True)
        return [node for _, node in matches[:limit]]

    async def edge_similarity_search(
        self, driver, search_vector, source_node_uuid, target_node_uuid,
        search_filter, group_ids=None, limit=100, min_score=0.7
    ):
        rows = await _rows(driver, "entity_edge", group_ids)
        matches = []
        for row in rows:
            if source_node_uuid and row.get("source_node_uuid") != source_node_uuid: continue
            if target_node_uuid and row.get("target_node_uuid") != target_node_uuid: continue
            score = _cosine(row.get("fact_embedding"), search_vector)
            if score > min_score: matches.append((score, EntityEdge.model_validate(row)))
        matches.sort(key=lambda item: item[0], reverse=True)
        return [edge for _, edge in matches[:limit]]

    async def edge_fulltext_search(self, driver, query, search_filter, group_ids=None, limit=100):
        rows = await _rows(driver, "entity_edge", group_ids)
        terms = [term.casefold() for term in query.split() if term]
        matches = []
        for row in rows:
            text = f"{row.get('name', '')} {row.get('fact', '')}".casefold()
            score = sum(text.count(term) for term in terms)
            if score: matches.append((score, EntityEdge.model_validate(row)))
        matches.sort(key=lambda item: item[0], reverse=True)
        return [edge for _, edge in matches[:limit]]

    async def episode_fulltext_search(self, driver, query, search_filter, group_ids=None, limit=100):
        rows = await _rows(driver, "episodic_node", group_ids)
        terms = [term.casefold() for term in query.split() if term]
        matches = []
        for row in rows:
            text = f"{row.get('name', '')} {row.get('content', '')}".casefold()
            score = sum(text.count(term) for term in terms)
            if score: matches.append((score, EpisodicNode.model_validate(row)))
        matches.sort(key=lambda item: item[0], reverse=True)
        return [episode for _, episode in matches[:limit]]

    async def community_fulltext_search(self, driver, query, group_ids=None, limit=100):
        rows = await _rows(driver, "community_node", group_ids)
        terms = [term.casefold() for term in query.split() if term]
        matches = []
        for row in rows:
            text = f"{row.get('name', '')} {row.get('summary', '')}".casefold()
            score = sum(text.count(term) for term in terms)
            if score: matches.append((score, CommunityNode.model_validate(row)))
        matches.sort(key=lambda item: item[0], reverse=True)
        return [node for _, node in matches[:limit]]

    async def community_similarity_search(self, driver, search_vector, group_ids=None, limit=100, min_score=0.6):
        rows = await _rows(driver, "community_node", group_ids)
        matches = []
        for row in rows:
            score = _cosine(row.get("name_embedding"), search_vector)
            if score > min_score: matches.append((score, CommunityNode.model_validate(row)))
        matches.sort(key=lambda item: item[0], reverse=True)
        return [node for _, node in matches[:limit]]

    async def get_embeddings_for_communities(self, driver, communities):
        rows = await driver.community_node_ops.get_by_uuids(driver, [c.uuid for c in communities])
        return {row.uuid: row.name_embedding for row in rows if row.name_embedding is not None}

    async def node_distance_reranker(self, driver, node_uuids, center_node_uuid, min_score=0):
        edges = await _rows(driver, "entity_edge")
        adjacency = {}
        for edge in edges:
            a, b = edge.get("source_node_uuid"), edge.get("target_node_uuid")
            adjacency.setdefault(a, set()).add(b)
            adjacency.setdefault(b, set()).add(a)
        distances = {center_node_uuid: 0}
        frontier = [center_node_uuid]
        while frontier:
            current = frontier.pop(0)
            for neighbor in adjacency.get(current, set()):
                if neighbor not in distances:
                    distances[neighbor] = distances[current] + 1
                    frontier.append(neighbor)
        scored = [(u, 0.1 if u == center_node_uuid else 1 / distances[u])
                  for u in node_uuids if u == center_node_uuid or u in distances]
        scored = [x for x in scored if x[1] >= min_score]
        scored.sort(key=lambda x: x[1], reverse=True)
        return [x[0] for x in scored], [x[1] for x in scored]

    async def episode_mentions_reranker(self, driver, node_uuids, min_score=0):
        ordered = []
        for group in node_uuids:
            for uuid in group:
                if uuid not in ordered: ordered.append(uuid)
        rows = await _rows(driver, "episodic_edge")
        counts = {u: 0 for u in ordered}
        for edge in rows:
            if edge.get("target_node_uuid") in counts:
                counts[edge["target_node_uuid"]] += 1
        scored = [(u, counts[u]) for u in ordered if counts[u] >= min_score]
        scored.sort(key=lambda x: x[1], reverse=True)
        return [x[0] for x in scored], [float(x[1]) for x in scored]

    def build_node_search_filters(self, search_filters):
        return search_filters

    def build_edge_search_filters(self, search_filters):
        return search_filters

    async def node_bfs_search(self, driver, bfs_origin_node_uuids, search_filter, bfs_max_depth, group_ids=None, limit=100):
        if not bfs_origin_node_uuids or bfs_max_depth < 1: return []
        edges = await _rows(driver, "entity_edge", group_ids)
        adjacency = {}
        for edge in edges:
            adjacency.setdefault(edge.get("source_node_uuid"), set()).add(edge.get("target_node_uuid"))
            adjacency.setdefault(edge.get("target_node_uuid"), set()).add(edge.get("source_node_uuid"))
        seen, frontier = set(bfs_origin_node_uuids), set(bfs_origin_node_uuids)
        for _ in range(bfs_max_depth):
            frontier = {v for u in frontier for v in adjacency.get(u, set())} - seen
            seen |= frontier
            if not frontier: break
        found = await driver.entity_node_ops.get_by_uuids(driver, list(seen - set(bfs_origin_node_uuids)))
        return found[:limit]

    async def edge_bfs_search(self, driver, bfs_origin_node_uuids, bfs_max_depth, search_filter, group_ids=None, limit=100):
        if not bfs_origin_node_uuids or bfs_max_depth < 1: return []
        edges = await _rows(driver, "entity_edge", group_ids)
        seen, frontier, found = set(bfs_origin_node_uuids), set(bfs_origin_node_uuids), []
        for _ in range(bfs_max_depth):
            next_frontier = set()
            for edge in edges:
                a, b = edge.get("source_node_uuid"), edge.get("target_node_uuid")
                if a in frontier or b in frontier:
                    found.append(EntityEdge.model_validate(edge))
                    if a not in seen: next_frontier.add(a)
                    if b not in seen: next_frontier.add(b)
            seen |= next_frontier
            frontier = next_frontier
            if not frontier: break
        return found[:limit]
