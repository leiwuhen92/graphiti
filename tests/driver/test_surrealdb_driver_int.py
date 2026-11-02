"""Real SurrealDB integration smoke tests.

Run with:
    docker compose -f docker-compose.surrealdb-test.yml up -d
    SURREALDB_TEST_URL=ws://localhost:8000/rpc pytest -m integration tests/driver/test_surrealdb_driver_int.py
"""

from __future__ import annotations

import os
from uuid import uuid4

import pytest
import pytest_asyncio

from graphiti_core.driver.surrealdb_driver import SurrealDBDriver
from graphiti_core.nodes import EntityNode
from graphiti_core.search.search_filters import SearchFilters


pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.integration,
]


@pytest_asyncio.fixture
async def driver():
    url = os.getenv("SURREALDB_TEST_URL", "ws://localhost:8000/rpc")
    db = SurrealDBDriver(
        url,
        os.getenv("SURREALDB_TEST_USER", "root"),
        os.getenv("SURREALDB_TEST_PASSWORD", "root"),
        namespace=os.getenv("SURREALDB_TEST_NAMESPACE", "graphiti_test"),
        database=os.getenv("SURREALDB_TEST_DATABASE", "graphiti_test"),
    )
    await db.build_indices_and_constraints()
    yield db
    await db.close()


async def test_entity_node_round_trip_and_similarity_search(driver: SurrealDBDriver):
    node = EntityNode(
        uuid=str(uuid4()),
        name="SurrealDB integration test entity",
        group_id=f"test-{uuid4()}",
        summary="A persisted entity used by the real database integration test.",
        name_embedding=[1.0, 0.0, 0.0],
        attributes={"origin": "integration-test"},
    )

    await node.save(driver)
    loaded = await EntityNode.get_by_uuid(driver, node.uuid)

    assert loaded.uuid == node.uuid
    assert loaded.name == node.name
    assert loaded.name_embedding == [1.0, 0.0, 0.0]
    assert loaded.attributes == {"origin": "integration-test"}

    results = await driver.search_interface.node_similarity_search(
        driver,
        [1.0, 0.0, 0.0],
        search_filter=SearchFilters(),
        group_ids=[node.group_id],
        min_score=0.99,
    )
    assert [item.uuid for item in results] == [node.uuid]

    await driver.entity_node_ops.delete(driver, node)
