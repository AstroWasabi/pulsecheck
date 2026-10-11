"""
tests/test_web.py
"""

import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from lightqueue.app import LightQueue
from lightqueue.broker.memory import InMemoryBroker
from pulsecheck.db import Database
from pulsecheck.web import create_app


@pytest_asyncio.fixture
async def web_client(tmp_path):
    db_file = str(tmp_path / "test_web.db")
    db = Database(db_file)
    await db.init_db()

    broker = InMemoryBroker()
    await broker.connect()
    queue = LightQueue(broker=broker)

    app = create_app(db=db, queue=queue)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client, db, broker
    await db.close()


@pytest.mark.asyncio
async def test_get_dashboard_empty(web_client):
    client, _, _ = web_client
    resp = await client.get("/")
    assert resp.status_code == 200
    assert "PulseCheck" in resp.text
    assert "No monitors configured" in resp.text


@pytest.mark.asyncio
async def test_monitor_crud_via_web(web_client):
    client, db, broker = web_client

    # 1. Add monitor via POST form
    create_resp = await client.post(
        "/monitors",
        data={
            "name": "GitHub Status",
            "url": "https://www.githubstatus.com",
            "interval_seconds": 60,
            "timeout_seconds": 5.0,
        },
        follow_redirects=False,
    )
    assert create_resp.status_code == 303

    # Check database
    monitors = await db.get_all_monitors()
    assert len(monitors) == 1
    m_id = monitors[0]["id"]
    assert monitors[0]["name"] == "GitHub Status"

    # Check LightQueue received the kickoff ping task
    assert broker._ready_queue.qsize() == 1

    # 2. History API endpoint returns valid JSON list
    hist_resp = await client.get(f"/api/monitors/{m_id}/history")
    assert hist_resp.status_code == 200
    assert isinstance(hist_resp.json(), list)

    # 3. Toggle pause/resume
    toggle_resp = await client.post(f"/monitors/{m_id}/toggle", follow_redirects=False)
    assert toggle_resp.status_code == 303
    m_after_toggle = await db.get_monitor(m_id)
    assert m_after_toggle["is_active"] == 0

    # 4. Delete monitor
    del_resp = await client.post(f"/monitors/{m_id}/delete", follow_redirects=False)
    assert del_resp.status_code == 303
    assert len(await db.get_all_monitors()) == 0
