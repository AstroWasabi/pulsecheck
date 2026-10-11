"""
tests/test_tasks.py
"""

import pytest
import pytest_asyncio
from unittest.mock import patch
from lightqueue.app import LightQueue
from lightqueue.broker.memory import InMemoryBroker
from pulsecheck.db import Database
from pulsecheck.tasks import init_tasks


@pytest_asyncio.fixture
async def app_and_db(tmp_path):
    db_file = str(tmp_path / "test_tasks.db")
    db = Database(db_file)
    await db.init_db()

    broker = InMemoryBroker()
    await broker.connect()
    queue = LightQueue(broker=broker)

    tasks = init_tasks(app_queue=queue, app_db=db)
    yield tasks, db, broker
    await db.close()


@pytest.mark.asyncio
async def test_check_endpoint_success_and_self_reschedule(app_and_db):
    tasks, db, broker = app_and_db
    m_id = await db.create_monitor(name="Example", url="https://example.com", interval=10)

    with patch("pulsecheck.tasks.perform_http_ping") as mock_ping:
        mock_ping.return_value = (200, 42.5, None)

        result = await tasks["check_endpoint"](m_id)

        assert result["status"] == "up"
        assert result["status_code"] == 200
        assert result["latency_ms"] == 42.5

        stats = await db.get_monitor_stats(m_id)
        assert stats["total_checks"] == 1
        assert stats["current_status"] == "up"

        assert len(broker.scheduled_heap) == 1
        _, _, next_task = broker.scheduled_heap[0]
        assert next_task.args == [m_id]
        assert next_task.name == "pulsecheck.check_endpoint"


@pytest.mark.asyncio
async def test_check_endpoint_failure_triggers_alert(app_and_db):
    tasks, db, broker = app_and_db
    m_id = await db.create_monitor(name="DownService", url="https://down.test", interval=20)

    with patch("pulsecheck.tasks.perform_http_ping") as mock_ping:
        mock_ping.return_value = (0, 5000.0, "Connection timed out")

        result = await tasks["check_endpoint"](m_id)

        assert result["status"] == "down"

        monitor = await db.get_monitor(m_id)
        assert monitor["status"] == "down"

        assert broker._ready_queue.qsize() == 1
        alert_task = await broker._ready_queue.get()
        assert alert_task.name == "pulsecheck.send_alert"
        assert alert_task.args[0] == m_id
