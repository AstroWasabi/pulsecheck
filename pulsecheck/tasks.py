"""
pulsecheck/tasks.py

LightQueue tasks for PulseCheck:
- check_endpoint: measures HTTP latency, updates DB, triggers alerts, and self-reschedules.
- send_alert: logs/emits alert notifications when an endpoint goes down.
"""

import httpx
import logging
import time
from typing import Optional, Tuple
from lightqueue.app import LightQueue
from pulsecheck.db import Database

logger = logging.getLogger("pulsecheck.tasks")

queue: Optional[LightQueue] = None
db: Optional[Database] = None


async def perform_http_ping(url: str, timeout: float = 5.0) -> Tuple[int, float, Optional[str]]:
    """
    Performs HTTP GET request and measures roundtrip latency in milliseconds.
    Returns: (status_code, latency_ms, error_message)
    """
    start_time = time.time()
    try:
        async with httpx.AsyncClient(follow_redirects=True) as client:
            response = await client.get(url, timeout=timeout)
            duration_ms = (time.time() - start_time) * 1000.0
            return response.status_code, round(duration_ms, 2), None
    except httpx.TimeoutException:
        duration_ms = (time.time() - start_time) * 1000.0
        return 0, round(duration_ms, 2), "Connection timed out"
    except httpx.ConnectError:
        duration_ms = (time.time() - start_time) * 1000.0
        return 0, round(duration_ms, 2), "Failed to connect to host (DNS or connection refused)"
    except Exception as exc:
        duration_ms = (time.time() - start_time) * 1000.0
        return 0, round(duration_ms, 2), f"Network error: {str(exc)}"


def init_tasks(app_queue: LightQueue, app_db: Database):
    """
    Registers LightQueue tasks with the active queue and database instances.
    """
    global queue, db
    queue = app_queue
    db = app_db

    @queue.task(name="pulsecheck.send_alert", max_retries=2)
    async def send_alert(monitor_id: int, error_message: str):
        """Dispatches an alert when an endpoint goes down."""
        monitor = await db.get_monitor(monitor_id)
        name = monitor["name"] if monitor else f"Monitor #{monitor_id}"
        logger.error(f"🚨 [ALERT] {name} is DOWN! Reason: {error_message}")
        print(f"\n🚨 [PULSECHECK ALERT] {name} is DOWN! Reason: {error_message}\n")
        return f"Alert dispatched for {name}"

    @queue.task(name="pulsecheck.check_endpoint", max_retries=1, timeout=15.0)
    async def check_endpoint(monitor_id: int):
        """
        Executes ping check for a monitor, records results, and self-reschedules.
        """
        monitor = await db.get_monitor(monitor_id)
        if not monitor:
            logger.info(f"Monitor #{monitor_id} was deleted; stopping schedule.")
            return None

        if not monitor["is_active"]:
            logger.info(f"Monitor '{monitor['name']}' is paused; stopping schedule.")
            return None

        url = monitor["url"]
        timeout_sec = monitor.get("timeout_seconds", 5.0)
        status_code, latency_ms, error = await perform_http_ping(url, timeout=timeout_sec)

        is_up = (200 <= status_code < 400)
        new_status = "up" if is_up else "down"

        # Record check result
        await db.record_result(
            monitor_id=monitor_id,
            status_code=status_code,
            latency_ms=latency_ms,
            is_up=is_up,
            error=error,
        )

        # Detect failure transition to trigger alert
        prev_status = monitor["status"]
        if not is_up and prev_status != "down":
            await send_alert.delay(monitor_id, error or f"HTTP {status_code}")

        # Update monitor status in DB
        await db.update_monitor_status(monitor_id, new_status)

        # Self-reschedule next check into LightQueue
        interval = monitor["interval_seconds"]
        await check_endpoint.apply_async(args=[monitor_id], countdown=interval)

        return {
            "monitor_id": monitor_id,
            "status": new_status,
            "status_code": status_code,
            "latency_ms": latency_ms,
        }

    return {
        "check_endpoint": check_endpoint,
        "send_alert": send_alert,
    }
