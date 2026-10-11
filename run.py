"""
run.py

Unified entrypoint that starts both the PulseCheck FastAPI web dashboard
and the LightQueue worker coordinator in a single asynchronous process.
"""

import asyncio
import logging
import sys
import uvicorn
from lightqueue.app import LightQueue
from lightqueue.broker.memory import InMemoryBroker
from lightqueue.worker.coordinator import WorkerCoordinator
from pulsecheck.db import Database
from pulsecheck.web import create_app

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("pulsecheck.launcher")


async def main():
    print("=" * 65)
    print("⚡ PulseCheck – Live Distributed Uptime & Telemetry Engine")
    print("=" * 65)

    # 1. Initialize SQLite Database
    db = Database("pulsecheck.db")
    await db.init_db()

    # 2. Initialize LightQueue with in-memory broker
    broker = InMemoryBroker()
    await broker.connect()
    queue = LightQueue(broker=broker)

    # 3. Create FastAPI app
    app = create_app(db=db, queue=queue)

    # 4. Initialize Worker Coordinator with 4 workers
    coordinator = WorkerCoordinator(
        broker=broker,
        concurrency=4,
        poll_interval=0.1,
    )
    coordinator.install_signal_handlers()

    # 5. Run Worker Coordinator in background task
    worker_task = asyncio.create_task(coordinator.run())

    # 6. Configure and start Uvicorn web server
    config = uvicorn.Config(
        app=app,
        host="127.0.0.1",
        port=8000,
        log_level="info",
    )
    server = uvicorn.Server(config)

    print("\n🌐 Dashboard running at: http://localhost:8000")
    print("⚙️  LightQueue Worker running with concurrency = 4")
    print("🛑 Press Ctrl+C to safely exit.\n")

    try:
        await server.serve()
    finally:
        logger.info("Stopping LightQueue coordinator...")
        await coordinator.stop()
        await worker_task
        await broker.disconnect()
        await db.close()
        logger.info("PulseCheck cleanly stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
