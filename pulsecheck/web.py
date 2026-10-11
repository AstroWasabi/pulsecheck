"""
pulsecheck/web.py

FastAPI application providing the web dashboard, form handlers,
and REST telemetry endpoints for PulseCheck.
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from pulsecheck.db import Database
from lightqueue.app import LightQueue
from pulsecheck.tasks import init_tasks

TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def create_app(db: Database, queue: LightQueue) -> FastAPI:
    """
    Factory function creating and configuring the FastAPI app.
    """
    tasks = init_tasks(app_queue=queue, app_db=db)
    check_endpoint = tasks["check_endpoint"]

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        """
        On server startup, initialize DB and resume monitoring active endpoints.
        """
        await db.init_db()
        active_monitors = await db.get_active_monitors()
        for m in active_monitors:
            await check_endpoint.delay(m["id"])
        yield

    app = FastAPI(title="PulseCheck", lifespan=lifespan)

    @app.get("/", response_class=HTMLResponse)
    async def get_dashboard(request: Request):
        monitors = await db.get_all_monitors()

        enriched = []
        up_count = 0
        down_count = 0
        total_latency = 0.0
        monitors_with_checks = 0

        for m in monitors:
            stats = await db.get_monitor_stats(m["id"], limit=30)
            m_dict = dict(m)
            m_dict["stats"] = stats
            enriched.append(m_dict)

            if m["is_active"]:
                if stats["current_status"] == "up":
                    up_count += 1
                elif stats["current_status"] == "down":
                    down_count += 1

                if stats["total_checks"] > 0 and stats["avg_latency_ms"] > 0:
                    total_latency += stats["avg_latency_ms"]
                    monitors_with_checks += 1

        avg_lat = round(total_latency / monitors_with_checks, 1) if monitors_with_checks > 0 else 0.0

        return templates.TemplateResponse(
            request=request,
            name="dashboard.html",
            context={
                "monitors": enriched,
                "total_count": len(monitors),
                "up_count": up_count,
                "down_count": down_count,
                "avg_latency": avg_lat,
            },
        )

    @app.post("/monitors")
    async def add_monitor(
        name: str = Form(...),
        url: str = Form(...),
        interval_seconds: int = Form(30),
        timeout_seconds: float = Form(5.0),
    ):
        """Creates a new monitor and immediately kicks off its first check."""
        monitor_id = await db.create_monitor(
            name=name.strip(),
            url=url.strip(),
            interval=interval_seconds,
            timeout=timeout_seconds,
        )
        await check_endpoint.delay(monitor_id)
        return RedirectResponse(url="/", status_code=303)

    @app.post("/monitors/{monitor_id}/toggle")
    async def toggle_monitor(monitor_id: int):
        """Pauses or resumes an endpoint's monitoring loop."""
        new_state = await db.toggle_monitor(monitor_id)
        if new_state == 1:
            await check_endpoint.delay(monitor_id)
        return RedirectResponse(url="/", status_code=303)

    @app.post("/monitors/{monitor_id}/delete")
    async def delete_monitor(monitor_id: int):
        """Deletes a monitor and its history."""
        await db.delete_monitor(monitor_id)
        return RedirectResponse(url="/", status_code=303)

    @app.get("/api/monitors/{monitor_id}/history")
    async def get_monitor_history(monitor_id: int):
        """Returns JSON array of recent checks for Chart.js."""
        stats = await db.get_monitor_stats(monitor_id, limit=30)
        return JSONResponse(content=stats.get("recent_results", []))

    return app
