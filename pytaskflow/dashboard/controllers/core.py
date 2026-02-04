"""Core dashboard routes."""

from datetime import UTC, datetime

from litestar import Controller, get
from litestar.response import Template
from litestar.di import Provide
from litestar.datastructures import State

from pytaskflow.client import Client


def get_client(state: State) -> Client:
    return state.client


class CoreController(Controller):
    path = "/"
    dependencies = {"client": Provide(get_client, sync_to_thread=False)}

    def _annotate_server_status(self, servers: list[dict]) -> list[dict]:
        now = datetime.now(UTC)
        annotated: list[dict] = []
        for server in servers:
            entry = dict(server)
            status = "unknown"
            last_heartbeat = server.get("last_heartbeat")
            if last_heartbeat:
                try:
                    heartbeat = datetime.fromisoformat(last_heartbeat)
                    if heartbeat.tzinfo is None:
                        heartbeat = heartbeat.replace(tzinfo=UTC)
                    age_seconds = max(0, int((now - heartbeat).total_seconds()))
                    if age_seconds <= 30:
                        status = "healthy"
                    elif age_seconds <= 60:
                        status = "stale"
                    else:
                        status = "offline"
                    entry["heartbeat_age"] = age_seconds
                except ValueError:
                    status = "unknown"
            entry["status"] = status
            annotated.append(entry)
        return annotated

    @get(name="home")
    async def home(self, client: "Client") -> Template:
        stats = client.get_state_counts()
        return Template(
            template_name="index.html.j2",
            context={"stats": stats, "state_counts": stats, "active_page": "home"},
        )

    @get("/servers", name="servers")
    async def servers(self, client: "Client") -> Template:
        servers_list = self._annotate_server_status(client.get_servers())
        state_counts = client.get_state_counts()
        return Template(
            template_name="servers.html.j2",
            context={
                "servers": servers_list,
                "state_counts": state_counts,
                "active_page": "servers",
            },
        )
