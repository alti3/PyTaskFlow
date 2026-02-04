"""Core dashboard routes."""
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

    @get(name="home")
    async def home(self, client: "Client") -> Template:
        stats = client.get_state_counts()
        return Template(
            template_name="index.html.j2",
            context={"stats": stats, "state_counts": stats, "active_page": "home"},
        )

    @get("/servers", name="servers")
    async def servers(self, client: "Client") -> Template:
        servers_list = client.get_servers()
        state_counts = client.get_state_counts()
        return Template(
            template_name="servers.html.j2",
            context={
                "servers": servers_list,
                "state_counts": state_counts,
                "active_page": "servers",
            },
        )
