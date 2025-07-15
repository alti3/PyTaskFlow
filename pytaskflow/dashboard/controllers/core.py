"""Core dashboard routes."""
from typing import TYPE_CHECKING

from litestar import Controller, get
from litestar.response import Template
from litestar.di import Provide

if TYPE_CHECKING:
    from pytaskflow.client import Client


class CoreController(Controller):
    path = "/"
    dependencies = {"client": Provide(lambda state: state.client)}

    @get()
    async def home(self, client: "Client") -> Template:
        stats = client.get_state_counts()
        return Template(name="index.html.j2", context={"stats": stats})

    @get("/servers")
    async def servers(self, client: "Client") -> Template:
        servers_list = client.get_servers()
        return Template(name="servers.html.j2", context={"servers": servers_list})

