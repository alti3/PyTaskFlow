"""Litestar application factory for the PyTaskFlow dashboard."""

from pathlib import Path
from litestar import Litestar
from litestar.contrib.jinja import JinjaTemplateEngine
from litestar.static_files import create_static_files_router
from litestar.template.config import TemplateConfig

from litestar.di import Provide
from litestar.datastructures import State

from .controllers.core import CoreController
from .controllers.jobs import JobsController

from pytaskflow.client import Client


async def get_client(state: State) -> "Client":
    return state.client


def create_dashboard_app(client: "Client", debug: bool = False) -> Litestar:
    """Create the Litestar application for the dashboard.

    Args:
        client: A PyTaskFlow client instance.
        debug: Whether to enable Litestar debug mode.

    Returns:
        A Litestar application.
    """
    here = Path(__file__).parent
    static_path = here / "static"
    templates_path = here / "templates"

    return Litestar(
        route_handlers=[
            CoreController,
            JobsController,
            create_static_files_router(path="/static", directories=[static_path]),
        ],
        template_config=TemplateConfig(
            directory=templates_path,
            engine=JinjaTemplateEngine,
        ),
        state=State({"client": client}),
        dependencies={"client": Provide(get_client)},
        debug=debug,
    )
