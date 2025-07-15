"""Job-related dashboard routes."""
from typing import TYPE_CHECKING
from uuid import UUID

from litestar import Controller, get
from litestar.response import Template
from litestar.di import Provide
from litestar.params import Parameter

if TYPE_CHECKING:
    from pytaskflow.client import Client


class JobsController(Controller):
    path = "/jobs"
    dependencies = {"client": Provide(lambda state: state.client)}

    @get("/{status:str}")
    async def list_jobs_by_status(
        self,
        client: "Client",
        status: str,
        page: int = Parameter(query="page", default=1),
    ) -> Template:
        jobs = client.get_jobs_by_state(status, page=page)
        return Template(
            name="jobs_list.html.j2",
            context={"jobs": jobs, "status": status, "page": page},
        )

    @get("/details/{job_id:uuid}")
    async def job_details(self, client: "Client", job_id: UUID) -> Template:
        job = client.get_job_details(str(job_id))
        return Template(name="job_details.html.j2", context={"job": job})

    @get("/recurring")
    async def recurring_jobs(
        self, client: "Client", page: int = Parameter(query="page", default=1)
    ) -> Template:
        jobs = client.get_recurring_jobs(page=page)
        return Template(name="recurring.html.j2", context={"jobs": jobs, "page": page})
