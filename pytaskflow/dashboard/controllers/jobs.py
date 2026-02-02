"""Job-related dashboard routes."""
import json
from typing import TYPE_CHECKING
from uuid import UUID

from litestar import Controller, get
from litestar.response import Template
from litestar.di import Provide
from litestar.params import Parameter

if TYPE_CHECKING:
    from pytaskflow.client import Client

PAGE_SIZE = 20


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
        jobs = client.get_jobs_by_state(status, page=page, page_size=PAGE_SIZE)
        total_count = client.get_state_counts().get(status, 0)
        total_pages = max(1, (total_count + PAGE_SIZE - 1) // PAGE_SIZE)
        has_prev = page > 1
        has_next = page < total_pages
        return Template(
            name="jobs_list.html.j2",
            context={
                "jobs": jobs,
                "status": status,
                "page": page,
                "total_pages": total_pages,
                "has_prev": has_prev,
                "has_next": has_next,
            },
        )

    @get("/details/{job_id:uuid}")
    async def job_details(self, client: "Client", job_id: UUID) -> Template:
        job = client.get_job_details(str(job_id))
        args = []
        kwargs = {}
        state_data = {}
        history = []
        if job:
            try:
                args, kwargs = client.serializer.deserialize_args(job.args, job.kwargs)
            except Exception:
                args, kwargs = [], {}
            state_data = job.state_data or {}
            history = client.storage.get_job_history(job.id)

            args = json.loads(json.dumps(args, default=str))
            kwargs = json.loads(json.dumps(kwargs, default=str))
            state_data = json.loads(json.dumps(state_data, default=str))
            history = json.loads(json.dumps(history, default=str))

        return Template(
            name="job_details.html.j2",
            context={
                "job": job,
                "args": args,
                "kwargs": kwargs,
                "state_data": state_data,
                "history": history,
            },
        )

    @get("/recurring")
    async def recurring_jobs(
        self, client: "Client", page: int = Parameter(query="page", default=1)
    ) -> Template:
        jobs = client.get_recurring_jobs(page=page, page_size=PAGE_SIZE)
        return Template(
            name="recurring.html.j2",
            context={"jobs": jobs, "page": page},
        )
