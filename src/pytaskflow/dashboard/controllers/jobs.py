"""Job-related dashboard routes."""

import json
from uuid import UUID

from litestar import Controller, Request, get, post
from litestar.response import Redirect, Template
from litestar.di import Provide
from litestar.datastructures import State
from litestar.params import Parameter
from litestar.status_codes import HTTP_303_SEE_OTHER

from pytaskflow.client import Client

PAGE_SIZE = 20


def get_client(state: State) -> Client:
    return state.client


class JobsController(Controller):
    path = "/jobs"
    dependencies = {"client": Provide(get_client, sync_to_thread=False)}

    @get("/{status:str}", name="list_jobs_by_status")
    async def list_jobs_by_status(
        self,
        client: "Client",
        status: str,
        page: int = Parameter(query="page", default=1),
        recovered: int | None = Parameter(query="recovered", default=None),
    ) -> Template:
        jobs = client.get_jobs_by_state(status, page=page, page_size=PAGE_SIZE)
        state_counts = client.get_state_counts()
        total_count = state_counts.get(status, 0)
        total_pages = max(1, (total_count + PAGE_SIZE - 1) // PAGE_SIZE)
        has_prev = page > 1
        has_next = page < total_pages
        return Template(
            template_name="jobs_list.html.j2",
            context={
                "jobs": jobs,
                "status": status,
                "page": page,
                "total_pages": total_pages,
                "has_prev": has_prev,
                "has_next": has_next,
                "state_counts": state_counts,
                "active_page": "jobs",
                "active_status": status,
                "supports_recovery": hasattr(client.storage, "recover_stuck_jobs"),
                "recovered_count": recovered,
            },
        )

    @get("/details/{job_id:uuid}", name="job_details")
    async def job_details(self, client: "Client", job_id: UUID) -> Template:
        job = client.get_job_details(str(job_id))
        args = []
        kwargs = {}
        state_data = {}
        history = []
        state_counts = client.get_state_counts()
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
            template_name="job_details.html.j2",
            context={
                "job": job,
                "args": args,
                "kwargs": kwargs,
                "state_data": state_data,
                "history": history,
                "state_counts": state_counts,
                "active_page": "jobs",
                "active_status": job.state_name if job else "",
            },
        )

    @get("/recurring", name="recurring_jobs")
    async def recurring_jobs(
        self, client: "Client", page: int = Parameter(query="page", default=1)
    ) -> Template:
        jobs = client.get_recurring_jobs(page=page, page_size=PAGE_SIZE)
        state_counts = client.get_state_counts()
        return Template(
            template_name="recurring.html.j2",
            context={
                "jobs": jobs,
                "page": page,
                "state_counts": state_counts,
                "active_page": "recurring",
            },
        )

    @post("/failed/requeue", name="requeue_failed_job")
    async def requeue_failed_job(self, client: "Client", request: Request) -> Redirect:
        form = await request.form()
        job_id = form.get("job_id")
        if job_id:
            client.requeue(str(job_id))
        return Redirect(path="/jobs/Failed", status_code=HTTP_303_SEE_OTHER)

    @post("/failed/delete", name="delete_failed_job")
    async def delete_failed_job(self, client: "Client", request: Request) -> Redirect:
        form = await request.form()
        job_id = form.get("job_id")
        if job_id:
            client.delete(str(job_id))
        return Redirect(path="/jobs/Failed", status_code=HTTP_303_SEE_OTHER)

    @post("/recover-stuck", name="recover_stuck_jobs")
    async def recover_stuck_jobs(self, client: "Client", request: Request) -> Redirect:
        if not hasattr(client.storage, "recover_stuck_jobs"):
            return Redirect(path="/jobs/Processing", status_code=HTTP_303_SEE_OTHER)
        form = await request.form()
        max_age_seconds = form.get("max_age_seconds", 300)
        limit = form.get("limit", 100)
        try:
            max_age_seconds = int(max_age_seconds)
        except (TypeError, ValueError):
            max_age_seconds = 300
        try:
            limit = int(limit)
        except (TypeError, ValueError):
            limit = 100
        recovered = client.recover_stuck_jobs(
            max_age_seconds=max_age_seconds, limit=limit
        )
        return Redirect(
            path=f"/jobs/Processing?recovered={len(recovered)}",
            status_code=HTTP_303_SEE_OTHER,
        )
