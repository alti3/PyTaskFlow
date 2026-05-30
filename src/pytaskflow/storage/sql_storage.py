# pytaskflow/storage/sql_storage.py
from __future__ import annotations

import json
import logging
import time
from datetime import UTC, datetime, timedelta
from typing import Any, Dict, List, Optional

from cronsim import CronSim
from sqlalchemy import (
    DateTime,
    Engine,
    Integer,
    String,
    Text,
    create_engine,
    delete,
    func,
    select,
    update,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

from pytaskflow.common.job import Job
from pytaskflow.common.states import (
    ALL_STATES,
    AwaitingState,
    BaseState,
    DeletedState,
    EnqueuedState,
    ProcessingState,
    ScheduledState,
)
from pytaskflow.storage.base import JobStorage

logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


class JobModel(Base):
    __tablename__ = "pytaskflow_jobs"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    target_module: Mapped[str] = mapped_column(String(255))
    target_function: Mapped[str] = mapped_column(String(255))
    args: Mapped[str] = mapped_column(Text)
    kwargs: Mapped[str] = mapped_column(Text)
    state_name: Mapped[str] = mapped_column(String(50), index=True)
    state_data: Mapped[str] = mapped_column(Text, default="{}")
    queue: Mapped[str] = mapped_column(String(100), index=True, default="default")
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class JobHistoryModel(Base):
    __tablename__ = "pytaskflow_job_history"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(String(64), index=True)
    state: Mapped[str] = mapped_column(String(50), index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    data: Mapped[str] = mapped_column(Text, default="{}")


class QueueEntryModel(Base):
    __tablename__ = "pytaskflow_job_queues"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    queue: Mapped[str] = mapped_column(String(100), index=True)
    status: Mapped[str] = mapped_column(String(20), index=True)
    enqueued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    fetched_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    server_id: Mapped[Optional[str]] = mapped_column(String(100))
    worker_id: Mapped[Optional[str]] = mapped_column(String(100))


class ScheduledJobModel(Base):
    __tablename__ = "pytaskflow_scheduled_jobs"

    job_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    enqueue_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class RecurringJobModel(Base):
    __tablename__ = "pytaskflow_recurring_jobs"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    job_data: Mapped[str] = mapped_column(Text)
    cron: Mapped[str] = mapped_column(String(120))
    last_execution: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))


class ServerModel(Base):
    __tablename__ = "pytaskflow_servers"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    worker_count: Mapped[int] = mapped_column(Integer)
    queues: Mapped[str] = mapped_column(Text)
    last_heartbeat: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class SqlStorage(JobStorage):
    def __init__(
        self,
        connection_url: Optional[str] = None,
        engine: Optional[Engine] = None,
        create_tables: bool = True,
    ) -> None:
        if engine is None and connection_url is None:
            raise ValueError("connection_url or engine is required")
        self.engine = engine or create_engine(connection_url)
        self._session_factory = sessionmaker(bind=self.engine, expire_on_commit=False)
        if create_tables:
            Base.metadata.create_all(self.engine)

        self._supports_skip_locked = self.engine.dialect.name in {
            "postgresql",
            "mysql",
            "mariadb",
        }

    def _serialize_state_data(self, state_data: Any) -> str:
        if state_data is None:
            return "{}"
        if isinstance(state_data, str):
            return state_data
        return json.dumps(state_data, default=str)

    def _deserialize_state_data(self, payload: Any) -> Dict[str, Any]:
        if not payload:
            return {}
        if isinstance(payload, dict):
            return payload
        try:
            return json.loads(payload)
        except (TypeError, json.JSONDecodeError):
            return {}

    def _job_from_model(self, model: JobModel) -> Job:
        return Job(
            id=model.id,
            target_module=model.target_module,
            target_function=model.target_function,
            args=model.args,
            kwargs=model.kwargs,
            state_name=model.state_name,
            created_at=model.created_at,
            state_data=self._deserialize_state_data(model.state_data),
            queue=model.queue,
            retry_count=model.retry_count,
        )

    def _record_history(self, session: Session, job_id: str, state: BaseState) -> None:
        entry = JobHistoryModel(
            job_id=job_id,
            state=state.name,
            timestamp=datetime.now(UTC),
            data=self._serialize_state_data(state.serialize_data()),
        )
        session.add(entry)

    def _upsert_queue_entry(
        self,
        session: Session,
        job_id: str,
        queue: str,
        status: str,
        enqueued_at: Optional[datetime] = None,
        fetched_at: Optional[datetime] = None,
        server_id: Optional[str] = None,
        worker_id: Optional[str] = None,
    ) -> None:
        entry = session.execute(
            select(QueueEntryModel).where(QueueEntryModel.job_id == job_id)
        ).scalar_one_or_none()
        if entry:
            entry.queue = queue
            entry.status = status
            entry.enqueued_at = enqueued_at or entry.enqueued_at
            entry.fetched_at = fetched_at
            entry.server_id = server_id
            entry.worker_id = worker_id
            return

        session.add(
            QueueEntryModel(
                job_id=job_id,
                queue=queue,
                status=status,
                enqueued_at=enqueued_at or datetime.now(UTC),
                fetched_at=fetched_at,
                server_id=server_id,
                worker_id=worker_id,
            )
        )

    def enqueue(self, job: Job) -> str:
        with self._session_factory.begin() as session:
            session.add(
                JobModel(
                    id=job.id,
                    target_module=job.target_module,
                    target_function=job.target_function,
                    args=job.args,
                    kwargs=job.kwargs,
                    state_name=job.state_name,
                    state_data=self._serialize_state_data(job.state_data),
                    queue=job.queue,
                    retry_count=job.retry_count,
                    created_at=job.created_at,
                )
            )
            self._upsert_queue_entry(
                session,
                job_id=job.id,
                queue=job.queue,
                status="enqueued",
                enqueued_at=job.created_at,
            )
            self._record_history(session, job.id, EnqueuedState(queue=job.queue))
        return job.id

    def schedule(self, job: Job, enqueue_at: datetime) -> str:
        with self._session_factory.begin() as session:
            session.add(
                JobModel(
                    id=job.id,
                    target_module=job.target_module,
                    target_function=job.target_function,
                    args=job.args,
                    kwargs=job.kwargs,
                    state_name=job.state_name,
                    state_data=self._serialize_state_data(job.state_data),
                    queue=job.queue,
                    retry_count=job.retry_count,
                    created_at=job.created_at,
                )
            )
            session.add(ScheduledJobModel(job_id=job.id, enqueue_at=enqueue_at))
            scheduled_at = job.created_at
            scheduled_at_raw = (job.state_data or {}).get("scheduled_at")
            if scheduled_at_raw:
                try:
                    scheduled_at = datetime.fromisoformat(scheduled_at_raw)
                except ValueError:
                    scheduled_at = job.created_at
            self._record_history(
                session, job.id, ScheduledState(enqueue_at, scheduled_at)
            )
        return job.id

    def add_recurring_job(
        self, recurring_job_id: str, job_template: Job, cron_expression: str
    ):
        payload = json.dumps(
            {
                "job": {
                    "target_module": job_template.target_module,
                    "target_function": job_template.target_function,
                    "args": job_template.args,
                    "kwargs": job_template.kwargs,
                    "queue": job_template.queue,
                },
                "cron": cron_expression,
                "last_execution": None,
            },
            default=str,
        )
        with self._session_factory.begin() as session:
            entry = session.get(RecurringJobModel, recurring_job_id)
            if entry:
                entry.job_data = payload
                entry.cron = cron_expression
                entry.last_execution = None
            else:
                session.add(
                    RecurringJobModel(
                        id=recurring_job_id,
                        job_data=payload,
                        cron=cron_expression,
                        last_execution=None,
                    )
                )

    def remove_recurring_job(self, recurring_job_id: str):
        with self._session_factory.begin() as session:
            session.execute(
                delete(RecurringJobModel).where(
                    RecurringJobModel.id == recurring_job_id
                )
            )

    def trigger_recurring_job(self, recurring_job_id: str):
        with self._session_factory.begin() as session:
            entry = session.get(RecurringJobModel, recurring_job_id)
            if not entry:
                return
            data = json.loads(entry.job_data)
            job_dict = data["job"]
            job = Job(
                target_module=job_dict["target_module"],
                target_function=job_dict["target_function"],
                args=job_dict["args"],
                kwargs=job_dict["kwargs"],
                state_name=EnqueuedState.NAME,
                state_data=EnqueuedState(
                    queue=job_dict.get("queue", "default")
                ).serialize_data(),
                queue=job_dict.get("queue", "default"),
            )
            session.add(
                JobModel(
                    id=job.id,
                    target_module=job.target_module,
                    target_function=job.target_function,
                    args=job.args,
                    kwargs=job.kwargs,
                    state_name=job.state_name,
                    state_data=self._serialize_state_data(job.state_data),
                    queue=job.queue,
                    retry_count=job.retry_count,
                    created_at=job.created_at,
                )
            )
            self._upsert_queue_entry(
                session,
                job_id=job.id,
                queue=job.queue,
                status="enqueued",
                enqueued_at=job.created_at,
            )
            self._record_history(session, job.id, EnqueuedState(queue=job.queue))

    def dequeue(
        self, queues: List[str], timeout_seconds: int, server_id: str, worker_id: str
    ) -> Optional[Job]:
        if not queues:
            return None

        deadline = time.monotonic() + timeout_seconds
        while True:
            with self._session_factory.begin() as session:
                now = datetime.now(UTC)
                query = (
                    select(QueueEntryModel)
                    .where(
                        QueueEntryModel.status == "enqueued",
                        QueueEntryModel.queue.in_(queues),
                    )
                    .order_by(QueueEntryModel.enqueued_at)
                    .limit(1)
                )
                if self._supports_skip_locked:
                    query = query.with_for_update(skip_locked=True)

                entry = session.execute(query).scalar_one_or_none()
                if not entry:
                    pass
                else:
                    updated = session.execute(
                        update(QueueEntryModel)
                        .where(
                            QueueEntryModel.id == entry.id,
                            QueueEntryModel.status == "enqueued",
                        )
                        .values(
                            status="processing",
                            fetched_at=now,
                            server_id=server_id,
                            worker_id=worker_id,
                        )
                    )
                    if updated.rowcount == 1:
                        job = session.get(JobModel, entry.job_id)
                        if job:
                            processing_state = ProcessingState(server_id, worker_id)
                            job.state_name = processing_state.name
                            job.state_data = self._serialize_state_data(
                                processing_state.serialize_data()
                            )
                            self._record_history(session, job.id, processing_state)
                            return self._job_from_model(job)

                        session.execute(
                            delete(QueueEntryModel).where(
                                QueueEntryModel.id == entry.id
                            )
                        )

            if timeout_seconds <= 0:
                return None
            if time.monotonic() >= deadline:
                return None
            time.sleep(0.05)

    def acknowledge(self, job_id: str) -> None:
        with self._session_factory.begin() as session:
            session.execute(
                delete(QueueEntryModel).where(QueueEntryModel.job_id == job_id)
            )

    def set_job_state(
        self, job_id: str, state: BaseState, expected_old_state: Optional[str] = None
    ) -> bool:
        with self._session_factory.begin() as session:
            job = session.get(JobModel, job_id)
            if not job:
                return False
            if expected_old_state and job.state_name != expected_old_state:
                return False

            job.state_name = state.name
            job.state_data = self._serialize_state_data(state.serialize_data())

            if isinstance(state, EnqueuedState):
                job.queue = state.queue
                self._upsert_queue_entry(
                    session,
                    job_id=job.id,
                    queue=state.queue,
                    status="enqueued",
                    enqueued_at=datetime.now(UTC),
                    fetched_at=None,
                )
            elif isinstance(state, (DeletedState, AwaitingState)):
                session.execute(
                    delete(QueueEntryModel).where(QueueEntryModel.job_id == job_id)
                )
                session.execute(
                    delete(ScheduledJobModel).where(ScheduledJobModel.job_id == job_id)
                )

            self._record_history(session, job.id, state)
            return True

    def get_job_data(self, job_id: str) -> Optional[Job]:
        with self._session_factory() as session:
            model = session.get(JobModel, job_id)
            return self._job_from_model(model) if model else None

    def get_jobs_by_state(self, state_name: str, start: int, count: int) -> List[Job]:
        with self._session_factory() as session:
            rows = (
                session.execute(
                    select(JobModel)
                    .where(JobModel.state_name == state_name)
                    .order_by(JobModel.created_at.desc())
                    .offset(start)
                    .limit(count)
                )
                .scalars()
                .all()
            )
            return [self._job_from_model(row) for row in rows]

    def get_job_ids_by_state(
        self, state_name: str, start: int, count: int
    ) -> List[str]:
        with self._session_factory() as session:
            rows = session.execute(
                select(JobModel.id)
                .where(JobModel.state_name == state_name)
                .order_by(JobModel.created_at.desc())
                .offset(start)
                .limit(count)
            ).scalars()
            return list(rows)

    def get_state_job_count(self, state_name: str) -> int:
        with self._session_factory() as session:
            result = session.execute(
                select(func.count(JobModel.id)).where(JobModel.state_name == state_name)
            ).scalar_one()
            return int(result or 0)

    def get_all_servers(self) -> List[dict]:
        return self.get_servers()

    def server_heartbeat(self, server_id: str, worker_count: int, queues: List[str]):
        with self._session_factory.begin() as session:
            entry = session.get(ServerModel, server_id)
            if entry:
                entry.worker_count = worker_count
                entry.queues = json.dumps(list(queues), default=str)
                entry.last_heartbeat = datetime.now(UTC)
            else:
                session.add(
                    ServerModel(
                        id=server_id,
                        worker_count=worker_count,
                        queues=json.dumps(list(queues), default=str),
                        last_heartbeat=datetime.now(UTC),
                    )
                )

    def remove_server(self, server_id: str):
        with self._session_factory.begin() as session:
            session.execute(delete(ServerModel).where(ServerModel.id == server_id))

    def get_servers(self) -> List[dict]:
        with self._session_factory() as session:
            rows = session.execute(select(ServerModel)).scalars().all()
            servers: List[dict] = []
            for row in rows:
                try:
                    queues = json.loads(row.queues)
                except (TypeError, json.JSONDecodeError):
                    queues = []
                servers.append(
                    {
                        "id": row.id,
                        "worker_count": row.worker_count,
                        "queues": queues,
                        "last_heartbeat": row.last_heartbeat.isoformat(),
                    }
                )
            return servers

    def get_recurring_jobs(self, start: int, count: int) -> List[dict]:
        with self._session_factory() as session:
            rows = (
                session.execute(
                    select(RecurringJobModel)
                    .order_by(RecurringJobModel.id)
                    .offset(start)
                    .limit(count)
                )
                .scalars()
                .all()
            )
            jobs: List[dict] = []
            for row in rows:
                try:
                    payload = json.loads(row.job_data)
                except (TypeError, json.JSONDecodeError):
                    payload = {"job": {}, "cron": row.cron, "last_execution": None}
                payload["id"] = row.id
                jobs.append(payload)
            return jobs

    def update_job_field(self, job_id: str, field_name: str, value: Any) -> None:
        if field_name not in {
            "target_module",
            "target_function",
            "args",
            "kwargs",
            "state_name",
            "state_data",
            "queue",
            "retry_count",
        }:
            raise ValueError(f"Unsupported field update: {field_name}")

        if field_name == "state_data":
            value = self._serialize_state_data(value)
        with self._session_factory.begin() as session:
            session.execute(
                update(JobModel)
                .where(JobModel.id == job_id)
                .values({field_name: value})
            )

    def get_job_history(self, job_id: str) -> List[dict]:
        with self._session_factory() as session:
            rows = (
                session.execute(
                    select(JobHistoryModel)
                    .where(JobHistoryModel.job_id == job_id)
                    .order_by(JobHistoryModel.timestamp)
                )
                .scalars()
                .all()
            )
            history = []
            for row in rows:
                history.append(
                    {
                        "state": row.state,
                        "timestamp": row.timestamp.isoformat(),
                        "data": self._deserialize_state_data(row.data),
                    }
                )
            return history

    def get_statistics(self) -> dict:
        with self._session_factory() as session:
            rows = session.execute(
                select(JobModel.state_name, func.count(JobModel.id)).group_by(
                    JobModel.state_name
                )
            ).all()
            counts = {state: 0 for state in ALL_STATES}
            for state_name, count in rows:
                counts[state_name] = int(count)
            return counts

    def enqueue_due_scheduled_jobs(self, batch_size: int = 100) -> List[str]:
        now = datetime.now(UTC)
        moved: List[str] = []
        with self._session_factory.begin() as session:
            query = (
                select(ScheduledJobModel)
                .where(ScheduledJobModel.enqueue_at <= now)
                .order_by(ScheduledJobModel.enqueue_at)
                .limit(batch_size)
            )
            if self._supports_skip_locked:
                query = query.with_for_update(skip_locked=True)

            rows = session.execute(query).scalars().all()
            for row in rows:
                job = session.get(JobModel, row.job_id)
                if not job:
                    session.delete(row)
                    continue

                enqueued_state = EnqueuedState(queue=job.queue)
                job.state_name = enqueued_state.name
                job.state_data = self._serialize_state_data(
                    enqueued_state.serialize_data()
                )
                self._upsert_queue_entry(
                    session,
                    job_id=job.id,
                    queue=job.queue,
                    status="enqueued",
                    enqueued_at=now,
                    fetched_at=None,
                )
                self._record_history(session, job.id, enqueued_state)
                session.delete(row)
                moved.append(job.id)
        return moved

    def enqueue_due_recurring_jobs(self, batch_size: int = 100) -> List[str]:
        now = datetime.now(UTC)
        triggered: List[str] = []
        with self._session_factory.begin() as session:
            query = (
                select(RecurringJobModel)
                .order_by(RecurringJobModel.id)
                .limit(batch_size)
            )
            if self._supports_skip_locked:
                query = query.with_for_update(skip_locked=True)

            rows = session.execute(query).scalars().all()
            for row in rows:
                try:
                    payload = json.loads(row.job_data)
                except (TypeError, json.JSONDecodeError):
                    payload = None
                if not payload:
                    continue

                last_execution = row.last_execution or now
                cron = CronSim(row.cron, last_execution)
                next_execution = next(cron)
                if next_execution > now:
                    continue

                job_dict = payload["job"]
                job = Job(
                    target_module=job_dict["target_module"],
                    target_function=job_dict["target_function"],
                    args=job_dict["args"],
                    kwargs=job_dict["kwargs"],
                    state_name=EnqueuedState.NAME,
                    state_data=EnqueuedState(
                        queue=job_dict.get("queue", "default")
                    ).serialize_data(),
                    queue=job_dict.get("queue", "default"),
                )
                session.add(
                    JobModel(
                        id=job.id,
                        target_module=job.target_module,
                        target_function=job.target_function,
                        args=job.args,
                        kwargs=job.kwargs,
                        state_name=job.state_name,
                        state_data=self._serialize_state_data(job.state_data),
                        queue=job.queue,
                        retry_count=job.retry_count,
                        created_at=job.created_at,
                    )
                )
                self._upsert_queue_entry(
                    session,
                    job_id=job.id,
                    queue=job.queue,
                    status="enqueued",
                    enqueued_at=job.created_at,
                )
                self._record_history(session, job.id, EnqueuedState(queue=job.queue))
                row.last_execution = now
                triggered.append(job.id)
        return triggered

    def recover_stuck_jobs(self, max_age_seconds: int, limit: int = 100) -> List[str]:
        cutoff = datetime.now(UTC) - timedelta(seconds=max_age_seconds)
        recovered: List[str] = []
        with self._session_factory.begin() as session:
            query = (
                select(QueueEntryModel)
                .where(
                    QueueEntryModel.status == "processing",
                    QueueEntryModel.fetched_at.is_not(None),
                    QueueEntryModel.fetched_at <= cutoff,
                )
                .order_by(QueueEntryModel.fetched_at)
                .limit(limit)
            )
            if self._supports_skip_locked:
                query = query.with_for_update(skip_locked=True)

            rows = session.execute(query).scalars().all()
            for entry in rows:
                job = session.get(JobModel, entry.job_id)
                if not job:
                    session.delete(entry)
                    continue

                queue_name = entry.queue or job.queue or "default"
                enqueued_state = EnqueuedState(queue=queue_name)
                job.state_name = enqueued_state.name
                job.state_data = self._serialize_state_data(
                    enqueued_state.serialize_data()
                )
                job.queue = queue_name

                entry.status = "enqueued"
                entry.enqueued_at = datetime.now(UTC)
                entry.fetched_at = None
                entry.server_id = None
                entry.worker_id = None

                self._record_history(session, job.id, enqueued_state)
                recovered.append(job.id)

        return recovered
