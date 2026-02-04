import asyncio

from pytaskflow.execution.performer import perform_job, perform_job_async


def test_perform_job_runs_async_function():
    result = perform_job(
        "tests.test_tasks",
        "async_success_task",
        (1, 2),
        {},
    )
    assert result == 3


def test_perform_job_async_runs_sync_function():
    result = asyncio.run(
        perform_job_async(
            "tests.test_tasks",
            "success_task",
            (1, 2),
            {},
        )
    )
    assert result == 3


def test_perform_job_async_runs_async_function():
    result = asyncio.run(
        perform_job_async(
            "tests.test_tasks",
            "async_success_task",
            (1, 2),
            {},
        )
    )
    assert result == 3
