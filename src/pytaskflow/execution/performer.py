# pytaskflow/execution/performer.py
import asyncio
import importlib
from typing import Any, Callable

from pytaskflow.common.exceptions import JobLoadError


def _load_target(module_name: str, func_name: str) -> Callable[..., Any]:
    try:
        module = importlib.import_module(module_name)
        return getattr(module, func_name)
    except (ImportError, AttributeError) as e:
        raise JobLoadError(
            f"Could not load job target: {module_name}.{func_name}"
        ) from e


def _is_async_func(func: Callable[..., Any]) -> bool:
    return asyncio.iscoroutinefunction(func)


def perform_job(module_name: str, func_name: str, args: tuple, kwargs: dict) -> Any:
    """Dynamically imports and executes the target job function."""
    target_func = _load_target(module_name, func_name)

    if _is_async_func(target_func):
        coro = target_func(*args, **kwargs)
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(coro)
        raise RuntimeError(
            "Cannot run async job from a running event loop in sync mode. "
            "Use the asyncio worker mode."
        )

    return target_func(*args, **kwargs)


async def perform_job_async(
    module_name: str, func_name: str, args: tuple, kwargs: dict
) -> Any:
    """Async version of job execution, supporting both sync and async callables."""
    target_func = _load_target(module_name, func_name)

    if _is_async_func(target_func):
        return await target_func(*args, **kwargs)

    return await asyncio.to_thread(target_func, *args, **kwargs)
