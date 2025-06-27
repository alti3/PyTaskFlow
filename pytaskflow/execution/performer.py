# pytaskflow/execution/performer.py
import importlib
from typing import Any
from pytaskflow.common.exceptions import JobLoadError


def perform_job(module_name: str, func_name: str, args: tuple, kwargs: dict) -> Any:
    """Dynamically imports and executes the target job function."""
    try:
        module = importlib.import_module(module_name)
        target_func = getattr(module, func_name)
        return target_func(*args, **kwargs)
    except (ImportError, AttributeError) as e:
        raise JobLoadError(
            f"Could not load job target: {module_name}.{func_name}"
        ) from e
