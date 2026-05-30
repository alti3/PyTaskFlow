# pytaskflow/common/exceptions.py


class PyTaskFlowException(Exception):
    """Base exception for the PyTaskFlow library."""

    pass


class JobLoadError(PyTaskFlowException):
    """Raised when a job's target function cannot be loaded."""

    pass
