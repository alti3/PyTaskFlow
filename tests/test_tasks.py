# tests/test_tasks.py

def success_task(x, y):
    """A simple task that succeeds."""
    return x + y

def failure_task():
    """A task that is designed to fail."""
    raise ValueError("This task is designed to fail")

def side_effect_task(path, content):
    """A task that writes to a file to check for side effects."""
    with open(path, "w") as f:
        f.write(content)
