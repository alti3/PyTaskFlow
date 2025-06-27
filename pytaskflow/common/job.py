# pytaskflow/common/job.py
import uuid
from dataclasses import dataclass, field
from datetime import datetime, UTC
from typing import Optional, Tuple, Dict, Any


@dataclass
class Job:
    """
    Represents a unit of work to be executed in the background.

    This is the central data model that gets stored and passed around.
    Inspired by Hangfire.Common.Job.
    """

    # Target function information
    target_module: str
    target_function: str

    # Serialized arguments
    args: str  # JSON-serialized tuple
    kwargs: str  # JSON-serialized dict

    # State information
    state_name: str

    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    state_data: Dict[str, Any] = field(default_factory=dict)

    # Job metadata
    queue: str = "default"
    retry_count: int = 0

    # For future phases
    # scheduled_at: Optional[datetime] = None
    # recurring_job_id: Optional[str] = None
