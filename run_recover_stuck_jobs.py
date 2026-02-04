"""CLI utility to requeue stuck jobs for SQL storage."""

from __future__ import annotations

import argparse

from pytaskflow.storage.sql_storage import SqlStorage


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Recover stuck PyTaskFlow jobs")
    parser.add_argument(
        "--connection-url",
        required=True,
        help="SQLAlchemy connection URL (e.g., sqlite:///pytaskflow.db)",
    )
    parser.add_argument(
        "--max-age-seconds",
        type=int,
        default=300,
        help="Requeue jobs stuck in processing longer than this many seconds.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of jobs to recover.",
    )
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    storage = SqlStorage(connection_url=args.connection_url)
    recovered = storage.recover_stuck_jobs(
        max_age_seconds=args.max_age_seconds,
        limit=args.limit,
    )
    if not recovered:
        print("No stuck jobs recovered.")
        return
    print(f"Recovered {len(recovered)} stuck jobs:")
    for job_id in recovered:
        print(f"- {job_id}")


if __name__ == "__main__":
    main()
