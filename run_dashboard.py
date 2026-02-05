"""Example of how to run the PyTaskFlow dashboard."""
from __future__ import annotations

import argparse
import os

import uvicorn
import redis

from pytaskflow.client import Client
from pytaskflow.dashboard import create_dashboard_app
from pytaskflow.storage.memory_storage import MemoryStorage
from pytaskflow.storage.redis_storage import RedisStorage


def create_client(storage: str, redis_url: str | None) -> Client:
    storage = storage.strip().lower()
    if storage == "memory":
        return Client(storage=MemoryStorage())
    if storage != "redis":
        raise ValueError("storage must be 'redis' or 'memory'")

    if redis_url:
        redis_client = redis.Redis.from_url(redis_url, decode_responses=True)
        return Client(storage=RedisStorage(redis_client=redis_client))
    return Client()


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the PyTaskFlow dashboard")
    parser.add_argument(
        "--storage",
        choices=["redis", "memory"],
        default=os.getenv("PYTASKFLOW_STORAGE", "memory"),
        help="Storage backend to use (env: PYTASKFLOW_STORAGE).",
    )
    parser.add_argument(
        "--redis-url",
        default=os.getenv("PYTASKFLOW_REDIS_URL"),
        help="Redis URL for the redis backend (env: PYTASKFLOW_REDIS_URL).",
    )
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    return parser


default_client = create_client(
    os.getenv("PYTASKFLOW_STORAGE", "memory"),
    os.getenv("PYTASKFLOW_REDIS_URL"),
)
app = create_dashboard_app(default_client, debug=True)


if __name__ == "__main__":
    args = build_arg_parser().parse_args()
    client = create_client(args.storage, args.redis_url)
    app = create_dashboard_app(client, debug=True)
    uvicorn.run(app, host=args.host, port=args.port)
