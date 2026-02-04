# Installation

PyTaskFlow is designed to work with Python 3.13+.

## Core Package

```bash
uv add pytaskflow
```

## Optional Extras

Dashboard (Litestar + Uvicorn):

```bash
uv add "pytaskflow[dashboard]"
```

FastAPI integration:

```bash
uv add "pytaskflow[fastapi]"
```

Documentation tooling:

```bash
uv add "pytaskflow[docs]"
```

SQLAlchemy storage backend:

```bash
uv add "pytaskflow[sql]"
```
