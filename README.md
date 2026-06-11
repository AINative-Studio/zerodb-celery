# zerodb-celery

**Celery broker + result backend powered by ZeroDB. Replace Redis/RabbitMQ with one line.**

[![PyPI](https://img.shields.io/pypi/v/zerodb-celery)](https://pypi.org/project/zerodb-celery/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

## Why?

Celery requires Redis or RabbitMQ for its broker and result backend. That means provisioning, configuring, and paying for infrastructure you shouldn't need.

`zerodb-celery` replaces both with [ZeroDB](https://ainative.studio) — a cloud database that auto-provisions on first use. No signup, no credit card, no infrastructure.

## Install

```bash
pip install zerodb-celery
```

## Quick Start

```python
from celery import Celery
from zerodb_celery import ZeroDBBroker, ZeroDBBackend

app = Celery('tasks')

# Configure both broker and backend
ZeroDBBroker.configure(app)
ZeroDBBackend.configure(app)

@app.task
def add(x, y):
    return x + y

# Trigger a task
result = add.delay(4, 6)
print(result.get(timeout=30))  # 10
```

## Configuration

### Auto-provisioning (default)

Just use `zerodb://auto` — a free ZeroDB project is created on first use:

```python
app.config_from_object({
    'broker_url': 'zerodb://auto',
    'broker_transport': 'zerodb_celery.broker:Transport',
    'result_backend': 'zerodb_celery.backend:ZeroDBBackend',
})
```

### Explicit credentials

Set environment variables:

```bash
export ZERODB_API_KEY=zdb_your_key_here
export ZERODB_PROJECT_ID=proj_your_id_here
```

Or pass them directly:

```python
ZeroDBBroker.configure(app, api_key='zdb_...', project_id='proj_...')
ZeroDBBackend.configure(app, api_key='zdb_...', project_id='proj_...')
```

### Credential resolution order

1. Explicit `api_key` / `project_id` parameters
2. `ZERODB_API_KEY` / `ZERODB_PROJECT_ID` environment variables
3. Cached credentials in `~/.zerodb/credentials.json`
4. Auto-provision a new free project via API

## How It Works

### Broker (message queue)

The broker uses ZeroDB's event stream as a message queue:

- **Publish task**: `POST /api/v1/zerodb/events` with topic `celery:{queue_name}`
- **Consume task**: `GET /api/v1/zerodb/events?topic=celery:{queue_name}&consume=true`

### Result backend (task results)

The backend stores results in a ZeroDB table called `celery_results`:

- **Store result**: `POST /api/v1/zerodb/tables/celery_results/rows`
- **Get result**: `GET /api/v1/zerodb/tables/celery_results/rows/{task_id}`

The table is auto-created on first use.

## Use Cases

- **ML pipelines**: Queue training jobs, store results — no Redis needed
- **AI agent workflows**: Background task processing for agent swarms
- **Serverless apps**: No infrastructure to manage alongside your Celery workers
- **Prototyping**: Get Celery running in 30 seconds without Docker

## Comparison

| Feature | Redis | RabbitMQ | zerodb-celery |
|---------|-------|----------|---------------|
| Setup time | 5-30 min | 10-60 min | 0 min (auto) |
| Infrastructure | Self-hosted or managed | Self-hosted or managed | None |
| Cost | $15-100+/mo | $15-100+/mo | Free tier |
| Persistence | Optional (AOF/RDB) | Yes | Yes |
| Auto-provisioning | No | No | Yes |

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/ -v
```

## License

MIT
