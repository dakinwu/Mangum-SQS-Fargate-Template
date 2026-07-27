# mangum-sqs-fargate-template

A minimal, runnable reference implementation of a common serverless async-job
pattern: a **Mangum-wrapped FastAPI Lambda API**, an **SQS queue**, and a
**standalone worker process** (built for Fargate/ECS, but it's just a plain
Python process - runs anywhere).

The job body in this template is a placeholder (`asyncio.sleep`, with a
variant that randomly fails). The point of the repo is the *plumbing* around
that placeholder: back-pressure, visibility heartbeats, idempotent claiming,
transient-vs-terminal error handling, startup recovery, and the API/worker
split itself.

## Architecture

```
        POST /job-run/                 GET /sub-task/{id}
              |                               |
              v                               v
   +----------------------+        +----------------------+
   |  API Gateway (REST)  |        |  API Gateway (REST)  |
   +----------------------+        +----------------------+
              |                               |
              v                               v
   +-------------------------------------------------------+
   |     Lambda (Mangum + FastAPI) -- app/main.py           |
   |     writes JobRun + SubTask rows, enqueues SQS msg     |
   +-------------------------------------------------------+
              |                               ^
              v                               |
        +-----------+                         |
        |    SQS     |------------------------>|
        +-----------+   worker polls, dispatches, writes status
              |
              v
   +-------------------------------------------------------+
   |  Worker (Fargate/ECS/any container) -- worker_entry.py |
   |  poll_loop -> dispatch_task -> placeholder processor   |
   +-------------------------------------------------------+
              |
              v
        +-----------+
        | Postgres  |
        +-----------+
```

The API and the worker are two independently deployable processes sharing
only the SQS queue and the database. See `docs/architecture-notes.md` for the
reasoning behind that split and every other non-obvious decision here.

## Why this pattern

Some work shouldn't block a request/response cycle (it's slow, or it retries,
or a client shouldn't have to hold a connection open for it), but it also
shouldn't just be an in-process background task: Lambda's short, bursty
invocation model doesn't suit a "poll forever" loop, and an in-process task
in a long-lived server dies with the server. SQS provides the delivery
guarantees (at-least-once, visibility timeouts, redelivery) that make a
*separate* worker process safe to restart, scale, and redeploy independently
of the API.

## Quickstart (local, no AWS account needed)

```bash
# 1. Start Postgres + LocalStack (SQS)
docker compose up -d

# 2. Install dependencies
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt

# 3. Create tables and seed the two placeholder job types
# (PYTHONPATH=. so `app.*` resolves from the repo root, not scripts/)
cp .env.example .env
PYTHONPATH=. python scripts/init_db.py
PYTHONPATH=. python scripts/seed_types.py

# 4. Run the API
# DISABLE_POLLER=true mirrors the Lambda deployment: the API never runs
# poll_loop itself, so the standalone worker (step 5) is the only consumer.
# Omit it if you want to try the legacy single-process mode instead.
DISABLE_POLLER=true PYTHONPATH=. uvicorn app.main:app --reload

# 5. In a second terminal, run the worker
PYTHONPATH=. python -m worker_entry
```

Then, in a third terminal:

```bash
# Create a run with one "quick" task and one "flaky" task
curl -s -X POST localhost:8000/job-run/ \
  -H 'Content-Type: application/json' \
  -d '{"case_id": "demo-1", "sub_task_type_names": ["quick", "flaky"]}' | tee /tmp/run.json

# List available task types
curl -s localhost:8000/sub-task-type/

# Poll a task's status (copy a task_id from the run response above)
curl -s localhost:8000/sub-task/<task_id>
```

Watch the worker's terminal - you'll see `TASK_RECEIVED`, `HEARTBEAT_EXTENDED`
(if the job runs long enough), and the task/run transitioning to
`Completed`/`completed` or `Failed`/`failed`.

## Project layout

| Path | Role |
|------|------|
| `lambda_handler.py` | Lambda entrypoint (Mangum-wrapped FastAPI app) |
| `worker_entry.py` | Standalone worker process entrypoint |
| `app/main.py` | FastAPI app factory + lifespan |
| `app/routers/` | The 3 API endpoints |
| `app/worker/` | `poll_loop`, heartbeat, recovery, queue backend abstraction |
| `app/services/job_dispatcher/` | Idempotent claim, transient/terminal error handling, job processors |
| `app/models/` | `JobRun` / `SubTask` / `SubTaskType` (SQLModel) |
| `app/core/` | DB session factories, concurrency semaphore |
| `template.yaml` | SAM template: Lambda API + API Gateway |
| `infra/fargate.yaml` | CloudFormation: worker as an ECS Fargate service |
| `docker-compose.yml` | Local Postgres + LocalStack (SQS) |
| `docs/architecture-notes.md` | The "why" behind every non-obvious decision |
| `tests/` | pytest suite (consumer mechanics, dispatcher, API) |

## Configuration

See `.env.example` for the full list with inline descriptions. Highlights:

| Variable | Purpose |
|----------|---------|
| `ENV` | `local` \| `dev` \| `prod` - controls log format and a couple of lifespan behaviors |
| `DB_*` | Postgres connection (used to build both the sync and async DSNs) |
| `SQS_QUEUE_URL` | Queue the API enqueues to and the worker polls |
| `AWS_ENDPOINT_URL` | Point at LocalStack locally; unset for real AWS |
| `API_ROOT_PATH` | Set by API Gateway to the stage path (e.g. `/dev`) |
| `MAX_CONCURRENT_TASKS` | Worker's in-flight task ceiling (back-pressure) |

## Deploying for real

```bash
# API (Lambda + API Gateway)
sam build --template template.yaml
sam deploy --guided

# Worker (Fargate)
aws cloudformation deploy \
  --template-file infra/fargate.yaml \
  --stack-name template-worker-dev \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides Env=dev ImageUri=<ecr-uri> TaskRoleArn=<arn> ExecutionRoleArn=<arn> \
    SubnetIds=<subnet-ids> SecurityGroupIds=<sg-ids> SqsQueueUrl=<queue-url>
```

These are two independent stacks sharing only `SQS_QUEUE_URL` and DB
connection info - deploy, scale, and roll back each on its own schedule.

## Key design decisions

- Mangum runs with `lifespan="off"` - short-lived Lambda invocations don't
  amortize lifespan startup cost.
- API Gateway REST API (v1), not HTTP API (v2) - only v1 supports native
  ApiKey + UsagePlan auth, so auth lives entirely at the gateway.
- The visibility heartbeat runs on a real OS thread, not a coroutine - so a
  blocking/CPU-bound job handler can't starve it past the visibility timeout.
- The API uses a sync DB session; the worker uses an async one - the worker
  runs many concurrent long-lived jobs in one process and can't afford a
  blocking query to stall the whole poll loop.
- Task claiming is an atomic `UPDATE ... WHERE status IN (...)`, not a
  `SELECT` then `UPDATE` - avoids a TOCTOU race under SQS's at-least-once
  redelivery.

Full rationale for each: `docs/architecture-notes.md`.

## Testing

```bash
docker compose up -d db   # tests use real Postgres (JSONB columns, no SQLite)
pip install -r requirements-dev.txt
pytest
```

Covers: consumer back-pressure and heartbeat mechanics, transient-vs-terminal
message handling, idempotent claim races, and the 3 API endpoints. Not
covered: load/chaos testing, LocalStack-specific SQS edge cases (real AWS SQS
may differ subtly from LocalStack's emulation).

## What this template deliberately omits

- Job bodies are placeholders (`asyncio.sleep`) - no OCR, no LLM calls, no
  PDF/file processing. Swap `app/services/job_dispatcher/processors/` for
  your real work.
- No in-app authentication - the API Gateway's ApiKey/UsagePlan is the only
  auth layer, by design (see `docs/architecture-notes.md`).
- No observability stack wired up beyond structured stdout log lines - bring
  your own log aggregator/alerting.
- Migrations use a plain `create_all()` bootstrap, not Alembic - add Alembic
  if this grows into a project with real schema history.

## License

MIT - see `LICENSE`.
