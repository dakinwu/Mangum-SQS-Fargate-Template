# Architecture Notes: Lambda API + SQS + Real Worker

*Draft source material for a technical blog post. Written as the "why" behind
every non-obvious decision in this repo, in roughly the order a reader would
encounter them.*

## 1. The problem this solves

A FastAPI request handler that needs to do slow, unreliable, or retryable
work (call an external API, process a file, run a multi-step pipeline) has
two bad default options:

- **Await it inline.** The client holds a connection open for the whole
  duration, the request can time out, and a crash mid-request loses the work
  with no record of how far it got.
- **Fire an in-process background task** (`asyncio.create_task` from within
  the handler, a thread pool, whatever). This survives the *request* but not
  the *process* - a deploy, a crash, an autoscaling-triggered instance
  replacement, or (on Lambda specifically) the platform freezing/reclaiming
  the execution environment between invocations, all silently drop the task.

What you actually want is a durable record of "this work needs to happen,"
independent of any single process's lifetime - which is exactly what a queue
gives you: an SQS message. The consumer of that message can crash, retry, and
be replaced without losing track of anything, because the source of truth
(the message + a status row in the DB) outlives the process handling it.

## 2. Why two deployables instead of one process doing both

It's tempting to just run the same FastAPI process as both the request
handler *and* the queue consumer - start a `poll_loop()` background task in
the app's lifespan and call it done. This repo actually keeps that mode alive
(see `DISABLE_POLLER` in `app/main.py`) as a legacy/simple-deployment option,
but production traffic should split them:

- **Blast radius.** A bug that wedges the event loop in a job handler now
  also stalls every API request being served by the same process.
- **Independent scaling.** API traffic and job-processing load rarely track
  each other. Fixing them to the same instance count wastes money in one
  direction or starves the other.
- **Independent deploy cadence.** Shipping a bug fix to job logic shouldn't
  require redeploying (and risking) the API, and vice versa.
- **Lambda specifically can't host a poll loop at all** - see the next
  section. Once the API is on Lambda, the worker *must* live somewhere else.

## 3. Why `lifespan="off"` on the Mangum handler

```python
handler = Mangum(app, lifespan="off")
```

FastAPI's lifespan context manager is designed to run startup code once per
*process*, amortized across many requests. On Lambda, a "process" is a cold
execution environment that may serve one invocation and then freeze or die -
there's no guarantee of amortization, and the startup cost (even a small one)
gets paid disproportionately often. Turning lifespan off skips that cost
entirely; anything that would have lived in it (an HTTP client, a cache) gets
initialized lazily on first use instead. The worker process, by contrast, *is*
long-lived, so it's exactly the right place to run one-time startup work (see
`_recover_stuck_tasks` in section 9).

## 4. Why REST API (v1) over HTTP API (v2) on API Gateway

HTTP API (v2) is cheaper and lower-latency, and would normally be the
default choice. It doesn't support native API-Gateway-managed API keys and
usage plans, though - that's a REST API (v1) feature. Since this template
puts *all* authentication at the gateway layer (see next paragraph), v1 wins
despite the cost/latency trade-off.

Putting auth at the gateway instead of in the FastAPI app is itself a
deliberate choice: it means the application code carries zero auth logic and
stays deployable behind whatever auth scheme a given environment wants
(API keys today, an authorizer Lambda or Cognito later) without a code
change - only the SAM template changes.

## 5. Why a thread-based visibility heartbeat, not a coroutine

This is the one decision in this repo that looks over-engineered until you
hit the failure mode it prevents.

SQS's `VisibilityTimeout` is how the queue knows a message is "being worked
on" - after it elapses without the consumer deleting the message, SQS assumes
the consumer died and redelivers it to someone else. For a job that might run
longer than one timeout window, the consumer has to periodically call
`ChangeMessageVisibility` to push the deadline forward. The obvious way to do
that inside an asyncio consumer is another coroutine that wakes up every N
seconds and extends it.

The problem: if the job handler itself does something that blocks the event
loop for a while - a CPU-bound computation, a library call that isn't
actually async under the hood, anything - every coroutine on that loop stalls
with it, *including the heartbeat*. The heartbeat coroutine's tick gets
delayed past the visibility timeout, SQS assumes the worker died, and
redelivers the message to another worker while the *original* worker is
still very much alive and still processing it. Now the same job runs twice,
concurrently, against the same DB rows.

A heartbeat on a real OS thread doesn't share the event loop, so it keeps
ticking regardless of what the async side is doing. It's a small amount of
extra complexity (`threading.Thread`, `threading.Event.wait` instead of
`asyncio.sleep`) that closes off an entire class of duplicate-processing bug.

## 6. Why sync DB session in the API, async in the worker

Two engines, two drivers (`psycopg2` for sync, `asyncpg` for async), against
the same database - not a stylistic inconsistency, but matched to what each
process actually needs:

- API request handlers are short and one-shot. A blocking `Session` query is
  simple and, given typical request concurrency, doesn't meaningfully cost
  anything relative to an async session.
- The worker runs several jobs concurrently *in one process* (bounded by
  `MAX_CONCURRENT_TASKS`). If one job's DB query blocked the event loop, it
  would stall the poll loop's ability to keep receiving messages and
  extending heartbeats for every *other* in-flight job at the same time. The
  async session is what lets N jobs' DB I/O interleave instead of serializing
  behind each other.

The cost is maintaining two dependency sets instead of one - worth it here
specifically because of what the worker's concurrency model demands.

## 7. Why atomic `UPDATE ... WHERE status IN (...)` for claiming

SQS is an at-least-once queue: the same message can be delivered a second
time, usually because visibility expired before the first attempt finished
(the process was slow, or crashed outright). The job dispatcher has to
handle "I've been asked to process a task_id that might already be done, or
might already be in progress" safely.

A naive approach - `SELECT` the task, check its status in Python, then
`UPDATE` it - has a classic TOCTOU (time-of-check-to-time-of-use) race: a
dispatch can read `Pending`, get preempted, and only write its `UPDATE` after
some *other* call has already taken the task all the way to `Completed` -
silently clobbering a finished result back to `Processing`.

```sql
UPDATE sub_tasks
SET status = 'Processing'
WHERE task_id = :id AND status IN ('Pending', 'Processing')
```

A single atomic conditional `UPDATE`, read via its `rowcount`, closes that
gap for the case that matters: a task already in a *terminal* state
(`Completed`/`Failed`) never matches the `WHERE`, so `rowcount == 0` and the
caller returns early without disturbing it - no `SELECT ... FOR UPDATE`, no
external lock service, no read-then-write window.

**What this does *not* do**: provide mutual exclusion between two callers
racing on the *same still-in-flight* task. The `WHERE` deliberately still
matches `Processing`, not just `Pending` - that's what lets a task whose
previous attempt crashed get picked back up after redelivery, rather than
being stuck forever once claimed. The trade-off is that if two dispatches
for the same task_id were *truly* simultaneous, both could pass the claim.
In practice that scenario doesn't arise from normal operation: SQS's
`VisibilityTimeout` already prevents a message from reaching a second
consumer while the first is still working it, which is the actual mutual
exclusion mechanism here - the DB claim's job is specifically the *later*,
non-overlapping redelivery case, not concurrent-call safety in general.

## 8. Why transient vs terminal error handling are different code paths

Not every exception in a job handler means the same thing:

- A `ConnectionError` talking to the database, or a timeout, usually means
  *the infrastructure had a bad moment* - the job would probably succeed if
  retried right now. Marking the task `Failed` here is wrong: it's not the
  job's fault, and it throws away the chance to just try again.
- A `RuntimeError` raised by the job's own logic (bad input, a bug, a
  business-rule violation) means *this exact job is broken* - retrying it
  immediately just burns the same failure again, and holding the SQS message
  hostage for a retry delays every other message behind it in the same
  worker's queue slot for no benefit.

The dispatcher distinguishes these by walking the exception's `__cause__`
chain (from `raise ... from e`) looking for known transient-infrastructure
types, and deliberately does *not* look at `__context__` (the "this was being
handled when another exception occurred" chain) - `__context__` would
misclassify an unrelated exception that merely happened to occur while a
transient one was being handled, which could cause a genuinely broken
message to be endlessly retried instead of ever reaching `Failed`.

Terminal failures still delete the SQS message even though the task is
`Failed` - message delivery and business status are different concerns.
Once the DB has recorded the outcome, the queue's job is done; keeping the
message around wouldn't add information, only delay.

## 9. Backpressure and startup recovery: two sides of the same coin

Two independent mechanisms handle "the worker or a task got interrupted,"
covering different failure windows:

- **Back-pressure** (the receive loop never pulls more messages than there
  are free `MAX_CONCURRENT_TASKS` slots) prevents the worker from *ever*
  getting into a bad state under normal load - it simply doesn't accept more
  work than it can service before those messages' visibility windows would
  need extending.
- **Startup recovery** (`_recover_stuck_tasks` / `_abandon_ancient_processing_tasks`,
  run once when a worker process starts) handles the case where the *process
  itself* died - crash, deploy, OOM kill - leaving DB rows claimed but never
  finished. SQS's own visibility-timeout expiry already re-surfaces a message
  whose consumer vanished, but only up to that timeout's window; a task whose
  row is hours stale (message long gone, assumptions about it likely stale
  too) gets swept explicitly instead of trusted to a queue mechanism that
  wasn't built to reason about DB-side staleness.

Together: back-pressure keeps things from breaking during normal operation,
and startup recovery cleans up after the operation *wasn't* normal.

## 10. What was simplified for this template vs. a real system

Being transparent about scope, since that's what keeps a blog post honest:
this template collapses what would realistically be several concurrency
semaphores (one per scarce external resource - an LLM API's rate limit, a
memory-bound conversion step, a connection pool) into a single example
semaphore, since there's no real external dependency here to protect. It
ships two placeholder job types instead of a realistic handful, uses
LocalStack instead of a real AWS account for local development, and skips a
CPU-bound work pool entirely (an `asyncio.sleep` placeholder has nothing to
offload to a process pool). None of that changes the mechanics being
demonstrated - it just removes domain-specific weight that would obscure them.

## 11. Where this pattern breaks down / when not to use it

- **Very high volume of very short jobs.** SQS's per-message overhead (a
  receive/delete round trip, a heartbeat thread per in-flight message) is
  wasted on jobs that complete in milliseconds - an in-process queue or a
  simple batch endpoint will beat this every time at that scale.
- **Strict ordering requirements.** SQS *standard* queues (used here) don't
  guarantee order and can deliver duplicates; if a workload genuinely needs
  strict FIFO processing, a FIFO queue (with its own throughput trade-offs)
  is the right tool, not a change to this pattern.
- **Exactly-once semantics.** This pattern is deliberately at-least-once
  end-to-end and pushes idempotency down into the data layer (the atomic
  claim). If a workload can't tolerate *any* duplicate side effects and can't
  be made idempotent, this shape needs real transactional outbox / dedup
  machinery on top - it's not free with SQS alone.
