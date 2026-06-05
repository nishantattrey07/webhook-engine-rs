# Webhook Engine (Rust)

A learning-focused project that incrementally builds a production-inspired webhook delivery system from first principles.

The goal is not to copy a production architecture on day one.

The goal is to understand why production systems evolve into their final form by building the system one layer at a time, measuring bottlenecks, and only introducing additional complexity when a real limitation is discovered.

---

## Philosophy

Every version follows the same process:

```text
Build the simplest version
↓
Measure behavior
↓
Identify the bottleneck
↓
Upgrade only what is necessary
↓
Measure again
```

No architecture is added simply because “production systems use it”.

Every upgrade must solve a discovered problem.

---

## Learning Goals

This project is being used to deeply understand:

- Channels
- Shared state
- Backpressure
- Retry systems
- Dead-letter queues
- Reliability engineering
- Webhook delivery architecture
- Outbox pattern
- Distributed systems fundamentals

---

## Current Progress

### Version 0 — Event Lifecycle and Outbox Simulation ✅

Implemented:

```text
Payment Creation
Event Creation
In-Memory Store
Outbox Simulation
Delivery Simulation
Event State Transitions
Metrics
Invariant Validation
```

### Version 1 — Retry System and Event Recovery ✅

Implemented:

```text
Dispatcher
Worker
Retry Queue
Retry Processor
Attempt Tracking
Dead Lettering
Final Invariant Check
```

Current retry semantics:

```text
1 initial attempt
+ 4 retries
= 5 total attempts
```

The field used in the code is `attempt_count`, and it means:

```text
current attempt number
```

---

## Next

```text
Version 2 → Real HTTP Delivery
Version 3 → Worker Pool
Version 4 → Concurrency & Channels
Version 5 → Load Testing
Version 6 → Backpressure
Version 7 → Async Runtime Investigation
```

---

# Version 0

Version 0 focused on validating the core webhook event lifecycle before introducing retries, networking, concurrency, or persistence.

The objective was correctness, not performance.

---

## Architecture

```text
Payment
↓
Domain Event
↓
Pending
↓
Delivered / DeadLettered
```

---

## In-Memory Store

The system uses an in-memory store built with Rust HashMaps.

Stored entities:

```text
Payments
Domain Events
```

The store simulates a database while keeping the system simple enough to reason about.

---

## Auto Increment IDs

Real databases generate primary keys automatically.

Since Version 0 uses an in-memory store, IDs are generated manually using counters:

```text
next_payment_id
next_event_id
```

These simulate database-generated identifiers.

---

## Payment Creation

Each payment contains:

```text
payment_id
merchant_id
order_id
amount
payment_status
payment_method
created_at
```

---

## Event Creation

Every payment creates exactly one domain event.

Mappings:

```text
Succeeded → PaymentSucceeded
Failed    → PaymentFailed
Refunded  → PaymentRefund
```

Initial event state:

```text
Pending
```

---

## Atomic Capture Simulation

Version 0 simulated the core idea behind the Outbox Pattern.

A single operation:

```text
create_payment_and_event()
```

creates:

```text
Payment
+
Domain Event
```

together.

Business invariant:

```text
Every successfully created payment
must have exactly one corresponding event.
```

---

## Event States

Current event states:

```text
Pending
Delivered
DeadLettered
```

---

## Delivery Simulation

Version 0 intentionally avoided real HTTP delivery.

Instead:

```rust
simulate_delivery(merchant_id)
```

returns:

```text
Success
TemporaryFailure
PermanentFailure
```

based on deterministic rules.

This keeps test runs reproducible and debugging simple.

---

## Metrics

Current metrics:

```text
Pending Events
Delivered Events
DeadLettered Events
```

---

## Invariant Validation

Version 0 validated:

```text
payments.len() == domain_events.len()
```

This ensured every payment had a corresponding domain event.

---

# Version 1

Version 1 adds retry handling for temporary failures while keeping the system in-memory and single-threaded.

The objective is to prove that temporary failures are not terminal and that every event eventually reaches a terminal state.

---

## Version 1 Architecture

```text
Payment
↓
Domain Event
↓
Pending
↓
Dispatcher
↓
Worker
↓
Delivered

TemporaryFailure
↓
Retry Queue
↓
Retry Processor
↓
Delivered

TemporaryFailure
↓
Retry Queue
↓
Retry Processor
↓
Max attempts exceeded
↓
DeadLettered

PermanentFailure
↓
DeadLettered
```

---

## Retry Queue

Version 1 uses a retry queue that stores:

```text
event_id
```

instead of full event objects.

That keeps the queue lightweight and ensures the in-memory store remains the single source of truth.

The queue is intentionally thread-local because Version 1 is still a single-threaded simulation.

---

## Attempt Tracking

Each event tracks:

```text
attempt_count
```

which means:

```text
current attempt number
```

not “retries only”.

Example:

```text
Attempt 1 -> attempt_count = 1
Attempt 2 -> attempt_count = 2
Attempt 3 -> attempt_count = 3
```

---

## Retry Policy

Version 1 uses:

```text
MAX_ATTEMPT = 5
```

Meaning:

```text
1 initial attempt
+ 4 retries
= 5 total attempts
```

If the event still fails temporarily after those attempts, it is dead-lettered.

---

## Dispatcher

The dispatcher scans the store for pending events and hands them off for delivery.

It does not own the event data.

It only moves work forward.

---

## Worker

The worker performs the first delivery attempt.

If delivery succeeds, the event becomes `Delivered`.

If delivery fails temporarily, the event is returned to the retry queue.

If delivery fails permanently, the event becomes `DeadLettered`.

---

## Retry Processor

The retry processor drains the retry queue and tries the event again using the stored `attempt_count`.

It updates the real event in the store, not a copied event in the queue.

That keeps state consistent.

---

## Event States

Version 1 still uses:

```text
Pending
Delivered
DeadLettered
```

`Pending` is used for events that have not yet reached a terminal state.

---

## Delivery Simulation

Version 1 still uses deterministic delivery simulation instead of real HTTP.

This is intentional.

The purpose of Version 1 is to validate retry behavior before network complexity is added in Version 2.

---

## Final Invariant

Version 1 validates:

```text
Delivered + DeadLettered == Total Events
```

This ensures every event reaches a terminal state.

---

## Known Limitations of Version 1

### Simulated delivery

Delivery outcomes are still generated by `simulate_delivery(...)` instead of real HTTP requests.

### Single-threaded execution

Dispatcher, worker, and retry logic are still executed in a single-threaded flow.

### Simplified state model

The event lifecycle does not yet include richer states such as `RetryScheduled` or `Processing`.

### In-memory only

All state is lost when the process exits.

---

# What the Current System Does Not Include Yet

## Real HTTP Delivery

Webhook delivery is still simulated.

No network requests occur yet.

---

## Worker Pool

No worker threads exist yet.

All processing is still single-threaded.

---

## Channels and Concurrency

The project does not yet use:

```text
Arc
Mutex
RwLock
Channels
Thread Pools
```

---

## Persistence

All data lives in memory.

Restarting the application loses all state.

---

# Known Limitations

1. Atomicity is simulated, not guaranteed by a database transaction.
2. Delivery outcomes are simulated instead of coming from real merchant endpoints.
3. Events do not yet model richer delivery states.
4. No real retry backoff exists yet.
5. No persistence exists after process restart.
6. No concurrency exists yet.

---

# Why This Project Exists

Modern payment systems cannot safely send webhooks directly from request handlers.

They must deal with:

```text
Retries
Failures
Timeouts
Backpressure
Concurrency
Process Crashes
Network Partitions
Dead Letter Queues
```

This project explores those problems incrementally.

Each version introduces a new reliability mechanism and demonstrates why it is necessary.

The final goal is not simply a webhook sender.

The final goal is understanding how reliable event delivery systems are designed.

---

# Planned Roadmap

## Version 2 — Real HTTP Delivery

Version 2 replaces simulated outcomes with actual HTTP delivery to a mock merchant server.

```text
Webhook Engine
↓
HTTP Client
↓
Mock Merchant Server
```

This is where the system starts testing real network behavior, then breaking under failure and fixing the resulting edge cases.

---

## Version 3 — Worker Pool

Version 3 introduces a dispatcher feeding a work queue and multiple workers consuming delivery jobs.

```text
Dispatcher
↓
Work Queue
↓
Multiple Workers
```

---

## Version 4 — Concurrency

Version 4 focuses on shared state, coordination, and the Rust concurrency primitives needed to make the system safe under load.

```text
Arc
Mutex
Channels
Thread Coordination
```

---

## Version 5 — Load Testing

Version 5 focuses on scale and stress testing.

```text
100
1,000
10,000+
Webhook Deliveries
```

---

## Version 6 — Backpressure

Version 6 investigates what happens when delivery is slower than production and how bounded queues force the system to slow down safely.

---

## Version 7 — Async Runtime Investigation

Version 7 evaluates when blocking threads stop being enough and whether an async runtime is actually justified.

---

# Long-Term Goal

Build a production-inspired webhook delivery engine that explores:

```text
Outbox Pattern
Retry Scheduling
Dead Letter Queues
Backpressure
Worker Pools
Concurrency
Async Runtimes
Reliability Engineering
```

while understanding why each architectural component exists before introducing it.
