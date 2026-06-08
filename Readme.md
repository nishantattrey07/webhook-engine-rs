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

No architecture is added simply because "production systems use it".

Every upgrade must solve a discovered problem.

---

## Learning Goals

This project is being used to deeply understand:

* Webhook delivery systems
* Reliability engineering
* Retry systems
* Dead-letter queues
* Outbox pattern
* Worker pools
* Rust concurrency
* Channels
* Shared state
* Backpressure
* Distributed systems fundamentals

---

## Current Progress

### Version 0 — Event Lifecycle & Outbox Simulation ✅

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

---

### Version 1 — Retry System & Event Recovery ✅

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

Retry semantics:

```text
1 initial attempt
+ 4 retries
= 5 total attempts
```

---

### Version 2 — Real HTTP Delivery ✅

Implemented:

```text
Real HTTP Delivery
Webhook Payload Serialization
HTTP Status Classification
Client Timeout Handling
Retryable Failures
Permanent Failures
Dead Letter Handling
Invariant Validation
Attempt History
Timestamp Tracking
Latency Measurement
Engine Reporting
Invariant Reporting
```

Delivery classification:

```text
2xx                     -> Success

429                     -> TemporaryFailure

5xx                     -> TemporaryFailure

4xx                     -> PermanentFailure

Client Timeout          -> Timeout
```

---

## Next

```text
Version 3 → Worker Pool
Version 4 → Concurrency & Channels
Version 5 → Load Testing
Version 6 → Backpressure
Version 7 → Async Runtime Investigation
```

---

# Version 0

Version 0 focused on validating the core webhook lifecycle before introducing retries, networking, concurrency, or persistence.

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

The system uses Rust HashMaps to simulate a database.

Stored entities:

```text
Payments
Domain Events
```

---

## Auto Increment IDs

Database-generated IDs are simulated using:

```text
next_payment_id
next_event_id
```

---

## Atomic Capture Simulation

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

```text
Pending
Delivered
DeadLettered
```

---

## Delivery Simulation

Version 0 intentionally avoided networking.

```rust
simulate_delivery(...)
```

returned deterministic outcomes:

```text
Success
TemporaryFailure
PermanentFailure
```

---

## Metrics

```text
Pending Events
Delivered Events
DeadLettered Events
```

---

## Invariant Validation

Version 0 validates:

```text
payments.len() == domain_events.len()
```

---

# Version 1

Version 1 introduces retries while remaining entirely single-threaded.

The objective is to prove that temporary failures are not terminal failures.

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
Max Attempts Exceeded
↓
DeadLettered

PermanentFailure
↓
DeadLettered
```

---

## Retry Queue

The retry queue stores:

```text
event_id
```

instead of full event objects.

The in-memory store remains the source of truth.

Current implementation:

```rust
VecDeque<u64>
```

---

## Attempt Tracking

Each event tracks:

```text
attempt_count
```

Meaning:

```text
current attempt number
```

Example:

```text
Attempt 1 -> attempt_count = 1
Attempt 2 -> attempt_count = 2
Attempt 3 -> attempt_count = 3
```

---

## Retry Policy

```text
MAX_ATTEMPT = 5
```

Meaning:

```text
1 initial attempt
+ 4 retries
= 5 total attempts
```

---

## Dispatcher

The dispatcher scans for pending events and schedules work.

It does not own event state.

---

## Worker

The worker performs delivery attempts and updates event state.

Possible outcomes:

```text
Success
TemporaryFailure
PermanentFailure
```

---

## Final Invariant

Version 1 validates:

```text
Delivered + DeadLettered == Total Events
```

---


# Version 2

Version 2 replaces simulated delivery with real HTTP communication.

The objective is to validate delivery behavior against real network conditions before introducing concurrency.

Version 2 intentionally remains single-threaded.

Version 2 focuses on correctness first and observability second before introducing concurrency.

---

## Version 2 Architecture

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
HTTP POST
↓
Mock Merchant Server
```

---

## Real Webhook Delivery

Version 2 removes:

```rust
simulate_delivery(...)
```

and replaces it with:

```rust
send_webhook(...)
```

using:

```text
ureq
```

as a blocking HTTP client.

---

## Webhook Payload

A dedicated transport model is used instead of exposing internal storage models.

Payload contains:

```text
event_id
event_type

payment_id
order_id
amount
payment_status
payment_method
```

This separates internal storage concerns from external API contracts.

---

## Delivery Outcome Classification

HTTP responses are mapped into delivery outcomes.

Current rules:

```text
2xx                     -> Success

429                     -> TemporaryFailure

5xx                     -> TemporaryFailure

4xx                     -> PermanentFailure

Client Timeout          -> Timeout
```

This is the first version where the engine becomes network-aware.

---

## Timeout Handling

Version 2 introduces client-side delivery deadlines.

A timeout is defined by the webhook engine, not by the merchant.

Example:

```text
Request Sent
↓
Merchant Responds Too Slowly
↓
Client Deadline Exceeded
↓
Timeout
```

---

## Retry Behavior

Retryable outcomes:

```text
TemporaryFailure
Timeout
```

Non-retryable outcomes:

```text
PermanentFailure
```

---

## Dead Letter Handling

Events become dead-lettered when:

```text
PermanentFailure
```

or

```text
Maximum Attempts Exceeded
```

Dead-lettered events remain visible in the event store.

This acts as a simple in-memory Dead Letter Queue.

---

## Delivery Attempt History

Every delivery attempt is persisted in memory.

Each attempt records:

```text
attempt_id
event_id
attempt_count
http_status
outcome
started_at
completed_at
```

This creates a complete audit trail of delivery behavior.

The history is used for:

```text
Invariant validation
Latency reporting
Debugging
Performance analysis
```

---

## Timestamp Tracking

Version 2 introduces lifecycle timestamps.

### Event

```text
created_at
first_attempt_at
final_state_at
```

### DeliveryAttempt

```text
started_at
completed_at
```

These timestamps are stored in the data model rather than logs.

This allows the engine to compute performance metrics directly from internal state.

---

## Latency Metrics

Version 2 measures two latency categories.

### End-to-End Event Latency

```text
final_state_at - created_at
```

Measures how long an event takes to reach a terminal state.

This includes:

```text
HTTP Calls
Retries
Timeouts
Dead Lettering
```

### HTTP Call Duration

```text
completed_at - started_at
```

Measures network and merchant response time only.

This excludes retry behavior and focuses purely on a single delivery attempt.

---

## Percentile Reporting

Latency is reported using:

```text
min
p50
p95
p99
max
```

Percentiles expose tail latency and provide a more realistic picture of delivery performance than averages alone.

Averages can hide slow outliers, while percentiles show the actual experience of the slowest deliveries.

---

## Engine Report

At the end of every run the engine generates a report derived entirely from store state.

The report contains:

```text
Configuration
Throughput
Outcome Counts
End-to-End Event Latency
HTTP Call Duration
Invariant Results
```

The report is generated from the in-memory store and does not parse logs.

The store remains the source of truth.

---

## Invariant Validation

Version 2 validates three correctness invariants.

### Invariant 1

```text
Delivered + DeadLettered == Total Events
```

Ensures no event disappears from the delivery lifecycle.

### Invariant 2

```text
PermanentFailure is terminal
```

No delivery attempt may occur after a permanent failure.

### Invariant 3

```text
Event.attempt_count == AttemptHistory Count
```

Attempt summaries must match recorded delivery history.

---

## Known Limitations of Version 2

### Blocking HTTP

The worker performs synchronous network requests.

No async runtime is used.

### Single-threaded execution

Dispatcher, worker, retry processing, and delivery all execute on a single thread.

### In-memory state

All state is lost when the process exits.

### Simplified state model

Current event states:

```text
Pending
Delivered
DeadLettered
```

No intermediate states currently exist.

### Immediate Retries

Retries happen immediately.

There is currently:

```text
No Delay
No Backoff
No Jitter
```

### No Worker Pool

Only a single worker executes delivery attempts.

One slow merchant can stall the entire engine.

### No Persistence

Payments, events, and delivery history exist only in memory.

A process restart loses all state.

---

# What The Current System Does Not Include Yet

## Worker Pool

No worker threads exist yet.

All processing is still single-threaded.

---

## Channels & Concurrency

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

Restarting the process loses all state.

---

## Retry Scheduling

Retries happen immediately.

There is currently:

```text
No Delay
No Backoff
No Jitter
```

---

# Known Limitations

1. Atomicity is simulated rather than enforced by database transactions.
2. Delivery uses blocking HTTP.
3. Events do not yet model richer delivery states.
4. Retry scheduling has no delay or backoff strategy.
5. State is not persisted across process restarts.
6. No concurrency exists yet.
7. Transport errors are not yet modeled with a dedicated error type.

---

# Why This Project Exists

Modern payment systems cannot safely send webhooks directly from request handlers.

They must handle:

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

Each version introduces a new reliability mechanism and demonstrates why it exists.

The goal is not simply to build a webhook sender.

The goal is to understand how reliable event delivery systems are designed.

---

# Planned Roadmap

## Version 3 — Worker Pool

Introduce a dispatcher feeding a shared work queue and multiple workers consuming delivery jobs.

```text
Dispatcher
↓
Work Queue
↓
Multiple Workers
```

---

## Version 4 — Concurrency

Introduce shared state and coordination primitives.

```text
Arc
Mutex
Channels
Thread Coordination
```

---

## Version 5 — Load Testing

Stress test the system under increasing delivery volume.

```text
100
1,000
10,000+
Webhook Deliveries
```

---

## Version 6 — Backpressure

Investigate what happens when event production exceeds delivery capacity.

Focus areas:

```text
Bounded Queues
Queue Growth
Flow Control
Backpressure
```

---

## Version 7 — Async Runtime Investigation

Evaluate when blocking threads stop being sufficient and whether async execution is justified.

Topics:

```text
Tokio
Async I/O
Task Scheduling
Runtime Tradeoffs
```

---

# Long-Term Goal

Build a production-inspired webhook delivery engine that explores:

```text
Outbox Pattern
Retry Scheduling
Dead Letter Queues
Worker Pools
Concurrency
Backpressure
Async Runtimes
Reliability Engineering
Distributed Systems
```

while understanding why each architectural component exists before introducing it.
