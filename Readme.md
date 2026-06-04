# Webhook Engine (Rust)

A learning-focused project that incrementally builds a production-grade webhook delivery system from first principles.

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

* Rust concurrency
* Worker pools
* Channels
* Shared state
* Backpressure
* Retry systems
* Dead-letter queues
* Reliability engineering
* Webhook delivery architecture
* Outbox Pattern
* Distributed systems fundamentals

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

Planned:

```text
Version 1 → Retry System
Version 2 → Real HTTP Delivery
Version 3 → Worker Pool
Version 4 → Concurrency & Channels
Version 5 → Load Testing
Version 6 → Backpressure
Version 7 → Async Runtime Investigation
```

---

# Version 0

Version 0 focuses on validating the core webhook event lifecycle before introducing retries, networking, concurrency, or persistence.

The objective is correctness, not performance.

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

The system currently uses an in-memory store built with Rust HashMaps.

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

Version 0 simulates the core idea behind the Outbox Pattern.

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

Version 0 intentionally avoids real HTTP delivery.

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

Version 0 validates:

```text
payments.len() == domain_events.len()
```

This ensures every payment has a corresponding domain event.

---

# What Version 0 Does Not Include

## Retry System

Not implemented yet.

Temporary failures remain pending.

Missing:

```text
retry_count
max_retry_limit
retry scheduling
backoff strategy
```

---

## Real HTTP Delivery

Webhook delivery is currently simulated.

No network requests occur.

---

## Worker Pool

No worker threads exist.

All processing is single-threaded.

---

## Channels

No producer-consumer architecture exists yet.

---

## Concurrency

Version 0 does not use:

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

3. Events do not track delivery attempts.

4. No retry system exists.

5. No persistence exists after process restart.

6. No concurrency exists.

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

# next version

## Version 1

Retry System

```text
Pending
↓
Temporary Failure
↓
Retry
↓
Success

OR

Max Retries Reached
↓
DeadLettered
```

---


