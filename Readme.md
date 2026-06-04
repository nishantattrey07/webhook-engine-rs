# Concurrent Webhook Engine - Version 0

## Goal

Build the simplest possible webhook producer engine before introducing retries, worker pools, channels, HTTP delivery, concurrency, or databases.

The purpose of Version 0 is to validate the core lifecycle of a webhook event.

---

# What I have Built

## In-Memory Storage

I created an `InMemoryStore` that simulates a database.

It stores:

* Payments
* Domain Events

using Rust `HashMap`s.

---

## Auto Increment IDs

To simulate database-generated primary keys:

```text
next_payment_id
next_event_id
```

are maintained inside the store.

Every new payment/event receives a unique identifier.

---

## Payment Creation

A payment contains:

* payment_id
* merchant_id
* order_id
* amount
* payment status
* payment method
* created_at timestamp

---

## Event Creation

For every payment, a corresponding event is created.

Mapping:

```text
Succeeded -> PaymentSucceeded
Failed    -> PaymentFailed
Refunded  -> PaymentRefund
```

Initial state:

```text
Pending
```

---

## Atomic Capture Simulation

Version 0 simulates the Outbox Pattern.

A single business operation:

```text
create_payment_and_event()
```

creates:

```text
Payment
+
Event
```

together.

Invariant:

```text
Payment Exists
⇔
Event Exists
```

---

## Event Lifecycle

Events currently support:

```text
Pending
Delivered
DeadLettered
```

---

## Delivery Simulation

Version 0 does not use HTTP.

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

deterministically based on merchant_id.

This makes test runs reproducible.

---

## Metrics

Current metrics:

* Pending Events
* Delivered Events
* DeadLettered Events

---

## Invariant Validation

Version 0 validates:

```text
payments.len() == domain_events.len()
```

This ensures every payment has a corresponding event.

---

# What Version 0 Does NOT Have

## No Retry Logic

Temporary failures remain pending.

There is currently no retry counter.

---

## No Dead Letter Queue Logic

Events can be marked dead-lettered.

However:

* retry limits
* retry exhaustion
* DLQ processing

do not exist yet.

---

## No HTTP Delivery

Webhook delivery is simulated through a function.

No real network calls occur.

---

## No Worker Pool

Everything runs on a single thread.

---

## No Channels

No producer-consumer architecture exists yet.

---

## No Concurrency

No threads.

No Arc.

No Mutex.

No synchronization primitives.

---

## No Persistence

Data is stored entirely in memory.

Restarting the process loses all state.

---

# Known Design Limitations

1. Atomicity is simulated.
   HashMap inserts cannot truly model database transactions.

2. Delivery outcomes are simulated.
   No real merchant endpoints exist yet.

3. Events do not track retry attempts.

4. No recovery exists after process restart.

---

# Why Version 0 Exists

Version 0 is intentionally simple.

The objective is not performance.

The objective is proving the core event lifecycle:

```text
Payment
↓
Event
↓
Pending
↓
Delivered / DeadLettered
```

before introducing additional complexity.

---

# Next Planned Upgrade (Version 1)

Introduce retry support.

Changes:

```text
Event.retry_count
```

Workflow:

```text
Pending
↓
Temporary Failure
↓
retry_count += 1
↓
Retry
↓
Success

OR

Max Retries Reached
↓
DeadLettered
```

This will be the first meaningful reliability improvement.
