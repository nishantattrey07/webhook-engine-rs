use std::fmt::Display;
use std::time::{Duration, SystemTime};

use crate::queue::{max_queue_len, queue_len};
use crate::types::Db;

#[derive(Debug, Clone, Copy)]
struct LatencyStats {
    min: f64,
    p50: f64,
    p95: f64,
    p99: f64,
    max: f64,
}

fn duration_to_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }

    if sorted.len() == 1 {
        return sorted[0];
    }

    let p = p.clamp(0.0, 1.0);
    let rank = p * (sorted.len() - 1) as f64;

    let low = rank.floor() as usize;
    let high = rank.ceil() as usize;

    if low == high {
        sorted[low]
    } else {
        let weight = rank - low as f64;
        sorted[low] + (sorted[high] - sorted[low]) * weight
    }
}

fn summarize(values: &[f64]) -> Option<LatencyStats> {
    if values.is_empty() {
        return None;
    }

    let mut sorted = values.to_vec();

    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

    Some(LatencyStats {
        min: sorted[0],
        p50: percentile(&sorted, 0.50),
        p95: percentile(&sorted, 0.95),
        p99: percentile(&sorted, 0.99),
        max: *sorted.last().unwrap(),
    })
}

fn print_metric_row(label: &str, value: impl Display, suffix: Option<&str>) {
    match suffix {
        Some(suffix) => {
            println!(
                "║    {:<24} {:>12} {:<13}║",
                label, value, suffix
            );
        }

        None => {
            println!(
                "║    {:<24} {:>12}              ║",
                label, value
            );
        }
    }
}

fn print_percent_row(label: &str, value: impl Display, percent: f64) {
    println!(
        "║    {:<24} {:>6}  ({:>5.1}%)        ║",
        label, value, percent
    );
}

fn print_latency_row(label: &str, value: f64) {
    println!(
        "║    {:<24} {:>12.1}               ║",
        label, value
    );
}

fn print_latency_section(title: &str, stats: Option<LatencyStats>) {
    println!("╠══════════════════════════════════════════════════════════╣");
    println!("║  {:<54}║", title);

    match stats {
        Some(stats) => {
            print_latency_row("min:", stats.min);
            print_latency_row("p50:", stats.p50);
            print_latency_row("p95:", stats.p95);
            print_latency_row("p99:", stats.p99);
            print_latency_row("max:", stats.max);
        }

        None => {
            print_metric_row("min:", "N/A", None);
            print_metric_row("p50:", "N/A", None);
            print_metric_row("p95:", "N/A", None);
            print_metric_row("p99:", "N/A", None);
            print_metric_row("max:", "N/A", None);
        }
    }
}

fn time_diff_ms(start: SystemTime, end: SystemTime) -> Option<f64> {
    end.duration_since(start).ok().map(duration_to_ms)
}

pub fn print_engine_report(db: Db, total_elapsed: Duration) -> bool {
    let store = db.lock().unwrap();

    let events_created = store.domain_events.len() as u64;
    let events_pending = store.event_pending_count();
    let events_queued = store.event_queued_count();
    let events_processing = store.event_processing_count();
    let events_delivered = store.event_delivered_count();
    let events_deadlettered = store.event_deadlettered_count();
    let total_attempts = store.attempt_history.len() as u64;
    let retried = total_attempts.saturating_sub(events_created);

    let total_elapsed_ms = total_elapsed.as_millis();
    let events_per_second = if total_elapsed.as_secs_f64() > 0.0 {
        events_created as f64 / total_elapsed.as_secs_f64()
    } else {
        0.0
    };

    let delivered_pct = if events_created > 0 {
        (events_delivered as f64 * 100.0) / events_created as f64
    } else {
        0.0
    };

    let deadlettered_pct = if events_created > 0 {
        (events_deadlettered as f64 * 100.0) / events_created as f64
    } else {
        0.0
    };

    let end_to_end_latencies_ms: Vec<f64> = store
        .domain_events
        .values()
        .filter_map(|event| {
            event.final_state_at.and_then(|final_state| {
                time_diff_ms(event.created_at, final_state)
            })
        })
        .collect();

    let http_call_durations_ms: Vec<f64> = store
        .attempt_history
        .iter()
        .filter_map(|attempt| time_diff_ms(attempt.started_at, attempt.completed_at))
        .collect();

    let e2e_stats = summarize(&end_to_end_latencies_ms);
    let http_stats = summarize(&http_call_durations_ms);
    let invariant_report = store.verify_invariant();
    let all_passed = invariant_report.all_passed();

    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║                    ENGINE REPORT                         ║");

    println!("╠══════════════════════════════════════════════════════════╣");
    println!("║  CONFIGURATION                                           ║");

    print_metric_row("engine_version:", "v2.5", None);
    print_metric_row("max_attempts:", 5, None);
    print_metric_row("events_in_run:", events_created, None);

    println!("╠══════════════════════════════════════════════════════════╣");
    println!("║  THROUGHPUT                                              ║");

    print_metric_row("total_elapsed_ms:", total_elapsed_ms, None);
    print_metric_row("events_per_second:", format!("{:.2}", events_per_second), None);
    print_metric_row("queue_len_current:", queue_len(), None);
    print_metric_row("queue_len_max:", max_queue_len(), None);

    println!("╠══════════════════════════════════════════════════════════╣");
    println!("║  OUTCOMES                                                ║");

    print_percent_row("events_created:", events_created, 100.0);
    print_percent_row("events_pending:", events_pending, if events_created > 0 { (events_pending as f64 * 100.0) / events_created as f64 } else { 0.0 });
    print_percent_row("events_queued:", events_queued, if events_created > 0 { (events_queued as f64 * 100.0) / events_created as f64 } else { 0.0 });
    print_percent_row("events_processing:", events_processing, if events_created > 0 { (events_processing as f64 * 100.0) / events_created as f64 } else { 0.0 });
    print_percent_row("events_delivered:", events_delivered, delivered_pct);
    print_percent_row("events_dead_lettered:", events_deadlettered, deadlettered_pct);

    print_metric_row("total_attempts:", total_attempts, None);
    print_metric_row("retried:", retried, None);

    print_latency_section("END-TO-END EVENT LATENCY (ms)", e2e_stats);
    print_latency_section("HTTP CALL DURATION (ms)", http_stats);

    println!("╠══════════════════════════════════════════════════════════╣");
    println!("║  INVARIANT CHECK                                         ║");

    println!(
        "║    [1] delivered+dlq == events_created {:>5}=={:<5} {} ║",
        invariant_report.lifecycle_lhs,
        invariant_report.lifecycle_rhs,
        if invariant_report.lifecycle_passed {
            "✓ PASS"
        } else {
            "✗ FAIL"
        }
    );

    println!(
        "║    [2] perm failures never retried      count={:<5} {} ║",
        invariant_report.permanent_failure_violations,
        if invariant_report.permanent_failure_passed {
            "✓ PASS"
        } else {
            "✗ FAIL"
        }
    );

    println!(
        "║    [3] attempt count consistency        count={:<5} {} ║",
        invariant_report.attempt_count_violations,
        if invariant_report.attempt_consistency_passed {
            "✓ PASS"
        } else {
            "✗ FAIL"
        }
    );

    println!("╠══════════════════════════════════════════════════════════╣");

    if all_passed {
        println!("║  RESULT:  ✅  ALL INVARIANTS PASS                        ║");
    } else {
        println!("║  RESULT:  ❌  INVARIANT FAILURE DETECTED                 ║");
    }

    println!("╚══════════════════════════════════════════════════════════╝");

    all_passed
}
