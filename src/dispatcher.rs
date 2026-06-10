use std::convert::TryFrom;

use crate::queue::enqueue;
use crate::types::Db;

fn collect_event_ids(client: &mut postgres::Client, sql: &str) -> Vec<u64> {
    client
        .query(sql, &[])
        .unwrap()
        .into_iter()
        .map(|row| {
            let event_id: i64 = row.get(0);
            u64::try_from(event_id).expect("event_id should fit into u64")
        })
        .collect()
}

pub fn run_dispatcher(db: Db) -> usize {
    let mut client = db.lock().unwrap();
    let mut event_ids = Vec::new();

    // Crash recovery for queued rows that were marked queued but never
    // actually drained by a worker queue.
    event_ids.extend(collect_event_ids(
        &mut client,
        "UPDATE webhook_events
             SET updated_at = NOW()
             WHERE event_id IN (
                 SELECT event_id
                 FROM webhook_events
                 WHERE status = 'queued'
                   AND updated_at < NOW() - INTERVAL '5 seconds'
                 ORDER BY event_id
             )
             RETURNING event_id",
    ));

    // Crash recovery for stuck processing rows.
    client
        .execute(
            "UPDATE webhook_events
             SET status = 'pending',
                 next_attempt_at = NOW(),
                 updated_at = NOW()
             WHERE event_id IN (
                 SELECT event_id
                 FROM webhook_events
                 WHERE status = 'processing'
                   AND updated_at < NOW() - INTERVAL '5 minutes'
                 ORDER BY event_id
             )",
            &[],
        )
        .unwrap();

    // Queue due pending rows.
    event_ids.extend(collect_event_ids(
        &mut client,
        "UPDATE webhook_events
             SET status = 'queued',
                 updated_at = NOW()
             WHERE event_id IN (
                 SELECT event_id
                 FROM webhook_events
                 WHERE status = 'pending'
                   AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
                 ORDER BY event_id
             )
             RETURNING event_id",
    ));

    drop(client);

    for event_id in &event_ids {
        enqueue(*event_id);
    }

    event_ids.len()
}
