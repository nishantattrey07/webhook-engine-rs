use sqlx::PgPool;

use super::{ALLOWED_EVENT_TYPES, list_deliveries_for_event};
use crate::{
    error::{AppError, AppResult},
    models::{EventFanoutResponse, EventListItem, EventListQuery, PaginatedEventsResponse},
};

use super::validation::{extract_search_number, normalize_exact_filter};

#[derive(Debug, Clone)]
pub(crate) struct EventSearch {
    pub(crate) event_id: Option<i64>,
    pub(crate) merchant_id: Option<i64>,
    pub(crate) object_id: Option<i64>,
    pub(crate) event_type: Option<String>,
    pub(crate) object_type: Option<String>,
}

pub(crate) fn event_search(value: Option<&str>) -> EventSearch {
    let Some(raw_value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return EventSearch {
            event_id: None,
            merchant_id: None,
            object_id: None,
            event_type: None,
            object_type: None,
        };
    };

    let normalized = raw_value.to_ascii_lowercase();
    let number = extract_search_number(Some(raw_value));
    let mut search = EventSearch {
        event_id: None,
        merchant_id: None,
        object_id: None,
        event_type: None,
        object_type: None,
    };

    if normalized.starts_with("evt_") || normalized.starts_with("event ") {
        search.event_id = number;
        return search;
    }

    if normalized.starts_with("merchant ") {
        search.merchant_id = number;
        return search;
    }

    if normalized.starts_with("object ") || normalized.starts_with("payment ") {
        search.object_id = number;
        return search;
    }

    if let Some(number) = number {
        search.event_id = Some(number);
        search.merchant_id = Some(number);
        search.object_id = Some(number);
        return search;
    }

    if ALLOWED_EVENT_TYPES.contains(&normalized.as_str()) {
        search.event_type = Some(normalized);
    } else if normalized == "payment" {
        search.object_type = Some(normalized);
    }

    search
}

pub async fn list_events(
    pool: &PgPool,
    query: EventListQuery,
) -> AppResult<PaginatedEventsResponse> {
    let limit = query.limit.unwrap_or(50).clamp(1, 100);
    let fetch_limit = limit + 1;
    let event_type = normalize_exact_filter(query.event_type);
    let search = event_search(query.search.as_deref());

    let rows = sqlx::query_as::<_, EventListItem>(
        "SELECT
            e.event_id,
            e.merchant_id,
            e.object_type,
            e.object_id,
            e.event_type,
            e.created_at,
            e.scenario_id,
            COUNT(d.delivery_id)::BIGINT AS delivery_count,
            COUNT(d.delivery_id) FILTER (WHERE d.status = 'pending')::BIGINT AS pending_count,
            COUNT(d.delivery_id) FILTER (WHERE d.status = 'queued')::BIGINT AS queued_count,
            COUNT(d.delivery_id) FILTER (WHERE d.status = 'processing')::BIGINT AS processing_count,
            COUNT(d.delivery_id) FILTER (WHERE d.status = 'retrying')::BIGINT AS retrying_count,
            COUNT(d.delivery_id) FILTER (WHERE d.status = 'delivered')::BIGINT AS delivered_count,
            COUNT(d.delivery_id) FILTER (WHERE d.status = 'dead_lettered')::BIGINT AS dead_lettered_count
         FROM domain_events e
         LEFT JOIN webhook_deliveries d
            ON d.event_id = e.event_id
         WHERE ($1::BIGINT IS NULL OR e.merchant_id = $1)
           AND ($2::TEXT IS NULL OR e.event_type = $2)
           AND ($10::BIGINT IS NULL OR e.scenario_id = $10)
           AND (
                (
                    $3::BIGINT IS NULL
                    AND $4::BIGINT IS NULL
                    AND $5::BIGINT IS NULL
                    AND $6::TEXT IS NULL
                    AND $7::TEXT IS NULL
                )
                OR e.event_id = $3
                OR e.merchant_id = $4
                OR e.object_id = $5
                OR e.event_type = $6
                OR e.object_type = $7
           )
           AND ($8::BIGINT IS NULL OR e.event_id < $8)
         GROUP BY e.event_id
         ORDER BY e.event_id DESC
         LIMIT $9",
    )
    .bind(query.merchant_id)
    .bind(event_type)
    .bind(search.event_id)
    .bind(search.merchant_id)
    .bind(search.object_id)
    .bind(search.event_type)
    .bind(search.object_type)
    .bind(query.cursor)
    .bind(fetch_limit)
    .bind(query.scenario_id)
    .fetch_all(pool)
    .await?;

    let mut items = rows;
    let next_cursor = if items.len() > limit as usize {
        items.pop().map(|item| item.event_id)
    } else {
        None
    };

    Ok(PaginatedEventsResponse {
        items,
        next_cursor,
        limit,
    })
}

pub async fn get_event(pool: &PgPool, event_id: i64) -> AppResult<EventListItem> {
    let event = sqlx::query_as::<_, EventListItem>(
        "SELECT
            e.event_id,
            e.merchant_id,
            e.object_type,
            e.object_id,
            e.event_type,
            e.created_at,
            e.scenario_id,
            COUNT(d.delivery_id)::BIGINT AS delivery_count,
            COUNT(d.delivery_id) FILTER (WHERE d.status = 'pending')::BIGINT AS pending_count,
            COUNT(d.delivery_id) FILTER (WHERE d.status = 'queued')::BIGINT AS queued_count,
            COUNT(d.delivery_id) FILTER (WHERE d.status = 'processing')::BIGINT AS processing_count,
            COUNT(d.delivery_id) FILTER (WHERE d.status = 'retrying')::BIGINT AS retrying_count,
            COUNT(d.delivery_id) FILTER (WHERE d.status = 'delivered')::BIGINT AS delivered_count,
            COUNT(d.delivery_id) FILTER (WHERE d.status = 'dead_lettered')::BIGINT AS dead_lettered_count
         FROM domain_events e
         LEFT JOIN webhook_deliveries d
            ON d.event_id = e.event_id
         WHERE e.event_id = $1
         GROUP BY e.event_id",
    )
    .bind(event_id)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| AppError::NotFound(format!("event {} not found", event_id)))?;

    Ok(event)
}

pub async fn get_event_fanout(pool: &PgPool, event_id: i64) -> AppResult<EventFanoutResponse> {
    let event = get_event(pool, event_id).await?;
    let deliveries = list_deliveries_for_event(pool, event_id).await?;

    Ok(EventFanoutResponse { event, deliveries })
}
