pub fn redis_value_to_i64(value: &redis::Value) -> Option<i64> {
    match value {
        redis::Value::Int(value) => Some(*value),
        redis::Value::BulkString(bytes) => std::str::from_utf8(bytes).ok()?.parse().ok(),
        redis::Value::SimpleString(value) => value.parse().ok(),
        redis::Value::Okay => None,
        _ => None,
    }
}

pub fn redis_value_to_uuid_text(value: &redis::Value) -> Option<String> {
    let text = match value {
        redis::Value::BulkString(bytes) => std::str::from_utf8(bytes).ok()?.to_string(),
        redis::Value::SimpleString(value) => value.clone(),
        _ => return None,
    };
    let normalized = text.trim().to_ascii_lowercase();

    is_uuid_text(&normalized).then_some(normalized)
}

fn is_uuid_text(value: &str) -> bool {
    if value.len() != 36 {
        return false;
    }

    value.chars().enumerate().all(|(index, ch)| match index {
        8 | 13 | 18 | 23 => ch == '-',
        _ => ch.is_ascii_hexdigit(),
    })
}
