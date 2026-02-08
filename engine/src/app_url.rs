use std::collections::HashMap;

const DOMAIN_KEY: &str = "DOMAIN";
const LOCAL_DOMAIN_PREFIXES: [&str; 3] = ["localhost", "127.0.0.1", "[::1]"];

fn is_local_domain(value: &str) -> bool {
    let lower = value.to_lowercase();
    LOCAL_DOMAIN_PREFIXES
        .iter()
        .any(|prefix| lower.starts_with(prefix))
}

pub fn normalize_domain(value: Option<&str>) -> Option<String> {
    let trimmed = value?.trim();
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.contains("://")
        || trimmed.contains('/')
        || trimmed.contains('?')
        || trimmed.contains('#')
        || trimmed.contains(':')
    {
        return None;
    }
    if !trimmed
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-')
    {
        return None;
    }
    Some(trimmed.to_string())
}

pub fn resolve_app_domain(settings: &HashMap<String, String>) -> Option<String> {
    if let Ok(env_value) = std::env::var("DOMAIN") {
        if let Some(normalized) = normalize_domain(Some(&env_value)) {
            return Some(normalized);
        }
    }
    settings
        .get(DOMAIN_KEY)
        .and_then(|value| normalize_domain(Some(value.as_str())))
}

pub fn resolve_app_base_url(settings: &HashMap<String, String>) -> Option<String> {
    let domain = resolve_app_domain(settings)?;
    let scheme = if is_local_domain(&domain) {
        "http"
    } else {
        "https"
    };
    Some(format!("{}://{}", scheme, domain))
}

pub fn resolve_api_base_url(settings: &HashMap<String, String>) -> Option<String> {
    resolve_app_base_url(settings).map(|base| format!("{}/api", base))
}
