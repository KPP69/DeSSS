//! Unified network transport: proxy resolution, the shared HTTP client,
//! and the one WebSocket dial (direct + HTTP CONNECT proxy).
//!
//! Everything that touches the network goes through here, so a proxy or
//! TLS fix lands once instead of once per adapter.
//!
//! Two rules live here and nowhere else:
//! - Read-only requests (`read_json`) retry ONCE on a stale pooled
//!   connection (`SendRequest` / `UnexpectedEof`). Writes (orders,
//!   cancels) must NEVER use it — a retried write can double-execute.
//! - Error text for the console never carries URL query parameters:
//!   `http_error` formats with `Display` (no URL) and `redact_url_query`
//!   cuts `?...` off any URL that slips into a message, so a signature
//!   can never be printed into a log line.

use std::error::Error as _;

pub mod proxy;
pub mod ws;

/// The one shared HTTP client for the whole process: a single connection
/// pool for every adapter and the market catalog. Creating a client per
/// request — or one per adapter — spawns a pool per client and exhausts
/// connections under concurrency, which surfaces as spurious `TimedOut`
/// errors. `reqwest::Client` is cheap to clone: clones share the pool.
/// 20s timeout covers the slowest endpoint (Lighter can be slow); the
/// timeout only bounds the worst case, it does not slow fast requests.
static SHARED_CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();

pub fn shared_http_client() -> reqwest::Client {
    SHARED_CLIENT.get_or_init(build_shared_client).clone()
}

fn build_shared_client() -> reqwest::Client {
    let mut b = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(20))
        // 15s idle pool: a proxy that drops idle connections sooner than
        // reqwest's default surfaces as spurious `UnexpectedEof` on the
        // next request; a short pool timeout retires idle connections
        // before the proxy kills them.
        .pool_idle_timeout(std::time::Duration::from_secs(15))
        .user_agent("DeSSS/0.1.0");
    if let Some(url) = proxy::resolve_proxy() {
        match reqwest::Proxy::all(&url) {
            Ok(p) => {
                b = b.proxy(p);
            }
            Err(e) => {
                tracing::warn!("[proxy] ignoring invalid proxy url {url}: {e}");
            }
        }
    }
    b.build().unwrap_or_default()
}

/// Cut the query string off every URL inside `text`: `https://h/p?a=b`
/// becomes `https://h/p`. Only `?` that follows `://` is touched, so a
/// plain `?` in prose is left alone. Applied to every console log line
/// (see `EventBus::log`) and to HTTP error text, so a signed request's
/// `signature` / `timestamp` parameters can never reach a log.
pub fn redact_url_query(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    loop {
        let Some(scheme) = rest.find("://") else {
            out.push_str(rest);
            break;
        };
        // The URL starts at the scheme's host part; find where it ends
        // (whitespace, quote or bracket) and cut the query there.
        let url_start = scheme + 3;
        let tail = &rest[url_start..];
        let url_end = tail
            .find(|c: char| {
                c.is_whitespace() || c == '"' || c == '\'' || c == ')' || c == ']' || c == '>'
            })
            .unwrap_or(tail.len());
        let url = &tail[..url_end];
        out.push_str(&rest[..url_start]);
        match url.find('?') {
            Some(q) => out.push_str(&url[..q]),
            None => out.push_str(url),
        }
        rest = &tail[url_end..];
    }
    out
}

/// One-line error text for a failed HTTP request, safe for the console:
/// `Display` (which, unlike `Debug`, never prints the request URL) with
/// any URL query stripped as a second layer.
pub fn http_error(e: &reqwest::Error) -> String {
    redact_url_query(&e.to_string())
}

/// True when the failure looks like a stale pooled connection that a
/// single retry may fix: the request never left (`is_request`, e.g.
/// `SendRequest`) or an I/O `UnexpectedEof` anywhere in the error's
/// source chain (a proxy-killed idle connection mid-send). Anything
/// else (timeout, status, DNS, TLS) is not retried.
pub fn is_retryable_http_error(e: &reqwest::Error) -> bool {
    if e.is_request() {
        return true;
    }
    let mut src: Option<&(dyn std::error::Error + 'static)> = e.source();
    while let Some(s) = src {
        if let Some(io) = s.downcast_ref::<std::io::Error>() {
            if io.kind() == std::io::ErrorKind::UnexpectedEof {
                return true;
            }
        }
        src = s.source();
    }
    false
}

/// Send a READ-ONLY request (`build` constructs it fresh on every
/// attempt) and return the raw response. On a stale-connection error
/// (`SendRequest` / `UnexpectedEof`) the request is rebuilt and sent
/// exactly once more; the old pooled connection is already dead by
/// then, so the retry opens a new one.
///
/// NEVER use this for orders, cancels or any other write: a retried
/// write can execute twice. Writes send once, plainly, and report
/// `Unresolved` when the outcome is unknown.
pub async fn send_read(
    build: impl Fn() -> reqwest::RequestBuilder,
) -> Result<reqwest::Response, String> {
    let mut attempt = 0;
    loop {
        attempt += 1;
        match build().send().await {
            Ok(resp) => return Ok(resp),
            Err(e) => {
                if attempt == 1 && is_retryable_http_error(&e) {
                    continue;
                }
                return Err(http_error(&e));
            }
        }
    }
}

/// Send a READ-ONLY JSON request and parse the response as JSON, with
/// the same single-retry rule as `send_read`. Non-success statuses and
/// parse failures are errors, never retried.
pub async fn read_json(
    build: impl Fn() -> reqwest::RequestBuilder,
) -> Result<serde_json::Value, String> {
    let resp = send_read(build).await?;
    let status = resp.status();
    let text = resp.text().await.map_err(|e| http_error(&e))?;
    if !status.is_success() {
        let snippet: String = text.chars().take(200).collect();
        return Err(redact_url_query(&format!("HTTP {status}：{snippet}")));
    }
    serde_json::from_str(&text).map_err(|e| format!("解析响应失败：{e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redact_url_query_cuts_signature_params() {
        let signed =
            "请求失败：https://fapi.asterdex.com/fapi/v1/account?timestamp=123&signature=deadbeef";
        assert_eq!(
            redact_url_query(signed),
            "请求失败：https://fapi.asterdex.com/fapi/v1/account"
        );
    }

    #[test]
    fn redact_url_query_keeps_plain_urls_and_questions() {
        assert_eq!(
            redact_url_query("见 https://api.hyperliquid.xyz/info 说明，失败了吗？"),
            "见 https://api.hyperliquid.xyz/info 说明，失败了吗？"
        );
        assert_eq!(
            redact_url_query("两个地址 https://a.example/x?a=1 和 http://b.example/y?c=2 结束"),
            "两个地址 https://a.example/x 和 http://b.example/y 结束"
        );
        assert_eq!(
            redact_url_query("没有 URL，只有问号？"),
            "没有 URL，只有问号？"
        );
    }
}
