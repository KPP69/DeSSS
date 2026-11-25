//! Local proxy resolution (no dialing — see `ws`).
//!
//! Resolution order (first hit wins):
//! 1. custom proxy file `./data/proxy.json` (`{"url": "http://127.0.0.1:7897"}`),
//!    set from the System page;
//! 2. `HTTP_PROXY` / `HTTPS_PROXY` environment variables;
//! 3. the Windows system proxy registry
//!    (`HKCU\Software\Microsoft\Windows\CurrentVersion\Internet Settings`).
//!
//! reqwest's own default only reads env vars, so the file/registry sources
//! are applied explicitly (REST in `new_http_client_with_timeout`, WS in
//! `ws::connect_ws`). Only plain `http://` proxies are supported — that is
//! what Clash Verge exposes locally.

/// Effective proxy URL, if any.
pub fn resolve_proxy() -> Option<String> {
    read_proxy_file().or_else(env_proxy).or_else(registry_proxy)
}

/// Env vars, either case (reqwest checks both too; this VM's egress
/// proxy, for example, is set in lowercase).
fn env_proxy() -> Option<String> {
    for k in ["HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy"] {
        if let Ok(v) = std::env::var(k) {
            let v = v.trim().to_string();
            if !v.is_empty() {
                return Some(v);
            }
        }
    }
    None
}

fn read_proxy_file() -> Option<String> {
    let text = std::fs::read_to_string(crate::portable::data_dir().join("proxy.json")).ok()?;
    let v: serde_json::Value = serde_json::from_str(&text).ok()?;
    v.get("url")
        .and_then(|u| u.as_str())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// The user-configured proxy from `./data/proxy.json`, if set.
pub fn read_proxy_file_pub() -> Option<String> {
    read_proxy_file()
}

/// Persist (or clear with `None`) the user-configured proxy.
pub fn write_proxy_file(url: Option<&str>) -> Result<(), String> {
    let path = crate::portable::data_dir().join("proxy.json");
    match url.map(|s| s.trim()).filter(|s| !s.is_empty()) {
        Some(u) => {
            let _ = parse_proxy_url(u)?;
            if let Some(parent) = path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            let body = serde_json::json!({ "url": u });
            std::fs::write(
                &path,
                serde_json::to_string_pretty(&body).unwrap_or_default(),
            )
            .map_err(|e| format!("写入 proxy.json 失败：{e}"))?;
        }
        None => {
            let _ = std::fs::remove_file(&path);
        }
    }
    Ok(())
}

/// Parse an `http://host:port` proxy URL. `https://` proxies are rejected:
/// Clash-style local proxies are plain HTTP.
pub(crate) fn parse_proxy_url(raw: &str) -> Result<(String, u16), String> {
    let rest = raw
        .strip_prefix("http://")
        .ok_or_else(|| "代理地址须以 http:// 开头".to_string())?;
    let hostport = rest.split(['/', '?']).next().unwrap_or("");
    let (host, port) = match hostport.split_once(':') {
        Some((h, p)) => (
            h.to_string(),
            p.parse::<u16>()
                .map_err(|_| "代理地址端口须为数字".to_string())?,
        ),
        None => (hostport.to_string(), 8080),
    };
    if host.is_empty() {
        return Err("代理地址缺少主机名".to_string());
    }
    Ok((host, port))
}

/// Parse a Windows `ProxyServer` registry value: `127.0.0.1:7897`, or a
/// per-protocol list like `http=127.0.0.1:7897;https=127.0.0.1:7897`.
fn parse_proxy_server(server: &str) -> Option<String> {
    let first = server.split(';').next()?.trim();
    if first.is_empty() {
        return None;
    }
    let addr = first.split('=').last()?.trim();
    if addr.is_empty() {
        return None;
    }
    Some(if addr.contains("://") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    })
}

#[cfg(windows)]
fn registry_proxy() -> Option<String> {
    use winreg::enums::HKEY_CURRENT_USER;
    use winreg::RegKey;
    let key = RegKey::predef(HKEY_CURRENT_USER)
        .open_subkey("Software\\Microsoft\\Windows\\CurrentVersion\\Internet Settings")
        .ok()?;
    let enabled: u32 = key.get_value("ProxyEnable").ok()?;
    if enabled == 0 {
        return None;
    }
    let server: String = key.get_value("ProxyServer").ok()?;
    parse_proxy_server(&server)
}

#[cfg(not(windows))]
fn registry_proxy() -> Option<String> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proxy_server_parses_plain_and_per_protocol_forms() {
        assert_eq!(
            parse_proxy_server("127.0.0.1:7897"),
            Some("http://127.0.0.1:7897".to_string())
        );
        assert_eq!(
            parse_proxy_server("http=127.0.0.1:7897;https=127.0.0.1:7897"),
            Some("http://127.0.0.1:7897".to_string())
        );
        assert_eq!(parse_proxy_server(""), None);
        assert_eq!(
            parse_proxy_server("http://127.0.0.1:7897"),
            Some("http://127.0.0.1:7897".to_string())
        );
    }

    #[test]
    fn proxy_url_requires_http_scheme_and_port() {
        assert_eq!(
            parse_proxy_url("http://127.0.0.1:7897").unwrap(),
            ("127.0.0.1".to_string(), 7897)
        );
        assert!(parse_proxy_url("https://127.0.0.1:7897").is_err());
        assert!(parse_proxy_url("127.0.0.1:7897").is_err());
        assert!(parse_proxy_url("http://127.0.0.1:abc").is_err());
    }

    #[test]
    fn proxy_file_roundtrips_and_clears() {
        // Env vars are process-global: save, clear, restore.
        let saved: Vec<(String, Option<String>)> =
            ["HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy"]
                .iter()
                .map(|k| (k.to_string(), std::env::var(k).ok()))
                .collect();
        for k in ["HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy"] {
            std::env::remove_var(k);
        }
        write_proxy_file(Some("http://127.0.0.1:7897")).expect("write");
        assert_eq!(resolve_proxy(), Some("http://127.0.0.1:7897".to_string()));
        write_proxy_file(None).expect("clear");
        assert_eq!(resolve_proxy(), None);
        for (k, v) in saved {
            match v {
                Some(v) => std::env::set_var(&k, v),
                None => std::env::remove_var(&k),
            }
        }
    }
}
