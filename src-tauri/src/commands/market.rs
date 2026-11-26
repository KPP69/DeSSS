use crate::AppState;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tauri::State;

/// Search the market catalog by base symbol (exact or prefix) or native
/// symbol (substring, case insensitive), restricted to the checked
/// exchanges. Zero-network (PR-0.3): it reads only the in-memory
/// catalog (SQLite-backed at startup) and never awaits a fetch. An
/// empty catalog answers "目录未就绪，正在拉取" and triggers one
/// background refresh; a miss on a loaded catalog triggers one
/// background refresh of the checked exchanges (60s backoff inside),
/// and the page re-searches once when `CatalogUpdated` arrives.
/// Prices come from WS frames; rows with no frame after 5s get one
/// background REST fill for the top 5 rows. An empty query returns
/// nothing — the Data page never dumps the whole market over IPC.
#[tauri::command]
pub async fn search_market_info(
    state: State<'_, Arc<AppState>>,
    query: String,
    exchange_ids: Vec<String>,
) -> Result<Vec<crate::core::market::MarketInfo>, String> {
    use crate::market::catalog::MarketCatalog;
    let started = Instant::now();
    if query.trim().is_empty() {
        return Ok(vec![]);
    }
    let rows = state.catalog.search(&query, &exchange_ids);
    let venues = exchange_ids.len();
    if rows.is_empty() {
        // Miss: one background refresh of the checked exchanges (the
        // gate inside skips fresh/backed-off ones); the page
        // re-searches once when `CatalogUpdated` arrives.
        let catalog = state.catalog.clone();
        let ids = exchange_ids.clone();
        tauri::async_runtime::spawn(async move {
            catalog.refresh_exchanges(&ids, false).await;
        });
        let took = started.elapsed().as_millis();
        tracing::info!("search q={query} venues={venues} hits=0 took={took}ms network=0");
        if !exchange_ids.iter().any(|id| state.catalog.has_rows_for(id)) {
            return Err("目录未就绪，正在拉取".to_string());
        }
        return Ok(vec![]);
    }
    // Rows on screen: claim the 5s-staleness fallback slot (at most one
    // task per 10s) and let WS frames arrive first.
    if state.catalog.claim_fallback_slot() {
        let catalog = state.catalog.clone();
        let streams = state.streams.clone();
        let bus = state.bus.clone();
        let snapshot = rows.clone();
        tauri::async_runtime::spawn(async move {
            tokio::time::sleep(Duration::from_secs(5)).await;
            let stale = MarketCatalog::stale_top5(&snapshot, |ex, sym| {
                streams.is_stale(&(ex.to_string(), sym.to_string()))
            });
            if !stale.is_empty() && catalog.merge_enriched(stale).await {
                bus.publish(crate::bus::AppEvent::CatalogUpdated);
            }
        });
    }
    let took = started.elapsed().as_millis();
    tracing::info!(
        "search q={query} venues={venues} hits={} took={took}ms network=0",
        rows.len()
    );
    Ok(rows)
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogRefresh {
    pub count: usize,
    pub updated_at: i64,
}

/// Manually refresh the checked exchanges (ignoring the 30s cache).
/// Fire-and-forget (PR-0.3): the refresh runs in the background and
/// this returns immediately with the current snapshot; the page
/// re-searches when `CatalogUpdated` arrives. Async commands with a
/// `State` input must return a `Result` (Tauri requirement).
#[tauri::command]
pub async fn refresh_market_catalog(
    state: State<'_, Arc<AppState>>,
    exchange_ids: Vec<String>,
) -> Result<CatalogRefresh, String> {
    let catalog = state.catalog.clone();
    tauri::async_runtime::spawn(async move {
        catalog.refresh_exchanges(&exchange_ids, true).await;
    });
    Ok(CatalogRefresh {
        count: state.catalog.len(),
        updated_at: state.catalog.last_success_at().unwrap_or(0),
    })
}

/// Per-venue catalog status for the System page and the Data page
/// refresh button: row count, newest row time ("目录更新于"), and
/// the last refresh error, plus the log directory path. Sync
/// command — reads memory only.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VenueCatalogStatus {
    pub exchange_id: String,
    pub updated_at: Option<i64>,
    pub rows: usize,
    pub error: Option<String>,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogStatus {
    pub venues: Vec<VenueCatalogStatus>,
    pub log_dir: String,
}

#[tauri::command]
pub fn get_catalog_status(state: State<'_, Arc<AppState>>) -> CatalogStatus {
    let venues = crate::venues::all()
        .iter()
        .map(|v| v.id())
        .map(|id| {
            let (rows, updated_at) = state.catalog.venue_status(id);
            VenueCatalogStatus {
                exchange_id: id.to_string(),
                updated_at,
                rows,
                error: state.catalog.last_refresh_error(id),
            }
        })
        .collect();
    CatalogStatus {
        venues,
        log_dir: state.paths.log_dir.display().to_string(),
    }
}

/// Reconcile the on-demand realtime streams with the symbols on screen.
/// The Data page calls this whenever its search hits change (and with
/// an empty list when the query is cleared or the page is left): new
/// keys start streaming, removed keys are unsubscribed at once. Sync
/// command — the engine diffs the set itself, so repeat calls with the
/// same keys are no-ops.
#[tauri::command]
pub fn set_stream_symbols(
    state: State<'_, Arc<AppState>>,
    keys: Vec<crate::data_engine::realtime::StreamKeyInput>,
) {
    let keys: Vec<(String, String)> = keys
        .into_iter()
        .map(|k| (k.exchange_id, k.symbol))
        .collect();
    state.streams.set_wanted(keys);
}
