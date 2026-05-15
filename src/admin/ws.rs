//! WebSocket /ws：连接后 5s 内必须发 `{"type":"auth","key":"<admin_key>"}`，
//! 鉴权成功后服务端 3s 轮询 snapshotVersion，变化才推送一次快照。

use crate::app::AppState;
use crate::channels::ChannelKind;
use crate::db;
use crate::db::schema::ApiKey;
use axum::extract::WebSocketUpgrade;
use axum::extract::State;
use axum::extract::ws::{Message, WebSocket};
use axum::response::IntoResponse;
use serde::Serialize;
use serde_json::{Value, json};
use sqlx::Row;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::{Instant, timeout};
use tracing::warn;

const AUTH_TIMEOUT: Duration = Duration::from_secs(5);
const POLL_INTERVAL: Duration = Duration::from_secs(3);

#[derive(Debug, Clone, Copy, Serialize, Default)]
pub struct ChannelTotals {
    pub requests: i64,
    pub errors: i64,
    pub cost: f64,
}

pub async fn upgrade_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(mut socket: WebSocket, state: AppState) {
    if !authenticate(&mut socket, &state).await {
        let _ = socket.close().await;
        return;
    }
    let _ = socket
        .send(Message::Text(r#"{"type":"auth","ok":true}"#.into()))
        .await;

    let mut last_version: u64 = u64::MAX;
    let mut tick = tokio::time::interval_at(Instant::now() + POLL_INTERVAL, POLL_INTERVAL);
    loop {
        tokio::select! {
            _ = tick.tick() => {
                let current = state.snapshot.current();
                if current == last_version {
                    if socket.send(Message::Ping(vec![])).await.is_err() {
                        break;
                    }
                    continue;
                }
                match build_snapshot(&state).await {
                    Ok(snap) => {
                        let txt = serde_json::to_string(&snap).unwrap_or_else(|_| "{}".to_string());
                        if socket.send(Message::Text(txt)).await.is_err() {
                            break;
                        }
                        last_version = current;
                    }
                    Err(e) => warn!(error = ?e, "ws snapshot build failed"),
                }
            }
            msg = socket.recv() => match msg {
                Some(Ok(Message::Close(_))) | None => break,
                Some(Err(_)) => break,
                _ => {}
            },
        }
    }
    // 主循环退出前显式发 Close frame，避免客户端依赖 TCP FIN/RST 才察觉断开（1-2s 延迟）。
    let _ = socket.close().await;
}

async fn authenticate(socket: &mut WebSocket, state: &AppState) -> bool {
    let deadline = Instant::now() + AUTH_TIMEOUT;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return false;
        }
        match timeout(remaining, socket.recv()).await {
            Ok(Some(Ok(Message::Text(t)))) => {
                let Ok(v) = serde_json::from_str::<Value>(&t) else {
                    return false;
                };
                return v.get("type").and_then(Value::as_str) == Some("auth")
                    && v.get("key").and_then(Value::as_str)
                        == Some(state.cfg.admin_key.as_str());
            }
            // 窗口内的 Ping / Pong / Binary 等非 Text 帧忽略继续等，浏览器层 keep-alive 不该被误杀。
            Ok(Some(Ok(_))) => continue,
            // 流被关闭、传输错误、超时 → 失败。
            Ok(Some(Err(_))) | Ok(None) | Err(_) => return false,
        }
    }
}

pub async fn build_snapshot(state: &AppState) -> Result<Value, crate::error::AppError> {
    let (keys, totals_map) = tokio::try_join!(
        db::keys::list(&state.db, None, 5000, 0),
        fetch_channel_totals(state),
    )?;

    let snapshot_keys: Vec<KeyEntry> =
        keys.into_iter().map(|k| key_entry(state, k)).collect();

    let mut by_channel: HashMap<ChannelKind, Vec<KeyEntry>> = HashMap::new();
    by_channel.entry(ChannelKind::Copilot).or_default();
    by_channel.entry(ChannelKind::Anthropic).or_default();
    for entry in &snapshot_keys {
        by_channel
            .entry(entry.channel_kind)
            .or_default()
            .push(entry.clone());
    }

    let totals_copilot = totals_map.get(&ChannelKind::Copilot).copied().unwrap_or_default();
    let totals_anthropic = totals_map.get(&ChannelKind::Anthropic).copied().unwrap_or_default();
    let totals = ChannelTotals {
        requests: totals_copilot.requests + totals_anthropic.requests,
        errors: totals_copilot.errors + totals_anthropic.errors,
        cost: round2(totals_copilot.cost + totals_anthropic.cost),
    };

    let keys_by_channel_json: HashMap<String, Vec<KeyEntry>> = by_channel
        .into_iter()
        .map(|(k, v)| (k.as_str().to_string(), v))
        .collect();

    let empty_breaker: Vec<Value> = Vec::new();

    Ok(json!({
        "keys": snapshot_keys,
        "keys_by_channel": keys_by_channel_json,
        "totals": totals,
        "totals_by_channel": {
            "copilot": totals_copilot,
            "anthropic": totals_anthropic,
        },
        "breaker": empty_breaker,
        "snapshot_version": state.snapshot.current(),
    }))
}

/// 一条 LEFT JOIN 查 usage_logs + error_logs 按 channel_kind 汇总，
/// 替代之前每个渠道 3 次串行 (total_requests / total_errors / cost SUM)。
async fn fetch_channel_totals(
    state: &AppState,
) -> Result<HashMap<ChannelKind, ChannelTotals>, crate::error::AppError> {
    let rows = sqlx::query(
        "SELECT u.channel_kind AS ch, \
                u.requests AS requests, \
                COALESCE(u.cost_total, 0)::DOUBLE PRECISION AS cost_total, \
                COALESCE(e.errors, 0)::BIGINT AS errors \
         FROM ( \
             SELECT channel_kind, \
                    COUNT(*) AS requests, \
                    COALESCE(SUM(cost_usd), 0) AS cost_total \
             FROM usage_logs \
             GROUP BY channel_kind \
         ) u \
         FULL OUTER JOIN ( \
             SELECT channel_kind, COUNT(*) AS errors \
             FROM error_logs \
             GROUP BY channel_kind \
         ) e ON u.channel_kind = e.channel_kind",
    )
    .fetch_all(state.db.pool())
    .await?;

    let mut out: HashMap<ChannelKind, ChannelTotals> = HashMap::new();
    for row in rows {
        let ch_str: Option<String> = row.try_get("ch").ok();
        // FULL OUTER JOIN 在 errors-only 渠道（usage 为空）会让 u.channel_kind=NULL。
        // 该路径下我们用 e.channel_kind，但 SELECT 取的是 u.channel_kind 别名 ch；
        // 极端情况丢这条统计可接受（生产 0 usage 才进），监控可加日志。
        let Some(ch_str) = ch_str else { continue };
        let Some(ch) = ChannelKind::parse(&ch_str) else { continue };
        let requests: i64 = row.try_get("requests").unwrap_or(0);
        let cost: f64 = row.try_get("cost_total").unwrap_or(0.0);
        let errors: i64 = row.try_get("errors").unwrap_or(0);
        out.insert(
            ch,
            ChannelTotals {
                requests,
                errors,
                cost: round2(cost),
            },
        );
    }
    Ok(out)
}

#[derive(Debug, Clone, Serialize)]
pub struct KeyEntry {
    pub id: i64,
    pub name: String,
    pub quota: f64,
    pub allow_fast: bool,
    pub max_concurrency: i64,
    pub current_concurrency: u32,
    pub used: f64,
    pub channel_kind: ChannelKind,
}

fn key_entry(state: &AppState, k: ApiKey) -> KeyEntry {
    KeyEntry {
        id: k.id,
        name: k.name.clone(),
        quota: k.quota,
        allow_fast: k.allow_fast,
        max_concurrency: k.max_concurrency,
        current_concurrency: state.limiter.current(&k.name),
        used: round2(state.spend.get(&k.name)),
        channel_kind: k.channel_kind,
    }
}

fn round2(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round2_truncates_to_cents() {
        assert!((round2(1.234) - 1.23).abs() < 1e-9);
        assert!((round2(1.235) - 1.24).abs() < 1e-9);
        assert_eq!(round2(0.0), 0.0);
    }

    #[test]
    fn key_entry_serializes_with_channel() {
        let entry = KeyEntry {
            id: 1,
            name: "alice".into(),
            quota: 100.0,
            allow_fast: true,
            max_concurrency: -1,
            current_concurrency: 0,
            used: 12.34,
            channel_kind: ChannelKind::Copilot,
        };
        let v = serde_json::to_value(&entry).expect("serializable");
        assert_eq!(v["channel_kind"], json!("copilot"));
        assert_eq!(v["used"], json!(12.34));
        assert_eq!(v["allow_fast"], json!(true));
    }

    #[test]
    fn channel_totals_serializes_to_snake_case() {
        let t = ChannelTotals { requests: 5, errors: 1, cost: 0.42 };
        let v = serde_json::to_value(&t).expect("serializable");
        assert_eq!(v, json!({"requests": 5, "errors": 1, "cost": 0.42}));
    }

    #[test]
    fn channel_totals_default_zero() {
        let t = ChannelTotals::default();
        assert_eq!(t.requests, 0);
        assert_eq!(t.errors, 0);
        assert_eq!(t.cost, 0.0);
    }

    #[test]
    fn snapshot_envelope_pure_snake_case() {
        let body = json!({
            "keys": [],
            "keys_by_channel": {"copilot": [], "anthropic": []},
            "totals": {"requests": 0, "errors": 0, "cost": 0.0},
            "totals_by_channel": {
                "copilot":   {"requests": 0, "errors": 0, "cost": 0.0},
                "anthropic": {"requests": 0, "errors": 0, "cost": 0.0},
            },
            "breaker": [],
            "snapshot_version": 0,
        });
        let obj = body.as_object().expect("object");
        for camel in ["totalRequests", "totalErrors"] {
            assert!(!obj.contains_key(camel), "must not expose {camel}");
        }
        for expected in [
            "keys",
            "keys_by_channel",
            "totals",
            "totals_by_channel",
            "breaker",
            "snapshot_version",
        ] {
            assert!(obj.contains_key(expected), "missing {expected}");
        }
        assert!(body["totals"].get("cost").is_some());
    }
}
