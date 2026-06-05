use axum::{
    Router,
    routing::{get, post},
    Json, extract::State,
    response::{IntoResponse, Sse, sse::Event},
    http::StatusCode,
};
use std::sync::Arc;
use std::convert::Infallible;
use tokio::sync::RwLock;
use futures::StreamExt;

use crate::config::Config;
use crate::pipeline::{Pipeline, RequestContext, ResponseContext};
use crate::client::HttpClient;
use crate::adapter::{self, BackendConfig, Protocol};
use crate::types::*;
use crate::metrics::MetricsCollector;
use crate::middleware::cache::ResponseCacheMiddleware;
use crate::middleware::circuit_breaker::CircuitBreakerMiddleware;
use crate::security;

pub struct AppState {
    pub config: Arc<RwLock<Config>>,
    pub pipeline: Arc<Pipeline>,
    pub client: HttpClient,
    pub metrics: Arc<MetricsCollector>,
    pub cache_mw: Option<Arc<ResponseCacheMiddleware>>,
    pub breaker_mw: Option<Arc<CircuitBreakerMiddleware>>,
}

pub fn create_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/v1/chat/completions", post(handle_chat_completions))
        .route("/v1/models", get(list_models))
        .route("/health", get(health))
        .with_state(state)
}

fn build_backend_request(ctx: &RequestContext) -> OpenAIRequest {
    let mut req = ctx.request.clone();
    if !ctx.modified_messages.is_empty() {
        let mut messages = Vec::new();
        for m in &ctx.modified_messages {
            let role = m.get("role").and_then(|r| r.as_str()).unwrap_or("user").to_string();
            let content = m.get("content").cloned().unwrap_or(serde_json::Value::String(String::new()));
            messages.push(Message { role, content });
        }
        req.messages = messages;
    }
    req.model = ctx.selected_model.clone();
    req
}

fn select_backend_by_model(model: &str) -> (BackendConfig, Arc<dyn adapter::Adapter>) {
    let (name, url, protocol, api_key) = if model.starts_with("MiniMax") {
        ("minimax", "https://api.minimax.chat/v1/chat/completions", "openai", "sk-cp-xd6FHMUfN6JslhCb2iaIE1v_MhMwDXxQfmiRxKzaZ76MdiNnuX8xa7o7nlpRQqXa8T8jouMx0lXKDeKLcFu7fYSpGEklosxZHWfex53oStaVxRur84H596E")
    } else if model.starts_with("deepseek") {
        ("deepseek", "https://api.deepseek.com/v1/chat/completions", "openai", "sk-90e6e06042fc46218f2afb5a1617871f")
    } else if model.starts_with("kimi") {
        ("kimi", "https://api.kimi.com/coding/v1/messages", "anthropic", "sk-kimi-Mnlb5NHl6iZ9a1T5A1VrQChs7yMSFUI62BsW4rnmez03TxcNgUrZanP74KxMePVd")
    } else if model.starts_with("mimo") {
        ("xiaomi", "https://token-plan-cn.xiaomimimo.com/v1/chat/completions", "openai", "tp-c1kxv1frr13mq2abankq2i88is8u0fnch9pclyi725fgaxhb")
    } else if model.starts_with("glm") {
        ("zhipu", "https://open.bigmodel.cn/api/anthropic/v1/messages", "anthropic", "375dbdd83a004011a620a893447a5307.3qucNr8e986yU37E")
    } else {
        ("bailian", "https://coding.dashscope.aliyuncs.com/apps/anthropic/v1/messages", "anthropic", "sk-sp-eedb8e9310da40eab57bc573b6f3cd67")
    };
    let backend = BackendConfig {
        name: name.to_string(),
        url: url.to_string(),
        protocol: Protocol::from_str(protocol),
        api_key: api_key.to_string(),
        model: model.to_string(),
    };
    let adapter = adapter::create_adapter(&backend.protocol);
    (backend, adapter)
}

async fn handle_chat_completions(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    Json(request): Json<OpenAIRequest>,
) -> impl IntoResponse {
    // --- 鐎瑰鍙忕仦?---
    {
        let cfg = state.config.read().await;
        if cfg.auth_enabled && !cfg.api_keys.is_empty() {
            let token = security::extract_bearer_token(&headers);
            match token {
                Some(t) if security::verify_api_key(&cfg.api_keys.iter().cloned().collect(), &t) => {}
                _ => {
                    let err = serde_json::json!({"error":{"message":"Unauthorized","type":"auth_error"}});
                    return (StatusCode::UNAUTHORIZED, Json(err)).into_response();
                }
            }
        }
    }

    let is_stream = request.stream.unwrap_or(false);
    let model_name = request.model.clone();

    let (mut backend, adapter) = select_backend_by_model(&model_name);

    // --- Pipeline 鐠囬攱鐪伴梼鑸殿唽 ---
    let mut req_ctx = RequestContext::new(request.clone(), state.config.clone());
    state.pipeline.execute_request(&mut req_ctx).await;

    // --- 閻旀梹鏌囬崳銊︻梾閺?---
    {
        let breaker = &state.breaker_mw;
        if let Some(ref b) = breaker {
            if !b.allow_request(&req_ctx.selected_model).await {
                state.metrics.record_error();
                let err = serde_json::json!({"error":{"message":"circuit breaker open","type":"server_error"}});
                return (StatusCode::SERVICE_UNAVAILABLE, Json(err)).into_response();
            }
        }
    }

    // --- 缂傛挸鐡ㄩ崨鎴掕厬濡偓閺?---
    {
        let cache = &state.cache_mw;
        if let Some(ref c) = cache {
            let key = ResponseCacheMiddleware::compute_cache_key(&request);
            if let Some(cached) = c.get(&key).await {
                state.metrics.record_cache(true);
                if let Ok(resp) = serde_json::from_value::<OpenAIResponse>(cached) {
                    return Json(resp).into_response();
                }
            }
        }
    }

    // --- 閺嬪嫬缂撶€圭偤妾拠閿嬬湴 ---
    let backend_req = build_backend_request(&req_ctx);
    backend.model = req_ctx.selected_model.clone();

    // --- 濞翠礁绱?---
    if is_stream {
        return match state.client.send_stream(&backend, adapter, &backend_req).await {
            Ok(backend_resp) => {
                state.metrics.record_request(&backend_req.model, 0, 0, 0.0, 0);
                if let Some(ref b) = state.breaker_mw {
                    b.record_success(&backend.name).await;
                }
                let stream = backend_resp.bytes_stream();
                let stream = stream.map(|chunk| {
                    let bytes = match chunk {
                        Ok(b) => b,
                        Err(_) => return Ok::<_, Infallible>(Event::default().data("data: [DONE]")),
                    };
                    let text = String::from_utf8_lossy(&bytes);
                    let mut last = None;
                    for line in text.lines() {
                        if line.is_empty() { continue; }
                        last = Some(Ok::<_, Infallible>(Event::default().data(line.to_string())));
                    }
                    last.unwrap_or_else(|| Ok(Event::default().data("")))
                });
                Sse::new(stream).into_response()
            }
            Err(e) => {
                state.metrics.record_error();
                if let Some(ref b) = state.breaker_mw {
                    b.record_failure(&backend.name).await;
                }
                let err = serde_json::json!({"error":{"message":e.to_string(),"type":"server_error"}});
                (StatusCode::BAD_GATEWAY, Json(err)).into_response()
            }
        };
    }

    // --- 闂堢偞绁﹀?---
    let start = std::time::Instant::now();
    match state.client.send_request(&backend, adapter, &backend_req).await {
        Ok(mut resp) => {
            let latency = start.elapsed().as_millis() as u64;
            let tokens_in = resp.usage.as_ref().map(|u| u.prompt_tokens as u64).unwrap_or(0);
            let tokens_out = resp.usage.as_ref().map(|u| u.completion_tokens as u64).unwrap_or(0);
            state.metrics.record_request(&resp.model, tokens_in, tokens_out, 0.0, latency);

            if let Some(ref b) = state.breaker_mw {
                b.record_success(&backend.name).await;
            }

            // --- Pipeline 閸濆秴绨查梼鑸殿唽 ---
            let mut resp_ctx = ResponseContext::new(resp.clone());
            let cache_key = ResponseCacheMiddleware::compute_cache_key(&request);
            resp_ctx.cache_key = Some(cache_key.clone());
            resp_ctx.raw_response = Some(serde_json::to_value(&resp).unwrap_or_default());
            state.pipeline.execute_response(&mut resp_ctx).await;

            // 閸愭瑥鍙嗙紓鎾崇摠
            if let Some(ref c) = state.cache_mw {
                if let Some(ref raw) = resp_ctx.raw_response {
                    c.set(&cache_key, raw.clone()).await;
                }
            }

            // 閸濆秴绨查崥搴☆槱閻?            if let Some(choice) = resp.choices.first_mut() {
                if let Some(ref mut content) = choice.message.content {
                    let trimmed = content.trim().to_string();
                    *content = trimmed;
                }
            }

            Json(resp).into_response()
        }
        Err(e) => {
            state.metrics.record_error();
            if let Some(ref b) = state.breaker_mw {
                b.record_failure(&backend.name).await;
            }
            let err = serde_json::json!({"error":{"message":e.to_string(),"type":"server_error"}});
            (StatusCode::BAD_GATEWAY, Json(err)).into_response()
        }
    }
}

async fn list_models() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "object": "list",
        "data": [
            { "id": "qwen3.6-plus", "object": "model", "owned_by": "bailian" },
            { "id": "MiniMax-M3", "object": "model", "owned_by": "minimax" },
            { "id": "MiniMax-M2.5", "object": "model", "owned_by": "minimax" },
            { "id": "MiniMax-M2.7", "object": "model", "owned_by": "minimax" },
            { "id": "deepseek-chat", "object": "model", "owned_by": "deepseek" },
            { "id": "deepseek-coder", "object": "model", "owned_by": "deepseek" },
            { "id": "kimi-code", "object": "model", "owned_by": "kimi" },
            { "id": "kimi-v4", "object": "model", "owned_by": "kimi" },
            { "id": "mimo-v2-omni", "object": "model", "owned_by": "xiaomi" },
            { "id": "mimo-v2-pro", "object": "model", "owned_by": "xiaomi" },
            { "id": "mimo-v2.5", "object": "model", "owned_by": "xiaomi" },
            { "id": "mimo-v2.5-pro", "object": "model", "owned_by": "xiaomi" },
            { "id": "glm-4-plus", "object": "model", "owned_by": "zhipu" },
            { "id": "glm-4-air", "object": "model", "owned_by": "zhipu" },
            { "id": "glm-4v-plus", "object": "model", "owned_by": "zhipu" },
        ]
    }))
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok" }))
}