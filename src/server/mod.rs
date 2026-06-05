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
use crate::middleware::semantic_cache::SemanticCacheMiddleware;
use crate::middleware::circuit_breaker::CircuitBreakerMiddleware;
use crate::security;

pub struct AppState {
    pub config: Arc<RwLock<Config>>,
    pub pipeline: Arc<Pipeline>,
    pub client: HttpClient,
    pub metrics: Arc<MetricsCollector>,
    pub cache_mw: Option<Arc<ResponseCacheMiddleware>>,
    pub semantic_cache: Option<Arc<SemanticCacheMiddleware>>,
    pub breaker_mw: Option<Arc<CircuitBreakerMiddleware>>,
}

pub fn create_router(state: Arc<AppState>) -> Router {
    Router::new()
        // OpenAI 兼容
        .route("/v1/chat/completions", post(handle_chat_completions))
        .route("/v1/compare", post(handle_compare))
        .route("/v1/models", get(list_models))
        .route("/chat/completions", post(handle_chat_completions))
        .route("/models", get(list_models))
        // Ollama 兼容
        .route("/api/show", post(ollama_show))
        .route("/api/generate", post(handle_chat_completions))
        .route("/api/chat", post(handle_chat_completions))
        .route("/api/tags", get(list_models))
        .route("/api/v1/models", get(list_models))
        .route("/api/v1/chat/completions", post(handle_chat_completions))
        .route("/v1/props", get(generic_ok))
        .route("/props", get(generic_ok))
        .route("/version", get(version_handler))
        // 内部
        .route("/health", get(health))
        .route("/stats", get(stats_handler))
        .route("/semantic-cache/stats", get(semantic_cache_stats))
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

fn select_backend_by_model(
    model: &str,
    protocol_override: Option<&str>,
    request: &OpenAIRequest,
) -> (BackendConfig, Arc<dyn adapter::Adapter>) {
    // 模型配置表: 模型名前缀 -> (后端名, Anthropic端点, 默认协议, 环境变量名, OpenAI端点)
    let backends: Vec<(&str, &str, &str, &str, &str, Option<&str>)
    > = vec![
        ("minimax", "MiniMax", "https://api.minimaxi.com/anthropic/v1/messages", "anthropic", "MINIMAX_API_KEY", Some("https://api.minimaxi.com/v1/chat/completions")),
        ("deepseek", "deepseek", "https://api.deepseek.com/anthropic/v1/messages", "anthropic", "DEEPSEEK_API_KEY", Some("https://api.deepseek.com/v1/chat/completions")),
        ("kimi", "kimi", "https://api.kimi.com/coding/v1/messages", "anthropic", "KIMI_API_KEY", None),
        ("mimo", "xiaomi", "https://token-plan-cn.xiaomimimo.com/v1/chat/completions", "openai", "MIMO_API_KEY", None),
        ("glm", "zhipu", "https://open.bigmodel.cn/api/anthropic/v1/messages", "anthropic", "ZHIPU_API_KEY", Some("https://open.bigmodel.cn/api/paas/v4/chat/completions")),
    ];

    let mut matched = None;
    for (prefix, name, url, proto, env_key, alt_url) in &backends {
        if model.to_lowercase().starts_with(prefix) {
            matched = Some((*name, *url, *proto, *env_key, *alt_url));
            break;
        }
    }

    let (name, default_url, default_protocol, env_key, alt_url) = matched.unwrap_or(
        ("bailian", "https://coding.dashscope.aliyuncs.com/apps/anthropic/v1/messages", "anthropic", "DASHSCOPE_API_KEY", Some("https://coding.dashscope.aliyuncs.com/v1/chat/completions"))
    );

    // 智能协议选择
    // 优先级: 请求头覆盖 > 请求特征 > 模型默认
    let protocol = if let Some(override_proto) = protocol_override {
        override_proto.to_string()
    } else {
        // 根据请求特征选择最优协议
        select_optimal_protocol(name, default_protocol, alt_url, request)
    };

    // 根据协议选择端点
    // default_url 是 Anthropic 端点，alt_url 是 OpenAI 端点（如果有的话）
    let url = if protocol == "anthropic" {
        default_url
    } else if let Some(openai_url) = alt_url {
        openai_url
    } else {
        default_url
    };

    let api_key = std::env::var(env_key).unwrap_or_default();
    let backend = BackendConfig {
        name: name.to_string(),
        url: url.to_string(),
        protocol: Protocol::from_str(&protocol),
        api_key: api_key.to_string(),
        model: model.to_string(),
    };
    let adapter = adapter::create_adapter(&backend.protocol);
    (backend, adapter)
}

/// 根据请求特征选择最优协议
fn select_optimal_protocol(
    backend_name: &str,
    default_protocol: &str,
    alt_url: Option<&str>,
    request: &OpenAIRequest,
) -> String {
    // 如果没有替代端点，只能用默认
    if alt_url.is_none() {
        return default_protocol.to_string();
    }

    let has_tools = request.tools.is_some();
    let _has_stream = request.stream.unwrap_or(false);

    match backend_name {
        "bailian" => {
            // 百炼双协议都支持tools/thinking
            // Anthropic协议thinking更标准，默认用A
            // 但流式+tools时O协议更好
            if has_tools {
                "openai".to_string()
            } else {
                "anthropic".to_string()
            }
        }
        "deepseek" => {
            // DeepSeek双协议都支持tools/thinking
            // OpenAI是原生协议，默认用O
            "openai".to_string()
        }
        "zhipu" => {
            // GLM双协议
            // Anthropic: chat正常，无thinking
            // OpenAI: thinking有，chat空(只有reasoning)
            // 有tools → OpenAI
            // 普通chat → Anthropic
            if has_tools {
                "openai".to_string()
            } else {
                "anthropic".to_string()
            }
        }
        "minimax" => {
            // MiniMax双协议
            // OpenAI: chat正常，有thinking
            // Anthropic: chat空(只有thinking)
            // 默认用OpenAI
            "openai".to_string()
        }
        _ => default_protocol.to_string(),
    }
}

async fn handle_chat_completions(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    Json(request): Json<OpenAIRequest>,
) -> impl IntoResponse {
    let req_id = uuid::Uuid::new_v4().to_string();
    let span = tracing::info_span!("chat", req_id = %req_id);
    let _enter = span.enter();

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
    
    // 读取 X-Protocol 请求头
    let protocol_override = headers.get("x-protocol")
        .and_then(|v| v.to_str().ok());
    
    tracing::info!(model = %model_name, stream = is_stream, protocol = ?protocol_override, "request received");

    let (mut backend, adapter) = select_backend_by_model(&model_name, protocol_override, &request);

    let mut req_ctx = RequestContext::new(request.clone(), state.config.clone());
    state.pipeline.execute_request(&mut req_ctx).await;

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

    {
        let cache = &state.cache_mw;
        if let Some(ref c) = cache {
            let key = ResponseCacheMiddleware::compute_cache_key(&request);
            if let Some(cached) = c.get_with_prefix(&key, &request).await {
                state.metrics.record_cache(true);
                if let Ok(resp) = serde_json::from_value::<OpenAIResponse>(cached) {
                    return Json(resp).into_response();
                }
            }
        }
    }

    // 语义缓存：embedding 相似度匹配
    {
        let sem_cache = &state.semantic_cache;
        if let Some(ref sc) = sem_cache {
            if let Some(cached) = sc.get(&request).await {
                state.metrics.record_cache(true);
                if let Ok(resp) = serde_json::from_value::<OpenAIResponse>(cached) {
                    tracing::info!("semantic cache HIT, returning");
                    return Json(resp).into_response();
                }
            }
        }
    }

    // 上下文控制：截断旧消息保留最新 N 条
    let max_msgs = state.config.read().await.max_messages;
    if req_ctx.request.messages.len() > max_msgs {
        let system_msgs: Vec<Message> = req_ctx.request.messages.iter()
            .filter(|m| m.role == "system")
            .cloned()
            .collect();
        let non_system: Vec<Message> = req_ctx.request.messages.iter()
            .filter(|m| m.role != "system")
            .cloned()
            .collect();
        
        let keep_count = max_msgs.saturating_sub(system_msgs.len());
        let start = non_system.len().saturating_sub(keep_count);
        let kept_non_system = &non_system[start..];
        
        let mut truncated = system_msgs;
        truncated.extend_from_slice(kept_non_system);
        
        tracing::info!(
            original = req_ctx.request.messages.len(),
            truncated = truncated.len(),
            max_messages = max_msgs,
            "messages truncated"
        );
        
        req_ctx.request.messages = truncated;
    }

    let backend_req = build_backend_request(&req_ctx);
    backend.model = req_ctx.selected_model.clone();

    if is_stream {
        return match state.client.send_stream(&backend, adapter.clone(), &backend_req).await {
            Ok(backend_resp) => {
                state.metrics.record_request(&backend_req.model, 0, 0, 0.0, 0);
                if let Some(ref b) = state.breaker_mw {
                    b.record_success(&backend.name).await;
                }
                let is_anthropic = matches!(backend.protocol, crate::adapter::Protocol::Anthropic);
                let stream = backend_resp.bytes_stream();
                let model_name = backend_req.model.clone();
                let adapter_ref = adapter.clone();
                let stream = stream.filter_map(move |chunk| {
                    let bytes = match chunk {
                        Ok(b) => b,
                        Err(_) => return futures::future::ready(Some(Ok::<_, Infallible>(Event::default().data("[DONE]")))),
                    };
                    let text = String::from_utf8_lossy(&bytes);
                    let mut events = Vec::new();
                    for line in text.lines() {
                        if line.is_empty() { continue; }
                        if is_anthropic {
                            // Anthropic backend: convert SSE events to OpenAI format
                            // Anthropic SSE has "event:" and "data:" pairs; skip "event:" lines
                            if line.starts_with("event:") { continue; }
                            if let Some(chunk) = adapter_ref.stream_event_to_client(line, &model_name) {
                                if let Ok(json) = serde_json::to_string(&chunk) {
                                    events.push(Ok::<_, Infallible>(Event::default().data(json)));
                                }
                            }
                        } else {
                            // OpenAI backend: strip "data: " prefix and pass through
                            let cleaned = line.strip_prefix("data: ").unwrap_or(line);
                            if cleaned == "[DONE]" {
                                events.push(Ok::<_, Infallible>(Event::default().data("[DONE]")));
                            } else {
                                events.push(Ok::<_, Infallible>(Event::default().data(cleaned.to_string())));
                            }
                        }
                    }
                    futures::future::ready(if events.is_empty() { None } else { Some(events.into_iter().next().unwrap()) })
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

    let start = std::time::Instant::now();
    match state.client.send_request(&backend, adapter, &backend_req).await {
        Ok(mut resp) => {
            let latency = start.elapsed().as_millis() as u64;
            let tokens_in = resp.usage.as_ref().map(|u| u.prompt_tokens as u64).unwrap_or(0);
            let tokens_out = resp.usage.as_ref().map(|u| u.completion_tokens as u64).unwrap_or(0);
            let cost = compute_cost(&resp.model, tokens_in, tokens_out);
            state.metrics.record_request(&resp.model, tokens_in, tokens_out, cost, latency);

            if let Some(ref b) = state.breaker_mw {
                b.record_success(&backend.name).await;
            }

            let mut resp_ctx = ResponseContext::new(resp.clone());
            let cache_key = ResponseCacheMiddleware::compute_cache_key(&request);
            resp_ctx.cache_key = Some(cache_key.clone());
            resp_ctx.raw_response = Some(serde_json::to_value(&resp).unwrap_or_default());
            state.pipeline.execute_response(&mut resp_ctx).await;

            if let Some(ref c) = state.cache_mw {
                if let Some(ref raw) = resp_ctx.raw_response {
                    c.set_with_prefix(&cache_key, &request, raw.clone()).await;
                }
            }

            // 写入语义缓存
            if let Some(ref sc) = state.semantic_cache {
                if let Some(ref raw) = resp_ctx.raw_response {
                    sc.set(&request, raw.clone()).await;
                }
            }

            if let Some(choice) = resp.choices.first_mut() {
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
            
            // Fallback: 主模型失败时尝试备用模型
            let err_str = e.to_string();
            let err_json = if let Ok(val) = serde_json::from_str::<serde_json::Value>(&err_str) {
                val
            } else {
                serde_json::json!({"error":{"message":err_str,"type":"server_error"}})
            };
            
            let status_code = err_json.get("error")
                .and_then(|e| e.get("code"))
                .and_then(|c| c.as_u64())
                .unwrap_or(500);
            
            // 只有 429/5xx 才触发 fallback
            let should_fallback = status_code == 429 || (500..=599).contains(&status_code);
            
            if should_fallback {
                // 定义 fallback 链：同平台不同模型
                let fallback_chain = match backend.name.as_str() {
                    "bailian" => vec!["qwen3.5-plus", "qwen3.6-plus"],
                    "deepseek" => vec!["deepseek-chat", "deepseek-v4-flash"],
                    "zhipu" => vec!["glm-4", "glm-5.1"],
                    "minimax" => vec!["MiniMax-M2.5", "MiniMax-M3"],
                    _ => vec![],
                };
                
                for fallback_model in fallback_chain {
                    if fallback_model == backend_req.model.as_str() {
                        continue;
                    }
                    tracing::info!("fallback: trying model={}", fallback_model);
                    
                    let (mut fb_backend, fb_adapter) = select_backend_by_model(fallback_model, None, &req_ctx.request);
                    let mut fb_req = backend_req.clone();
                    fb_req.model = fallback_model.to_string();
                    
                    match state.client.send_request(&fb_backend, fb_adapter, &fb_req).await {
                        Ok(mut resp) => {
                            tracing::info!("fallback: success model={}", fallback_model);
                            state.metrics.record_request(&resp.model, 0, 0, 0.0, 0);
                            if let Some(ref b) = state.breaker_mw {
                                b.record_success(&fb_backend.name).await;
                            }
                            return Json(resp).into_response();
                        }
                        Err(_) => {
                            tracing::warn!("fallback: failed model={}", fallback_model);
                            continue;
                        }
                    }
                }
            }
            
            let http_status = match status_code {
                400 => StatusCode::BAD_REQUEST,
                401 => StatusCode::UNAUTHORIZED,
                403 => StatusCode::FORBIDDEN,
                404 => StatusCode::NOT_FOUND,
                429 => StatusCode::TOO_MANY_REQUESTS,
                500..=599 => StatusCode::BAD_GATEWAY,
                _ => StatusCode::BAD_GATEWAY,
            };
            (http_status, Json(err_json)).into_response()
        }
    }
}

async fn list_models() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "object": "list",
        "data": [
            { "id": "qwen3.6-plus", "object": "model", "owned_by": "bailian" },
            { "id": "qwen3.5-plus", "object": "model", "owned_by": "bailian" },
            { "id": "MiniMax-M3", "object": "model", "owned_by": "minimax" },
            { "id": "MiniMax-M2.5", "object": "model", "owned_by": "minimax" },
            { "id": "MiniMax-M2.7", "object": "model", "owned_by": "minimax" },
            { "id": "deepseek-chat", "object": "model", "owned_by": "deepseek" },
            { "id": "deepseek-coder", "object": "model", "owned_by": "deepseek" },
            { "id": "deepseek-v4-flash", "object": "model", "owned_by": "deepseek" },
            { "id": "deepseek-v4-pro", "object": "model", "owned_by": "deepseek" },
            { "id": "kimi-code", "object": "model", "owned_by": "kimi" },
            { "id": "kimi-v4", "object": "model", "owned_by": "kimi" },
            { "id": "kimi-v3", "object": "model", "owned_by": "kimi" },
            { "id": "kimi-2.6", "object": "model", "owned_by": "kimi" },
            { "id": "kimi-k2.6", "object": "model", "owned_by": "kimi" },
            { "id": "kimi-k2.5", "object": "model", "owned_by": "kimi" },
            { "id": "kimi-code-2.6", "object": "model", "owned_by": "kimi" },
            { "id": "kimi-code-v3", "object": "model", "owned_by": "kimi" },
            { "id": "kimi-latest", "object": "model", "owned_by": "kimi" },
            { "id": "mimo-v2-omni", "object": "model", "owned_by": "xiaomi" },
            { "id": "mimo-v2-pro", "object": "model", "owned_by": "xiaomi" },
            { "id": "mimo-v2.5", "object": "model", "owned_by": "xiaomi" },
            { "id": "mimo-v2.5-pro", "object": "model", "owned_by": "xiaomi" },
            { "id": "glm-4-plus", "object": "model", "owned_by": "zhipu" },
            { "id": "glm-4-air", "object": "model", "owned_by": "zhipu" },
            { "id": "glm-4v-plus", "object": "model", "owned_by": "zhipu" },
            { "id": "glm-5.1", "object": "model", "owned_by": "zhipu" },
        ]
    }))
}

async fn health(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    // 检查各后端健康状态
    let backends = vec![
        ("bailian", "https://coding.dashscope.aliyuncs.com/apps/anthropic/v1/messages"),
        ("deepseek", "https://api.deepseek.com/v1/chat/completions"),
        ("zhipu", "https://open.bigmodel.cn/api/anthropic/v1/messages"),
        ("minimax", "https://api.minimaxi.com/v1/chat/completions"),
    ];
    
    let mut backend_status = serde_json::Map::new();
    for (name, url) in backends {
        let healthy = check_backend_health(url, "***").await;
        backend_status.insert(name.to_string(), serde_json::json!({
            "url": url,
            "healthy": healthy,
        }));
    }
    
    // 熔断器状态
    let breaker_status = if let Some(ref breaker) = state.breaker_mw {
        serde_json::json!({"enabled": true})
    } else {
        serde_json::json!({"enabled": false})
    };
    
    Json(serde_json::json!({
        "status": "ok",
        "backends": backend_status,
        "circuit_breaker": breaker_status,
    }))
}

/// 简单健康检查：GET 请求后端，带 minimal payload
async fn check_backend_health(url: &str, api_key: &str) -> bool {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build();
    if let Ok(client) = client {
        let mut req = client.get(url).build().unwrap();
        // 添加认证头
        req.headers_mut().insert(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", api_key).parse().unwrap(),
        );
        if let Ok(resp) = client.execute(req).await {
            return resp.status().is_success() 
                || resp.status().as_u16() == 404 
                || resp.status().as_u16() == 405;
            // 405 Method Not Allowed 也算通（端点存在只是不支持 GET）
        }
    }
    false
}

async fn stats_handler(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    Json((*state.metrics).snapshot())
}

async fn semantic_cache_stats(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    if let Some(ref sc) = state.semantic_cache {
        Json(sc.stats().await)
    } else {
        Json(serde_json::json!({"enabled": false}))
    }
}

async fn ollama_show(
    State(_state): State<Arc<AppState>>,
    Json(body): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let model = body.get("name").and_then(|v| v.as_str()).unwrap_or("unknown");
    Json(serde_json::json!({
        "license": "",
        "modelfile": "",
        "parameters": "",
        "template": "",
        "details": {
            "parent_model": "",
            "format": "gguf",
            "family": "xingdu",
            "families": ["xingdu"],
            "parameter_size": "proxy",
            "quantization_level": "Q4_0",
        },
        "model_info": {
            "general.architecture": "xingdu",
            "general.name": model,
        },
        "name": model,
    }))
}

async fn generic_ok() -> Json<serde_json::Value> {
    Json(serde_json::json!({}))
}

async fn version_handler() -> Json<serde_json::Value> {
    Json(serde_json::json!({"version": "0.1.0"}))
}

/// 计算请求成本（人民币元）
fn compute_cost(model: &str, tokens_in: u64, tokens_out: u64) -> f64 {
    // 单价：元/百万token
    let (input_price, output_price) = match model {
        m if m.starts_with("qwen") => (2.0, 6.0),
        m if m.starts_with("deepseek") => (1.0, 2.0),
        m if m.starts_with("glm") => (5.0, 5.0),
        m if m.starts_with("MiniMax") => (10.0, 10.0),
        m if m.starts_with("kimi") => (12.0, 60.0),
        _ => (5.0, 5.0),
    };
    
    let input_cost = tokens_in as f64 * input_price / 1_000_000.0;
    let output_cost = tokens_out as f64 * output_price / 1_000_000.0;
    input_cost + output_cost
}

/// 模型擂台：并行对比多个模型回答
async fn handle_compare(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    Json(request): Json<OpenAIRequest>,
) -> impl IntoResponse {
    let req_id = uuid::Uuid::new_v4().to_string();
    let span = tracing::info_span!("compare", req_id = %req_id);
    let _enter = span.enter();

    // 认证
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

    // 从请求头 X-Compare-Models 读取要对比的模型列表
    let models = headers.get("x-compare-models")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.split(',').map(|m| m.trim().to_string()).collect::<Vec<_>>())
        .unwrap_or_else(|| vec![
            "qwen3.6-plus".to_string(),
            "deepseek-chat".to_string(),
        ]);

    if models.is_empty() {
        let err = serde_json::json!({"error":{"message":"No models specified","type":"invalid_request"}});
        return (StatusCode::BAD_REQUEST, Json(err)).into_response();
    }

    tracing::info!(models = ?models, "compare request");

    // 并行发请求
    let mut tasks = Vec::new();
    let protocol_override = headers.get("x-protocol")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    for model in &models {
        let state = state.clone();
        let model = model.clone();
        let mut req = request.clone();
        req.model = model.clone();
        req.stream = Some(false);
        let proto = protocol_override.clone();

        let task = tokio::spawn(async move {
            let start = std::time::Instant::now();

            let (mut backend, adapter) = select_backend_by_model(&model, proto.as_deref(), &req);

            let mut req_ctx = RequestContext::new(req.clone(), state.config.clone());
            state.pipeline.execute_request(&mut req_ctx).await;

            let backend_req = build_backend_request(&req_ctx);
            backend.model = req_ctx.selected_model.clone();

            match state.client.send_request(&backend, adapter, &backend_req).await {
                Ok(mut resp) => {
                    let latency = start.elapsed().as_millis() as u64;
                    let tokens_in = resp.usage.as_ref().map(|u| u.prompt_tokens as u64).unwrap_or(0);
                    let tokens_out = resp.usage.as_ref().map(|u| u.completion_tokens as u64).unwrap_or(0);
                    let cost = compute_cost(&resp.model, tokens_in, tokens_out);
                    state.metrics.record_request(&resp.model, tokens_in, tokens_out, cost, latency);

                    let content = resp.choices.first()
                        .and_then(|c| c.message.content.as_ref())
                        .cloned()
                        .unwrap_or_default();

                    serde_json::json!({
                        "model": resp.model,
                        "content": content,
                        "latency_ms": latency,
                        "cost_yuan": cost,
                        "tokens_input": tokens_in,
                        "tokens_output": tokens_out,
                        "success": true,
                        "error": null,
                    })
                }
                Err(e) => {
                    state.metrics.record_error();
                    serde_json::json!({
                        "model": model,
                        "content": null,
                        "latency_ms": start.elapsed().as_millis() as u64,
                        "cost_yuan": 0.0,
                        "tokens_input": 0,
                        "tokens_output": 0,
                        "success": false,
                        "error": e.to_string(),
                    })
                }
            }
        });
        tasks.push(task);
    }

    // 收集结果
    let mut results = Vec::new();
    let mut total_cost = 0.0;
    let mut fastest_ms: u64 = u64::MAX;
    let mut fastest_model = String::new();
    let mut cheapest_yuan: f64 = f64::MAX;
    let mut cheapest_model = String::new();

    for task in tasks {
        if let Ok(result) = task.await {
            if let Some(latency) = result.get("latency_ms").and_then(|v| v.as_u64()) {
                if let Some(model) = result.get("model").and_then(|v| v.as_str()) {
                    if latency < fastest_ms {
                        fastest_ms = latency;
                        fastest_model = model.to_string();
                    }
                }
            }
            if let Some(cost) = result.get("cost_yuan").and_then(|v| v.as_f64()) {
                if let Some(model) = result.get("model").and_then(|v| v.as_str()) {
                    total_cost += cost;
                    if cost < cheapest_yuan {
                        cheapest_yuan = cost;
                        cheapest_model = model.to_string();
                    }
                }
            }
            results.push(result);
        }
    }

    let summary = serde_json::json!({
        "total_cost": total_cost,
        "fastest": fastest_model,
        "fastest_ms": if fastest_ms == u64::MAX { 0 } else { fastest_ms },
        "cheapest": cheapest_model,
        "cheapest_yuan": if cheapest_yuan == f64::MAX { 0.0 } else { cheapest_yuan },
        "model_count": models.len(),
        "success_count": results.iter().filter(|r| r.get("success").and_then(|v| v.as_bool()).unwrap_or(false)).count(),
    });

    Json(serde_json::json!({
        "results": results,
        "summary": summary,
    })).into_response()
}
