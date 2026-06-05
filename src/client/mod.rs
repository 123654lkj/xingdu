use std::sync::Arc;
use crate::adapter::{Adapter, BackendConfig};
use crate::types::{OpenAIRequest, OpenAIResponse};

#[derive(Clone)]
pub struct HttpClient {
    inner: reqwest::Client,
}

impl HttpClient {
    pub fn new() -> anyhow::Result<Self> {
        let client = reqwest::Client::builder().user_agent("curl/8.5.0")
            .pool_max_idle_per_host(20)
            .pool_idle_timeout(std::time::Duration::from_secs(30))
            .timeout(std::time::Duration::from_secs(120))
            .build()?;
        Ok(HttpClient { inner: client })
    }

    pub async fn send_request(
        &self,
        backend: &BackendConfig,
        adapter: Arc<dyn Adapter>,
        request: &OpenAIRequest,
    ) -> anyhow::Result<OpenAIResponse> {
        let payload = adapter.request_to_backend(request, backend)?;
        let body_str = serde_json::to_string(&payload)?;
        tracing::info!("xingdu sending to {}: {}", backend.url, body_str);
        tracing::info!("xingdu headers: protocol={:?}, api_key=***", backend.protocol);
        let hdrs = self.build_headers(backend);
        let resp = self.inner
            .post(&backend.url)
            .headers(hdrs)
            .body(body_str)
            .send()
            .await?;
        let status = resp.status();
        let body_text = resp.text().await?;
        if !status.is_success() {
            // 将后端错误映射为 OpenAI 标准错误格式
            let (error_type, error_message) = map_backend_error(status, &body_text);
            let openai_error = serde_json::json!({
                "error": {
                    "message": error_message,
                    "type": error_type,
                    "code": status.as_u16(),
                }
            });
            anyhow::bail!(openai_error.to_string());
        }
        let body_val: serde_json::Value = serde_json::from_str(&body_text)?;
        let openai_resp = adapter.response_to_client(body_val, &request.model)?;
        Ok(openai_resp)
    }

    pub async fn send_stream(
        &self,
        backend: &BackendConfig,
        adapter: Arc<dyn Adapter>,
        request: &OpenAIRequest,
    ) -> anyhow::Result<reqwest::Response> {
        let payload = adapter.request_to_backend(request, backend)?;
        let body_str = serde_json::to_string(&payload)?;
        tracing::info!("xingdu stream sending to {}: {}", backend.url, body_str);
        tracing::info!("xingdu stream headers: protocol={:?}, api_key=***", backend.protocol);
        let hdrs = self.build_headers(backend);
        let resp = self.inner
            .post(&backend.url)
            .headers(hdrs)
            .body(body_str)
            .send()
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await?;
            // 将后端错误映射为 OpenAI 标准错误格式
            let (error_type, error_message) = map_backend_error(status, &body);
            let openai_error = serde_json::json!({
                "error": {
                    "message": error_message,
                    "type": error_type,
                    "code": status.as_u16(),
                }
            });
            anyhow::bail!(openai_error.to_string());
        }
        Ok(resp)
    }

    fn build_headers(&self, backend: &BackendConfig) -> reqwest::header::HeaderMap {
        use reqwest::header;
        let mut hdrs = header::HeaderMap::new();
        hdrs.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
        match backend.protocol {
            crate::adapter::Protocol::OpenAI => {
                hdrs.insert(
                    reqwest::header::AUTHORIZATION,
                    format!("Bearer {}", backend.api_key).parse().unwrap(),
                );
            }
            crate::adapter::Protocol::Anthropic => {
                hdrs.insert("x-api-key", backend.api_key.parse().unwrap());
                hdrs.insert("anthropic-version", "2023-06-01".parse().unwrap());
            }
        }
        hdrs
    }
}

/// 将后端 HTTP 错误映射为 OpenAI 标准错误类型
fn map_backend_error(status: reqwest::StatusCode, body: &str) -> (String, String) {
    // 尝试解析后端错误 JSON
    let backend_msg = if let Ok(val) = serde_json::from_str::<serde_json::Value>(body) {
        val.get("error")
            .and_then(|e| e.get("message"))
            .and_then(|m| m.as_str())
            .unwrap_or(body)
            .to_string()
    } else {
        body.to_string()
    };

    let error_type = match status.as_u16() {
        400 => "invalid_request_error",
        401 => "authentication_error",
        403 => "permission_error",
        404 => "not_found_error",
        429 => "rate_limit_error",
        500 | 502 | 503 | 504 => "server_error",
        _ => "api_error",
    };

    let message = format!("{} (HTTP {})", backend_msg, status.as_u16());
    (error_type.to_string(), message)
}