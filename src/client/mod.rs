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
            anyhow::bail!("backend returned {}: {}", status, body_text);
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
            anyhow::bail!("backend returned {}: {}", status, body);
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