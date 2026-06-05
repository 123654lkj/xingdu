use async_trait::async_trait;
use std::time::Duration;

use crate::pipeline::{Middleware, RequestContext, ResponseContext};

/// 星枢 RAG 集成中间件
/// 在转发前调用星枢向量搜索，将相关知识注入 system prompt
pub struct StarhubMiddleware {
    /// 是否启用
    pub enabled: bool,
    /// 星枢 URL
    pub url: String,
    /// 返回结果数
    pub limit: usize,
    /// 超时（秒）
    pub timeout: u64,
    /// 最小查询长度
    pub min_query_len: usize,
    /// HTTP 客户端
    client: reqwest::Client,
}

impl StarhubMiddleware {
    pub fn new(enabled: bool, url: String, limit: usize, timeout: u64) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(timeout.max(1)))
            .build()
            .unwrap_or_default();

        StarhubMiddleware {
            enabled,
            url,
            limit,
            timeout,
            min_query_len: 10,
            client,
        }
    }

    /// 从消息中提取搜索查询（取最后一条用户消息）
    fn extract_query(ctx: &RequestContext) -> Option<String> {
        for msg in ctx.request.messages.iter().rev() {
            if msg.role == "user" {
                if let Some(text) = msg.content.as_str() {
                    let trimmed = text.trim();
                    if trimmed.len() >= 10 {
                        return Some(trimmed[..trimmed.len().min(200)].to_string());
                    }
                }
            }
        }
        None
    }

    /// 调用星枢搜索 API
    async fn search(&self, query: &str) -> Vec<String> {
        if !self.enabled || query.len() < self.min_query_len {
            return Vec::new();
        }

        let search_url = format!("{}/search?q={}&limit={}", 
            self.url.trim_end_matches('/'), 
            urlencoding(query),
            self.limit,
        );

        match self.client.get(&search_url).send().await {
            Ok(resp) => {
                if let Ok(body) = resp.json::<serde_json::Value>().await {
                    // 星枢返回 {"results": [...]}
                    if let Some(results) = body.get("results").and_then(|r| r.as_array()) {
                        return results.iter()
                            .filter_map(|r| r.get("content").and_then(|c| c.as_str()))
                            .map(|s| s.to_string())
                            .collect();
                    }
                }
                Vec::new()
            }
            Err(_) => {
                tracing::warn!("starhub search failed (silent degrade)");
                Vec::new()
            }
        }
    }

    /// 将 RAG 结果注入 system prompt
    fn inject_rag(ctx: &mut RequestContext, results: &[String]) {
        if results.is_empty() {
            return;
        }

        let rag_text = format!(
            "\n\n[相关背景知识]\n{}",
            results.join("\n---\n")
        );

        // 尝试找到 system 消息并追加
        for msg in &mut ctx.modified_messages {
            if msg.get("role").and_then(|r| r.as_str()) == Some("system") {
                if let Some(content) = msg.get_mut("content") {
                    if let Some(s) = content.as_str() {
                        *content = serde_json::Value::String(format!("{}{}", s, rag_text));
                    }
                }
                ctx.rag_context = Some(rag_text.clone());
                return;
            }
        }

        // 没有 system 消息，创建一个
        ctx.rag_context = Some(rag_text.clone());
        ctx.modified_messages.insert(0, serde_json::json!({
            "role": "system",
            "content": rag_text
        }));
    }
}

/// 简单 URL 编码（避免添加依赖）
fn urlencoding(s: &str) -> String {
    s.bytes().map(|b| match b {
        b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => (b as char).to_string(),
        b' ' => "+".to_string(),
        _ => format!("%{:02X}", b),
    }).collect()
}

#[async_trait]
impl Middleware for StarhubMiddleware {
    async fn process_request(&self, ctx: &mut RequestContext) {
        if !self.enabled {
            return;
        }

        if ctx.rag_context.is_some() {
            return; // 已注入
        }

        let query = match Self::extract_query(ctx) {
            Some(q) => q,
            None => return,
        };

        tracing::debug!("starhub RAG search: query={}", &query[..query.len().min(50)]);
        let results = self.search(&query).await;

        if !results.is_empty() {
            tracing::info!("starhub RAG: found {} results, injecting", results.len());
            Self::inject_rag(ctx, &results);
        }
    }

    async fn process_response(&self, _ctx: &mut ResponseContext) {}

    fn name(&self) -> &'static str {
        "starhub_rag"
    }
}