use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

use crate::pipeline::{Middleware, RequestContext, ResponseContext};
use crate::types::OpenAIRequest;

#[derive(Debug, Clone)]
struct SemanticEntry {
    embedding: Vec<f32>,
    response: serde_json::Value,
    created_at: Instant,
    ttl: Duration,
    access_count: u64,
}

/// 语义缓存：用 embedding 向量相似度匹配
pub struct SemanticCacheMiddleware {
    pub enabled: bool,
    pub similarity_threshold: f32,
    pub ttl: u64,
    pub max_entries: usize,
    pub embedding_model: String,
    pub embedding_url: String,
    pub embedding_key: String,
    store: Arc<RwLock<HashMap<String, Vec<SemanticEntry>>>>,
    client: reqwest::Client,
}

impl SemanticCacheMiddleware {
    pub fn new(enabled: bool, ttl: u64) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_default();

        SemanticCacheMiddleware {
            enabled,
            similarity_threshold: 0.92,
            ttl,
            max_entries: 200,
            embedding_model: "ark-30728f3f-eb86-4160-a46e-a15b6f233c3c-7d646".to_string(),
            embedding_url: "https://ark.cn-beijing.volces.com/api/coding/v3/embeddings".to_string(),
            embedding_key: std::env::var("ARK_API_KEY").unwrap_or_default(),
            store: Arc::new(RwLock::new(HashMap::new())),
            client,
        }
    }

    /// 提取查询文本（取最后一条 user 消息）
    fn extract_query(request: &OpenAIRequest) -> String {
        request.messages
            .iter()
            .rev()
            .find(|m| m.role == "user")
            .and_then(|m| m.content.as_str())
            .unwrap_or("")
            .to_string()
    }

    /// 调用 embedding 接口获取向量
    async fn get_embedding(&self, text: &str) -> Option<Vec<f32>> {
        if text.is_empty() || self.embedding_key.is_empty() {
            return None;
        }

        // Doubao/ARK embedding API 格式
        let body = serde_json::json!({
            "model": self.embedding_model,
            "input": [text]
        });

        let resp = self.client
            .post(&self.embedding_url)
            .header("Authorization", format!("Bearer {}", self.embedding_key))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .ok()?;

        let json: serde_json::Value = resp.json().await.ok()?;
        
        // Doubao 返回格式: data[0].embedding
        // 兼容两种格式
        if let Some(embedding) = json.get("data")
            .and_then(|d| d.as_array())
            .and_then(|a| a.first())
            .and_then(|e| e.get("embedding"))
            .and_then(|e| e.as_array())
        {
            return embedding.iter()
                .map(|v| v.as_f64().map(|f| f as f32))
                .collect::<Option<Vec<_>>>();
        }
        
        // 兼容百炼格式: output.embeddings[0].embedding
        if let Some(embedding) = json.get("output")
            .and_then(|o| o.get("embeddings"))
            .and_then(|e| e.as_array())
            .and_then(|a| a.first())
            .and_then(|e| e.get("embedding"))
            .and_then(|e| e.as_array())
        {
            return embedding.iter()
                .map(|v| v.as_f64().map(|f| f as f32))
                .collect::<Option<Vec<_>>>();
        }
        
        tracing::warn!("unexpected embedding response: {:?}", json.get("error"));
        None
    }

    /// cosine similarity
    fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
        if a.len() != b.len() || a.is_empty() {
            return 0.0;
        }
        let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
        let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm_a == 0.0 || norm_b == 0.0 {
            return 0.0;
        }
        dot / (norm_a * norm_b)
    }

    /// 用模型名做分区 key
    fn partition_key(model: &str) -> String {
        model.to_lowercase()
    }

    pub async fn get(&self, request: &OpenAIRequest) -> Option<serde_json::Value> {
        if !self.enabled {
            return None;
        }

        let query = Self::extract_query(request);
        let embedding = self.get_embedding(&query).await?;
        let partition = Self::partition_key(&request.model);

        let store = self.store.read().await;
        let entries = store.get(&partition)?;

        let mut best_match: Option<(f32, serde_json::Value)> = None;
        for entry in entries {
            if entry.created_at.elapsed() > entry.ttl {
                continue;
            }
            let sim = Self::cosine_similarity(&embedding, &entry.embedding);
            if sim >= self.similarity_threshold {
                if best_match.as_ref().map_or(true, |(best_sim, _)| sim > *best_sim) {
                    best_match = Some((sim, entry.response.clone()));
                }
            }
        }

        if let Some((sim, resp)) = best_match {
            tracing::info!("semantic cache HIT sim={:.3} model={}", sim, partition);
            return Some(resp);
        }

        None
    }

    pub async fn set(&self, request: &OpenAIRequest, response: serde_json::Value) {
        if !self.enabled {
            return;
        }

        let query = Self::extract_query(request);
        let Some(embedding) = self.get_embedding(&query).await else {
            return;
        };

        let partition = Self::partition_key(&request.model);
        let entry = SemanticEntry {
            embedding,
            response,
            created_at: Instant::now(),
            ttl: Duration::from_secs(self.ttl),
            access_count: 1,
        };

        let mut store = self.store.write().await;
        let entries = store.entry(partition).or_insert_with(Vec::new);

        // 淘汰最旧的
        if entries.len() >= self.max_entries {
            entries.sort_by_key(|e| e.created_at);
            entries.remove(0);
        }

        // 清理过期
        entries.retain(|e| e.created_at.elapsed() < e.ttl);

        entries.push(entry);
    }

    pub async fn cleanup(&self) {
        if !self.enabled { return; }
        let mut store = self.store.write().await;
        for entries in store.values_mut() {
            entries.retain(|e| e.created_at.elapsed() < e.ttl);
        }
        store.retain(|_, entries| !entries.is_empty());
    }

    /// 统计信息
    pub async fn stats(&self) -> serde_json::Value {
        let store = self.store.read().await;
        let total_entries: usize = store.values().map(|v| v.len()).sum();
        let partitions: Vec<String> = store.keys().cloned().collect();

        serde_json::json!({
            "enabled": self.enabled,
            "threshold": self.similarity_threshold,
            "ttl_seconds": self.ttl,
            "max_entries": self.max_entries,
            "total_entries": total_entries,
            "partitions": partitions,
        })
    }
}

#[async_trait]
impl Middleware for SemanticCacheMiddleware {
    async fn process_request(&self, ctx: &mut RequestContext) {
        // 语义缓存由 server handler 直接调用，不在 pipeline 中处理
    }

    async fn process_response(&self, _ctx: &mut ResponseContext) {}

    fn name(&self) -> &'static str { "semantic_cache" }
}
