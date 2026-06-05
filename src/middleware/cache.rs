use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use sha2::{Sha256, Digest};

use crate::pipeline::{Middleware, RequestContext, ResponseContext};
use crate::types::{OpenAIRequest, Message};

#[derive(Debug, Clone)]
struct CacheEntry {
    response: serde_json::Value,
    created_at: Instant,
    ttl: Duration,
    access_count: u64,
}

#[derive(Clone)]
pub struct ResponseCacheMiddleware {
    pub mode: u8,
    pub ttl: u64,
    pub strip_formatting: bool,
    pub prefix_cache: bool,
    pub starhub_persist: bool,
    pub starhub_url: String,
    pub min_prefix_len: usize,
    pub max_entries: usize,
    store: Arc<RwLock<HashMap<String, CacheEntry>>>,
    prefix_store: Arc<RwLock<HashMap<String, Vec<(String, serde_json::Value)>>>>,
    client: reqwest::Client,
}

impl ResponseCacheMiddleware {
    pub fn new(mode: u8, ttl: u64) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap_or_default();
        ResponseCacheMiddleware {
            mode,
            ttl,
            strip_formatting: true,
            prefix_cache: true,
            starhub_persist: false,
            starhub_url: String::new(),
            min_prefix_len: 50,
            max_entries: 500,
            store: Arc::new(RwLock::new(HashMap::new())),
            prefix_store: Arc::new(RwLock::new(HashMap::new())),
            client,
        }
    }

    pub fn with_starhub(mut self, enabled: bool, url: String) -> Self {
        self.starhub_persist = enabled;
        self.starhub_url = url;
        self
    }

    fn normalize_text(text: &str) -> String {
        let s: String = text.chars()
            .map(|c| match c {
                '\u{2018}' | '\u{2019}' | '\u{201c}' | '\u{201d}' => '\'',
                '\u{3000}' => ' ',
                '\u{ff0c}' | '\u{3001}' => ',',
                '\u{3002}' => '.',
                '\u{ff01}' => '!',
                '\u{ff1f}' => '?',
                '\u{2014}' | '\u{2015}' | '\u{2013}' => '-',
                _ => c,
            })
            .collect();
        let mut result = String::with_capacity(s.len());
        let mut prev_space = false;
        for c in s.chars() {
            if c == ' ' {
                if !prev_space { result.push(c); }
                prev_space = true;
            } else if c == '\t' || c == '\r' {
                continue;
            } else {
                result.push(c);
                prev_space = false;
            }
        }
        result.trim().to_string()
    }

    fn normalize_messages(messages: &[Message]) -> Vec<serde_json::Value> {
        messages.iter().map(|msg| {
            let mut m = serde_json::to_value(msg).unwrap_or_default();
            if let Some(content) = m.get("content") {
                if let Some(text) = content.as_str() {
                    m["content"] = serde_json::Value::String(Self::normalize_text(text));
                } else if let Some(arr) = content.as_array() {
                    let normalized: Vec<serde_json::Value> = arr.iter().map(|part| {
                        let mut p = part.clone();
                        if let Some(text) = p.get("text").and_then(|t| t.as_str()) {
                            p["text"] = serde_json::Value::String(Self::normalize_text(text));
                        }
                        p
                    }).collect();
                    m["content"] = serde_json::Value::Array(normalized);
                }
            }
            m
        }).collect()
    }

    pub fn compute_cache_key(request: &OpenAIRequest) -> String {
        let normalized_msgs = Self::normalize_messages(&request.messages);
        let mut hasher = Sha256::new();
        hasher.update(request.model.as_bytes());
        hasher.update(b"\0");
        if let Ok(msg_json) = serde_json::to_string(&normalized_msgs) {
            hasher.update(msg_json.as_bytes());
        }
        hasher.update(b"\0");
        if let Some(temp) = &request.temperature {
            hasher.update(temp.to_string().as_bytes());
        }
        hasher.update(b"\0");
        if let Some(tools) = &request.tools {
            if let Ok(tools_json) = serde_json::to_string(tools) {
                hasher.update(tools_json.as_bytes());
            }
        }
        format!("{:x}", hasher.finalize())
    }

    fn compute_prefix_key(request: &OpenAIRequest) -> Option<(String, String)> {
        if request.messages.len() < 2 {
            return None;
        }
        let prefix_msgs = &request.messages[..request.messages.len() - 1];
        let last_msg = &request.messages[request.messages.len() - 1];

        let norm_prefix = Self::normalize_messages(prefix_msgs);
        let norm_suffix = Self::normalize_messages(&[last_msg.clone()]);

        let mut hasher = Sha256::new();
        hasher.update(request.model.as_bytes());
        hasher.update(b"\0");
        if let Ok(json) = serde_json::to_string(&norm_prefix) {
            hasher.update(json.as_bytes());
        }
        let prefix_key = format!("{:x}", hasher.finalize());

        let mut hasher2 = Sha256::new();
        if let Ok(json) = serde_json::to_string(&norm_suffix) {
            hasher2.update(json.as_bytes());
        }
        let suffix_key = format!("{:x}", hasher2.finalize());

        Some((prefix_key, suffix_key))
    }

    pub async fn set(&self, key: &str, response: serde_json::Value) {
        if self.mode == 0 { return; }
        let entry = CacheEntry {
            response: response.clone(),
            created_at: Instant::now(),
            ttl: Duration::from_secs(self.ttl),
            access_count: 1,
        };
        let mut store = self.store.write().await;
        let needs_evict = store.len() >= self.max_entries && !store.contains_key(key);
        if needs_evict {
            let oldest_key = store.iter().min_by_key(|(_, e)| e.created_at).map(|(k, _)| k.clone());
            if let Some(k) = oldest_key {
                store.remove(&k);
            }
        }
        store.insert(key.to_string(), entry);
    }

    pub async fn set_with_prefix(&self, key: &str, request: &OpenAIRequest, response: serde_json::Value) {
        if self.mode == 0 { return; }
        let entry = CacheEntry {
            response: response.clone(),
            created_at: Instant::now(),
            ttl: Duration::from_secs(self.ttl),
            access_count: 1,
        };
        let mut store = self.store.write().await;
        let needs_evict = store.len() >= self.max_entries && !store.contains_key(key);
        if needs_evict {
            let oldest_key = store.iter().min_by_key(|(_, e)| e.created_at).map(|(k, _)| k.clone());
            if let Some(k) = oldest_key {
                store.remove(&k);
            }
        }
        store.insert(key.to_string(), entry);

        if self.prefix_cache {
            if let Some((prefix_key, suffix_key)) = Self::compute_prefix_key(request) {
                let mut pstore = self.prefix_store.write().await;
                let entries = pstore.entry(prefix_key).or_insert_with(Vec::new);
                if entries.len() >= 5 {
                    entries.remove(0);
                }
                entries.push((suffix_key, response.clone()));
            }
        }

        // 寮傛鎸佷箙鍖栧埌鏄熸灑
        if self.starhub_persist {
            let starhub_url = self.starhub_url.clone();
            let resp_clone = response.clone();
            let model_owned = request.model.clone();
            let key_owned = key.to_string();
            let user_query_owned = request.messages.last()
                .and_then(|m| m.content.as_str())
                .unwrap_or("")
                .to_string();
            tokio::spawn(async move {
                let search_url = format!("{}/search", starhub_url.trim_end_matches('/'));
                let add_url = format!("{}/memory/add", starhub_url.trim_end_matches('/'));

                // 先检查 key 的前 16 位是否已存在，避免重复存储
                let search_body = serde_json::json!({
                    "query": &key_owned[..16],
                    "limit": 1,
                });
                let already_exists = match reqwest::Client::new()
                    .post(&search_url)
                    .json(&search_body)
                    .send()
                    .await
                {
                    Ok(resp) => {
                        if let Ok(body) = resp.json::<serde_json::Value>().await {
                            if let Some(results) = body.get("results").and_then(|r| r.as_array()) {
                                results.iter().any(|r| {
                                    r.get("content").and_then(|c| c.as_str())
                                        .map(|s| s.contains(&key_owned[..16]))
                                        .unwrap_or(false)
                                })
                            } else { false }
                        } else { false }
                    }
                    Err(_) => false,
                };

                if !already_exists {
                    let cache_content = if user_query_owned.len() > 500 {
                        format!("[CACHE] {}...", &user_query_owned[..497])
                    } else {
                        format!("[CACHE] {}", user_query_owned)
                    };
                    let add_body = serde_json::json!({
                        "content": cache_content,
                        "category": "cache",
                        "importance": 0.3,
                        "metadata": serde_json::json!({
                            "cache_model": model_owned,
                            "cache_key": &key_owned[..16],
                            "cache_response": serde_json::to_string(&resp_clone).unwrap_or_default()
                        }),
                    });
                    let _ = reqwest::Client::new()
                        .post(&add_url)
                        .json(&add_body)
                        .send()
                        .await;
                }
            });
        }
    }

    pub async fn get(&self, key: &str) -> Option<serde_json::Value> {
        if self.mode == 0 { return None; }
        let store = self.store.read().await;
        if let Some(entry) = store.get(key) {
            if entry.created_at.elapsed() < entry.ttl {
                return Some(entry.response.clone());
            }
        }
        None
    }

    pub async fn get_with_prefix(&self, key: &str, request: &OpenAIRequest) -> Option<serde_json::Value> {
        if self.mode == 0 { return None; }

        // 1. 绮剧‘鍖归厤
        {
            let store = self.store.read().await;
            if let Some(entry) = store.get(key) {
                if entry.created_at.elapsed() < entry.ttl {
                    tracing::info!("exact cache HIT");
                    return Some(entry.response.clone());
                }
            }
        }

        // 2. 鍓嶇紑鍖归厤
        if self.prefix_cache {
            if let Some((prefix_key, suffix_key)) = Self::compute_prefix_key(request) {
                let pstore = self.prefix_store.read().await;
                if let Some(entries) = pstore.get(&prefix_key) {
                    for (stored_suffix, response) in entries {
                        if *stored_suffix == suffix_key {
                            tracing::info!("prefix cache HIT prefix={}", &prefix_key[..16]);
                            return Some(response.clone());
                        }
                    }
                }
            }
        }

        // 3. 鏄熸灑璇箟缂撳瓨锛堝甫瓒呮椂鍏滃簳锛?       
        if self.starhub_persist && !self.starhub_url.is_empty() {
            let search_url = format!("{}/search", self.starhub_url.trim_end_matches('/'));
            let query = request.messages.last()
                .and_then(|m| m.content.as_str())
                .unwrap_or("")
                .to_string();

            let search_body = serde_json::json!({
                "query": query,
                "category": "cache",
                "limit": 3,
                "similarity_threshold": 0.5,
            });

            // 鐢?tokio::time::timeout 閬垮厤闃诲澶箙
            if let Ok(Ok(resp)) = tokio::time::timeout(
                Duration::from_secs(5),
                self.client.post(&search_url).json(&search_body).send(),
            ).await {
                if let Ok(body) = resp.json::<serde_json::Value>().await {
                    if let Some(results) = body.get("results").and_then(|r| r.as_array()) {
                        for result in results {
                            // 从metadata中提取缓存响应
                            if let Some(meta) = result.get("metadata").and_then(|m| m.as_str()) {
                                if let Ok(meta_val) = serde_json::from_str::<serde_json::Value>(meta) {
                                    if let Some(cached_resp_str) = meta_val.get("cache_response").and_then(|c| c.as_str()) {
                                        if let Ok(cached_resp) = serde_json::from_str::<serde_json::Value>(cached_resp_str) {
                                            tracing::info!("starhub semantic cache HIT");
                                            // 写入进程缓存以便下次 0 延迟
                                            let entry = CacheEntry {
                                                response: cached_resp.clone(),
                                                created_at: Instant::now(),
                                                ttl: Duration::from_secs(self.ttl),
                                                access_count: 1,
                                            };
                                            let mut store = self.store.write().await;
                                            store.insert(key.to_string(), entry);
                                            return Some(cached_resp);
                                        }
                                    }
                                }
                            }
                            // 兼容旧格式：从content解析 [CACHE] model= key= response={...}
                            if let Some(content_str) = result.get("content").and_then(|c| c.as_str()) {
                                if content_str.starts_with("[CACHE]") {
                                    if let Some(resp_part) = content_str.split("response=").nth(1) {
                                        if let Ok(cached_resp) = serde_json::from_str::<serde_json::Value>(resp_part) {
                                            tracing::info!("starhub semantic cache HIT (legacy)");
                                            let entry = CacheEntry {
                                                response: cached_resp.clone(),
                                                created_at: Instant::now(),
                                                ttl: Duration::from_secs(self.ttl),
                                                access_count: 1,
                                            };
                                            let mut store = self.store.write().await;
                                            store.insert(key.to_string(), entry);
                                            return Some(cached_resp);
                                        }
                                    }
                                }
                            }
                        }
                        return None;
                    }
                }
            }
        }

        None
    }

    pub async fn cleanup(&self) {
        if self.mode == 0 { return; }
        let mut store = self.store.write().await;
        store.retain(|_, entry| entry.created_at.elapsed() < entry.ttl);
        let mut pstore = self.prefix_store.write().await;
        pstore.retain(|_, entries| !entries.is_empty());
    }
}

#[async_trait]
impl Middleware for ResponseCacheMiddleware {
    async fn process_request(&self, ctx: &mut RequestContext) {
        if self.mode == 0 || ctx.skip_cache { return; }
        let key = Self::compute_cache_key(&ctx.request);
        if let Some(cached) = self.get_with_prefix(&key, &ctx.request).await {
            ctx.skip_cache = true;
            ctx.cached_response = Some(cached);
            tracing::info!("cache HIT for key={}", &key[..16]);
        }
    }

    async fn process_response(&self, _ctx: &mut ResponseContext) {}

    fn name(&self) -> &'static str { "response_cache" }
}