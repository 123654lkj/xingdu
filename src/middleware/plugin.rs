use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;

use crate::pipeline::{Middleware, RequestContext, ResponseContext};

/// 插件定义
pub trait Plugin: Send + Sync {
    fn name(&self) -> &'static str;
    fn on_request(&self, ctx: &mut RequestContext);
    fn on_response(&self, _ctx: &mut ResponseContext) {}
}

/// 插件系统中间件
pub struct PluginMiddleware {
    pub enabled: bool,
    plugins: Vec<Box<dyn Plugin>>,
}

impl PluginMiddleware {
    pub fn new(enabled: bool) -> Self {
        PluginMiddleware { enabled, plugins: Vec::new() }
    }

    pub fn register(&mut self, plugin: Box<dyn Plugin>) {
        self.plugins.push(plugin);
    }
}

#[async_trait]
impl Middleware for PluginMiddleware {
    async fn process_request(&self, ctx: &mut RequestContext) {
        if !self.enabled { return; }
        for plugin in &self.plugins {
            plugin.on_request(ctx);
        }
    }

    async fn process_response(&self, ctx: &mut ResponseContext) {
        if !self.enabled { return; }
        for plugin in &self.plugins {
            plugin.on_response(ctx);
        }
    }

    fn name(&self) -> &'static str {
        "plugin_system"
    }
}

// --- Webhook ---

/// Webhook 中间件
pub struct WebhookMiddleware {
    pub enabled: bool,
    pub webhook_url: String,
    client: reqwest::Client,
}

impl WebhookMiddleware {
    pub fn new(enabled: bool, webhook_url: String) -> Self {
        let client = reqwest::Client::new();
        WebhookMiddleware { enabled, webhook_url, client }
    }
}

#[async_trait]
impl Middleware for WebhookMiddleware {
    async fn process_request(&self, _ctx: &mut RequestContext) {}

    async fn process_response(&self, ctx: &mut ResponseContext) {
        if !self.enabled || self.webhook_url.is_empty() {
            return;
        }
        // 异步发送 webhook（不阻塞响应）
        let url = self.webhook_url.clone();
        let client = self.client.clone();
        let resp = ctx.response.clone();
        tokio::spawn(async move {
            let payload = serde_json::json!({
                "event": "response",
                "data": resp,
            });
            if let Err(e) = client.post(&url).json(&payload).send().await {
                tracing::warn!("webhook failed: {}", e);
            }
        });
    }

    fn name(&self) -> &'static str {
        "webhook"
    }
}

// --- 多租户 ---

/// 租户配置
#[derive(Debug, Clone)]
pub struct TenantConfig {
    pub id: String,
    pub api_key: String,
    pub rate_limit: u32,
    pub models: Vec<String>,
}

/// 多租户中间件
pub struct TenantMiddleware {
    pub enabled: bool,
    tenants: Arc<HashMap<String, TenantConfig>>,
}

impl TenantMiddleware {
    pub fn new(enabled: bool) -> Self {
        TenantMiddleware { enabled, tenants: Arc::new(HashMap::new()) }
    }

    pub fn add_tenant(&mut self, config: TenantConfig) {
        Arc::get_mut(&mut self.tenants).unwrap().insert(config.id.clone(), config);
    }
}

#[async_trait]
impl Middleware for TenantMiddleware {
    async fn process_request(&self, ctx: &mut RequestContext) {
        if !self.enabled { return; }
        // 从请求中识别租户（实际由 API Key 匹配）
        let _ = ctx;
    }

    async fn process_response(&self, _ctx: &mut ResponseContext) {}

    fn name(&self) -> &'static str {
        "tenant"
    }
}