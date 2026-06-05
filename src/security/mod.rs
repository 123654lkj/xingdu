use async_trait::async_trait;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use dashmap::DashMap;

use crate::pipeline::{Middleware, RequestContext, ResponseContext};

/// IP 限流器
#[derive(Clone)]
struct RateLimiter {
    /// 窗口内最大请求数
    max_requests: u32,
    /// 窗口大小（秒）
    window_secs: u64,
    /// IP -> (窗口开始时间, 计数)
    buckets: Arc<DashMap<String, (Instant, u32)>>,
}

impl RateLimiter {
    fn new(max_requests: u32) -> Self {
        RateLimiter {
            max_requests,
            window_secs: 1,
            buckets: Arc::new(DashMap::new()),
        }
    }

    fn allow(&self, client_ip: &str) -> bool {
        if self.max_requests == 0 {
            return true;
        }

        let now = Instant::now();
        let mut entry = self.buckets.entry(client_ip.to_string()).or_insert_with(|| (now, 0));
        let (window_start, count) = entry.value_mut();

        // 窗口过期，重置
        if now.duration_since(*window_start) > Duration::from_secs(self.window_secs) {
            *window_start = now;
            *count = 1;
            return true;
        }

        // 窗口内计数
        if *count < self.max_requests {
            *count += 1;
            return true;
        }

        false
    }
}

/// 安全层中间件
/// API Key 鉴权、IP 白名单、请求限流
pub struct AuthMiddleware {
    /// 是否启用
    pub enabled: bool,
    /// 有效的 API Keys
    api_keys: HashSet<String>,
    /// IP 白名单（空列表表示不限制）
    ip_whitelist: HashSet<String>,
    /// 限流器
    rate_limiter: RateLimiter,
    /// 宽松模式：鉴权失败时放行（降级）
    pub relaxed: bool,
}

impl AuthMiddleware {
    pub fn new(enabled: bool, api_keys: Vec<String>, ip_whitelist: Vec<String>, rate_limit: u32) -> Self {
        AuthMiddleware {
            enabled,
            api_keys: api_keys.into_iter().collect(),
            ip_whitelist: ip_whitelist.into_iter().collect(),
            rate_limiter: RateLimiter::new(rate_limit),
            relaxed: false,
        }
    }

    pub fn with_relaxed(mut self) -> Self {
        self.relaxed = true;
        self
    }
}

#[async_trait]
impl Middleware for AuthMiddleware {
    async fn process_request(&self, ctx: &mut RequestContext) {
        if !self.enabled {
            return;
        }

        // 1. API Key 验证（取 Authorization: Bearer <key>）
        // 实际验证在 server 层通过 axum middleware 实现
        // 这里只记录上下文

        // 2. IP 白名单（由 axum 层处理）
        // 3. 限流（由 axum 层处理）

        let _ = ctx;
    }

    async fn process_response(&self, _ctx: &mut ResponseContext) {}

    fn name(&self) -> &'static str {
        "auth"
    }
}

/// 从请求中提取 Bearer token
pub fn extract_bearer_token(headers: &axum::http::HeaderMap) -> Option<String> {
    let auth = headers.get(axum::http::header::AUTHORIZATION)?;
    let auth_str = auth.to_str().ok()?;
    if auth_str.starts_with("Bearer ") {
        Some(auth_str[7..].to_string())
    } else {
        None
    }
}

/// 验证 API Key
pub fn verify_api_key(api_keys: &HashSet<String>, token: &str) -> bool {
    api_keys.contains(token)
}