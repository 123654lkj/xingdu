use async_trait::async_trait;
use std::collections::HashSet;
use std::time::Duration;

use crate::pipeline::{Middleware, RequestContext, ResponseContext};

/// Fallback 中间件
/// 主模型失败时自动按 fallback 链依次尝试
pub struct FallbackMiddleware {
    /// 是否启用
    pub enabled: bool,
    /// Fallback 模型链
    pub fallback_chain: Vec<String>,
    /// 超时时间（秒）
    pub timeout: u64,
    /// 触发 fallback 的状态码
    pub retry_codes: HashSet<u16>,
}

impl FallbackMiddleware {
    pub fn new(enabled: bool, fallback_chain: Vec<String>, timeout: u64) -> Self {
        let retry_codes: HashSet<u16> = [429, 500, 502, 503, 504].iter().cloned().collect();
        FallbackMiddleware { enabled, fallback_chain, timeout, retry_codes }
    }

    pub fn with_retry_codes(mut self, codes: Vec<u16>) -> Self {
        self.retry_codes = codes.into_iter().collect();
        self
    }

    /// 判断是否应该触发 fallback
    fn should_fallback(status: u16) -> bool {
        // 默认：429, 5xx
        status == 429 || (500..=599).contains(&status)
    }
}

#[async_trait]
impl Middleware for FallbackMiddleware {
    async fn process_request(&self, ctx: &mut RequestContext) {
        if !self.enabled || self.fallback_chain.is_empty() {
            return;
        }

        // 记录原始模型，后续由客户端实现 fallback 逻辑
        let chain = self.fallback_chain.clone();
        tracing::info!("fallback enabled: chain={:?}, timeout={}s", chain, self.timeout);
        let _ = ctx;
    }

    async fn process_response(&self, _ctx: &mut ResponseContext) {}

    fn name(&self) -> &'static str {
        "fallback"
    }
}