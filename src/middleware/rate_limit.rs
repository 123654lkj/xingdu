use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Semaphore;

use crate::pipeline::{Middleware, RequestContext, ResponseContext};

/// 请求排队中间件（背压控制）
/// 限制并发请求数，超出时排队
pub struct RateLimitMiddleware {
    /// 最大并发数（0=不限）
    max_concurrent: u32,
    /// 信号量
    semaphore: Option<Arc<Semaphore>>,
}

impl RateLimitMiddleware {
    pub fn new(max_concurrent: u32) -> Self {
        let semaphore = if max_concurrent > 0 {
            Some(Arc::new(Semaphore::new(max_concurrent as usize)))
        } else {
            None
        };

        RateLimitMiddleware { max_concurrent, semaphore }
    }

    /// 尝试获取许可（返回 None 表示不需要等待）
    pub async fn try_acquire(&self) -> Option<tokio::sync::SemaphorePermit<'_>> {
        match &self.semaphore {
            Some(sem) => Some(sem.acquire().await.unwrap()),
            None => None,
        }
    }
}

#[async_trait]
impl Middleware for RateLimitMiddleware {
    async fn process_request(&self, _ctx: &mut RequestContext) {
        // 排队逻辑在 server 层由调用方显式处理
        // 这里只是标记中间件存在
    }

    async fn process_response(&self, _ctx: &mut ResponseContext) {}

    fn name(&self) -> &'static str {
        "rate_limit"
    }
}