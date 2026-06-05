use async_trait::async_trait;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

use crate::pipeline::{Middleware, RequestContext, ResponseContext};

/// 令牌桶限流器
struct TokenBucket {
    /// 每秒令牌数
    rate: f64,
    /// 桶容量
    capacity: f64,
    /// 当前令牌
    tokens: f64,
    /// 上次填充时间
    last_refill: Instant,
}

impl TokenBucket {
    fn new(rate: f64, capacity: f64) -> Self {
        TokenBucket {
            rate,
            capacity,
            tokens: capacity,
            last_refill: Instant::now(),
        }
    }

    fn try_consume(&mut self, tokens: f64) -> bool {
        self.refill();
        if self.tokens >= tokens {
            self.tokens -= tokens;
            true
        } else {
            false
        }
    }

    fn refill(&mut self) {
        let elapsed = self.last_refill.elapsed().as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.rate).min(self.capacity);
        self.last_refill = Instant::now();
    }
}

/// 流控中间件（令牌桶）
pub struct FlowControlMiddleware {
    pub enabled: bool,
    /// 每秒请求数限制
    pub requests_per_second: f64,
    /// 令牌桶
    bucket: Arc<Mutex<TokenBucket>>,
}

impl FlowControlMiddleware {
    pub fn new(enabled: bool, rps: f64) -> Self {
        FlowControlMiddleware {
            enabled,
            requests_per_second: rps,
            bucket: Arc::new(Mutex::new(TokenBucket::new(rps, rps * 2.0))),
        }
    }

    /// 尝试获取许可
    pub async fn try_acquire(&self) -> bool {
        if !self.enabled {
            return true;
        }
        let mut bucket = self.bucket.lock().await;
        bucket.try_consume(1.0)
    }
}

#[async_trait]
impl Middleware for FlowControlMiddleware {
    async fn process_request(&self, ctx: &mut RequestContext) {
        if !self.enabled {
            return;
        }
        let allowed = self.bucket.lock().await.try_consume(1.0);
        if !allowed {
            ctx.errors.push("rate limit exceeded".to_string());
            tracing::warn!("flow control: rate limit exceeded");
        }
    }

    async fn process_response(&self, _ctx: &mut ResponseContext) {}

    fn name(&self) -> &'static str {
        "flow_control"
    }
}