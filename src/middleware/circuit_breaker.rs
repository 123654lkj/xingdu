use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

use crate::pipeline::{Middleware, RequestContext, ResponseContext};

/// 熔断器状态
#[derive(Debug, Clone, PartialEq)]
enum CircuitState {
    /// 正常
    Closed,
    /// 断开（快速失败）
    Open { opened_at: Instant },
    /// 半开（探活中）
    HalfOpen,
}

/// 单个模型的后端熔断器
#[derive(Debug, Clone)]
struct CircuitBreakerInner {
    state: CircuitState,
    failure_count: u32,
    last_failure: Option<Instant>,
}

/// 熔断器中间件
/// 后端连续失败时自动断开，定期探活恢复
pub struct CircuitBreakerMiddleware {
    /// 每后端的熔断器
    breakers: Arc<RwLock<HashMap<String, CircuitBreakerInner>>>,
    /// 触发断开的连续失败次数（0=禁用）
    threshold: u32,
    /// 恢复间隔（秒），到达后进入 Half-Open
    recovery_interval: Duration,
    /// 是否启用
    pub enabled: bool,
}

impl CircuitBreakerMiddleware {
    pub fn new(enabled: bool, threshold: u32, recovery_seconds: u64) -> Self {
        CircuitBreakerMiddleware {
            breakers: Arc::new(RwLock::new(HashMap::new())),
            threshold: if enabled { threshold } else { 0 },
            recovery_interval: Duration::from_secs(recovery_seconds),
            enabled,
        }
    }

    /// 检查特定后端是否允许请求通过
    pub async fn allow_request(&self, backend_name: &str) -> bool {
        if !self.enabled || self.threshold == 0 {
            return true;
        }

        let breakers = self.breakers.read().await;
        if let Some(cb) = breakers.get(backend_name) {
            match cb.state {
                CircuitState::Closed => true,
                CircuitState::HalfOpen => true, // 半开状态放行一个探活请求
                CircuitState::Open { opened_at } => {
                    if opened_at.elapsed() >= self.recovery_interval {
                        true // 时间到了，允许探活
                    } else {
                        false // 还在断开期
                    }
                }
            }
        } else {
            true // 新后端，默认允许
        }
    }

    /// 记录成功
    pub async fn record_success(&self, backend_name: &str) {
        if !self.enabled || self.threshold == 0 {
            return;
        }
        let mut breakers = self.breakers.write().await;
        if let Some(cb) = breakers.get_mut(backend_name) {
            cb.failure_count = 0;
            cb.state = CircuitState::Closed;
        }
    }

    /// 记录失败
    pub async fn record_failure(&self, backend_name: &str) {
        if !self.enabled || self.threshold == 0 {
            return;
        }
        let mut breakers = self.breakers.write().await;
        let cb = breakers.entry(backend_name.to_string()).or_insert(CircuitBreakerInner {
            state: CircuitState::Closed,
            failure_count: 0,
            last_failure: None,
        });

        cb.failure_count += 1;
        cb.last_failure = Some(Instant::now());

        if cb.failure_count >= self.threshold {
            tracing::warn!("circuit breaker OPEN for backend={}", backend_name);
            cb.state = CircuitState::Open { opened_at: Instant::now() };
        }
    }

    /// 获取当前状态统计
    pub async fn get_stats(&self) -> HashMap<String, String> {
        let breakers = self.breakers.read().await;
        breakers.iter().map(|(k, v)| {
            let state = match &v.state {
                CircuitState::Closed => "closed".to_string(),
                CircuitState::HalfOpen => "half_open".to_string(),
                CircuitState::Open { .. } => "open".to_string(),
            };
            (k.clone(), state)
        }).collect()
    }
}

#[async_trait]
impl Middleware for CircuitBreakerMiddleware {
    async fn process_request(&self, ctx: &mut RequestContext) {
        if !self.enabled || self.threshold == 0 {
            return;
        }

        // 在请求阶段检查熔断器状态
        if !self.allow_request(&ctx.selected_model).await {
            let err = format!("circuit breaker OPEN for model '{}', request rejected", ctx.selected_model);
            tracing::warn!("{}", err);
            ctx.errors.push(err);
            ctx.skip_cache = true;
        }
    }

    async fn process_response(&self, _ctx: &mut ResponseContext) {}

    fn name(&self) -> &'static str {
        "circuit_breaker"
    }
}