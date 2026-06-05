use async_trait::async_trait;

use crate::pipeline::{Middleware, RequestContext, ResponseContext};

/// 投机执行中间件
/// 便宜模型先生成草稿，如果被选中的是主模型则跳过
/// 目前设计为标记上下文，实际执行由 server 层处理
pub struct SpeculativeMiddleware {
    pub enabled: bool,
    /// 草稿模型（便宜模型）
    pub draft_model: String,
    /// 验证模型（主模型）
    pub verify_model: String,
}

impl SpeculativeMiddleware {
    pub fn new(enabled: bool, draft_model: String, verify_model: String) -> Self {
        SpeculativeMiddleware { enabled, draft_model, verify_model }
    }

    /// 判断是否应使用投机执行
    pub fn should_speculate(&self, ctx: &RequestContext) -> bool {
        if !self.enabled {
            return false;
        }
        // 只有非流式请求可以用投机执行
        if ctx.request.stream.unwrap_or(false) {
            return false;
        }
        // 选中的模型是验证模型时才投机
        ctx.selected_model == self.verify_model
    }
}

#[async_trait]
impl Middleware for SpeculativeMiddleware {
    async fn process_request(&self, ctx: &mut RequestContext) {
        if !self.enabled {
            return;
        }
        if !self.should_speculate(ctx) {
            return;
        }
        tracing::info!("speculative: draft={}, verify={}", self.draft_model, self.verify_model);
    }

    async fn process_response(&self, _ctx: &mut ResponseContext) {}

    fn name(&self) -> &'static str {
        "speculative"
    }
}