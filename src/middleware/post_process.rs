use async_trait::async_trait;

use crate::pipeline::{Middleware, RequestContext, ResponseContext};
use crate::types::OpenAIResponse;

/// 响应后处理中间件
/// 对 LLM 响应进行后处理（去前后空白、格式化等）
pub struct PostProcessMiddleware {
    pub enabled: bool,
    /// 是否去除首尾空白
    pub trim: bool,
    /// 是否合并空行
    pub collapse_blank: bool,
}

impl PostProcessMiddleware {
    pub fn new(enabled: bool) -> Self {
        PostProcessMiddleware { enabled, trim: true, collapse_blank: true }
    }

    fn process_response_inner(resp: &mut OpenAIResponse) {
        for choice in &mut resp.choices {
            if let Some(ref mut content) = choice.message.content {
                // 去除首尾空白
                let trimmed = content.trim().to_string();
                *content = trimmed;
            }
        }
    }
}

#[async_trait]
impl Middleware for PostProcessMiddleware {
    async fn process_request(&self, _ctx: &mut RequestContext) {}

    async fn process_response(&self, ctx: &mut ResponseContext) {
        if !self.enabled {
            return;
        }
        Self::process_response_inner(&mut ctx.response);
    }

    fn name(&self) -> &'static str {
        "post_process"
    }
}