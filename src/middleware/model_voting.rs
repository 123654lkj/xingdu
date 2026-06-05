use async_trait::async_trait;

use crate::pipeline::{Middleware, RequestContext, ResponseContext};

/// 投票策略
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum VotingStrategy {
    /// 取最长回复
    Longest,
    /// 取最短回复
    Shortest,
    /// 取第一个完成
    First,
}

impl VotingStrategy {
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "longest" => VotingStrategy::Longest,
            "shortest" => VotingStrategy::Shortest,
            "first" => VotingStrategy::First,
            _ => VotingStrategy::Longest,
        }
    }
}

/// 多模型投票中间件
/// 同一请求发给多个模型，按策略取最优
pub struct ModelVotingMiddleware {
    pub enabled: bool,
    /// 投票模型列表（逗号分隔）
    pub models: Vec<String>,
    /// 选择策略
    pub strategy: VotingStrategy,
}

impl ModelVotingMiddleware {
    pub fn new(enabled: bool, models: Vec<String>, strategy: VotingStrategy) -> Self {
        ModelVotingMiddleware { enabled, models, strategy }
    }

    /// 判断是否启用投票
    pub fn should_vote(&self, ctx: &RequestContext) -> bool {
        if !self.enabled || self.models.is_empty() {
            return false;
        }
        // 只有非流式请求适用
        if ctx.request.stream.unwrap_or(false) {
            return false;
        }
        true
    }

    /// 按策略选择最佳响应
    pub fn select_best(&self, responses: &[serde_json::Value]) -> Option<serde_json::Value> {
        if responses.is_empty() {
            return None;
        }
        if responses.len() == 1 {
            return Some(responses[0].clone());
        }

        match self.strategy {
            VotingStrategy::First => Some(responses[0].clone()),
            VotingStrategy::Longest => {
                responses.iter()
                    .max_by_key(|r| extract_content_len(r))
                    .cloned()
            }
            VotingStrategy::Shortest => {
                responses.iter()
                    .min_by_key(|r| extract_content_len(r))
                    .cloned()
            }
        }
    }
}

/// 从 OpenAI 响应中提取 content 长度
fn extract_content_len(resp: &serde_json::Value) -> usize {
    resp.get("choices")
        .and_then(|c| c.as_array())
        .and_then(|arr| arr.first())
        .and_then(|c| c.get("message"))
        .and_then(|m| m.get("content"))
        .and_then(|c| c.as_str())
        .map(|s| s.len())
        .unwrap_or(0)
}

#[async_trait]
impl Middleware for ModelVotingMiddleware {
    async fn process_request(&self, ctx: &mut RequestContext) {
        if !self.enabled || self.models.is_empty() {
            return;
        }
        if ctx.request.stream.unwrap_or(false) {
            return;
        }
        tracing::info!("model voting: strategy={:?}, models={:?}", self.strategy, self.models);
    }

    async fn process_response(&self, _ctx: &mut ResponseContext) {}

    fn name(&self) -> &'static str {
        "model_voting"
    }
}