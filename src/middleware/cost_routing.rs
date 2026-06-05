use async_trait::async_trait;
use crate::pipeline::{Middleware, RequestContext, ResponseContext};

/// 成本路由中间件
/// 根据请求复杂度自动选择不同成本的模型
pub struct CostRoutingMiddleware {
    /// 0=关闭, 1=启用
    pub enabled: bool,
    /// 便宜模型名称
    pub cheap_model: String,
    /// 昂贵模型名称
    pub expensive_model: String,
}

#[derive(Debug, PartialEq)]
enum Complexity {
    Low,
    Medium,
    High,
}

impl CostRoutingMiddleware {
    pub fn new(enabled: bool, cheap_model: String, expensive_model: String) -> Self {
        CostRoutingMiddleware { enabled, cheap_model, expensive_model }
    }

    /// 分类请求复杂度
    fn classify_complexity(ctx: &RequestContext) -> Complexity {
        let total_chars: usize = ctx.request.messages.iter()
            .map(|m| match &m.content {
                serde_json::Value::String(s) => s.len(),
                serde_json::Value::Array(arr) => arr.iter()
                    .filter_map(|v| v.as_str().or_else(|| v.get("text").and_then(|t| t.as_str())))
                    .map(|s| s.len())
                    .sum(),
                v => v.to_string().len(),
            })
            .sum();

        let has_tools = ctx.request.tools.as_ref()
            .map(|t| !t.is_empty())
            .unwrap_or(false);

        if has_tools || total_chars > 8000 {
            Complexity::High
        } else if total_chars < 800 {
            Complexity::Low
        } else {
            Complexity::Medium
        }
    }
}

#[async_trait]
impl Middleware for CostRoutingMiddleware {
    async fn process_request(&self, ctx: &mut RequestContext) {
        if !self.enabled {
            return;
        }

        let complexity = Self::classify_complexity(ctx);
        let original = ctx.selected_model.clone();

        match complexity {
            Complexity::Low if !self.cheap_model.is_empty() => {
                ctx.selected_model = self.cheap_model.clone();
                tracing::info!("cost routing: LOW complexity, model {} -> {}", original, ctx.selected_model);
            }
            Complexity::High if !self.expensive_model.is_empty() => {
                ctx.selected_model = self.expensive_model.clone();
                tracing::info!("cost routing: HIGH complexity, model {} -> {}", original, ctx.selected_model);
            }
            _ => {
                tracing::debug!("cost routing: MEDIUM complexity, keeping model {}", original);
            }
        }
    }

    async fn process_response(&self, _ctx: &mut ResponseContext) {}

    fn name(&self) -> &'static str {
        "cost_routing"
    }
}