use async_trait::async_trait;
use std::sync::Arc;
use crate::config::Config;
use crate::types::OpenAIRequest;

/// 请求上下文，贯穿整个管道
#[derive(Debug, Clone)]
pub struct RequestContext {
    /// 原始 OpenAI 请求
    pub request: OpenAIRequest,
    /// 经过中间件修改后的消息（JSON Value 方便修改）
    pub modified_messages: Vec<serde_json::Value>,
    /// 原始消息备份
    pub original_messages: Vec<serde_json::Value>,
    /// 错误列表（中间件不中断，但记录错误）
    pub errors: Vec<String>,
    /// 配置引用
    pub config: Arc<tokio::sync::RwLock<Config>>,
    /// 是否跳过缓存
    pub skip_cache: bool,
    /// 选中的后端模型
    pub selected_model: String,
    /// RAG 注入内容
    pub rag_context: Option<String>,
    /// 缓存命中的响应（如果有）
    pub cached_response: Option<serde_json::Value>,
}

impl RequestContext {
    pub fn new(request: OpenAIRequest, config: Arc<tokio::sync::RwLock<Config>>) -> Self {
        let original_messages: Vec<serde_json::Value> = request.messages.iter().map(|m| {
            serde_json::json!({ "role": m.role, "content": m.content })
        }).collect();

        let model = request.model.clone();

        RequestContext {
            modified_messages: original_messages.clone(),
            original_messages,
            errors: Vec::new(),
            config,
            skip_cache: false,
            selected_model: model,
            rag_context: None,
            cached_response: None,
            request,
        }
    }

    /// 获取当前消息列表（修改后）
    pub fn messages(&self) -> &Vec<serde_json::Value> {
        &self.modified_messages
    }
}

/// 响应上下文
#[derive(Debug, Clone)]
pub struct ResponseContext {
    pub response: crate::types::OpenAIResponse,
    pub errors: Vec<String>,
    pub cache_key: Option<String>,
    pub raw_response: Option<serde_json::Value>,
}

impl ResponseContext {
    pub fn new(response: crate::types::OpenAIResponse) -> Self {
        ResponseContext {
            response,
            errors: Vec::new(),
            cache_key: None,
            raw_response: None,
        }
    }
}

/// 中间件 trait
#[async_trait]
pub trait Middleware: Send + Sync {
    /// 请求阶段处理
    async fn process_request(&self, ctx: &mut RequestContext);

    /// 响应阶段处理
    async fn process_response(&self, ctx: &mut ResponseContext) {
        let _ = ctx; // 默认无操作
    }

    /// 中间件名称
    fn name(&self) -> &'static str;
}

/// 管道引擎
pub struct Pipeline {
    middlewares: Vec<Box<dyn Middleware>>,
}

impl Pipeline {
    pub fn new() -> Self {
        Pipeline { middlewares: Vec::new() }
    }

    /// 注册中间件（运行时）
    pub fn add(&mut self, middleware: Box<dyn Middleware>) {
        self.middlewares.push(middleware);
    }

    /// 执行请求管道
    pub async fn execute_request(&self, ctx: &mut RequestContext) {
        for mw in &self.middlewares {
            mw.process_request(ctx).await;
        }
    }

    /// 执行响应管道
    pub async fn execute_response(&self, ctx: &mut ResponseContext) {
        for mw in &self.middlewares {
            mw.process_response(ctx).await;
        }
    }

    /// 中间件数量
    pub fn len(&self) -> usize {
        self.middlewares.len()
    }
}