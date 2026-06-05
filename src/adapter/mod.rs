use async_trait::async_trait;
use std::sync::Arc;
use crate::types::*;

/// 后端协议类型
#[derive(Debug, Clone, PartialEq)]
pub enum Protocol {
    OpenAI,
    Anthropic,
}

impl Protocol {
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "openai" | "open-ai" => Protocol::OpenAI,
            _ => Protocol::Anthropic,
        }
    }
}

/// 后端配置
#[derive(Debug, Clone)]
pub struct BackendConfig {
    pub name: String,
    pub url: String,
    pub protocol: Protocol,
    pub api_key: String,
    pub model: String,
}

/// 适配器错误
#[derive(Debug, thiserror::Error)]
pub enum AdapterError {
    #[error("serialization error: {0}")]
    Serialization(String),
    #[error("unsupported protocol: {0}")]
    UnsupportedProtocol(String),
}

/// 适配器 trait
#[async_trait]
pub trait Adapter: Send + Sync {
    /// 将 OpenAI 请求转换为后端请求
    fn request_to_backend(&self, request: &OpenAIRequest, backend: &BackendConfig) -> Result<serde_json::Value, AdapterError>;

    /// 将后端响应转换为 OpenAI 响应
    fn response_to_client(&self, response: serde_json::Value, model: &str) -> Result<OpenAIResponse, AdapterError>;

    /// 将后端流式事件转换为 OpenAI 流式 chunk
    fn stream_event_to_client(&self, event: &str, model: &str) -> Option<OpenAIStreamChunk>;
}

/// OpenAI 适配器（直通，不做转换）
pub struct OpenAiAdapter;

#[async_trait]
impl Adapter for OpenAiAdapter {
    fn request_to_backend(&self, request: &OpenAIRequest, _backend: &BackendConfig) -> Result<serde_json::Value, AdapterError> {
        serde_json::to_value(request).map_err(|e| AdapterError::Serialization(e.to_string()))
    }

    fn response_to_client(&self, response: serde_json::Value, _model: &str) -> Result<OpenAIResponse, AdapterError> {
        serde_json::from_value(response).map_err(|e| AdapterError::Serialization(e.to_string()))
    }

    fn stream_event_to_client(&self, event: &str, _model: &str) -> Option<OpenAIStreamChunk> {
        if let Ok(chunk) = serde_json::from_str::<OpenAIStreamChunk>(event) {
            Some(chunk)
        } else {
            None
        }
    }
}

/// Anthropic 适配器（OpenAI ↔ Anthropic 双向转换）
pub struct AnthropicAdapter;

#[async_trait]
impl Adapter for AnthropicAdapter {
    fn request_to_backend(&self, request: &OpenAIRequest, backend: &BackendConfig) -> Result<serde_json::Value, AdapterError> {
        // 提取 system 消息
        let mut system_text: Option<String> = None;
        let mut anthropic_messages = Vec::new();

        for msg in &request.messages {
            if msg.role == "system" {
                if let Some(text) = msg.content.as_str() {
                    system_text = Some(text.to_string());
                }
            } else {
                let content = match &msg.content {
                    serde_json::Value::String(s) => s.clone(),
                    _ => msg.content.to_string(),
                };
                anthropic_messages.push(AnthropicMessage {
                    role: msg.role.clone(),
                    content,
                });
            }
        }

        let max_tokens = request.max_tokens.unwrap_or(16384);

        let mut payload = serde_json::json!({
            "model": backend.model,
            "max_tokens": max_tokens,
            "messages": anthropic_messages,
        });

        if let Some(system) = system_text {
            payload["system"] = serde_json::json!(system);
        }

        if request.stream.unwrap_or(false) {
            payload["stream"] = serde_json::json!(true);
        }

        // 转换 tools
        if let Some(tools) = &request.tools {
            let anthropic_tools: Vec<serde_json::Value> = tools.iter().map(|t| {
                serde_json::json!({
                    "name": t.function.name,
                    "description": t.function.description,
                    "input_schema": t.function.parameters,
                })
            }).collect();
            payload["tools"] = serde_json::Value::Array(anthropic_tools);
        }

        Ok(payload)
    }

    fn response_to_client(&self, response: serde_json::Value, model: &str) -> Result<OpenAIResponse, AdapterError> {
        let ar: AnthropicResponse = serde_json::from_value(response)
            .map_err(|e| AdapterError::Serialization(format!("Anthropic parse error: {}", e)))?;

        let mut content = String::new();
        let mut tool_calls = Vec::new();
        for block in &ar.content {
            match block.block_type.as_str() {
                "text" => {
                    if let Some(ref text) = block.text {
                        content.push_str(text);
                    }
                }
                "tool_use" => {
                    if let (Some(ref id), Some(ref name), Some(ref input)) = (&block.id, &block.name, &block.input) {
                        tool_calls.push(ToolCall {
                            id: id.clone(),
                            call_type: "function".into(),
                            function: FunctionCall {
                                name: name.clone(),
                                arguments: serde_json::to_string(input).unwrap_or_else(|_| "{}".into()),
                            },
                        });
                    }
                }
                _ => {}
            }
        }

        let finish_reason = match ar.stop_reason.as_deref() {
            Some("end_turn" | "stop_sequence") => Some("stop".to_string()),
            Some("max_tokens") => Some("length".to_string()),
            Some("tool_use") => Some("tool_calls".to_string()),
            _ => None,
        };

        let message = ResponseMessage {
            role: "assistant".into(),
            content: if content.is_empty() { None } else { Some(content) },
            tool_calls: if tool_calls.is_empty() { None } else { Some(tool_calls) },
        };

        Ok(OpenAIResponse {
            id: ar.id.clone(),
            object: "chat.completion".into(),
            created: chrono::Utc::now().timestamp(),
            model: model.to_string(),
            choices: vec![Choice {
                index: 0,
                message,
                finish_reason,
            }],
            usage: Some(Usage {
                prompt_tokens: ar.usage.input_tokens,
                completion_tokens: ar.usage.output_tokens,
                total_tokens: ar.usage.input_tokens + ar.usage.output_tokens,
            }),
        })
    }

    fn stream_event_to_client(&self, event: &str, model: &str) -> Option<OpenAIStreamChunk> {
        // Anthropic SSE: data:{"type":"content_block_delta",...} or data: {"type":...}
        // Strip "data:" prefix (with or without trailing space)
        if !event.starts_with("data:") {
            return None;
        }
        let body = event[5..].strip_prefix(' ').unwrap_or(&event[5..]);
        if body.trim() == "[DONE]" {
            return Some(OpenAIStreamChunk {
                id: "chatcmpl-proxy".into(),
                object: "chat.completion.chunk".into(),
                created: chrono::Utc::now().timestamp(),
                model: model.to_string(),
                choices: vec![StreamChoice {
                    index: 0,
                    delta: Delta { role: None, content: None },
                    finish_reason: Some("stop".into()),
                }],
            });
        }

        let evt: serde_json::Value = serde_json::from_str(body).ok()?;
        let evt_type = evt.get("type")?.as_str()?;

        if evt_type == "content_block_delta" {
            let delta = evt.get("delta")?;
            let delta_type = delta.get("type").and_then(|v| v.as_str()).unwrap_or("");
            if delta_type == "thinking_delta" {
                // Pass through thinking as reasoning_content
                let thinking = delta.get("thinking").and_then(|v| v.as_str())?;
                return Some(OpenAIStreamChunk {
                    id: "chatcmpl-proxy".into(),
                    object: "chat.completion.chunk".into(),
                    created: chrono::Utc::now().timestamp(),
                    model: model.to_string(),
                    choices: vec![StreamChoice {
                        index: 0,
                        delta: Delta { role: None, content: Some(thinking.to_string()) },
                        finish_reason: None,
                    }],
                });
            }
            if delta_type == "input_json_delta" {
                // Tool call arguments streaming
                let partial = delta.get("partial_json").and_then(|v| v.as_str())?;
                return Some(OpenAIStreamChunk {
                    id: "chatcmpl-proxy".into(),
                    object: "chat.completion.chunk".into(),
                    created: chrono::Utc::now().timestamp(),
                    model: model.to_string(),
                    choices: vec![StreamChoice {
                        index: 0,
                        delta: Delta {
                            role: None,
                            content: Some(format!("[tool_args:{}]", partial)),
                        },
                        finish_reason: None,
                    }],
                });
            }
            let text = delta.get("text").and_then(|v| v.as_str())?;
            Some(OpenAIStreamChunk {
                id: "chatcmpl-proxy".into(),
                object: "chat.completion.chunk".into(),
                created: chrono::Utc::now().timestamp(),
                model: model.to_string(),
                choices: vec![StreamChoice {
                    index: 0,
                    delta: Delta { role: None, content: Some(text.to_string()) },
                    finish_reason: None,
                }],
            })
        } else if evt_type == "content_block_start" {
            let cb = evt.get("content_block")?;
            let cbt = cb.get("type")?.as_str()?;
            if cbt == "tool_use" {
                // Tool call start
                let name = cb.get("name")?.as_str()?;
                let id = cb.get("id")?.as_str()?;
                return Some(OpenAIStreamChunk {
                    id: "chatcmpl-proxy".into(),
                    object: "chat.completion.chunk".into(),
                    created: chrono::Utc::now().timestamp(),
                    model: model.to_string(),
                    choices: vec![StreamChoice {
                        index: 0,
                        delta: Delta {
                            role: None,
                            content: Some(format!("[tool_start:{}:{}]", id, name)),
                        },
                        finish_reason: None,
                    }],
                });
            }
            // thinking block start — skip
            None
        } else if evt_type == "message_stop" {
            Some(OpenAIStreamChunk {
                id: "chatcmpl-proxy".into(),
                object: "chat.completion.chunk".into(),
                created: chrono::Utc::now().timestamp(),
                model: model.to_string(),
                choices: vec![StreamChoice {
                    index: 0,
                    delta: Delta { role: None, content: None },
                    finish_reason: Some("stop".into()),
                }],
            })
        } else {
            None
        }
    }
}

/// 适配器工厂
pub fn create_adapter(protocol: &Protocol) -> Arc<dyn Adapter> {
    match protocol {
        Protocol::OpenAI => Arc::new(OpenAiAdapter),
        Protocol::Anthropic => Arc::new(AnthropicAdapter),
    }
}