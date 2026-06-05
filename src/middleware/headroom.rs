use async_trait::async_trait;
use crate::pipeline::{Middleware, RequestContext, ResponseContext};

/// 工具压缩中间件
/// 拦截 tool_result 消息，对工具输出进行智能压缩以减少 token 消耗
pub struct ToolCompressionMiddleware {
    /// 0=关闭, 1=开启
    pub enabled: bool,
    /// 超过此字符数才压缩
    pub min_chars: usize,
}

impl ToolCompressionMiddleware {
    pub fn new(enabled: bool, min_chars: usize) -> Self {
        ToolCompressionMiddleware { enabled, min_chars }
    }
}

#[async_trait]
impl Middleware for ToolCompressionMiddleware {
    async fn process_request(&self, ctx: &mut RequestContext) {
        if !self.enabled {
            return;
        }

        for msg in &mut ctx.modified_messages {
            if !is_tool_result(msg) {
                continue;
            }

            // 提取 content 文本
            let content = match msg.get("content") {
                Some(serde_json::Value::String(s)) => s.clone(),
                Some(serde_json::Value::Array(arr)) => {
                    // 可能是 [{type: "tool_result", content: ...}] 格式
                    arr.iter()
                        .filter_map(|b| b.get("text").and_then(|t| t.as_str()))
                        .collect::<Vec<_>>()
                        .join("\n")
                }
                Some(v) => v.to_string(),
                None => continue,
            };

            if content.len() <= self.min_chars {
                continue;
            }

            // 智能检测内容类型并压缩
            let compressed = smart_compress(&content);
            msg["content"] = serde_json::Value::String(compressed);
        }
    }

    async fn process_response(&self, _ctx: &mut ResponseContext) {}

    fn name(&self) -> &'static str {
        "tool_compression"
    }
}

/// 判断消息是否为 tool_result
fn is_tool_result(msg: &serde_json::Value) -> bool {
    if let Some(role) = msg.get("role").and_then(|r| r.as_str()) {
        if role == "tool" {
            return true;
        }
        if role == "assistant" {
            // 检查是否有 tool_calls
            if msg.get("tool_calls").is_some() || msg.get("tool_calls").is_some() {
                return false;
            }
        }
    }
    // 检查 content 是否为 tool_result 数组
    if let Some(content) = msg.get("content") {
        if let Some(arr) = content.as_array() {
            for item in arr {
                if let Some(t) = item.get("type").and_then(|t| t.as_str()) {
                    if t == "tool_result" || t == "tool_use" {
                        return true;
                    }
                }
            }
        }
    }
    false
}

/// 智能压缩：检测内容类型后执行相应压缩策略
fn smart_compress(content: &str) -> String {
    let trimmed = content.trim();
    if trimmed.is_empty() {
        return content.to_string();
    }

    // 检测是否为 JSON
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        return compress_json(trimmed);
    }

    // 检测是否为代码（含代码特征的行数较多）
    let lines: Vec<&str> = trimmed.lines().collect();
    if lines.len() > 20 {
        let code_like = lines.iter().filter(|l| {
            l.contains("fn ") || l.contains("def ") || l.contains("function ")
                || l.contains("=>") || l.contains("->") || l.contains("class ")
                || l.contains("import ") || l.contains("//") || l.contains("/*")
                || l.contains("pub ") || l.contains("let ") || l.contains("var ")
        }).count();
        if code_like as f64 / lines.len() as f64 > 0.3 {
            return compress_code(trimmed);
        }
    }

    // 长文本：保留首尾
    compress_text(trimmed)
}

/// JSON 压缩：移除冗余空格，如果太长则扁平化
fn compress_json(content: &str) -> String {
    // 先尝试漂亮的 JSON 扁平化
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(content) {
        match &v {
            serde_json::Value::Object(map) => {
                // 移除 null 和空数组
                let filtered: serde_json::Map<String, serde_json::Value> = map.iter()
                    .filter(|(_, val)| !val.is_null() && !(val.is_array() && val.as_array().unwrap().is_empty()))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                let compact = serde_json::to_string(&filtered).unwrap_or_default();
                if compact.len() < content.len() / 2 {
                    return compact;
                }
            }
            serde_json::Value::Array(arr) => {
                // 数组太长时只保留前 20 项
                if arr.len() > 20 {
                    let truncated: Vec<&serde_json::Value> = arr.iter().take(20).collect();
                    if let Ok(compact) = serde_json::to_string(&truncated) {
                        return format!("{}... [truncated {} items]", compact, arr.len() - 20);
                    }
                }
            }
            _ => {}
        }
    }
    // fallback：直接紧凑化
    let compact = serde_json::to_string(
        &serde_json::from_str::<serde_json::Value>(content).unwrap_or_default()
    ).unwrap_or_else(|_| content.to_string());
    if compact.len() < content.len() {
        compact
    } else {
        content.to_string()
    }
}

/// 代码压缩：去注释、合并空行
fn compress_code(content: &str) -> String {
    let mut result = String::new();
    let mut prev_empty = false;
    for line in content.lines() {
        let trimmed = line.trim();
        // 跳过注释行
        if trimmed.starts_with("//") || trimmed.starts_with("# ") || trimmed.starts_with("/*") {
            continue;
        }
        // 合并连续空行
        if trimmed.is_empty() {
            if prev_empty {
                continue;
            }
            prev_empty = true;
        } else {
            prev_empty = false;
        }
        result.push_str(line);
        result.push('\n');
    }
    result.trim().to_string()
}

/// 长文本压缩：保留首部 30% 和尾部 10%
fn compress_text(content: &str) -> String {
    let total = content.len();
    if total <= self::MIN_COMPRESS_LEN {
        return content.to_string();
    }

    let head_len = (total as f64 * 0.3) as usize;
    let tail_len = (total as f64 * 0.1) as usize;

    let head = &content[..head_len];
    let tail = &content[total - tail_len..];

    format!("{}...\n[中间省略 {} 字符]\n...{}", head, total - head_len - tail_len, tail)
}

const MIN_COMPRESS_LEN: usize = 2000;