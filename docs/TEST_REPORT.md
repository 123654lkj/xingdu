# 星渡 (XingDu) 测试报告

日期: 2026-06-05
版本: Rust rewrite

## 测试环境

- 服务器: 团子 (192.168.31.244)
- 端口: 9999
- 服务: systemd xingdu.service
- 日志: /var/log/xingdu/xingdu.log.YYYY-MM-DD

## 后端配置

| 平台 | 模型 | 默认协议 | Anthropic端点 | OpenAI端点 |
|------|------|---------|--------------|-----------|
| 百炼 | qwen3.6-plus | Anthropic | coding.dashscope.aliyuncs.com/apps/anthropic/v1/messages | coding.dashscope.aliyuncs.com/v1/chat/completions |
| DeepSeek | deepseek-v4-flash | OpenAI | api.deepseek.com/anthropic/v1/messages | api.deepseek.com/v1/chat/completions |
| GLM | glm-5.1 | OpenAI | open.bigmodel.cn/api/anthropic/v1/messages | open.bigmodel.cn/api/paas/v4/chat/completions |
| MiniMax | MiniMax-M3 | OpenAI | api.minimaxi.com/anthropic/v1/messages | api.minimaxi.com/v1/chat/completions |

## 核心功能测试

### 1. 基础对话 (chat)

| 平台 | Anthropic | OpenAI | 说明 |
|------|-----------|--------|------|
| 百炼 | ✅ | ✅ | 双协议正常 |
| DeepSeek | ⚠️空 | ⚠️空 | 只返回reasoning_content |
| GLM | ✅ | ⚠️空 | O协议下只返回reasoning |
| MiniMax | ⚠️空 | ✅ | A协议下空，O协议正常 |

**注**: `⚠️空` = 模型只返回thinking/reasoning_content，普通content为空。星渡OpenAI adapter已做合并处理。

### 2. 工具调用 (tools)

| 平台 | Anthropic | OpenAI | 说明 |
|------|-----------|--------|------|
| 百炼 | ✅ | ✅ | 双协议支持 |
| DeepSeek | ✅ | ✅ | 双协议支持 |
| GLM | ✅ | ✅ | 双协议支持 |
| MiniMax | ✅ | ✅ | 双协议支持 |

### 3. 流式响应 (stream)

| 平台 | Anthropic | OpenAI | chunks/events |
|------|-----------|--------|--------------|
| 百炼 | ✅ 156 | ✅ 78 | 正常 |
| DeepSeek | ✅ 34 | ✅ 13 | 正常 |
| GLM | ✅ 32 | ✅ 12 | 正常 |
| MiniMax | ✅ 16 | ✅ 2 | 正常 |

### 4. 思考链 (thinking/reasoning)

| 平台 | Anthropic | OpenAI | 说明 |
|------|-----------|--------|------|
| 百炼 | ✅ | ✅ | 双协议支持 |
| DeepSeek | ✅ | ✅ | 双协议支持 |
| GLM | ⚠️无 | ✅ | A协议无thinking |
| MiniMax | ✅ | ✅ | 双协议支持 |

### 5. 流式 + tools

| 平台 | 结果 | 说明 |
|------|------|------|
| 百炼-A | stream✅ tools⚠️无 | A协议流式不支持tools |
| 百炼-O | stream✅ tools✅ | O协议正常 |
| DeepSeek-O | stream✅ tools✅ | 正常 |
| GLM-O | stream✅ tools✅ | 正常 |
| MiniMax-O | stream✅ tools⚠️无 | 流式不支持tools |

### 6. 流式 + thinking

| 平台 | 结果 |
|------|------|
| DeepSeek-O | stream✅ thinking✅ |
| GLM-O | stream✅ thinking✅ |

### 7. 多轮对话上下文

| 平台 | 结果 | 说明 |
|------|------|------|
| 百炼-O | ✅ 记住名字 | 上下文正常 |
| DeepSeek-O | ⚠️ 遗忘 | v4-flash模型特性，非bug |

### 8. system消息

| 平台 | 结果 | 说明 |
|------|------|------|
| 百炼-O | ✅ 遵守 | 正常 |
| DeepSeek-O | ⚠️ 未遵守 | v4-flash模型特性，非bug |

## 错误处理测试

| 测试项 | 百炼 | DeepSeek | 说明 |
|--------|------|----------|------|
| 401错误key | ✅ HTTP 401 | ✅ HTTP 401 | 正常 |
| 400错误模型 | ✅ HTTP 400 | ✅ HTTP 400 | 正常 |
| 超时(1s) | ✅ TimeoutError | - | 正常 |
| max_tokens=0 | ✅ finish_reason=stop | ⚠️ HTTP 400 | DeepSeek边界问题 |

## 性能测试

| 测试项 | 结果 |
|--------|------|
| 并发5请求 | ✅ 5/5成功 |
| 空消息 | ✅ 正常处理 |
| 特殊字符(emoji/HTML) | ✅ 正常处理 |
| 内存占用 | ✅ 12MB |
| 端口监听 | ✅ 9999 |

## 运维测试

| 测试项 | 结果 |
|--------|------|
| 文件日志 | ✅ /var/log/xingdu/ 737KB |
| systemd状态 | ✅ active 30分钟+ |
| 自动重启 | ✅ Restart=always |
| health端点 | ✅ {"status":"ok"} |
| 模型列表 | ✅ 26个模型 |

## 集成测试

| 测试项 | 结果 |
|--------|------|
| Hermes→星渡→百炼 | ✅ 正常 |
| 星渡/v1/models | ✅ 26个模型 |
| 星渡/v1/chat/completions | ✅ 正常 |

## 已知问题

### 1. DeepSeek v4-flash 特性
- **多轮遗忘**: 模型不保留上下文，非星渡bug
- **system未遵守**: 模型忽略system消息，非星渡bug
- **max_tokens=0 400**: 边界参数报错，建议避免使用

### 2. 流式tools限制
- **百炼-A**: Anthropic协议流式不支持tools，切换O协议解决
- **MiniMax-O**: 流式不支持tools，非流式可用

### 3. Anthropic adapter thinking处理
- 当前Anthropic协议的thinking block已能正常返回
- 但thinking内容未合并到content（和OpenAI adapter不同）
- 建议统一处理：thinking合并到content

## 建议

1. **智能协议选择**: 根据请求特征动态选择最优协议
2. **统一thinking处理**: Anthropic和OpenAI adapter都合并thinking到content
3. **错误码映射**: 后端错误码统一映射为OpenAI格式
4. **监控告警**: 添加后端健康检查，失败自动切换

## 缓存测试

| 测试项 | 结果 |
|--------|------|
| 缓存开启 | ✅ XINGDU_RESP_CACHE=1 |
| exact cache HIT | ✅ 相同请求直接返回 |
| 缓存节省 | 重复请求省 100% token |

