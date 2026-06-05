use std::env;

/// 全局配置（Arc<RwLock<Config>> 共享）
#[derive(Debug, Clone)]
pub struct Config {
    // --- 基础 ---
    pub host: String,
    pub port: u16,
    pub dump_dir: String,
    pub max_messages: usize,
    pub auto_retry: bool,
    pub auto_retry_max: u32,

    // --- 工具压缩 ---
    pub tool_compression: u8,
    pub tool_compression_min: usize,

    // --- Prompt Cache ---
    pub prompt_cache: bool,

    // --- 响应缓存 ---
    pub resp_cache: u8,
    pub resp_cache_ttl: u64,
    pub redis_url: String,

    // --- 星枢 ---
    pub starhub_enabled: bool,
    pub starhub_url: String,
    pub starhub_limit: usize,
    pub starhub_timeout: u64,

    // --- 成本路由 ---
    pub cost_routing: u8,
    pub cheap_model: String,
    pub expensive_model: String,

    // --- Token 预算 ---
    pub token_budget: u8,

    // --- Fallback ---
    pub fallback_enabled: bool,
    pub fallback_chain: Vec<String>,
    pub fallback_timeout: u64,

    // --- 熔断器 ---
    pub circuit_breaker_threshold: u32,
    pub circuit_breaker_recovery: u64,

    // --- 安全 ---
    pub auth_enabled: bool,
    pub api_keys: Vec<String>,
    pub ip_whitelist: Vec<String>,
    pub rate_limit: u32,

    // --- 后端 ---
    pub backend_name: String,
    pub backend_url: String,
    pub backend_api_key: String,
    pub backend_model: String,

    // --- 指标 ---
    pub metrics_enabled: bool,
    pub metrics_sqlite_path: String,

    // --- 审计 ---
    pub audit_log: bool,
    pub audit_log_path: String,

    // --- Web 面板 ---
    pub dashboard_enabled: bool,
    pub dashboard_port: u16,
}

impl Config {
    /// 从环境变量加载配置
    pub fn from_env() -> Self {
        Config {
            host: env::var("XINGDU_HOST").unwrap_or_else(|_| "0.0.0.0".into()),
            port: env::var("XINGDU_PORT").unwrap_or_else(|_| "9999".into()).parse().unwrap_or(9999),
            dump_dir: env::var("XINGDU_DUMP_DIR").unwrap_or_else(|_| "./dumps".into()),
            max_messages: env::var("XINGDU_MAX_MESSAGES").unwrap_or_else(|_| "100".into()).parse().unwrap_or(100),
            auto_retry: env::var("XINGDU_AUTO_RETRY").unwrap_or_else(|_| "1".into()) == "1",
            auto_retry_max: env::var("XINGDU_AUTO_RETRY_MAX").unwrap_or_else(|_| "3".into()).parse().unwrap_or(3),

            tool_compression: env::var("XINGDU_TOOL_COMPRESSION").unwrap_or_else(|_| "0".into()).parse().unwrap_or(0),
            tool_compression_min: env::var("XINGDU_TOOL_COMPRESSION_MIN").unwrap_or_else(|_| "1000".into()).parse().unwrap_or(1000),

            prompt_cache: env::var("XINGDU_PROMPT_CACHE").unwrap_or_else(|_| "0".into()) == "1",

            resp_cache: env::var("XINGDU_RESP_CACHE").unwrap_or_else(|_| "0".into()).parse().unwrap_or(0),
            resp_cache_ttl: env::var("XINGDU_RESP_CACHE_TTL").unwrap_or_else(|_| "3600".into()).parse().unwrap_or(3600),
            redis_url: env::var("XINGDU_REDIS_URL").unwrap_or_else(|_| "".into()),

            starhub_enabled: env::var("XINGDU_STARHUB_ENABLED").unwrap_or_else(|_| "0".into()) == "1",
            starhub_url: env::var("XINGDU_STARHUB_URL").unwrap_or_else(|_| "http://localhost:26670".into()),
            starhub_limit: env::var("XINGDU_STARHUB_LIMIT").unwrap_or_else(|_| "5".into()).parse().unwrap_or(5),
            starhub_timeout: env::var("XINGDU_STARHUB_TIMEOUT").unwrap_or_else(|_| "3".into()).parse().unwrap_or(3),

            cost_routing: env::var("XINGDU_COST_ROUTING").unwrap_or_else(|_| "0".into()).parse().unwrap_or(0),
            cheap_model: env::var("XINGDU_CHEAP_MODEL").unwrap_or_else(|_| "".into()),
            expensive_model: env::var("XINGDU_EXPENSIVE_MODEL").unwrap_or_else(|_| "".into()),

            token_budget: env::var("XINGDU_TOKEN_BUDGET").unwrap_or_else(|_| "0".into()).parse().unwrap_or(0),

            fallback_enabled: env::var("XINGDU_FALLBACK_ENABLED").unwrap_or_else(|_| "0".into()) == "1",
            fallback_chain: env::var("XINGDU_FALLBACK_CHAIN").unwrap_or_else(|_| "".into()).split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect(),
            fallback_timeout: env::var("XINGDU_FALLBACK_TIMEOUT").unwrap_or_else(|_| "30".into()).parse().unwrap_or(30),

            circuit_breaker_threshold: env::var("XINGDU_CIRCUIT_BREAKER_THRESHOLD").unwrap_or_else(|_| "5".into()).parse().unwrap_or(5),
            circuit_breaker_recovery: env::var("XINGDU_CIRCUIT_BREAKER_RECOVERY").unwrap_or_else(|_| "60".into()).parse().unwrap_or(60),

            auth_enabled: env::var("XINGDU_AUTH_ENABLED").unwrap_or_else(|_| "0".into()) == "1",
            api_keys: env::var("XINGDU_API_KEYS").unwrap_or_else(|_| "".into()).split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect(),
            ip_whitelist: env::var("XINGDU_IP_WHITELIST").unwrap_or_else(|_| "".into()).split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect(),
            rate_limit: env::var("XINGDU_RATE_LIMIT").unwrap_or_else(|_| "0".into()).parse().unwrap_or(0),

            backend_name: env::var("XINGDU_BACKEND_NAME").unwrap_or_else(|_| "dashscope".into()),
            backend_url: env::var("XINGDU_BACKEND_URL").unwrap_or_else(|_| "https://coding.dashscope.aliyuncs.com/apps/anthropic/v1/messages".into()),
            backend_api_key: env::var("XINGDU_BACKEND_API_KEY").unwrap_or_else(|_| "".into()),
            backend_model: env::var("XINGDU_BACKEND_MODEL").unwrap_or_else(|_| "qwen3.6-plus".into()),

            metrics_enabled: env::var("XINGDU_METRICS_ENABLED").unwrap_or_else(|_| "0".into()) == "1",
            metrics_sqlite_path: env::var("XINGDU_METRICS_SQLITE_PATH").unwrap_or_else(|_| "./metrics.db".into()),

            audit_log: env::var("XINGDU_AUDIT_LOG").unwrap_or_else(|_| "0".into()) == "1",
            audit_log_path: env::var("XINGDU_AUDIT_LOG_PATH").unwrap_or_else(|_| "./audit.log".into()),

            dashboard_enabled: env::var("XINGDU_DASHBOARD_ENABLED").unwrap_or_else(|_| "0".into()) == "1",
            dashboard_port: env::var("XINGDU_DASHBOARD_PORT").unwrap_or_else(|_| "9998".into()).parse().unwrap_or(9998),
        }
    }

    /// 验证配置
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();
        if self.port == 0 {
            errors.push("XINGDU_PORT must be > 0".into());
        }
        if self.backend_api_key.is_empty() && self.auth_enabled {
            errors.push("XINGDU_BACKEND_API_KEY is required when auth is enabled".into());
        }
        if errors.is_empty() { Ok(()) } else { Err(errors) }
    }
}
