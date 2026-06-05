mod config;
mod types;
mod adapter;
mod pipeline;
mod server;
mod client;
mod middleware;
mod metrics;
mod dashboard;
mod security;

use std::sync::Arc;
use tokio::sync::RwLock;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "xingdu", version, about = "LLM API Gateway - Rust rewrite")]
struct Cli {
    #[arg(long, default_value = None)] host: Option<String>,
    #[arg(long, default_value_t = 9999)] port: u16,
    #[arg(long, default_value_t = false)] verbose: bool,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let filter = if cli.verbose { "xingdu=debug,tower_http=debug" } else { "xingdu=info,tower_http=info" };
    // 初始化日志：stdout + 文件
    let log_dir = std::path::PathBuf::from("/var/log/xingdu");
    std::fs::create_dir_all(&log_dir).ok();
    let file_appender = tracing_appender::rolling::daily(&log_dir, "xingdu.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(filter));

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .with_writer(non_blocking)
        .init();

    let mut cfg = config::Config::from_env();
    if let Some(host) = cli.host { cfg.host = host; }
    cfg.port = cli.port;
    if let Err(errors) = cfg.validate() {
        for e in &errors { tracing::error!("config error: {}", e); }
        anyhow::bail!("invalid configuration: {}", errors.join("; "));
    }
    let config = Arc::new(RwLock::new(cfg));

    let metrics = {
        let cfg = config.read().await;
        if cfg.metrics_enabled { tracing::info!("metrics enabled"); }
        Arc::new(metrics::MetricsCollector::new(cfg.metrics_enabled))
    };

    // --- 缂傛挸鐡ㄩ崪宀€鍟嶉弬顓炴珤閿涘牆宕熼悪顒€绱╅悽顭掔礉娑撳秷铔?pipeline閿?---
    let cache_mw: Option<Arc<middleware::cache::ResponseCacheMiddleware>> = {
        let cfg = config.read().await;
        if cfg.resp_cache > 0 {
            Some(Arc::new(middleware::cache::ResponseCacheMiddleware::new(cfg.resp_cache, cfg.resp_cache_ttl)))
        } else { None }
    };
    let breaker_mw: Option<Arc<middleware::circuit_breaker::CircuitBreakerMiddleware>> = {
        let cfg = config.read().await;
        if cfg.circuit_breaker_threshold > 0 {
            Some(Arc::new(middleware::circuit_breaker::CircuitBreakerMiddleware::new(true, cfg.circuit_breaker_threshold, cfg.circuit_breaker_recovery)))
        } else { None }
    };

    // --- Pipeline閿涘牊澧嶉張澶夎厬闂傜繝娆㈠▔銊ュ弳 Pipeline閿涘奔绲?cache/breaker 閸欘亜浠涢柅鏄忕帆閺嶅洩顔囬敍?---
    let mut pipeline = pipeline::Pipeline::new();
    {
        let cfg = config.read().await;
        pipeline.add(Box::new(middleware::headroom::ToolCompressionMiddleware::new(cfg.tool_compression > 0, cfg.tool_compression_min)));
        pipeline.add(Box::new(middleware::cost_routing::CostRoutingMiddleware::new(cfg.cost_routing > 0, cfg.cheap_model.clone(), cfg.expensive_model.clone())));
        pipeline.add(Box::new(middleware::fallback::FallbackMiddleware::new(cfg.fallback_enabled, cfg.fallback_chain.clone(), cfg.fallback_timeout)));
        pipeline.add(Box::new(middleware::starhub::StarhubMiddleware::new(cfg.starhub_enabled, cfg.starhub_url.clone(), cfg.starhub_limit, cfg.starhub_timeout)));
        pipeline.add(Box::new(middleware::rate_limit::RateLimitMiddleware::new(cfg.rate_limit)));
        pipeline.add(Box::new(middleware::speculative::SpeculativeMiddleware::new(false, String::new(), cfg.backend_model.clone())));
        pipeline.add(Box::new(middleware::model_voting::ModelVotingMiddleware::new(false, Vec::new(), middleware::model_voting::VotingStrategy::Longest)));
        pipeline.add(Box::new(middleware::post_process::PostProcessMiddleware::new(true)));
        pipeline.add(Box::new(middleware::flow_control::FlowControlMiddleware::new(false, 10.0)));
        pipeline.add(Box::new(middleware::plugin::PluginMiddleware::new(false)));
        pipeline.add(Box::new(middleware::plugin::WebhookMiddleware::new(false, String::new())));
        pipeline.add(Box::new(middleware::plugin::TenantMiddleware::new(false)));
    }

    let pipeline = Arc::new(pipeline);
    tracing::info!("pipeline initialized with {} middlewares", pipeline.len());

    let client = client::HttpClient::new()?;
    let state = Arc::new(server::AppState {
        config: config.clone(),
        pipeline,
        client,
        metrics: metrics.clone(),
        cache_mw,
        breaker_mw,
    });

    let mut app = server::create_router(state);
    {
        let cfg = config.read().await;
        if cfg.dashboard_enabled {
            let ds = Arc::new(dashboard::DashboardState { metrics });
            app = app.merge(dashboard::create_dashboard_router(ds));
        }
    }

    let addr = { let c = config.read().await; format!("{}:{}", c.host, c.port) };
    tracing::info!("xingdu starting on {}", addr);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}