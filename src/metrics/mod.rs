use std::sync::atomic::{AtomicU64, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use dashmap::DashMap;

#[derive(Debug, Default)]
struct ModelMetrics {
    requests: AtomicU64,
    tokens_input: AtomicU64,
    tokens_output: AtomicU64,
    cost_cents: AtomicI64,
    errors: AtomicU64,
    latency_total_ms: AtomicU64,
}

/// 指标收集器
pub struct MetricsCollector {
    total_requests: AtomicU64,
    total_tokens_input: AtomicU64,
    total_tokens_output: AtomicU64,
    total_cost_cents: AtomicI64,
    error_count: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    tokens_saved: AtomicU64,
    fallback_count: AtomicU64,
    compression_before: AtomicU64,
    compression_after: AtomicU64,
    by_model: DashMap<String, ModelMetrics>,
    started_at: Instant,
    pub enabled: bool,
}

impl MetricsCollector {
    pub fn new(enabled: bool) -> Self {
        MetricsCollector {
            total_requests: AtomicU64::new(0),
            total_tokens_input: AtomicU64::new(0),
            total_tokens_output: AtomicU64::new(0),
            total_cost_cents: AtomicI64::new(0),
            error_count: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            tokens_saved: AtomicU64::new(0),
            fallback_count: AtomicU64::new(0),
            compression_before: AtomicU64::new(0),
            compression_after: AtomicU64::new(0),
            by_model: DashMap::new(),
            started_at: Instant::now(),
            enabled,
        }
    }

    pub fn record_request(&self, model: &str, tokens_in: u64, tokens_out: u64, cost: f64, latency_ms: u64) {
        if !self.enabled { return; }
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        self.total_tokens_input.fetch_add(tokens_in, Ordering::Relaxed);
        self.total_tokens_output.fetch_add(tokens_out, Ordering::Relaxed);
        self.total_cost_cents.fetch_add((cost * 100.0) as i64, Ordering::Relaxed);

        let entry = self.by_model.entry(model.to_string()).or_insert_with(ModelMetrics::default);
        entry.requests.fetch_add(1, Ordering::Relaxed);
        entry.tokens_input.fetch_add(tokens_in, Ordering::Relaxed);
        entry.tokens_output.fetch_add(tokens_out, Ordering::Relaxed);
        entry.cost_cents.fetch_add((cost * 100.0) as i64, Ordering::Relaxed);
        entry.latency_total_ms.fetch_add(latency_ms, Ordering::Relaxed);
    }

    pub fn record_error(&self) {
        if !self.enabled { return; }
        self.error_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_cache(&self, hit: bool) {
        if !self.enabled { return; }
        if hit { self.cache_hits.fetch_add(1, Ordering::Relaxed); }
        else { self.cache_misses.fetch_add(1, Ordering::Relaxed); }
    }

    pub fn record_fallback(&self) {
        if !self.enabled { return; }
        self.fallback_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_compression(&self, before: usize, after: usize) {
        if !self.enabled { return; }
        self.compression_before.fetch_add(before as u64, Ordering::Relaxed);
        self.compression_after.fetch_add(after as u64, Ordering::Relaxed);
    }

    /// 获取快照
    pub fn snapshot(&self) -> serde_json::Value {
        let total_req = self.total_requests.load(Ordering::Relaxed);
        let ch = self.cache_hits.load(Ordering::Relaxed);
        let cm = self.cache_misses.load(Ordering::Relaxed);
        let uptime = self.started_at.elapsed().as_secs();

        let mut models = serde_json::Map::new();
        for entry in self.by_model.iter() {
            let req = entry.requests.load(Ordering::Relaxed);
            let lat = entry.latency_total_ms.load(Ordering::Relaxed);
            models.insert(entry.key().clone(), serde_json::json!({
                "requests": req,
                "tokens_input": entry.tokens_input.load(Ordering::Relaxed),
                "tokens_output": entry.tokens_output.load(Ordering::Relaxed),
                "cost_cents": entry.cost_cents.load(Ordering::Relaxed),
                "errors": entry.errors.load(Ordering::Relaxed),
                "avg_latency_ms": if req > 0 { lat / req } else { 0 },
            }));
        }

        let cache_rate = if ch + cm > 0 {
            (ch as f64 / (ch + cm) as f64 * 10000.0).round() / 100.0
        } else { 0.0 };

        let before = self.compression_before.load(Ordering::Relaxed);
        let after = self.compression_after.load(Ordering::Relaxed);
        let comp_ratio = if before > 0 {
            (after as f64 / before as f64 * 10000.0).round() / 100.0
        } else { 100.0 };

        serde_json::json!({
            "uptime_seconds": uptime,
            "total_requests": total_req,
            "total_tokens_input": self.total_tokens_input.load(Ordering::Relaxed),
            "total_tokens_output": self.total_tokens_output.load(Ordering::Relaxed),
            "total_cost_cents": self.total_cost_cents.load(Ordering::Relaxed),
            "error_count": self.error_count.load(Ordering::Relaxed),
            "cache_hits": ch,
            "cache_misses": cm,
            "cache_hit_rate": cache_rate,
            "tokens_saved": self.tokens_saved.load(Ordering::Relaxed),
            "fallback_count": self.fallback_count.load(Ordering::Relaxed),
            "compression_ratio": comp_ratio,
            "by_model": serde_json::Value::Object(models),
        })
    }
}