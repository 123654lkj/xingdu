use axum::{
    Router,
    routing::get,
    Json, extract::State,
    response::Html,
};
use std::sync::Arc;

use crate::metrics::MetricsCollector;

/// 面板应用状态
pub struct DashboardState {
    pub metrics: Arc<MetricsCollector>,
}

/// 创建 Dashboard 路由
pub fn create_dashboard_router(state: Arc<DashboardState>) -> Router {
    Router::new()
        .route("/dashboard", get(dashboard_redirect))
        .route("/dashboard/", get(dashboard_page))
        .route("/dashboard/api/stats", get(dashboard_stats))
        .with_state(state)
}

async fn dashboard_redirect() -> axum::response::Redirect {
    axum::response::Redirect::permanent("/dashboard/")
}

async fn dashboard_page() -> Html<&'static str> {
    Html(DASHBOARD_HTML)
}

async fn dashboard_stats(
    State(state): State<Arc<DashboardState>>,
) -> Json<serde_json::Value> {
    Json(state.metrics.snapshot())
}

/// 内嵌仪表盘 HTML（Tailwind CSS + Chart.js CDN）
const DASHBOARD_HTML: &str = r#"<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>星渡 - 管理面板</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-gray-900 text-gray-100">
<div class="container mx-auto p-6">
  <h1 class="text-3xl font-bold mb-6">⭐ 星渡管理面板</h1>

  <!-- 概览卡片 -->
  <div id="overview" class="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
    <div class="bg-gray-800 rounded-lg p-4"><p class="text-gray-400 text-sm">总请求</p><p id="total-requests" class="text-2xl font-bold">-</p></div>
    <div class="bg-gray-800 rounded-lg p-4"><p class="text-gray-400 text-sm">错误数</p><p id="error-count" class="text-2xl font-bold text-red-400">-</p></div>
    <div class="bg-gray-800 rounded-lg p-4"><p class="text-gray-400 text-sm">总 Token (输入)</p><p id="tokens-in" class="text-2xl font-bold">-</p></div>
    <div class="bg-gray-800 rounded-lg p-4"><p class="text-gray-400 text-sm">总 Token (输出)</p><p id="tokens-out" class="text-2xl font-bold">-</p></div>
    <div class="bg-gray-800 rounded-lg p-4"><p class="text-gray-400 text-sm">总成本</p><p id="total-cost" class="text-2xl font-bold text-green-400">-</p></div>
    <div class="bg-gray-800 rounded-lg p-4"><p class="text-gray-400 text-sm">缓存命中率</p><p id="cache-rate" class="text-2xl font-bold text-blue-400">-</p></div>
    <div class="bg-gray-800 rounded-lg p-4"><p class="text-gray-400 text-sm">压缩率</p><p id="compress-ratio" class="text-2xl font-bold text-purple-400">-</p></div>
    <div class="bg-gray-800 rounded-lg p-4"><p class="text-gray-400 text-sm">运行时间</p><p id="uptime" class="text-2xl font-bold">-</p></div>
  </div>

  <!-- 模型统计 -->
  <div class="bg-gray-800 rounded-lg p-4 mb-6">
    <h2 class="text-xl font-semibold mb-4">按模型统计</h2>
    <div class="overflow-x-auto">
      <table id="model-table" class="w-full text-sm">
        <thead><tr class="text-gray-400 border-b border-gray-700"><th class="text-left py-2">模型</th><th class="text-right py-2">请求数</th><th class="text-right py-2">Token 输入</th><th class="text-right py-2">Token 输出</th><th class="text-right py-2">成本(分)</th><th class="text-right py-2">平均延迟(ms)</th><th class="text-right py-2">错误</th></tr></thead>
        <tbody id="model-tbody"></tbody>
      </table>
    </div>
  </div>
</div>

<script>
async function refresh() {
  const r = await fetch('/dashboard/api/stats');
  const d = await r.json();

  document.getElementById('total-requests').textContent = d.total_requests.toLocaleString();
  document.getElementById('error-count').textContent = d.error_count.toLocaleString();
  document.getElementById('tokens-in').textContent = d.total_tokens_input.toLocaleString();
  document.getElementById('tokens-out').textContent = d.total_tokens_output.toLocaleString();
  document.getElementById('total-cost').textContent = '¥' + (d.total_cost_cents / 100).toFixed(2);
  document.getElementById('cache-rate').textContent = d.cache_hit_rate.toFixed(1) + '%';
  document.getElementById('compress-ratio').textContent = d.compression_ratio.toFixed(1) + '%';

  const uptime = d.uptime_seconds;
  const h = Math.floor(uptime / 3600);
  const m = Math.floor((uptime % 3600) / 60);
  const s = uptime % 60;
  document.getElementById('uptime').textContent = h + 'h ' + m + 'm ' + s + 's';

  const tbody = document.getElementById('model-tbody');
  tbody.innerHTML = '';
  for (const [model, stats] of Object.entries(d.by_model)) {
    const tr = document.createElement('tr');
    tr.className = 'border-b border-gray-700';
    tr.innerHTML = '<td class="py-2">' + model + '</td>'
      + '<td class="text-right py-2">' + stats.requests.toLocaleString() + '</td>'
      + '<td class="text-right py-2">' + stats.tokens_input.toLocaleString() + '</td>'
      + '<td class="text-right py-2">' + stats.tokens_output.toLocaleString() + '</td>'
      + '<td class="text-right py-2">¥' + (stats.cost_cents / 100).toFixed(2) + '</td>'
      + '<td class="text-right py-2">' + stats.avg_latency_ms + '</td>'
      + '<td class="text-right py-2 text-red-400">' + stats.errors + '</td>';
    tbody.appendChild(tr);
  }
}
refresh();
setInterval(refresh, 3000);
</script>
</body>
</html>"#;