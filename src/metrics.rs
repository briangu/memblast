use once_cell::sync::{Lazy, OnceCell};
use prometheus::{Encoder, TextEncoder};
use std::net::SocketAddr;
use tokio_metrics_collector::{default_task_collector, RuntimeCollector, TaskCollector, TaskMonitor};
use tokio_metrics::RuntimeMonitor;
use axum::{Router, routing::get};

use crate::RUNTIME;

static TASK_COLLECTOR: Lazy<&'static TaskCollector> = Lazy::new(|| {
    default_task_collector()
});

static INIT: OnceCell<()> = OnceCell::new();

pub fn init(addr: SocketAddr) {
    INIT.get_or_init(|| {
        let handle = RUNTIME.handle().clone();
        RUNTIME.spawn(async move {
            let monitor = RuntimeMonitor::new(&handle);
            let rt_collector = RuntimeCollector::new(monitor, "");
            prometheus::default_registry()
                .register(Box::new(rt_collector))
                .unwrap();
            prometheus::default_registry()
                .register(Box::new(*TASK_COLLECTOR))
                .unwrap();
            let app = Router::new().route("/metrics", get(metrics));
            let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
            axum::serve(listener, app).await.unwrap();
        });
    });
}

pub fn monitor(name: &str) -> TaskMonitor {
    let monitor = TaskMonitor::new();
    TASK_COLLECTOR.add(name, monitor.clone()).unwrap_or(());
    monitor
}

async fn metrics() -> Result<String, String> {
    let encoder = TextEncoder::new();
    let mut buf = Vec::new();
    encoder
        .encode(&prometheus::default_registry().gather(), &mut buf)
        .map_err(|e| format!("encode metrics failed: {e}"))?;
    String::from_utf8(buf).map_err(|e| format!("utf8 error: {e}"))
}

