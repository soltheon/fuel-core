use crate::global_registry;
use prometheus_client::metrics::gauge::Gauge;
use std::sync::{
    atomic::{
        AtomicU32,
        AtomicU64,
    },
    OnceLock,
};

#[derive(Debug)]
pub struct CompressionMetrics {
    pub compression_ratio: Gauge<f64, AtomicU64>,
    pub compression_duration_ms: Gauge<u32, AtomicU32>,
    pub compression_block_height: Gauge<u32, AtomicU32>,
}

impl Default for CompressionMetrics {
    fn default() -> Self {
        let compression_ratio = Gauge::default();
        let compression_duration_ms = Gauge::default();
        let compression_block_height = Gauge::default();

        let metrics = CompressionMetrics {
            compression_ratio,
            compression_duration_ms,
            compression_block_height,
        };

        let mut registry = global_registry().registry.lock();
        registry.register(
            "compression_ratio",
            "Compression ratio",
            metrics.compression_ratio.clone(),
        );
        registry.register(
            "compression_duration_ms",
            "Compression duration in milliseconds",
            metrics.compression_duration_ms.clone(),
        );
        registry.register(
            "compression_block_height",
            "Compression block height",
            metrics.compression_block_height.clone(),
        );

        metrics
    }
}

static COMPRESSION_METRICS: OnceLock<CompressionMetrics> = OnceLock::new();

pub fn compression_metrics() -> &'static CompressionMetrics {
    COMPRESSION_METRICS.get_or_init(CompressionMetrics::default)
}
