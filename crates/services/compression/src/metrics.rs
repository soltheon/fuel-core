use fuel_core_metrics::compression_metrics::compression_metrics;

#[derive(Clone, Copy)]
pub(crate) struct CompressionMetricsManager;

impl CompressionMetricsManager {
    pub(crate) fn new() -> Self {
        CompressionMetricsManager
    }

    pub(crate) fn record_compression_ratio(
        &self,
        uncompressed_size: usize,
        compressed_size: usize,
    ) {
        compression_metrics()
            .compression_ratio
            .set(compressed_size as f64 / uncompressed_size as f64);
    }

    pub(crate) fn record_compression_duration_ms(&self, duration_ms: u128) {
        // this cast is safe. we would never record a compression longer than 49 days
        // lets assume that we would be able to detect this and fix it before it happens :)
        compression_metrics()
            .compression_duration_ms
            .set(u32::try_from(duration_ms).unwrap_or(u32::MAX));
    }

    pub(crate) fn record_compression_block_height(&self, height: u32) {
        compression_metrics().compression_block_height.set(height);
    }
}
