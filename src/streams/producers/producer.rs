use anyhow::Result;
use async_trait::async_trait;

/// A trait that defines methods to produce stream messages.
#[async_trait]
pub trait StreamProducer {
    /// Produce real-time messages.
    async fn produce_realtime(&self) -> Result<()>;

    /// Produce historical messages for a given block range.
    async fn produce_historical(&self, start_block: u64, end_block: u64) -> Result<()>;
}
