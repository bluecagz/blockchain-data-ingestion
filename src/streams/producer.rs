use crate::blockchain::adapters::BlockchainAdapter;
use log::{info, error};
use serde::{Serialize, Deserialize};
use futures_util::StreamExt;
use anyhow::Result;
use crate::utils::pulsar_utils::BlockProducer;
use pulsar::Producer;
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize)]
pub struct BlockMessage {
    pub chain_name: String,
    pub block_number: u64,
    pub hash: String,
    pub tx_count: usize,
}

/// Produce historical blocks and send to Pulsar
pub async fn produce_historical<A: BlockchainAdapter>(
    adapter: &A,
    start_block: u64,
    end_block: u64,
    pulsar_producer: Arc<Producer<String, TokioExecutor>>, // Take Arc<Producer> by value
) -> Result<(), anyhow::Error> {
    for block_number in start_block..=end_block {
        match adapter.get_block_by_number(block_number).await? {
            Some(block) => {
                let block_number = block.number.map_or(0, |n| n.as_u64()); // Handle Option<U64>
                let block_message = BlockMessage {
                    chain_name: adapter.chain_name().to_string(),
                    block_number,
                    hash: format!("{:#x}", block.hash.unwrap_or_default()),
                    tx_count: block.transactions.len(),
                };

                // Serialize to JSON string
                let msg = serde_json::to_string(&block_message)?;

                // Send the message
                pulsar_producer.send(msg).await.map_err(|e| {
                    error!("Failed to send message to Pulsar: {}", e);
                    anyhow::anyhow!(e)
                })?;

                info!("Sent block {} to Pulsar", block_number);
            }
            None => {
                info!(
                    "No block found at #{} on {}",
                    block_number,
                    adapter.chain_name(),
                );
            }
        }
    }
    Ok(())
}

/// Produce real-time blocks and send to Pulsar
pub async fn produce_realtime<A: BlockchainAdapter>(
    adapter: A,
    pulsar_producer: Arc<Producer<String, TokioExecutor>>, // Take Arc<Producer> by value
) -> Result<(), anyhow::Error> {
    let mut stream = adapter.subscribe_new_blocks();
    while let Some(block_result) = stream.next().await {
        match block_result {
            Ok(block) => {
                let block_number = block.number.map_or(0, |n| n.as_u64()); // Handle Option<U64>
                let block_message = BlockMessage {
                    chain_name: adapter.chain_name().to_string(),
                    block_number,
                    hash: format!("{:#x}", block.hash.unwrap_or_default()),
                    tx_count: block.transactions.len(),
                };

                // Serialize to JSON string
                let msg = serde_json::to_string(&block_message)?;

                // Send the message
                pulsar_producer.send(msg).await.map_err(|e| {
                    error!("Failed to send message to Pulsar: {}", e);
                    anyhow::anyhow!(e)
                })?;

                info!("Sent block {} to Pulsar", block_number);
            }
            Err(e) => {
                error!("Failed to get new block: {}", e);
            }
        }
    }
    Ok(())
}
