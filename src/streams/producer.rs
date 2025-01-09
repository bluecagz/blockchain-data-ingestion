use crate::blockchain::adapters::BlockchainAdapter;
use futures_util::StreamExt;
use log::{info, error};

/// Example: produce historical blocks from [start_block..=end_block].
pub async fn produce_historical<A: BlockchainAdapter>(
    adapter: &A,
    start_block: u64,
    end_block: u64,
) -> anyhow::Result<()> {
    for block_num in start_block..=end_block {
        match adapter.get_block_by_number(block_num).await {
            Ok(Some(block)) => {
                let hash = block.hash.unwrap_or_default();
                let tx_count = block.transactions.len();
                info!(
                    "Produced block #{} on {} => hash={:#x}, #tx={}",
                    block_num,
                    adapter.chain_name(),
                    hash,
                    tx_count
                );
                // TODO: send to MQ or store in DB
            }
            Ok(None) => {
                info!(
                    "No block found at #{} on {}",
                    block_num,
                    adapter.chain_name(),
                );
            }
            Err(e) => {
                error!("Error fetching block #{} on {}: {}", block_num, adapter.chain_name(), e);
            }
        }
    }
    Ok(())
}

/// Example: produce new blocks in real-time (just the block hashes).
pub async fn produce_realtime<A: BlockchainAdapter>(adapter: A) -> anyhow::Result<()> {
    let mut block_hash_stream = adapter.subscribe_new_blocks();
    info!("Starting real-time subscription for {}...", adapter.chain_name());

    while let Some(next_hash_res) = block_hash_stream.next().await {
        match next_hash_res {
            Ok(block_hash) => {
                info!("New block on {} => {:?}", adapter.chain_name(), block_hash);
                // In future, you could fetch the full block with get_block_by_number(...)
                // to get transactions, then send to MQ
            }
            Err(e) => {
                error!("Subscription error on {}: {}", adapter.chain_name(), e);
            }
        }
    }
    Ok(())
}

/// Fetch the latest block number and print its transaction hashes
pub async fn produce_latest_block<A: BlockchainAdapter>(adapter: &A) -> anyhow::Result<()> {
    let latest_num = adapter.get_latest_block_number().await?;
    info!("Latest block number on {} is {}", adapter.chain_name(), latest_num);

    match adapter.get_block_by_number(latest_num).await {
        Ok(Some(block)) => {
            let block_hash = block.hash.unwrap_or_default();
            info!(
                "Latest block #{} => hash={:#x}, #tx={}",
                latest_num,
                block_hash,
                block.transactions.len()
            );
            for tx in block.transactions {
                info!("Tx hash: {:?}", tx.hash);
            }
        }
        Ok(None) => {
            info!(
                "No block returned for the latest block number on {}",
                adapter.chain_name()
            );
        }
        Err(e) => {
            error!(
                "Error fetching the latest block on {}: {}",
                adapter.chain_name(),
                e
            );
        }
    }

    Ok(())
}
