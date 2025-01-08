pub mod blockchain;
pub mod utils;

use anyhow::{Context, Result};
use log::{error, info};
use serde::Deserialize;
use std::{
    collections::HashMap,
    env,
    fs,
};

use crate::blockchain::adapters::{BlockchainAdapter, EVMAdapter};

#[derive(Debug, Deserialize)]
pub struct BlockchainConfig {
    pub schemas: Vec<String>,
    pub start_block: u64,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub blockchains: HashMap<String, BlockchainConfig>,
}

// Load configuration from the specified path
pub fn load_config(path: &str) -> Result<Config> {
    let content = fs::read_to_string(path).context("Failed to read config file")?;
    let config = toml::from_str(&content).context("Failed to parse config file")?;
    Ok(config)
}

// Ingest blockchain data based on the configuration
pub async fn get_blockchain_data(config: &Config) -> Result<()> {
    for (chain_name, chain_config) in &config.blockchains {
        match chain_name.as_str() {
            "ETH" => {
                let rpc_url = env::var("ETHEREUM_URL").context("ETHEREUM_URL not set")?;
                let adapter = EVMAdapter::new(&rpc_url);
                ingest_with_adapter(chain_name, &adapter, &chain_config.schemas, chain_config.start_block).await?;
            }
            "ARB" => {
                let rpc_url = env::var("ARBITRUM_URL").context("ARBITRUM_URL not set")?;
                let adapter = EVMAdapter::new(&rpc_url);
                ingest_with_adapter(chain_name, &adapter, &chain_config.schemas, chain_config.start_block).await?;
            }
            _ => {
                error!("Unknown chain: {}. Skipping.", chain_name);
            }
        }
    }
    Ok(())
}

// Ingest data for a specific blockchain using the provided adapter
async fn ingest_with_adapter(
    chain_name: &str,
    adapter: &impl BlockchainAdapter,
    schemas: &[String],
    start_block: u64,
) -> Result<()> {
    info!("Starting ingestion for chain: {}", chain_name);
    let block = adapter.get_block_by_number(start_block).await.map_err(|e| {
        error!("Error getting block by number: {:?}", e);
        anyhow::Error::msg(e.to_string())
    }).context("Failed to get block by number")?;
    
    // Extract the block hash and transactions
    let block_hash = block.hash;
    let transactions = block.transactions.iter().map(|tx| tx.hash.to_string()).collect::<Vec<_>>();
    
    // Print the block hash and transactions
    info!("Fetched block #{} from {}: hash: {:?}, transactions: {:?}", start_block, chain_name, block_hash, transactions);

    info!("Requested schemas for {}: {:?}", chain_name, schemas);

    // Fetch native transactions and their receipts
    let transactions_with_receipts = adapter.get_native_transactions(start_block).await.map_err(|e| {
        error!("Error getting native transactions: {:?}", e);
        anyhow::Error::msg(e.to_string())
    }).context("Failed to get native transactions")?;
    for (tx_hash, receipt) in transactions_with_receipts {
        info!("Transaction blockNumber: {:?}, Transaction hash: {}", receipt.block_number, tx_hash);
    }

    info!("Finished ingestion for chain: {}", chain_name);
    Ok(())
}
