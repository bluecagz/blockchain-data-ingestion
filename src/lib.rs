pub mod blockchain;
pub mod streams;
pub mod utils;


use tokio::task;
use dotenv::dotenv;
use anyhow::{Context, Result};
use std::{env, collections::HashMap};
use futures_util::future;
use serde::Deserialize;
use log::{error};

// Re-export adapters, etc. so outside code can use them directly if needed
pub use blockchain::evm_adapter::EVMAdapter;
pub use blockchain::adapters::BlockchainAdapter;
pub use streams::producer::{produce_latest_block, produce_historical, produce_realtime};
pub use streams::consumer::consume_and_store;

/// Configuration for each chain in `blockchains.toml`
#[derive(Debug, Deserialize)]
pub struct BlockchainConfig {
    pub adapter_type: String,
    pub schemas: Vec<String>,
    pub start_block: Option<u64>,
    pub end_block: Option<u64>,
    pub http_url: String,
    pub ws_url: String,
}

#[derive(Debug, Deserialize)]
pub struct ConfigToml {
    pub blockchains: HashMap<String, BlockchainConfig>,
}

/// This function does the main “orchestration”:
/// 1) Reads `blockchains.toml`
/// 2) For each chain, spawns producer tasks for historical + real-time
/// 3) Optionally spawns a consumer
/// 4) Waits until tasks complete (or runs forever, if you like)
pub async fn run_ingestion() -> Result<()> {
    // Load environment variables from .env file
    dotenv().ok();

    // 1) Load `blockchains.toml`
    let config_str = std::fs::read_to_string("blockchains.toml")?;
    let mut config: ConfigToml = toml::from_str(&config_str)?;

    // 2) Replace placeholders with actual environment variable values
    //    i.e. if http_url = "HTTP_URL_MAINNET" then we do env::var("HTTP_URL_MAINNET")
    for (_, chain_cfg) in config.blockchains.iter_mut() {
        chain_cfg.http_url = env::var(&chain_cfg.http_url)
            .with_context(|| format!("Failed to get HTTP URL from environment for key `{}`", &chain_cfg.http_url))?;
        chain_cfg.ws_url = env::var(&chain_cfg.ws_url)
            .with_context(|| format!("Failed to get WebSocket URL from environment for key `{}`", &chain_cfg.ws_url))?;
    }

    // 3) Prepare a list of async tasks we’ll await later
    let mut tasks = Vec::new();

    for (chain_name, chain_cfg) in config.blockchains {
        match chain_cfg.adapter_type.as_str() {
            "EVM" => {
                let adapter = EVMAdapter::new(
                    &chain_name,
                    &chain_cfg.http_url,
                    &chain_cfg.ws_url
                ).await?;

                // +++ CALL produce_latest_block IMMEDIATELY +++
                if let Err(e) = produce_latest_block(&adapter).await {
                    error!("Error in produce_latest_block for {}: {}", chain_name, e);
                }

                // If we have a start_block, spawn historical ingestion
                if let Some(start_block) = chain_cfg.start_block {
                    let end_block = chain_cfg.end_block.unwrap_or(u64::MAX);
                    let adapter_for_hist = adapter.clone();
                    let chain_name_hist = chain_name.clone();
                    tasks.push(task::spawn(async move {
                        if let Err(e) = produce_historical(&adapter_for_hist, start_block, end_block).await {
                            error!("Error in produce_historical for {}: {}", chain_name_hist, e);
                        }
                    }));
                }

                // Also spawn real-time ingestion
                let adapter_for_rt = adapter.clone();
                let chain_name_rt = chain_name.clone();
                tasks.push(task::spawn(async move {
                    if let Err(e) = produce_realtime(adapter_for_rt).await {
                        error!("Error in produce_realtime for {}: {}", chain_name_rt, e);
                    }
                }));
            }

            _ => {
                error!("Unknown adapter_type `{}` for chain `{}`. Skipping.", chain_cfg.adapter_type, chain_name);
                continue;
            }
        }
    }


    // 4) (Optional) spawn a consumer in the same process
    tasks.push(task::spawn(async move {
        if let Err(e) = consume_and_store().await {
            error!("Consumer error: {}", e);
        }
    }));

    // 5) Wait for all tasks to complete
    //    If you want them to run indefinitely, you can do an infinite loop or keep using select_all.
    let (res, _idx, _remaining) = future::select_all(tasks).await;
    if let Err(e) = res {
        eprintln!("A task returned an error: {}", e);
    }

    Ok(())
}
