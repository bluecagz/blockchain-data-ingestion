pub mod blockchain;
pub mod streams;
pub mod utils;

use std::{env, collections::HashMap};
use tokio::task;
use dotenv::dotenv;
use anyhow::{Context, Result};
use serde::Deserialize;
use futures_util::future;
use log::error;
use crate::utils::pulsar_utils::{initialize_pulsar_client, create_block_producer};
use crate::streams::consumer::consume_and_store;
use crate::streams::producer::{produce_historical, produce_realtime};
use blockchain::evm_adapter::EVMAdapter;
use std::sync::Arc; // Import Arc

/// Configuration for each chain in `blockchains.toml`
#[derive(Debug, Deserialize)]
pub struct BlockchainConfig {
    pub adapter_type: String,          // e.g. "EVM"
    pub schemas: Vec<String>,
    pub start_block: Option<u64>,
    pub end_block: Option<u64>,
    pub http_url: String,              // Will be looked up in env for actual value
    pub ws_url: String,                // Will be looked up in env for actual value
}

#[derive(Debug, Deserialize)]
pub struct ConfigToml {
    pub blockchains: HashMap<String, BlockchainConfig>,
}

/// This function does the main “orchestration”:
/// 1) Reads `blockchains.toml`
/// 2) For each chain, spawns producer tasks for historical + real-time
/// 3) Spawns a consumer
/// 4) Waits until tasks complete (or runs forever, if you like)
pub async fn run_ingestion() -> Result<()> {
    // Load environment variables from .env file
    dotenv().ok();

    // 1) Load `blockchains.toml`
    let config_str = std::fs::read_to_string("blockchains.toml")
        .context("Failed to read blockchains.toml")?;
    let mut config: ConfigToml = toml::from_str(&config_str)
        .context("Failed to parse blockchains.toml")?;

    // 2) Replace placeholders with actual environment variable values
    for (_, chain_cfg) in config.blockchains.iter_mut() {
        chain_cfg.http_url = env::var(&chain_cfg.http_url)
            .with_context(|| format!("Failed to get HTTP URL from environment for key `{}`", &chain_cfg.http_url))?;
        chain_cfg.ws_url = env::var(&chain_cfg.ws_url)
            .with_context(|| format!("Failed to get WebSocket URL from environment for key `{}`", &chain_cfg.ws_url))?;
    }

    // 3) Initialize Pulsar client and producer
    let pulsar_url = env::var("PULSAR_URL").unwrap_or_else(|_| "pulsar://127.0.0.1:6650".to_string());
    let pulsar = initialize_pulsar_client(&pulsar_url).await
        .context("Failed to initialize Pulsar client")?;

    let producer_topic = env::var("PULSAR_PRODUCER_TOPIC").unwrap_or_else(|_| "persistent://public/default/block-events".to_string());
    let pulsar_producer = create_block_producer(&pulsar, &producer_topic).await
        .context("Failed to create Pulsar producer")?;

    // 4) Prepare a list of async tasks we’ll await later
    let mut tasks = Vec::new();

    // For each chain definition in config
    for (chain_name, chain_cfg) in config.blockchains {
        match chain_cfg.adapter_type.as_str() {
            "EVM" => {
                // Create an EVM-based adapter (ethers) for this chain
                let adapter = EVMAdapter::new(
                    &chain_name,
                    &chain_cfg.http_url,
                    &chain_cfg.ws_url
                ).await
                .context(format!("Failed to create EVMAdapter for {}", chain_name))?;

                // Clone adapter for multiple tasks
                let adapter_clone_hist = adapter.clone();
                let adapter_clone_rt = adapter.clone();

                // Clone the producer (Arc clone)
                let producer_clone_hist = Arc::clone(&pulsar_producer);
                let producer_clone_rt = Arc::clone(&pulsar_producer);

                // Historical ingestion task
                if let Some(start_block) = chain_cfg.start_block {
                    let end_block = chain_cfg.end_block.unwrap_or(u64::MAX);
                    let chain_name_hist = chain_name.clone();
                    tasks.push(task::spawn(async move {
                        if let Err(e) = produce_historical(&adapter_clone_hist, start_block, end_block, producer_clone_hist).await {
                            error!("Error in produce_historical for {}: {}", chain_name_hist, e);
                        }
                    }));
                }

                // Real-time ingestion task
                let chain_name_rt = chain_name.clone();
                tasks.push(task::spawn(async move {
                    if let Err(e) = produce_realtime(adapter_clone_rt, producer_clone_rt).await {
                        error!("Error in produce_realtime for {}: {}", chain_name_rt, e);
                    }
                }));
            },

            // Potentially handle other adapter types here:
            // e.g. "SOLANA", "BITCOIN", etc.
            _ => {
                error!("Unknown adapter_type `{}` for chain `{}`. Skipping.", chain_cfg.adapter_type, chain_name);
                continue;
            }
        }
    }

    // 5) Spawn a consumer task
    let consumer_subscription = env::var("PULSAR_CONSUMER_SUBSCRIPTION").unwrap_or_else(|_| "my-subscription".to_string());
    let consumer_topic = producer_topic.clone(); // Assuming same topic for simplicity
    let pg_connection_str = env::var("POSTGRES_CONNECTION").unwrap_or_else(|_| "host=localhost user=postgres password=secret dbname=mydb".to_string());

    let pulsar_clone_for_consumer = pulsar.clone();
    tasks.push(task::spawn(async move {
        if let Err(e) = consume_and_store(&pulsar_clone_for_consumer, &consumer_topic, &consumer_subscription, &pg_connection_str).await {
            error!("Consumer error: {}", e);
        }
    }));

    // 6) Wait for all tasks to complete
    //    Since producers and consumer run indefinitely, this will keep your service running
    future::join_all(tasks).await;

    Ok(())
}
