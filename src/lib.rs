pub mod blockchain;
pub mod storage;
pub mod streams;

use crate::blockchain::evm_adapter::EVMAdapter;
use std::{env, collections::HashMap};
use tokio::task;
use dotenv::dotenv;
use anyhow::{Context, Result};
use futures_util::future;
use log::error;
use std::sync::{Arc, Mutex};

// Import the new Pulsar utilities.
use crate::streams::message_queue::pulsar::{create_producer, create_consumer, PulsarClient};
use crate::streams::consumers::evm_consumer::EVMConsumer;
// Import the producer type.
use crate::streams::producers::evm_producer::EVMProducer;
use crate::streams::producers::producer::StreamProducer; // Ensure this is imported

/// Configuration for each chain in `blockchains.toml`
#[derive(Debug, serde::Deserialize)]
pub struct BlockchainConfig {
    pub adapter_type: String, // e.g. "EVM"
    pub schemas: Vec<String>,
    pub start_block: Option<u64>,
    pub end_block: Option<u64>,
    pub http_url: String, // Will be looked up in env for the actual value
    pub ws_url: String,   // Will be looked up in env for the actual value
}

/// The top-level configuration structure.
#[derive(Debug, serde::Deserialize)]
pub struct ConfigToml {
    pub blockchains: HashMap<String, BlockchainConfig>,
}

/// The main ingestion orchestration function.
/// 
/// This function does the following:
/// 1) Reads and parses `blockchains.toml`
/// 2) Replaces URL placeholders with actual environment values
/// 3) Initializes the Pulsar client and creates a producer using the new message_queue module
/// 4) For each blockchain defined in the configuration:
///      - Creates an appropriate blockchain adapter (e.g. EVMAdapter)
///      - Spawns tasks for historical and real-time ingestion using the new EVMProducer
/// 5) Spawns a consumer task (using the new EVMConsumer that relies on a generic MessageConsumer)
/// 6) Waits for all tasks to complete (or runs indefinitely)
pub async fn run_ingestion() -> Result<()> {
    // Load environment variables from the .env file.
    dotenv().ok();

    // 1) Load the configuration from `blockchains.toml`.
    let config_str = std::fs::read_to_string("blockchains.toml")
        .context("Failed to read blockchains.toml")?;
    let mut config: ConfigToml = toml::from_str(&config_str)
        .context("Failed to parse blockchains.toml")?;

    // 2) Substitute placeholders with actual values from environment variables.
    for (_, chain_cfg) in config.blockchains.iter_mut() {
        chain_cfg.http_url = env::var(&chain_cfg.http_url)
            .with_context(|| format!("Failed to get HTTP URL from environment for key `{}`", &chain_cfg.http_url))?;
        chain_cfg.ws_url = env::var(&chain_cfg.ws_url)
            .with_context(|| format!("Failed to get WebSocket URL from environment for key `{}`", &chain_cfg.ws_url))?;
    }

    // 3) Initialize Pulsar client and create a producer.
    let pulsar_url = env::var("PULSAR_URL").unwrap_or_else(|_| "pulsar://127.0.0.1:6650".to_string());
    let pulsar = PulsarClient::new(&pulsar_url).await
        .context("Failed to initialize Pulsar client")?;

    let producer_topic_prefix = "persistent://public/default/".to_string();

    // 4) Prepare tasks for producing messages.
    let mut tasks = Vec::new();

    // For each blockchain in the configuration.
    for (chain_name, chain_cfg) in config.blockchains {
        match chain_cfg.adapter_type.as_str() {
            "EVM" => {
                // Create an EVM-based adapter.
                let adapter = EVMAdapter::new(
                    &chain_name,
                    &chain_cfg.http_url,
                    &chain_cfg.ws_url,
                )
                .await
                .context(format!("Failed to create EVMAdapter for {}", chain_name))?;

                // For each schema in the chain_cfg.schemas create a producer for each schema.
                for schema in chain_cfg.schemas {
                    // Create a producer for each schema.
                    let producer_topic = format!("{}{}-{}", &producer_topic_prefix, &chain_name, &schema);
                    let producer = create_producer(&pulsar, &producer_topic).await
                        .context(format!("Failed to create Pulsar producer for schema `{}`", schema))?;

                    // Clone the adapter for different tasks.
                    let adapter_clone_rt = Arc::new(Mutex::new(adapter.clone()));

                    // Clone the Pulsar producer.
                    let producer_wrap = Arc::new(producer);
                    let producer_clone_rt = Arc::clone(&producer_wrap);

                    let producer_topic_hist = producer_topic.clone() + "-historical";

                    // Historical ingestion task (if a start_block is provided).
                    if let Some(start_block) = chain_cfg.start_block {
                        let producer_hist = create_producer(&pulsar, &producer_topic_hist).await
                            .context(format!("Failed to create Pulsar producer for schema `{}`", schema))?;
                        
                        let adapter_clone_hist = Arc::new(Mutex::new(adapter.clone()));

                        let end_block = chain_cfg.end_block.unwrap_or(u64::MAX);
                        let chain_name_hist = chain_name.clone();
                        tasks.push(task::spawn(async move {
                            // Create an EVMProducer for historical production.
                            let evm_producer = EVMProducer::new(adapter_clone_hist, producer_hist).await?;
                            evm_producer.produce_historical(start_block, end_block).await?;
                            Ok::<(), anyhow::Error>(())
                        }));
                    }

                    // Real-time ingestion task.
                    let chain_name_rt = chain_name.clone();
                    tasks.push(task::spawn(async move {
                        // Create an EVMProducer for real-time production.
                        let evm_producer = EVMProducer::new(adapter_clone_rt, Arc::try_unwrap(producer_clone_rt).unwrap()).await?;
                        evm_producer.produce_realtime().await?;
                        Ok::<(), anyhow::Error>(())
                    }));
                }
            }
            // Handle other adapter types if needed.
            _ => {
                error!("Unknown adapter_type `{}` for chain `{}`. Skipping.", chain_cfg.adapter_type, chain_name);
                continue;
            }
        }
    }

    // 5) Spawn a consumer task.
    let consumer_subscription = env::var("PULSAR_CONSUMER_SUBSCRIPTION")
        .unwrap_or_else(|_| "my-subscription".to_string());
    let consumer_topic = producer_topic_prefix.clone(); // Reusing the same topic.
    let pg_connection_str = env::var("POSTGRES_CONNECTION")
        .unwrap_or_else(|_| "host=localhost user=postgres password=secret dbname=mydb".to_string());

    let mut evm_consumer = EVMConsumer::new(
        pulsar.clone(),
        consumer_topic,
        consumer_subscription,
        pg_connection_str,
    );
    tasks.push(task::spawn(async move {
        if let Err(e) = evm_consumer.postgres_consume().await {
            error!("Consumer error: {}", e);
        }
    }));

    // 6) Wait for all tasks to complete.
    // Since producer and consumer tasks run indefinitely, join_all will keep the process alive.
    future::join_all(tasks).await;

    Ok(())
}