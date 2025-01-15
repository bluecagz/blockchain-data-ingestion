pub mod streams;
pub mod blockchain;
pub mod storage;

use anyhow::Context;
use tokio::runtime::Builder;
use tokio::task;
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::Mutex;
use log::error;
use futures_util::future;
use std::env;
use dotenv::dotenv;
use std::collections::HashMap;
use serde::Deserialize;
use sqlx::PgPool;

use crate::streams::message_queue::pulsar::PulsarClient;
use crate::blockchain::evm_adapter::EVMAdapter;

use crate::streams::producers::evm_producer::EVMProducer;
use crate::streams::consumers::evm_consumer::EVMConsumer;
use crate::streams::producers::producer::StreamProducer;
use crate::streams::consumers::consumer::StreamConsumer;

#[derive(Debug, Deserialize)]
pub struct BlockchainConfig {
    pub adapter_type: String,
    pub schemas: Vec<String>,
    pub http_url: String,
    pub ws_url: String,
    pub start_block: Option<u64>,
    pub end_block: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct ConfigToml {
    pub blockchains: HashMap<String, BlockchainConfig>,
}

pub async fn run_ingestion(pool: &sqlx::PgPool) -> Result<()> {
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
    let pulsar = Arc::new(Mutex::new(PulsarClient::new(&pulsar_url).await
        .context("Failed to initialize Pulsar client")?));

    let producer_topic_prefix = "persistent://public/default/".to_string();

    // 4) Prepare tasks for producing messages.
    let mut tasks = Vec::new();
    let mut consumers_vec = Vec::new();

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

                    // Add the producer_topic to the consumers_vec.
                    consumers_vec.push((&chain_name, producer_topic.clone()));

                    // Clone the adapter for different tasks.
                    let adapter_clone_rt = Arc::new(Mutex::new(adapter.clone()));

                    // Historical ingestion task (if a start_block is provided).
                    if let Some(start_block) = chain_cfg.start_block {
                        let producer_topic_hist = producer_topic.clone() + "-historical";
                        consumers_vec.push((&chain_name, producer_topic_hist.clone()));

                        let adapter_clone_hist = Arc::new(Mutex::new(adapter.clone()));
                        let pulsar_clone_hist = Arc::clone(&pulsar);

                        let end_block = chain_cfg.end_block.unwrap_or(u64::MAX);
                        tasks.push(task::spawn_blocking(move || {
                            let rt = Builder::new_multi_thread().enable_all().build().unwrap();
                            rt.block_on(async move {
                                // Create an EVMProducer for historical production.
                                let evm_producer = EVMProducer::new(adapter_clone_hist, pulsar_clone_hist, producer_topic_hist).await?;
                                evm_producer.produce_historical(start_block, end_block).await?;
                                Ok::<(), anyhow::Error>(())
                            })
                        }));
                    }

                    // Real-time ingestion task.
                    let pulsar_clone_rt = Arc::clone(&pulsar);
                    tasks.push(task::spawn_blocking(move || {
                        let rt = Builder::new_multi_thread().enable_all().build().unwrap();
                        rt.block_on(async move {
                            // Create an EVMProducer for real-time production.
                            let evm_producer = EVMProducer::new(adapter_clone_rt, pulsar_clone_rt, producer_topic).await?;
                            evm_producer.produce_realtime().await?;
                            Ok::<(), anyhow::Error>(())
                        })
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
    // create a subscription from each topic in the consumers_vec
    // by concating the topic with "-subscription"
    let consumer_subscription_vec = consumers_vec
                                    .iter()
                                    .map(|consumer| (consumer.0.clone(), consumer.1.clone(), consumer.1.clone() + "-subscription"))
                                    .collect::<Vec<(String, String, String)>>();

    let pg_pool_arc = Arc::new(Mutex::new(pool));

    for (chain_name, consumer_topic, consumer_subscription) in consumer_subscription_vec {
        let pulsar_clone_consumer = Arc::clone(&pulsar);

        let pg_pool_clone: Arc<Mutex<&PgPool>> = Arc::clone(&pg_pool_arc);
        tasks.push(task::spawn_blocking(move || -> Result<()> {
            let rt = Builder::new_multi_thread().enable_all().build().unwrap();
            rt.block_on(async move {
                let mut evm_consumer = EVMConsumer::new(
                    pulsar_clone_consumer,
                    consumer_topic.clone(),
                    consumer_subscription.clone(),
                    pg_pool_clone,
                ).await;

                if let Err(e) = evm_consumer.postgres_consume(pg_pool_clone, &chain_name).await {
                    error!("Consumer error: {}", e);
                }
            });
            Ok(())
        }));
    }

    // 6) Wait for all tasks to complete.
    // Since producer and consumer tasks run indefinitely, join_all will keep the process alive.
    future::join_all(tasks).await;

    Ok(())
}