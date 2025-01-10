use anyhow::Result;
use log::{info, error};
use serde::{Serialize, Deserialize};
use tokio_postgres::NoTls;
use futures_util::StreamExt;
use crate::utils::pulsar_utils::{create_block_consumer, BlockConsumer, PulsarClient};
use std::str;

#[derive(Debug, Serialize, Deserialize)]
struct BlockMessage {
    chain_name: String,
    block_number: u64,
    hash: String,
    tx_count: usize,
}

pub async fn consume_and_store(
    pulsar: &PulsarClient,
    topic: &str,
    subscription: &str,
    pg_connection_str: &str,
) -> Result<()> {
    // Create Pulsar consumer
    let mut consumer: BlockConsumer = create_block_consumer(pulsar, topic, subscription).await?;

    // Placeholder for PostgreSQL connection
    let (pg_client, connection) = tokio_postgres::connect(pg_connection_str, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("PostgreSQL connection error: {}", e);
        }
    });

    // Consume messages
    while let Some(msg_res) = consumer.next().await {
        match msg_res {
            Ok(msg) => {
                // Log message ID
                info!("Received message ID: {:?}", msg.message_id);
                println!("Received message ID: {:?}", msg.message_id);

                // Payload is already a String
                let payload_str = msg.payload.as_str();

                // Deserialize message payload
                let block_message: BlockMessage = serde_json::from_str(payload_str).map_err(|e| {
                    error!("Failed to deserialize message: {}", e);
                    anyhow::anyhow!(e)
                })?;

                // Insert data into PostgreSQL
                let block_number_i64 = block_message.block_number as i64;
                let tx_count_i64 = block_message.tx_count as i64;

                pg_client.execute(
                    "INSERT INTO my_table (chain_name, block_number, hash, tx_count) VALUES ($1, $2, $3, $4)",
                    &[&block_message.chain_name, &block_number_i64, &block_message.hash, &tx_count_i64],
                ).await.map_err(|e| {
                    error!("Failed to insert data into PostgreSQL: {}", e);
                    anyhow::anyhow!(e)
                })?;

                // Acknowledge the message
                consumer.ack(&msg).await.map_err(|e| { // Removed type annotation
                    error!("Failed to ACK message: {}", e);
                    anyhow::anyhow!(e)
                })?;
            }
            Err(e) => {
                error!("Failed to receive message: {}", e);
            }
        }
    }

    Ok(())
}
