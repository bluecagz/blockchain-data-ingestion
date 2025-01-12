use anyhow::Result;
use async_trait::async_trait;
use log::{error, info};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use serde_json::json;
use ethers::types::{Block, Transaction};
use futures_util::StreamExt;
use pulsar::{Consumer, Executor, TokioExecutor};
use pulsar::consumer::Message;

use crate::storage::db::run_migrations;
use crate::streams::message_queue::pulsar::{create_consumer, PulsarClient};
use crate::streams::consumers::consumer::StreamConsumer;
use crate::streams::schemas::evm::{BlockSchema, TransactionSchema};

/// EVMConsumer encapsulates the configuration needed for consuming messages
/// from a Pulsar topic and storing them in PostgreSQL.
pub struct EVMConsumer {
    pub pulsar: PulsarClient,
    pub topic: String,
    pub subscription: String,
    pub pg_pool: PgPool,
}

impl EVMConsumer {
    pub async fn new(pulsar: PulsarClient, topic: String, subscription: String, pg_connection_str: &str) -> Result<Self> {
        let pg_pool = PgPool::connect(pg_connection_str).await?;
        // Run migrations
        run_migrations(&pg_pool).await?;
        Ok(Self {
            pulsar,
            topic,
            subscription,
            pg_pool,
        })
    }

    pub async fn insert_block_data(&self, block: &BlockSchema) -> Result<()> {
        let block_number_i64 = block.number.as_u64() as i64;
        let gas_used_i64 = block.gas_used.as_u64() as i64;
        let gas_limit_i64 = block.gas_limit.as_u64() as i64;
        let size_i64 = block.size.expect("Block size error!").as_u64() as i64;
        let tx_count_i64 = block.transactions.len() as i64;

        let transactions_json = json!(block.transactions);

        let mut tx = self.pg_pool.begin().await?;

        sqlx::query!(
            "INSERT INTO blocks (block_number, hash, parent_hash, timestamp, miner, difficulty, total_difficulty, gas_used, gas_limit, size, receipts_root, tx_count, transactions) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
            block_number_i64,
            block.hash.to_string(),
            block.parent_hash.to_string(),
            block.timestamp,
            block.miner.to_string(),
            block.difficulty.to_string(),
            block.total_difficulty.to_string(),
            gas_used_i64,
            gas_limit_i64,
            size_i64,
            block.receipts_root.to_string(),
            tx_count_i64,
            transactions_json
        )
        .execute(&mut tx)
        .await
        .map_err(|e: sqlx::Error| {
            error!("Failed to insert block data into PostgreSQL: {}", e);
            anyhow::anyhow!(e)
        })?;

        for transaction in &block.transactions {
            self.insert_transaction_data(&mut tx, block_number_i64, transaction).await?;
        }

        tx.commit().await?;

        Ok(())
    }

    pub async fn insert_transaction_data(&self, tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, block_number: i64, transaction: &TransactionSchema) -> Result<()> {
        sqlx::query!(
            "INSERT INTO transactions (block_number, tx_hash, from_address, to_address, value, gas_price, gas, input, nonce) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
            block_number,
            transaction.hash.to_string(),
            transaction.from.to_string(),
            transaction.to.as_ref().map(|to| to.to_string()),
            transaction.value.to_string(),
            transaction.gas_price.to_string(),
            transaction.gas.to_string(),
            transaction.input.to_string(),
            transaction.nonce.as_u64() as i64
        )
        .execute(tx)
        .await
        .map_err(|e: sqlx::Error| {
            error!("Failed to insert transaction data into PostgreSQL: {}", e);
            anyhow::anyhow!(e)
        })?;

        Ok(())
    }
}

#[async_trait]
impl<T, Exe> StreamConsumer for EVMConsumer
where
    T: Send + Sync,
    Exe: Send + Sync,
{
    async fn postgres_consume(&mut self) -> Result<()> {
        // Create a Pulsar consumer using our utility function.
        let mut consumer: Consumer<Message<T>, TokioExecutor> = create_consumer(&self.pulsar, &self.topic, &self.subscription).await?;
        
        // Consume messages.
        while let Some(msg_res) = consumer.next().await {
            match msg_res {
                Ok(msg) => {
                    // Log message ID.
                    info!("Received message ID: {:?}", msg.message_id);
                    println!("Received message ID: {:?}", msg.message_id);

                    // Convert payload bytes to a String.
                    let payload_bytes: Vec<u8> = msg.payload.data.to_vec();
                    let payload_str = String::from_utf8(payload_bytes).map_err(|e| {
                        error!("Failed to convert payload to String: {}", e);
                        anyhow::anyhow!(e)
                    })?;

                    // Deserialize the message payload.
                    let block_message: BlockSchema = serde_json::from_str(&payload_str).map_err(|e| {
                        error!("Failed to deserialize message: {}", e);
                        anyhow::anyhow!(e)
                    })?;

                    // Insert data into PostgreSQL.
                    self.insert_block_data(&block_message).await?;

                    // Acknowledge the message.
                    consumer.ack(&msg).await.map_err(|e: pulsar::Error| {
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
}