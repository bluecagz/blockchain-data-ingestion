use anyhow::Result;
use async_trait::async_trait;
use log::error;
use sqlx::PgPool;
use serde_json::json;
use futures_util::StreamExt;
//use pulsar::{Consumer, TokioExecutor};
//use pulsar::consumer::Message;
use pulsar::message;
use pulsar::DeserializeMessage;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::streams::message_queue::pulsar::{create_consumer, PulsarClient};
use crate::streams::consumers::consumer::StreamConsumer;
use crate::streams::schemas::evm::{BlockSchema, TransactionSchema};

/// EVMConsumer encapsulates the configuration needed for consuming messages
/// from a Pulsar topic and storing them in PostgreSQL.
pub struct EVMConsumer {
    pulsar: Arc<PulsarClient>,
    consumer_topic: String,
    consumer_subscription: String
}

impl EVMConsumer {
    pub async fn new(
        pulsar: Arc<PulsarClient>,
        consumer_topic: String,
        consumer_subscription: String
    ) -> Self {
        Self {
            pulsar,
            consumer_topic,
            consumer_subscription
        }
    }
  
    pub async fn insert_block_data(&self, pg_pool: &PgPool, chain_name: &str, block: &BlockSchema) -> Result<()> {
        let block_number_i64 = block.number.as_u64() as i64;
        let gas_used_i64 = block.gas_used.as_u64() as i64;
        let gas_limit_i64 = block.gas_limit.as_u64() as i64;
        let size_i64 = block.size.expect("Block size error!").as_u64() as i64;
        let difficulty_i64 = block.difficulty.as_u64() as i64;
        let timestamp_i64 = block.timestamp.as_u64() as i64;
        let tx_count_i64 = block.transactions.len() as i64;
        let transactions_json = json!(block.transactions);

        let mut tx = pg_pool.begin().await?;


        sqlx::query!(
            "INSERT INTO blocks (block_number, chain_name, hash, parent_hash, timestamp, miner, difficulty, total_difficulty, gas_used, gas_limit, size, receipts_root, tx_count, transactions) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
            block_number_i64,
            chain_name,
            block.hash.to_string(),
            block.parent_hash.to_string(),
            timestamp_i64,
            block.miner.to_string(),
            difficulty_i64,
            block.total_difficulty.to_string(),
            gas_used_i64,
            gas_limit_i64,
            size_i64,
            block.receipts_root.to_string(),
            transactions_json,
            tx_count_i64,
            
        )
        .execute(&mut tx)
        .await
        .map_err(|e: sqlx::Error| {
            error!("Failed to insert block data into PostgreSQL: {}", e);
            anyhow::anyhow!(e)
        })?;

        // for transaction in &block.transactions {
        //     self.insert_transaction_data(&mut tx, block_number_i64, chain_name, transaction).await?;
        // }

        tx.commit().await?;

        Ok(())
    }

     // TODO: add transaction data to the database
    pub async fn insert_transaction_data(&self, pg_pool: &PgPool, block_number: i64, chain_name: &str, transaction: &TransactionSchema) -> Result<()> {
        let mut tx = pg_pool.begin().await?;

        // TODO: update casting to correct types       
        sqlx::query!(
            "INSERT INTO transactions (block_number, chain_name, tx_hash, from_address, to_address, value, gas_price, gas, input, nonce) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
            block_number,
            chain_name,
            transaction.hash.to_string(),
            transaction.from.to_string(),
            transaction.to.map(|to| to.to_string()),
            transaction.value.to_string(),
            transaction.gas_price.to_string(),
            transaction.gas.to_string(),
            transaction.input.to_string(),
            transaction.nonce.as_u64() as i64
        )
        .execute(&mut tx)
        .await
        .map_err(|e: sqlx::Error| {
            error!("Failed to insert transaction data into PostgreSQL: {}", e);
            anyhow::anyhow!(e)
        })?;

        tx.commit().await?;

        Ok(())
    }
}

#[async_trait]
impl StreamConsumer for EVMConsumer {
    async fn postgres_consume(&mut self, pg_pool: Arc<PgPool>, chain_name: &str) -> Result<()> {
        let mut consumer = create_consumer(&self.pulsar_client, &self.consumer_topic, &self.consumer_subscription).await?;
        
        while let Some(msg_res) = consumer.next().await {
            match msg_res {
                Ok(msg) => {
                    let block_message: BlockSchema = match msg.deserialize() {
                        Ok(data) => data,
                        Err(e) => {
                            error!("Failed to deserialize message: {:?}", e);
                            break;
                        }
                    };
        
                    self.insert_block_data(&pg_pool, &chain_name, &block_message).await?;
        
                    consumer.ack(&msg).await.map_err(|e: anyhow::Error| {
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
