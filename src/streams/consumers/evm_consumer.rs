use anyhow::Result;
use async_trait::async_trait;
use log::error;
use sqlx::PgPool;
use serde_json::{Value};
// use futures_util::StreamExt;
// use pulsar::message;
// use pulsar::DeserializeMessage;
use std::sync::Arc;
// use tokio::sync::Mutex;
// use alloy_primitives::{U256, Address, B256};
use alloy_network_primitives::{BlockResponse, TransactionResponse, BlockTransactions};
use sqlx::types::time::PrimitiveDateTime;

use crate::streams::message_queue::pulsar::{create_consumer, PulsarClient};
use crate::streams::consumers::consumer::StreamConsumer;

pub struct EVMConsumer {
    pulsar: Arc<PulsarClient>,
    consumer_topic: String,
    consumer_subscription: String,
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

    pub async fn insert_transaction_data(&self, pg_pool: &PgPool, block_number: i64, chain_name: &str, transaction: &impl TransactionResponse) -> Result<()> {
        let mut tx = pg_pool.begin().await?;

        sqlx::query!(
            "INSERT INTO transactions (block_number, chain_name, tx_hash, from_address, to_address, value, gas_price, gas, input, nonce) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
            block_number,
            chain_name,
            transaction.tx_hash().to_string(),
            transaction.from().to_string(),
            transaction.to().map(|to| to.to_string()),
            transaction.value().unwrap_or_default().to_string(),
            transaction.gas_price().unwrap_or_default().to_string(),
            transaction.gas().to_string(),
            transaction.input().to_string(),
            transaction.nonce().unwrap_or_default().as_u64() as i64
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

    pub async fn insert_block_data(&self, pg_pool: &PgPool, chain_name: &str, block: &impl BlockResponse) -> Result<()> {
        let header = block.header();
        let block_number_i64 = header.number().as_u64() as i64;
        let gas_used_i64 = header.gas_used().as_u64() as i64;
        let gas_limit_i64 = header.gas_limit().as_u64() as i64;
        let size_i64 = header.size().unwrap_or_default().as_u64() as i64;
        let timestamp_i64 = header.timestamp().as_u64() as i64;
        let timestamp: PrimitiveDateTime = PrimitiveDateTime::from_unix_timestamp(timestamp_i64).unwrap();
        let tx_count_i64 = block.transactions().len() as i64;
        let tx_count_value: Value = tx_count_i64.into();
        let transactions_json: Value = serde_json::to_value(&block.transactions()).unwrap();

        let mut tx = pg_pool.begin().await?;

        sqlx::query!(
            "INSERT INTO blocks (block_number, chain_name, hash, parent_hash, timestamp, miner, difficulty, total_difficulty, gas_used, gas_limit, size, receipts_root, tx_count, transactions) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
            block_number_i64,
            chain_name,
            header.hash().to_string(),
            header.parent_hash().to_string(),
            timestamp,
            header.miner().to_string(),
            header.difficulty().to_string(),
            header.total_difficulty().to_string(),
            gas_used_i64,
            gas_limit_i64,
            size_i64,
            header.receipts_root().to_string(),
            tx_count_value,
            transactions_json
        )
        .execute(&mut tx)
        .await
        .map_err(|e: sqlx::Error| {
            error!("Failed to insert block data into PostgreSQL: {}", e);
            anyhow::anyhow!(e)
        })?;

        tx.commit().await?;

        Ok(())
    }
}

#[async_trait]
impl StreamConsumer for EVMConsumer {
    async fn postgres_consume(&mut self, pg_pool: Arc<PgPool>, chain_name: &str) -> Result<()> {
        let mut consumer = create_consumer(&self.pulsar, &self.consumer_topic, &self.consumer_subscription).await?;
        
        while let Some(msg_res) = consumer.next().await {
            match msg_res {
                Ok(msg) => {
                    let block_message: BlockTransactions = match msg.deserialize() {
                        Ok(data) => data,
                        Err(e) => {
                            error!("Failed to deserialize message: {:?}", e);
                            break;
                        }
                    };
                    
                    for transaction in block_message.transactions() {
                        self.insert_transaction_data(&pg_pool, transaction.block_number().as_u64() as i64, chain_name, transaction).await?;
                    }
                    
                    self.insert_block_data(&pg_pool, chain_name, &block_message).await?;
                    
                    consumer.ack(&msg).await.map_err(|e| {
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
