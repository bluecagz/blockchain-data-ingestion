use async_trait::async_trait;
use anyhow::Result;
use futures_util::StreamExt;
use std::sync::Arc;
use tokio::sync::Mutex;
use std::future::Future;
use pulsar::{Producer, TokioExecutor};
// use pulsar::message::{Message, Payload};
use crate::blockchain::adapters::BlockchainAdapter;
use futures_core::Stream;
use std::pin::Pin;
use crate::streams::producers::producer::StreamProducer;
use crate::streams::message_queue::pulsar::{create_producer, PulsarClient};
use alloy_network_primitives::{BlockTransactions, BlockTransactionsKind};

pub struct EVMProducer {
    adapter: Arc<Mutex<dyn BlockchainAdapter>>,
    producer: Arc<Mutex<Producer<TokioExecutor>>>,
    producer_topic: String,
}

impl EVMProducer {
    pub async fn new(
        adapter: Arc<Mutex<dyn BlockchainAdapter>>,
        pulsar: Arc<PulsarClient>,
        producer_topic: String,
    ) -> Result<Self> {
        let producer = create_producer(&pulsar, producer_topic.clone()).await?;
        Ok(Self {
            adapter,
            producer: Arc::new(Mutex::new(producer)),
            producer_topic,
        })
    }
}

#[async_trait]
impl StreamProducer for EVMProducer {
    async fn produce_realtime(&self) -> Result<()> {
        let mut stream = self.adapter.lock().await.subscribe_new_blocks();
        while let Some(block_result) = stream.next().await {
            match block_result {
                Ok(block) => {
                    // Produce block to Pulsar
                    let mut producer = self.producer.lock().await;
                    let serialized_block = serde_json::to_string(&block)?;
                    producer.send(serialized_block).await?;
                }
                Err(e) => {
                    // Handle error
                    eprintln!("Error processing block: {:?}", e);
                }
            }
        }
        Ok(())
    }

    async fn produce_historical(&self, start_block: u64, end_block: u64) -> Result<()> {
        for block_number in start_block..=end_block {
            let block = self.adapter.lock().await.get_block_by_number(block_number).await?;
            if let Some(block) = block {
                // Produce block to Pulsar
                let mut producer = self.producer.lock().await;
                let serialized_block = serde_json::to_string(&block)?;
                producer.send(serialized_block).await?;
            }
        }
        Ok(())
    }
}

// Implement BlockchainAdapter for Arc<Mutex<A>> if A implements BlockchainAdapter
#[async_trait]
impl<A: BlockchainAdapter + Send + Sync + 'static> BlockchainAdapter for Arc<Mutex<A>> {
    // fn chain_name(&self) -> &str {
    //     tokio::task::block_in_place(|| self.blocking_lock().chain_name())
    // }

    fn get_block_by_number(
        &self,
        block_number: u64,
    ) -> Pin<Box<dyn Future<Output = Result<Option<BlockTransactions>>> + Send>> {
        let adapter = self.clone();
        Box::pin(async move {
            adapter.lock().await.get_block_by_number(block_number).await
        })
    }

    fn subscribe_new_blocks(
        &self,
        kind: BlockTransactionsKind, // Add a parameter to specify the kind
    ) -> Pin<Box<dyn Stream<Item = Result<BlockTransactions>> + Send>> {
        let adapter = self.adapter.clone();
        let kind = kind.unwrap_or(BlockTransactionsKind::Full);
        Box::pin(async_stream::stream! {
            let mut stream = adapter.lock().await.subscribe_new_blocks(kind);
            while let Some(block) = stream.next().await {
                yield block;
            }
        })
    }

    fn get_latest_block_number(
        &self,
    ) -> Pin<Box<impl Future<Output = Result<u64>> + Send>> {
        let adapter = self.adapter.clone();
        Box::pin(async move {
            adapter.lock().await.get_latest_block_number().await
        })
    }
}