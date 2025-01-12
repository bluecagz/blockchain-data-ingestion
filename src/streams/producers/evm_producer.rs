use async_trait::async_trait;
use anyhow::Result;
use futures_util::StreamExt;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::Mutex;
use std::future::Future;
use pulsar::{Producer, TokioExecutor};
use crate::blockchain::adapters::BlockchainAdapter;
use ethers::types::{Block, Transaction};
use futures_core::Stream;
use std::pin::Pin;
use crate::streams::producers::producer::StreamProducer;

/// The type parameter `A` represents a blockchain adapter that implements
/// the `BlockchainAdapter` trait.
pub struct EVMProducer<A: BlockchainAdapter + Send + Sync> {
    adapter: Arc<Mutex<A>>,
    pulsar_producer: Arc<Mutex<Producer<TokioExecutor>>>,
    // PhantomData is used here if you need to hold additional generic types
    _marker: PhantomData<A>,
}

impl<A: BlockchainAdapter + Send + Sync> EVMProducer<A> {
    pub async fn new(adapter: Arc<Mutex<A>>, pulsar_producer: Arc<Mutex<Producer<TokioExecutor>>>) -> Result<Self> {
        Ok(Self {
            adapter,
            pulsar_producer,
            _marker: PhantomData,
        })
    }
}

#[async_trait]
impl<A: BlockchainAdapter + Send + Sync> StreamProducer for EVMProducer<A> {
    async fn produce_realtime(&self) -> Result<()> {
        let mut stream = self.adapter.lock().await.subscribe_new_blocks();
        while let Some(block_result) = stream.next().await {
            match block_result {
                Ok(block) => {
                    // Produce block to Pulsar
                    // Example: self.pulsar_producer.lock().await.send(block).await?;
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
                // Example: self.pulsar_producer.lock().await.send(block).await?;
            }
        }
        Ok(())
    }
}

// Implement BlockchainAdapter for Arc<Mutex<A>> if A implements BlockchainAdapter
#[async_trait]
impl<A: BlockchainAdapter + Send + Sync> BlockchainAdapter for Arc<Mutex<A>> {
    fn chain_name(&self) -> &str {
        tokio::task::block_in_place(|| self.blocking_lock().chain_name())
    }

    fn get_block_by_number(
        &self,
        block_number: u64,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Block<Transaction>>>> + Send>> {
        let adapter = self.clone();
        Box::pin(async move {
            adapter.lock().await.get_block_by_number(block_number).await
        })
    }

    fn subscribe_new_blocks(
        &self,
    ) -> Pin<Box<dyn Stream<Item = Result<Block<Transaction>>> + Send>> {
        let adapter = self.clone();
        Box::pin(async_stream::stream! {
            let mut stream = adapter.lock().await.subscribe_new_blocks();
            while let Some(block) = stream.next().await {
                yield block;
            }
        })
    }

    fn get_latest_block_number(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<u64>> + Send>> {
        let adapter = self.clone();
        Box::pin(async move {
            adapter.lock().await.get_latest_block_number().await
        })
    }
}
