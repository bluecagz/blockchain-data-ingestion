use async_stream::try_stream;
use std::pin::Pin;
use crate::blockchain::adapters::BlockchainAdapter;
use ethers::{
    providers::{Http, Provider, Ws, Middleware},
    types::{Block, Transaction},
};
use std::sync::Arc;
use futures_core::{Future, Stream};
use anyhow::{Result as AnyResult, anyhow};
use futures_util::StreamExt;

#[derive(Clone)]
pub struct EVMAdapter {
    chain_name: String,
    http_provider: Arc<Provider<Http>>,
    ws_provider: Arc<Provider<Ws>>,
}

impl EVMAdapter {
    pub async fn new(
        chain_name: &str,
        http_url: &str,
        ws_url: &str,
    ) -> AnyResult<Self> {
        let http_provider = Provider::<Http>::try_from(http_url)
            .map_err(|e| anyhow!("HTTP provider error: {}", e))?;

        let ws_client = Ws::connect(ws_url).await
            .map_err(|e| anyhow!("WebSocket connect error: {}", e))?;
        let ws_provider = Provider::new(ws_client);

        Ok(Self {
            chain_name: chain_name.to_string(),
            http_provider: Arc::new(http_provider),
            ws_provider: Arc::new(ws_provider),
        })
    }
}

impl BlockchainAdapter for EVMAdapter {
    fn chain_name(&self) -> &str {
        &self.chain_name
    }

    fn get_block_by_number(
        &self,
        block_number: u64,
    ) -> Pin<Box<dyn Future<Output = AnyResult<Option<Block<Transaction>>>> + Send>> {
        let provider = Arc::clone(&self.http_provider);
        Box::pin(async move {
            let block_opt = provider
                .get_block_with_txs(block_number)
                .await
                .map_err(|e| anyhow!("Error fetching block {}: {}", block_number, e))?;

            Ok(block_opt)
        })
    }

    fn subscribe_new_blocks(
        &self,
    ) -> Pin<Box<dyn Stream<Item = AnyResult<Block<Transaction>>> + Send>> {
        let provider = Arc::clone(&self.ws_provider);
        let http_provider = Arc::clone(&self.http_provider);

        let stream = try_stream! {
            let mut sub = provider
                .watch_blocks()
                .await
                .map_err(|e| anyhow!("watch_blocks() failed: {}", e))?;

            while let Some(hash_val) = sub.next().await {
                // Fetch the full block data using the hash
                let block = http_provider
                    .get_block_with_txs(hash_val)
                    .await
                    .map_err(|e| anyhow!("Error fetching block for hash {}: {}", hash_val, e))?
                    .ok_or_else(|| anyhow!("Block not found for hash {}", hash_val))?;

                yield block;
            }
        };
        Box::pin(stream)
    }

    fn get_latest_block_number(
        &self,
    ) -> Pin<Box<dyn Future<Output = AnyResult<u64>> + Send>> {
        let provider = Arc::clone(&self.http_provider);
        Box::pin(async move {
            let block_num = provider
                .get_block_number()
                .await
                .map_err(|e| anyhow!("Error fetching latest block number: {}", e))?;

            Ok(block_num.as_u64())
        })
    }
}
