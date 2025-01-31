use async_stream::try_stream;
use std::pin::Pin;
use crate::blockchain::adapters::BlockchainAdapter;
use alloy::{
    providers::{Provider, ProviderBuilder, WsConnect},
    transports::http::Http
};
use reqwest::Client;
use alloy_network_primitives::{BlockResponse, BlockTransactionsKind, ReceiptResponse, BlockTransactions};
use std::sync::Arc;
use futures_core::{Future, Stream};
use anyhow::{Result as AnyResult, anyhow};
use futures_util::StreamExt;

#[derive(Clone)]
pub struct EVMAdapter {
    chain_name: String,
    http_provider: Arc<Provider<Http<Client>>>,
    ws_provider: Arc<Provider<WsConnect>>,
}

impl EVMAdapter {
    pub async fn new(
        chain_name: &str,
        http_url: &str,
        ws_url: &str
    ) -> AnyResult<Self> {
        let http_client = ProviderBuilder::new()
            .on_http(http_url)
            .await
            .map_err(|e| anyhow!("HTTP provider error: {}", e))?;

        let ws_client = WsConnect::new(ws_url)
            .await
            .map_err(|e| anyhow!("WebSocket connect error: {}", e))?;
        let ws_provider = ProviderBuilder::new()
            .on_ws(ws_client)
            .await
            .map_err(|e| anyhow!("WebSocket provider error: {}", e))?;

        Ok(Self {
            chain_name: chain_name.to_string(),
            http_provider: Arc::new(http_client),
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
        kind: BlockTransactionsKind, // Add a parameter to specify the kind
    ) -> Pin<Box<dyn Future<Output = AnyResult<Option<BlockTransactions>>> + Send>> {
        let provider = Arc::clone(&self.http_provider);
        Box::pin(async move {
            let block_opt = match kind {
                BlockTransactionsKind::Full => provider.get_block_with_txs(block_number).await,
                BlockTransactionsKind::Hashes => provider.get_block_with_hashes(block_number).await,
            }
            .map_err(|e| anyhow!("Error fetching block {}: {}", block_number, e))?;

            Ok(block_opt)
        })
    }
    
    fn subscribe_new_blocks(
        &self,
    ) -> Pin<Box<dyn Stream<Item = AnyResult<BlockTransactions>> + Send>> {
        let provider = Arc::clone(&self.ws_provider);
    
        let stream = try_stream! {
            let mut sub = provider
                .subscribe_blocks()
                .await
                .map_err(|e| anyhow!("subscribe_blocks() failed: {}", e))?;
    
            while let Some(header) = sub.next().await {
                yield header;
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
