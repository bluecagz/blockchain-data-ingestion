use futures::future::try_join_all;
use reqwest::Client;
use std::{error::Error, future::Future};
use web3::types::{Block, Transaction, TransactionReceipt};


// Define a trait for blockchain adapters
pub trait BlockchainAdapter {
    fn new(rpc_url: &str) -> Self
    where
        Self: Sized;
    fn get_block_by_number(&self, block_number: u64)
        -> impl Future<Output = Result<Block<Transaction>, Box<dyn Error + Send + Sync>>> + Send;
    fn get_native_transactions(&self, block_number: u64)
        -> impl Future<Output = Result<Vec<(String, TransactionReceipt)>, Box<dyn Error + Send + Sync>>> + Send;
}

// EVM adapter implementation
pub struct EVMAdapter {
    client: Client,
    rpc_url: String,
}

impl BlockchainAdapter for EVMAdapter {
    fn new(rpc_url: &str) -> Self {
        Self {
            client: Client::new(),
            rpc_url: rpc_url.to_string(),
        }
    }

    fn get_block_by_number(&self, block_number: u64)
        -> impl Future<Output = Result<Block<Transaction>, Box<dyn Error + Send + Sync>>> + Send
    {
        let client = self.client.clone();
        let rpc_url = self.rpc_url.clone();

        async move {
            let body = serde_json::json!({
                "jsonrpc": "2.0",
                "method": "eth_getBlockByNumber",
                "params": [format!("0x{:x}", block_number), true],
                "id": 1
            });

            let response = client
                .post(&rpc_url)
                .json(&body)
                .send()
                .await?;
            let response_json = response.json::<serde_json::Value>().await?;
            let block = response_json["result"].clone();
            let block: Block<Transaction> = serde_json::from_value(block)?;

            Ok(block)
        }
    }

    fn get_native_transactions(&self, block_number: u64)
        -> impl Future<Output = Result<Vec<(String, TransactionReceipt)>, Box<dyn Error + Send + Sync>>> + Send
    {
        async move {
            // Fetch the block with transactions
            let block = self.fetch_block_with_transactions(block_number).await?;

            // Convert transaction hashes to "0x..." strings properly
            let transaction_hashes: Vec<String> = block
                .transactions
                .iter()
                // Use "{:#x}" to ensure we get an 0x-prefixed hex string
                .map(|tx| format!("{:#x}", tx.hash))
                .collect();

            // Fetch receipts for each transaction
            let receipts = self.fetch_transaction_receipts(transaction_hashes).await?;
            Ok(receipts)
        }
    }
}

impl EVMAdapter {
    async fn fetch_block_with_transactions(&self, block_number: u64)
        -> Result<Block<Transaction>, Box<dyn Error + Send + Sync>>
    {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_getBlockByNumber",
            "params": [format!("0x{:x}", block_number), true],
            "id": 1
        });

        let response = self.client
            .post(&self.rpc_url)
            .json(&body)
            .send()
            .await?;
        let response_json = response.json::<serde_json::Value>().await?;
        let block = response_json["result"].clone();
        let block: Block<Transaction> = serde_json::from_value(block)?;

        Ok(block)
    }

    async fn fetch_transaction_receipts(
        &self,
        transaction_hashes: Vec<String>,
    ) -> Result<Vec<(String, TransactionReceipt)>, Box<dyn Error + Send + Sync>> {
        println!("Transaction hashes: {:?}", transaction_hashes);

        // Build a list of futures -- each fetches a single transaction receipt
        let futures = transaction_hashes.into_iter().map(|hash| {
            let client = self.client.clone();
            let rpc_url = self.rpc_url.clone();

            async move {
                // Construct the request body
                let body = serde_json::json!({
                    "jsonrpc": "2.0",
                    "method": "eth_getTransactionReceipt",
                    "params": [hash.clone()],
                    "id": 1
                });

                println!("Request body: {:?}", body);

                let response = client
                    .post(&rpc_url)
                    .json(&body)
                    .send()
                    .await?; // reqwest::Error can be turned into Box<dyn Error> 
                let response_json = response.json::<serde_json::Value>().await?;

                log::debug!("Response for transaction hash {}: {:?}", hash, response_json);

                // If `result` is null, treat it as an error
                if response_json["result"].is_null() {
                    log::warn!("Transaction receipt is null for hash: {}", hash);
                    return Err(Box::<dyn Error + Send + Sync>::from(
                        format!("Transaction receipt is null for hash {}", hash),
                    ));
                }

                // Deserialize the transaction receipt
                let receipt_value = response_json["result"].clone();
                let receipt: TransactionReceipt = serde_json::from_value(receipt_value)?;

                // Return tuple (hash, receipt)
                Ok::<(String, TransactionReceipt), Box<dyn Error + Send + Sync>>((hash, receipt))
            }
        });

        // If any future fails, `try_join_all` returns the first error it encounters
        let results: Vec<(String, TransactionReceipt)> = try_join_all(futures).await?;

        Ok(results)
    }
}
