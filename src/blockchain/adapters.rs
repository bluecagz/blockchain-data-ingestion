use std::pin::Pin;
use futures_core::{Future, Stream};
use ethers::types::{Block, Transaction};
use anyhow::Result as AnyResult;

pub trait BlockchainAdapter: Send + Sync {
    // fn chain_name(&self) -> &str;

    /// Fetches a block by its number.
    fn get_block_by_number(
        &self,
        block_number: u64,
    ) -> Pin<Box<dyn Future<Output = AnyResult<Option<Block<Transaction>>>> + Send>>;

    /// Subscribes to new blocks, yielding full Block structs.
    fn subscribe_new_blocks(
        &self,
    ) -> Pin<Box<dyn Stream<Item = AnyResult<Block<Transaction>>> + Send>>;

    /// Retrieves the latest block number.
    fn get_latest_block_number(
        &self,
    ) -> Pin<Box<dyn Future<Output = AnyResult<u64>> + Send>>;
}
