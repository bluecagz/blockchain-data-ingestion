use std::pin::Pin;
use futures_core::{Future, Stream};
use ethers::types::{Block, Transaction, H256};
use anyhow::Result as AnyResult;

pub trait BlockchainAdapter {
    fn chain_name(&self) -> &str;

    fn get_block_by_number(
        &self,
        block_number: u64,
    ) -> Pin<Box<dyn Future<Output = AnyResult<Option<Block<Transaction>>>> + Send>>;

    fn subscribe_new_blocks(
        &self,
    ) -> Pin<Box<dyn Stream<Item = AnyResult<H256>> + Send>>;

    fn get_latest_block_number(
        &self,
    ) -> Pin<Box<dyn Future<Output = AnyResult<u64>> + Send>>;
}
