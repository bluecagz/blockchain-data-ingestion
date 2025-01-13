
use serde::{Deserialize, Serialize};
use ethers::types::{H256, U256, Address, Bytes};

use super::schema::MessageSchema;

// Define the schema for a block
#[derive(Serialize, Deserialize, Debug)]
pub struct BlockSchema {
    pub number: U256,
    pub hash: H256,
    pub parent_hash: H256,
    pub nonce: Option<H256>,
    pub sha3_uncles: H256,
    pub logs_bloom: Option<Bytes>,
    pub transactions_root: H256,
    pub state_root: H256,
    pub receipts_root: H256,
    pub miner: Address,
    pub difficulty: U256,
    pub total_difficulty: U256,
    pub extra_data: Bytes,
    pub size: Option<U256>,
    pub gas_limit: U256,
    pub gas_used: U256,
    pub timestamp: U256,
    pub transactions: Vec<TransactionSchema>,
    pub uncles: Vec<H256>,
}

// Define the schema for a transaction
#[derive(Serialize, Deserialize, Debug)]
pub struct TransactionSchema {
    pub hash: H256,
    pub nonce: U256,
    pub block_hash: Option<H256>,
    pub block_number: Option<U256>,
    pub transaction_index: Option<U256>,
    pub from: Address,
    pub to: Option<Address>,
    pub value: U256,
    pub gas_price: U256,
    pub gas: U256,
    pub input: Bytes,
}

// Implement the MessageSchema trait for BlockSchema
impl MessageSchema for BlockSchema {
    fn serialize(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("Failed to serialize BlockSchema")
    }

    fn deserialize(data: &[u8]) -> Self {
        serde_json::from_slice(data).expect("Failed to deserialize BlockSchema")
    }
}
// Implement the MessageSchema trait for TransactionSchema
impl MessageSchema for TransactionSchema {
    fn serialize(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("Failed to serialize BlockSchema")
    }

    fn deserialize(data: &[u8]) -> Self {
        serde_json::from_slice(data).expect("Failed to deserialize BlockSchema")
    }
}