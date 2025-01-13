

// Define a trait for the message schema
pub trait MessageSchema {
    fn serialize(&self) -> Vec<u8>;
    fn deserialize(data: &[u8]) -> Self;
}
