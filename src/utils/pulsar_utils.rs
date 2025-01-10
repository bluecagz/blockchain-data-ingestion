use pulsar::{
    Pulsar,
    TokioExecutor,
    Producer,
    Consumer,
    consumer::{InitialPosition, ConsumerOptions},
    SubType,
};
use anyhow::{Result, Context};
use std::sync::Arc;

pub type PulsarClient = Pulsar<TokioExecutor>;
pub type BlockProducer = Arc<Producer<String, TokioExecutor>>; // Producer sends String
pub type BlockConsumer = Consumer<String, TokioExecutor>; // Consumer receives String

/// Initialize Pulsar client
pub async fn initialize_pulsar_client(pulsar_url: &str) -> Result<PulsarClient> {
    Pulsar::builder(pulsar_url, TokioExecutor)
        .build()
        .await
        .context("Failed to create Pulsar client")
}

/// Create a producer for a specific topic
pub async fn create_block_producer(pulsar: &PulsarClient, topic: &str) -> Result<BlockProducer> {
    let producer = pulsar
        .producer()
        .with_topic(topic)
        .with_name("block-producer")
        .build::<String>() // Producer sends String
        .await
        .context("Failed to create Pulsar producer")?;
    
    Ok(Arc::new(producer))
}

/// Create a consumer for a specific topic and subscription
pub async fn create_block_consumer(
    pulsar: &PulsarClient,
    topic: &str,
    subscription: &str,
) -> Result<Consumer<String, TokioExecutor>> {
    let mut options = ConsumerOptions::default();
    options.initial_position = InitialPosition::Earliest; // Assign directly

    pulsar
        .consumer()
        .with_topic(topic)
        .with_subscription(subscription)
        .with_subscription_type(SubType::Exclusive)
        .with_options(options) // Pass ConsumerOptions directly
        .build::<String>() // Consumer receives String
        .await
        .context("Failed to create Pulsar consumer")
}
