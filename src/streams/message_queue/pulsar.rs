use anyhow::Result;
use pulsar::{Pulsar, Producer, Consumer, ConsumerOptions, SubType, TokioExecutor};
use pulsar::consumer::InitialPosition;
use pulsar::DeserializeMessage;
use pulsar::message::Message;

#[derive(Clone)]
pub struct PulsarClient {
    client: Pulsar<TokioExecutor>,
}

impl PulsarClient {
    pub async fn new(url: &str) -> Result<Self> {
        let client = Pulsar::builder(url, TokioExecutor).build().await?;
        Ok(PulsarClient { client })
    }
}

pub async fn create_producer(client: &PulsarClient, topic: &str) -> Result<Producer<TokioExecutor>> {
    let producer = client.client.producer().with_topic(topic).build().await?;
    Ok(producer)
}

pub async fn create_consumer<T: DeserializeMessage>(client: &PulsarClient, topic: &str, subscription: &str) -> Result<Consumer<Message, TokioExecutor>> {
    let consumer = client.client
        .consumer()
        .with_topic(topic)
        .with_subscription_type(SubType::Exclusive)
        .with_subscription(subscription)
        .with_options(ConsumerOptions {
            initial_position: InitialPosition::Earliest,
            ..Default::default()
        })
        .build()
        .await?;
    Ok(consumer)
}