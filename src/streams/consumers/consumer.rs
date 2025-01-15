use anyhow::Result;
use async_trait::async_trait;
use sqlx::postgres::PgPool;


/// A trait representing a stream consumer that can consume messages and store them.
#[async_trait]
pub trait StreamConsumer {
    /// Consume messages from a stream (e.g. Pulsar) and store them (e.g. in PostgreSQL).
    async fn postgres_consume(&mut self, pg_pool: &PgPool, chain: &str) -> Result<()>;
}
