use sqlx::migrate::Migrator;
use sqlx::Pool;
use anyhow::Result;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

pub async fn run_migrations(pg_pool: &Pool<sqlx::Postgres>) -> Result<()> {
    MIGRATOR.run(pg_pool).await?;
    Ok(())
}
