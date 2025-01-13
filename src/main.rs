use dotenv::dotenv;
use env_logger;
use log::info;
use blockchain_data_ingestion::run_ingestion;
use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load environment variables
    dotenv().ok();

    // Initialize the logger
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    info!("Starting the ingestion service...");

    let database_url = env::var("DATABASE_URL")?;
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    // Start the ingestion process
    run_ingestion(&pool).await?;

    Ok(())
}
