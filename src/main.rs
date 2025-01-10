use dotenv::dotenv;
use env_logger;
use log::info;
use blockchain_data_ingestion::run_ingestion; // Ensure this matches your crate name

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    info!("Starting the ingestion service...");

    // Start the ingestion process
    run_ingestion().await?;

    Ok(())
}
