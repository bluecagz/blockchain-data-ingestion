use dotenv::dotenv;
use log::{error, info};
use blockchain_data_ingestion::{load_config, get_blockchain_data};

#[tokio::main]
async fn main() {
    dotenv().ok(); // Load environment variables from .env file

    //env_logger::init();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("debug")).init();
    info!("Starting the blockchain data ingestion service...");

    // In a real app, you'd parse CLI args or environment variables to get this path.
    let config_path = "blockchains.toml"; 

    // Load config
    let config = match load_config(config_path) {
        Ok(cfg) => cfg,
        Err(e) => {
            error!("Failed to load config: {}", e);
            std::process::exit(1);
        }
    };

    // Ingest blockchain data
    if let Err(e) = get_blockchain_data(&config).await {
        error!("Error ingesting blockchain data: {}", e);
    } else {
        info!("Successfully ingested blockchain data.");
    }
}
