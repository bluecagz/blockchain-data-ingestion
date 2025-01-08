# Blockchain Data Ingestion (WIP)

A **Rust-based** solution for ingesting, transforming, and storing blockchain data from various networks. The project aims to make it straightforward to collect meaningful on-chain data—like blocks, transactions, logs, or custom events—and load it into a **Postgres** or **DuckDB** database for further analysis. The codebase can be run locally on your machine or deployed to a server environment.

---

## Table of Contents
1. [Overview](#overview)  
2. [Features](#features)  
3. [Architecture](#architecture)  
4. [Getting Started](#getting-started)  
5. [Usage](#usage)  
6. [Configuration](#configuration)  
7. [Data Models](#data-models)  
8. [Roadmap](#roadmap)  
9. [Contributing](#contributing)  
10. [License](#license)  

---

## Overview
Blockchain Data Ingestion is designed to:
- Connect to various blockchain networks (e.g., Ethereum, Arbitrum, Polygon).
- Collect and parse data related to blocks, transactions, logs, and more.
- Transform raw data into organized structures for use in **Postgres** or **DuckDB**.
- Scale horizontally to handle large volumes of on-chain events.

This repository centralizes the ingestion logic, schemas, transformations, and connectors used to feed downstream analytics, machine learning pipelines, or any services building APIs/endpoints on top of your ingested data.

---

## Features
- **Multi-Chain Support:** Ingest data from various blockchain networks by simply updating configuration files or toggling relevant connectors.
- **Modular Connectors:** Easily add or remove connectors for new networks or data types (e.g., blocks, transactions, token transfers).
- **Scalable Architecture:** Support both batch and streaming ingestion modes for small or large data loads.
- **Multiple Storage Backends:** Choose between **Postgres** or **DuckDB** to persist your blockchain data.
- **Data Validation:** Basic schema validation to ensure that ingested data is consistent and accurate.

---

## Architecture

```mermaid
flowchart TD
    A[Blockchain Network] --> B[Data Connector/Fetcher]
    B --> C[Data Processing/Transformation]
    C --> D[Postgres / DuckDB]
    D --> E[Analytics & Visualization / Services / Endpoints]
```

1. **Data Connector/Fetcher**  
   Connect to blockchain nodes or provider APIs to fetch block and transaction data.

2. **Data Processing/Transformation**  
   Apply transformations, parse logs, decode ABI events, and clean data into a usable format.

3. **Postgres / DuckDB**  
   Store structured data in your preferred SQL engine (Postgres or DuckDB).  
   - **Postgres** for more traditional client-server setups.  
   - **DuckDB** for local analytics, small-footprint embedding, or single-file portability.

4. **Analytics & Visualization / Services / Endpoints**  
   - Consume data for queries or dashboards.  
   - Build custom endpoints on top of your ingested data to power web services or APIs.

---

## Getting Started

### Prerequisites
- **Rust** (version 1.60+ recommended). Install using [rustup](https://rustup.rs/).  
- **cargo** (comes with Rust, used to build and run the project).  
- **A blockchain node or provider** (Infura, Alchemy, or a self-hosted node for the chain(s) of your choice).  
- **Database**  
  - [Postgres](https://www.postgresql.org/) if you plan to store data in a traditional client-server database.  
  - [DuckDB](https://duckdb.org/) if you prefer a local, embedded database that stores data in a single file.  

### Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/bluecagz/blockchain-data-ingestion.git
   cd blockchain-data-ingestion
   ```

2. **Build the Project**  
   ```bash
   cargo build --release
   ```
   This will compile the code in **release** mode, optimizing for performance.  
   *For quicker dev cycles, you can use:*  
   ```bash
   cargo build
   ```

3. **Set Up Environment Variables**  
   - Copy or rename `.env.example` to `.env`.  
   - Update values (e.g., provider endpoints, Postgres/DuckDB credentials or file paths).

---

## Usage

Once you’ve built the project, you can run the data ingestion either locally or on a server.

### Local Run

```bash
cargo run --release
```

_(for debug mode):_
```bash
cargo run
```

Depending on your configuration, this might:
- Connect to a specified blockchain provider (Infura, Alchemy, etc.).
- Start fetching blocks/transactions from a given start block.
- Process data and store it in Postgres or DuckDB, as configured.

### Streaming vs. Batch
- **Batch Mode**: Useful for historical data ingestion over a specific block range.  
- **Streaming Mode**: For near real-time ingestion; automatically fetches new blocks as they are finalized.

---

## Configuration

**`config/` Folder**  
Contains network-specific configuration files...:
- RPC or provider endpoints  
- Rate limits  
- Data parsing rules  

**`.env` File**  
Holds environment variables such as:  
...


Adjust these configurations to match your environment before running the pipeline.

---

## Data Models

Depending on your schema, you might store data in tables such as:

- **Blocks**  
  - `block_number`  
  - `timestamp`  
  - `transactions_root`  
  - `miner`  
  - …  

- **Transactions**  
  - `hash`  
  - `from_address`  
  - `to_address`  
  - `value`  
  - `gas_used`  
  - `block_number`  
  - …  

- **Logs/Events**  
  - `log_index`  
  - `address`  
  - `topics`  
  - `data`  
  - `decoded_event`  
  - `transaction_hash`  
  - `block_number`  
  - …  

You can adapt these models to reflect more specialized data points (e.g., token transfers, NFT mint events, protocol-specific actions).

---

## Roadmap
- **Additional Chains**: Expand beyond Ethereum, Arbitrum, and Polygon, e.g., Avalanche, Optimism, BSC, etc.
- **Advanced Querying**: Explore custom SQL queries or engine optimizations for Postgres and DuckDB.
- **Orchestration**: Integrate with Airflow, Dagster, or other workflow tools for production pipelines.
- **Monitoring & Alerting**: Add metrics support (e.g., Prometheus) and real-time alerts.
- **Enhanced Data Validation**: Incorporate stricter schema checks for protocol-specific events.

---

## Contributing
We welcome contributions of all kinds:
1. Fork the repository  
2. Create a new feature branch (`git checkout -b feature/amazing-feature`)  
3. Commit your changes (`git commit -am 'Add amazing feature'`)  
4. Push the branch (`git push origin feature/amazing-feature`)  
5. Create a Pull Request  

Make sure to follow any coding conventions or style guidelines specified in this repo.

---

## License
This project is licensed under the [MIT License](LICENSE). Feel free to modify and distribute as per the license terms.

---

**Questions or Issues?**  
Feel free to open an issue on GitHub or reach out via email. Thank you for using and contributing to the Blockchain Data Ingestion project!