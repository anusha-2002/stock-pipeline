# Stock Market Data Pipeline

A real-time stock market data pipeline using Kafka for streaming, with historical data ingestion for AAPL, GOOGL, MSFT, TSLA, and AMZN.

## Architecture

```
yfinance API → data/ (CSVs) → producer → Kafka → consumer → storage → analysis
```

## Project Structure

```
stock-pipeline/
├── docker-compose.yml      # Kafka + Zookeeper
├── data/
│   └── download_data.py    # Downloads historical stock data via yfinance
├── producer/
│   └── producer.py         # Reads CSVs and publishes to Kafka topic
├── consumer/               # 
├── storage/                # 
└── analysis/               # 
```

## Prerequisites

- Docker and Docker Compose
- Python 3.8+
- pip packages: `kafka-python`, `pandas`, `yfinance`

```bash
pip install kafka-python pandas yfinance
```

## Getting Started

### 1. Start Kafka

```bash
docker-compose up -d
```

This starts Zookeeper on port `2181` and Kafka on port `9092`.

### 2. Download Historical Data

```bash
python data/download_data.py
```

Downloads OHLCV data from 2020-01-01 to 2024-12-31 for all 5 symbols and saves them as CSVs in `data/`.

### 3. Run the Producer

```bash
python producer/producer.py
```

Streams each CSV row as a JSON message to the `stock-market-data` Kafka topic at ~10 records/second.

## Kafka Topic

| Topic | Description |
|-------|-------------|
| `stock-market-data` | OHLCV records for AAPL, GOOGL, MSFT, TSLA, AMZN |

## Symbols Covered

| Symbol | Company |
|--------|---------|
| AAPL | Apple Inc. |
| GOOGL | Alphabet Inc. |
| MSFT | Microsoft Corp. |
| TSLA | Tesla Inc. |
| AMZN | Amazon.com Inc. |

## Roadmap

- [ ] Consumer — reads from Kafka topic
- [ ] Storage — persists messages to a database
- [ ] Analysis — moving averages, RSI, volatility metrics
