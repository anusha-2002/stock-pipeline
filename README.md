# Stock Market Data Processing Pipeline

A big data pipeline that ingests 5 years of historical stock market data, streams it through Apache Kafka to simulate real-time processing, enriches each record with computed analytics in a Python consumer, stores results in AWS DynamoDB, and generates statistical analysis and visualizations.

## Architecture

```
yfinance API → data/ (CSVs) → Kafka Producer → [Kafka Topic: stock-market-data]
     → Kafka Consumer + Processor → AWS DynamoDB → Analysis + Charts
```

**Component breakdown:**

| Component | Technology | Role |
|-----------|-----------|------|
| Data source | yfinance (Python) | Downloads 5 years of OHLCV data for 5 stocks |
| Data profiling | Apache Spark / PySpark | Validates and profiles raw CSV data |
| Streaming | Apache Kafka (Docker) | Simulates real-time market data feed |
| Processing | Python consumer | Computes pct_change, moving avg, anomaly flags |
| Storage | AWS DynamoDB | Persists enriched records; benchmarked for performance |
| Analysis | pandas + matplotlib | Generates 5 charts and statistical summary |

## Team Members

| Name | Email | Phase |
|------|-------|-------|
| Muthu Nageswaran | mkalyaninarayanamoor@hawk.illinoistech.edu | Phase 1 |
| Anusha Venkatesh | avenkatesh2@hawk.illinoistech.edu | Phase 1 |
| Shiva Raghav Rajasekar | srajasekar@hawk.illinoistech.edu | Phase 2 |
| Arya Shetty | ashetty19@hawk.illinoistech.edu | Phase 2 |
| Om Ashokkumar Patel | opatel8@hawk.illinoistech.edu | Phase 3 |
| Souptik Sinha | ssinha21@hawk.illinoistech.edu | Phase 3 |

Illinois Institute of Technology | CSP554 Big Data Technologies | Spring 2025

## Prerequisites

- Python 3.11+
- Docker Desktop (running)
- AWS account with DynamoDB access (free tier sufficient)
- AWS CLI configured (`aws configure`)

Install all Python dependencies:

```bash
pip install yfinance pandas kafka-python boto3 matplotlib pyspark awscli
```

## Project Structure

```
stock-pipeline/
├── docker-compose.yml          # Kafka + Zookeeper setup
├── data/
│   ├── download_data.py        # Downloads historical stock data via yfinance
│   ├── spark_profile.py        # PySpark data profiling script
│   ├── profile_report.txt      # Spark profiling output (row counts, stats, nulls)
│   ├── AAPL.csv
│   ├── AMZN.csv
│   ├── GOOGL.csv
│   ├── MSFT.csv
│   └── TSLA.csv
├── producer/
│   └── producer.py             # Reads CSVs and publishes rows to Kafka topic
├── consumer/
│   └── consumer.py             # Reads from Kafka, computes analytics, writes to DynamoDB
├── storage/
│   ├── dynamodb_benchmarks.py  # Measures DynamoDB operation latencies
│   ├── dynamodb_benchmarks.md  # Benchmark results and analysis
│   └── dynamodb_data_model.md  # Logical and physical data model documentation
└── analysis/
    ├── analysis.py             # Pulls DynamoDB data, generates 5 charts + statistics
    └── charts/
        ├── chart1_stock_prices.png
        ├── chart2_moving_average.png
        ├── chart3_anomalies.png
        ├── chart4_volume_spikes.png
        └── chart5_anomaly_counts.png
```

## Setup Instructions

### 1. Clone the repository

```bash
git clone <repo-url>
cd stock-pipeline
```

### 2. Install Python libraries

```bash
pip install yfinance pandas kafka-python boto3 matplotlib pyspark awscli
```

### 3. Configure AWS credentials

```bash
aws configure
# Enter your Access Key ID, Secret Access Key, region (us-east-2), output format (json)
```

### 4. Start Kafka

Make sure Docker Desktop is running, then:

```bash
docker compose up -d
```

This starts Zookeeper on port `2181` and Kafka on port `9092`.

### 5. Create the Kafka topic

```bash
docker exec -it kafka kafka-topics --create --topic stock-market-data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

## How to Run

Run each step in order:

**Step 1 — Download stock data:**
```bash
python data/download_data.py
```
Downloads ~1,258 rows per symbol (6,290 total) as CSV files in `data/`.

**Step 2 — Profile data with Spark (professor requirement):**
```bash
python data/spark_profile.py
```
Runs PySpark profiling. Output saved to `data/profile_report.txt`.

**Step 3 — Start the Kafka consumer** (Terminal 1):
```bash
python consumer/consumer.py
```
Connects to DynamoDB and waits for messages from Kafka.

**Step 4 — Run the Kafka producer** (Terminal 2, simultaneously with Step 3):
```bash
python producer/producer.py
```
Streams all 6,290 CSV rows as JSON messages to Kafka at ~10 records/second.

**Step 5 — Wait for all records to process.**
The consumer prints each enriched record as it saves to DynamoDB. When the producer prints "All data sent successfully", wait ~30 more seconds for the consumer to finish.

**Step 6 — Run analysis:**
```bash
python analysis/analysis.py
```
Pulls all data from DynamoDB, generates 5 charts saved to `analysis/charts/`, and prints the statistics summary.

**Step 7 (optional) — Run DynamoDB benchmarks:**
```bash
python storage/dynamodb_benchmarks.py
```
Measures insert/update/delete/query latencies. See `storage/dynamodb_benchmarks.md` for results.

## Expected Output

When everything works correctly you should see:

- `data/` folder: 5 CSV files (~1,258 rows each)
- `data/profile_report.txt`: Spark profiling report with row counts, null checks, and statistics
- DynamoDB `StockData` table: ~6,290 items visible in the AWS Console
- `analysis/charts/`: 5 PNG charts
- Terminal output from `analysis.py`:
  ```
  ===================================
     PHASE 3: ANALYSIS SUMMARY
  ===================================
  Total Records Processed: 6290
  Total Price Anomalies:   XXX
  Total Volume Spikes:     XXX
  Most Volatile Stock:     TSLA (XXX anomalies)
  ===================================
  ```

## Kafka Topic

| Topic | Description |
|-------|-------------|
| `stock-market-data` | OHLCV records for AAPL, GOOGL, MSFT, TSLA, AMZN, sent as JSON |

## Stocks Covered

| Symbol | Company | Date Range |
|--------|---------|-----------|
| AAPL | Apple Inc. | 2020-01-02 – 2024-12-30 |
| GOOGL | Alphabet Inc. | 2020-01-02 – 2024-12-30 |
| MSFT | Microsoft Corp. | 2020-01-02 – 2024-12-30 |
| TSLA | Tesla Inc. | 2020-01-02 – 2024-12-30 |
| AMZN | Amazon.com Inc. | 2020-01-02 – 2024-12-30 |

## Phase Summary

### Phase 1 — Data Collection & Kafka Setup
**Members:** Muthu Nageswaran, Anusha Venkatesh | **Deadline:** April 30

- Downloaded ~6,290 rows of historical OHLCV data (2020–2024) for 5 symbols via yfinance
- Profiled data using Apache Spark (PySpark) — 0 nulls, all types validated, report in `data/profile_report.txt`
- Set up Kafka and Zookeeper using Docker Compose
- Created Kafka topic `stock-market-data`
- Wrote and tested `producer.py` — streams all CSV records into Kafka as JSON at 10 records/sec

### Phase 2 — Data Processing, Consumer & DynamoDB
**Members:** Shiva Raghav Rajasekar, Arya Shetty | **Deadline:** May 2

- Created AWS DynamoDB table `StockData` (partition key: `symbol`, sort key: `date`)
- Wrote `consumer.py` that reads from Kafka and computes: `pct_change`, `moving_avg_7`, `is_anomaly`, `volume_spike`
- Saved all enriched records to DynamoDB using `boto3`
- Benchmarked DynamoDB operations (insert, bulk, update, delete, query) — results in `storage/dynamodb_benchmarks.md`
- Documented logical and physical data model in `storage/dynamodb_data_model.md`

### Phase 3 — Analysis, Charts & Final Report
**Members:** Om Ashokkumar Patel, Souptik Sinha | **Deadline:** May 4

- Wrote `analysis.py` to pull all DynamoDB records and generate 5 charts
- Generated: stock price history, AAPL moving average, anomaly detection, volume spikes, anomaly count per stock
- Printed pipeline statistics summary
- Completed README (this file) and project documentation

## References

- Apache Kafka Documentation: https://kafka.apache.org/documentation/
- yfinance Python Library: https://pypi.org/project/yfinance/
- AWS DynamoDB Developer Guide: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/
- Apache Spark / PySpark: https://spark.apache.org/docs/latest/api/python/
- boto3 (AWS SDK for Python): https://boto3.amazonaws.com/v1/documentation/api/latest/index.html
- pandas Documentation: https://pandas.pydata.org/docs/
- matplotlib Documentation: https://matplotlib.org/stable/contents.html

## Shutting Down

```bash
docker compose down
```
