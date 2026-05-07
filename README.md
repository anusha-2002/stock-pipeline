# Stock Market Data Processing Pipeline

A big data pipeline that downloads 5 years of historical stock market data, profiles it with Apache Spark, streams it through Apache Kafka to simulate real-time processing, enriches each record with computed analytics in a Python consumer, stores all results in AWS DynamoDB, and generates statistical analysis and visualizations.

---

## Architecture

```
yfinance API → CSV files (data/)
                    ↓
          Apache Spark (PySpark)          ← data profiling, null checks, statistics
                    ↓
         Kafka Producer (producer.py)     ← reads CSVs, sends JSON at 10 rec/sec
                    ↓
      Kafka Topic: stock-market-data
                    ↓
         Kafka Consumer (consumer.py)     ← computes pct_change, moving_avg_7,
                    ↓                        is_anomaly, volume_spike
             AWS DynamoDB                 ← persists ~6,290 enriched records
          (table: StockData)
                    ↓
          analysis/analysis.py            ← pulls DynamoDB, generates 5 charts
```

---

## Technology Stack

| Component | Technology | Role |
|-----------|-----------|------|
| Data download | yfinance (Python) | Downloads 5 years of OHLCV data for 5 stocks |
| Data profiling | Apache Spark / PySpark | Row counts, null checks, min/max/mean/stddev per symbol |
| Streaming broker | Apache Kafka (Docker) | Simulates real-time market data feed |
| Streaming producer | kafka-python | Reads CSVs, publishes JSON rows to Kafka at 10 rec/sec |
| Streaming consumer | kafka-python + boto3 | Reads from Kafka, computes derived fields, writes to DynamoDB |
| Storage | AWS DynamoDB (On-Demand) | Stores ~6,290 enriched records; auto-benchmarked on startup |
| Analysis | pandas + matplotlib | Generates 5 charts and prints statistics summary |

---

## Team Members

| Name | Email | Phase |
|------|-------|-------|
| Muthu Nageswaran | mkalyaninarayanamoor@hawk.illinoistech.edu | Phase 1 |
| Anusha Venkatesh | avenkatesh2@hawk.illinoistech.edu | Phase 1 |
| Shiva Raghav Rajasekar | srajasekar@hawk.illinoistech.edu | Phase 2 |
| Arya Shetty | ashetty19@hawk.illinoistech.edu | Phase 2 |
| Om Ashokkumar Patel | opatel8@hawk.illinoistech.edu | Phase 3 |
| Souptik Sinha | ssinha21@hawk.illinoistech.edu | Phase 3 |

Illinois Institute of Technology | CSP554 Big Data Technologies | Spring 2026

---

## Project Structure

```
stock-pipeline/
├── docker-compose.yml              # Kafka + Zookeeper containers
├── data/
│   ├── download_data.py            # Phase 1A+1B: downloads CSVs then runs Spark profiling
│   ├── profile_report.txt          # Generated Spark profiling output
│   ├── AAPL.csv                    # ~1,257 rows (2020-01-02 to 2024-12-30)
│   ├── AMZN.csv
│   ├── GOOGL.csv
│   ├── MSFT.csv
│   └── TSLA.csv
├── producer/
│   └── producer.py                 # Reads CSVs, publishes each row to Kafka as JSON
├── consumer/
│   └── consumer.py                 # Kafka consumer: enriches records, writes to DynamoDB;
│                                   # also auto-generates storage/dynamodb_benchmarks.md
│                                   # and storage/dynamodb_data_model.md on startup
├── storage/
│   ├── dynamodb_benchmarks.md      # Auto-generated benchmark results (insert/update/delete/query)
│   └── dynamodb_data_model.md      # Auto-generated logical + physical data model docs
└── analysis/
    ├── analysis.py                 # Scans DynamoDB, builds DataFrame, saves 5 charts
    └── charts/
        ├── chart1_stock_prices.png
        ├── chart2_moving_average.png
        ├── chart3_anomalies.png
        ├── chart4_volume_spikes.png
        └── chart5_anomaly_counts.png
```

---

## Prerequisites

- Python 3.11+
- Docker Desktop (must be running before starting Kafka)
- AWS account with DynamoDB access (free tier is sufficient)
- AWS CLI configured with your credentials

Install all Python dependencies:

```bash
pip install yfinance pandas kafka-python boto3 matplotlib pyspark awscli
```

Configure AWS credentials:

```bash
aws configure
# Prompts for: Access Key ID, Secret Access Key, region (us-east-2), output format (json)
```

---

## How to Run (Step by Step)

Run each step in order. Steps 3 and 4 must run simultaneously in two separate terminals.

---

### Step 1 — Download Stock Data and Profile with Spark

```bash
python data/download_data.py
```

**What this does — Phase 1A (download):**
- Connects to Yahoo Finance via the `yfinance` library
- Downloads daily OHLCV (Open, High, Low, Close, Volume) data for 5 symbols: AAPL, GOOGL, MSFT, TSLA, AMZN
- Date range: 2020-01-01 to 2024-12-31 (~1,257 trading days per symbol)
- Adds a `symbol` column to each row and saves to `data/SYMBOL.csv`
- Total output: 5 CSV files, ~6,285 rows combined

**What this does — Phase 1B (Spark profiling):**
- Starts a local PySpark session (`local[*]` — uses all CPU cores)
- Reads each CSV into a Spark DataFrame
- For each symbol computes: row count, column list, null count per column, descriptive statistics (count/mean/stddev/min/max) for Open/High/Low/Close/Volume, and date range
- Unions all 5 DataFrames into a combined dataset and repeats the statistics
- Saves the full report to `data/profile_report.txt`
- Stops the Spark session

**Expected terminal output:**
```
=======================================================
  PHASE 1A: Downloading stock data via yfinance
=======================================================
Saved 1257 rows for AAPL
Saved 1257 rows for GOOGL
...
All downloads complete.

=======================================================
  PHASE 1B: Profiling data with Apache Spark
=======================================================
--- AAPL ---
  Rows    : 1257
  Nulls   : 0
  ...
Profile report saved -> data/profile_report.txt
```

---

### Step 2 — Start Kafka (Docker)

Make sure Docker Desktop is open and running, then:

```bash
docker compose up -d
```

**What this does:**
- Starts two containers defined in `docker-compose.yml`:
  - **Zookeeper** on port `2181` — Kafka's coordination service
  - **Kafka broker** on port `9092` — the message broker
- The `-d` flag runs them in the background (detached mode)

Then create the Kafka topic that the producer and consumer will use:

```bash
docker exec -it kafka kafka-topics --create \
  --topic stock-market-data \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

**What this does:**
- Creates a topic named `stock-market-data` inside the running Kafka container
- 1 partition, replication factor 1 (suitable for local development)
- The producer will publish to this topic; the consumer will read from it

---

### Step 3 — Start the Kafka Consumer (Terminal 1)

Open a new terminal and run:

```bash
python consumer/consumer.py
```

**Start this BEFORE the producer.** The consumer must be running and ready before records start arriving.

**What this does on startup (before reading any Kafka messages):**

1. **Writes `storage/dynamodb_data_model.md`** — documents the DynamoDB table schema (logical and physical model) by generating the file programmatically from the constants in the script.

2. **Runs DynamoDB benchmarks** — executes five benchmark suites against the live `StockData` table and saves results to `storage/dynamodb_benchmarks.md`:
   - **Individual insert (100 calls):** inserts 100 random synthetic records one at a time using `put_item`, measures avg/min/max latency and throughput (ops/sec)
   - **Bulk insert (250 records in batches of 25):** uses DynamoDB `batch_writer` to write 250 records in 10 batches of 25, measures total time and records/sec
   - **Update (100 calls):** calls `update_item` on the previously inserted benchmark records, updating the `pct_change` field, measures avg/min/max latency
   - **Delete (100 calls):** calls `delete_item` on benchmark records, measures avg/min/max latency
   - **Query performance:** runs a full `Query` for each of the 5 symbols (returns all records for that symbol using the partition key), plus one date-range query for AAPL 2023 only using `BETWEEN`; records latency and item count for each
   - After benchmarks, **cleans up** all `BENCH-` prefixed synthetic records so the table only contains real stock data

3. **Connects to Kafka** and starts polling the `stock-market-data` topic, waiting for messages from the producer.

**What this does per Kafka message (during streaming):**

For each JSON message received from Kafka:
- Parses the raw OHLCV fields (handles both lowercase and capitalized column names)
- Computes `pct_change = ((close - open) / open) * 100` rounded to 2 decimal places
- Maintains a rolling window of the last 7 closing prices per symbol to compute `moving_avg_7` (only populated once 7 records have been seen for that symbol)
- Maintains a rolling window of the last 7 volumes per symbol to compute a rolling average volume
- Sets `is_anomaly = True` if `abs(pct_change) > 3%`
- Sets `volume_spike = True` if `volume > 2 × rolling_avg_volume`
- Converts all float fields to `Decimal` (required by DynamoDB's `boto3` resource interface to avoid floating-point precision issues)
- Writes the enriched record to DynamoDB using `put_item`
- Prints each saved record to the terminal

**Expected terminal output:**
```
=======================================================
  PHASE 2: Kafka Consumer + DynamoDB Writer
  Table: StockData  |  Topic: stock-market-data
=======================================================
  Data model saved -> storage/dynamodb_data_model.md
  [1] Individual insert (100 records)...
  [2] Bulk insert (250 records, batches of 25)...
  [3] Update (100 update_item calls)...
  [4] Delete (100 delete_item calls)...
  [5] Query performance...
  Benchmark results saved -> storage/dynamodb_benchmarks.md

Consumer started. Waiting for records from Kafka...
Saved: {'symbol': 'AAPL', 'date': '2020-01-02', 'open': Decimal('...'), ...}
...
```

---

### Step 4 — Run the Kafka Producer (Terminal 2, same time as Step 3)

Open a second terminal and run:

```bash
python producer/producer.py
```

**What this does:**
- Connects to Kafka at `localhost:9092`
- Reads each of the 5 CSV files in order: AAPL, GOOGL, MSFT, TSLA, AMZN
- For each row in each CSV, serializes the row as a JSON object and sends it to the `stock-market-data` Kafka topic
- Waits 0.1 seconds between each message (`time.sleep(0.1)`) — this simulates a real-time market data feed at ~10 records/second
- Prints a progress update every 100 records
- After all rows are sent, calls `producer.flush()` to ensure all buffered messages are delivered, then prints a final count

**Expected terminal output:**
```
Sent 100 records (current: AAPL)
Sent 200 records (current: AAPL)
...
Sent 6200 records (current: AMZN)
All data sent successfully — 6285 total records.
```

**Note:** At 10 records/second, sending ~6,285 records takes approximately **10 minutes**. Leave both terminals running.

---

### Step 5 — Wait for All Records to Process

Watch the consumer terminal (Terminal 1). When the producer prints `All data sent successfully`, wait an additional ~30 seconds for the consumer to finish processing the last batch of messages from Kafka.

The consumer will continue printing `Saved: {...}` lines until all records are written to DynamoDB.

You can verify in the **AWS Console → DynamoDB → Tables → StockData → Explore items** that ~6,285 items are present.

---

### Step 6 — Run Analysis and Generate Charts

```bash
python analysis/analysis.py
```

**What this does:**
- Connects to DynamoDB and performs a **paginated Scan** of the entire `StockData` table — DynamoDB returns at most 1 MB per call, so the script loops using `LastEvaluatedKey` until all pages are retrieved
- Loads all items into a pandas DataFrame
- Converts `Decimal` / string columns to float using `pd.to_numeric`
- Parses the `date` column to `datetime` and sorts by `(symbol, date)`
- Generates and saves 5 charts to `analysis/charts/`:

| Chart | File | What it shows |
|-------|------|---------------|
| 1 | `chart1_stock_prices.png` | Closing price over time for all 5 symbols (2020–2024) |
| 2 | `chart2_moving_average.png` | AAPL actual close vs. 7-day moving average |
| 3 | `chart3_anomalies.png` | All 5 stocks with anomaly days highlighted as scatter points |
| 4 | `chart4_volume_spikes.png` | Trading volume for all stocks with volume spike days highlighted |
| 5 | `chart5_anomaly_counts.png` | Bar chart: total anomaly count per stock symbol |

- Prints a statistics summary to the terminal

**Expected terminal output:**
```
Connecting to DynamoDB...
Successfully loaded 6285 records.

===================================
   PHASE 3: ANALYSIS SUMMARY
===================================
Total Records Processed: 6285
Total Price Anomalies:   XXX
Total Volume Spikes:     XXX
Most Volatile Stock:     TSLA (XXX anomalies)
===================================
Charts saved successfully in analysis/charts/
```

---

## DynamoDB Benchmarks

The benchmarks are run automatically by `consumer/consumer.py` on startup and written to `storage/dynamodb_benchmarks.md`. The table below shows a representative sample result:

| Operation | Avg Latency | Throughput |
|-----------|-------------|-----------|
| Individual insert (100 calls) | ~22 ms | ~45 ops/sec |
| Bulk insert — batch of 25 (250 total) | ~1 ms/record | ~1,000 records/sec |
| Update (100 calls) | ~20 ms | — |
| Delete (100 calls) | ~19 ms | — |
| Query — full symbol (~1,300 records) | ~136 ms | — |
| Query — AAPL 2023 range (~250 records) | ~38 ms | — |

**Key observations:**
- Batch writes (`batch_writer`) are ~20× faster per record than individual `put_item` calls because DynamoDB processes up to 25 items per network round trip.
- Full-symbol queries are fast (~136 ms for 1,300 records) because all records for a symbol live on the same partition (co-located by the partition key `symbol`).
- Date-range queries using `BETWEEN` on the sort key are significantly faster (~38 ms) when results are narrowed to a single year.

---

## DynamoDB Data Model

Full documentation is auto-generated by `consumer/consumer.py` to `storage/dynamodb_data_model.md`.

### Logical Model — StockRecord Entity

One `StockRecord` represents one trading day for one stock ticker. It stores raw market data from yfinance plus four fields computed by the Kafka consumer.

```
┌─────────────────────────────────────────────────────────┐
│                      StockRecord                        │
├─────────────────┬───────────┬──────────────────────────┤
│ Attribute       │ Type      │ Description               │
├─────────────────┼───────────┼──────────────────────────┤
│ symbol  (PK)    │ String    │ Stock ticker (AAPL etc.)  │
│ date    (SK)    │ String    │ Trading date YYYY-MM-DD   │
│ open            │ Decimal   │ Opening price ($)         │
│ high            │ Decimal   │ Day's high price ($)      │
│ low             │ Decimal   │ Day's low price ($)       │
│ close           │ Decimal   │ Closing price ($)         │
│ volume          │ Integer   │ Shares traded             │
│ pct_change      │ Decimal   │ (close-open)/open × 100   │
│ moving_avg_7    │ Decimal   │ 7-day rolling avg close   │
│ is_anomaly      │ Boolean   │ |pct_change| > 3%         │
│ volume_spike    │ Boolean   │ volume > 2× 7-day avg vol │
└─────────────────┴───────────┴──────────────────────────┘
```

One-to-many relationship: each symbol has ~1,257 records (one per trading day).

### Physical Model — Table Configuration

| Property | Value |
|----------|-------|
| Table name | `StockData` |
| Partition key (PK) | `symbol` (String) |
| Sort key (SK) | `date` (String, ISO 8601 `YYYY-MM-DD`) |
| Capacity mode | On-Demand (PAY_PER_REQUEST) |
| AWS Region | `us-east-2` |
| Total items | ~6,285 |

**Why this key design:**
- `symbol` as the partition key co-locates all records for one stock in one DynamoDB partition, making full-symbol queries a single-partition read (no scatter-gather).
- `date` as the sort key in ISO 8601 format means lexicographic sort = chronological sort, enabling efficient `BETWEEN` date-range queries without a secondary index.
- The composite key `(symbol, date)` is naturally unique — one trading day per stock — so DynamoDB enforces uniqueness for free.

### Access Patterns

| Access pattern | DynamoDB operation | Key condition |
|---------------|-------------------|---------------|
| All records for one stock | `Query` | `PK = symbol` |
| Records for a stock in a date range | `Query` | `PK = symbol AND SK BETWEEN d1 AND d2` |
| Single day for one stock | `GetItem` | `PK = symbol, SK = date` |
| Full table (for analysis) | `Scan` (paginated) | — |

---

## Spark Data Profiling

`data/download_data.py` runs a PySpark profiling job immediately after downloading the CSVs. The report is saved to `data/profile_report.txt`.

**What the profiling covers per symbol:**
- Row count
- Column list and data types
- Null count per column (using `isNull()` + `isnan()` for numeric columns)
- Descriptive statistics: `count`, `mean`, `stddev`, `min`, `max` for Open/High/Low/Close/Volume
- Date range (min and max date in that symbol's dataset)

**Combined dataset summary (across all 5 symbols):**
- Total row count
- Aggregated descriptive statistics
- Combined null check per column
- Final data quality verdict

**Profiling results summary:**

| Symbol | Rows | Nulls | Date Range |
|--------|------|-------|-----------|
| AAPL | 1,257 | 0 | 2020-01-02 to 2024-12-30 |
| GOOGL | 1,257 | 0 | 2020-01-02 to 2024-12-30 |
| MSFT | 1,257 | 0 | 2020-01-02 to 2024-12-30 |
| TSLA | 1,257 | 0 | 2020-01-02 to 2024-12-30 |
| AMZN | 1,257 | 0 | 2020-01-02 to 2024-12-30 |
| **Total** | **6,285** | **0** | — |

Data quality verdict: **PASS** — dataset is complete with zero nulls across all columns and all symbols.

---

## Stocks Covered

| Symbol | Company | Date Range | Rows |
|--------|---------|-----------|------|
| AAPL | Apple Inc. | 2020-01-02 – 2024-12-30 | 1,257 |
| GOOGL | Alphabet Inc. | 2020-01-02 – 2024-12-30 | 1,257 |
| MSFT | Microsoft Corp. | 2020-01-02 – 2024-12-30 | 1,257 |
| TSLA | Tesla Inc. | 2020-01-02 – 2024-12-30 | 1,257 |
| AMZN | Amazon.com Inc. | 2020-01-02 – 2024-12-30 | 1,257 |

---

## Kafka Setup Details

| Setting | Value |
|---------|-------|
| Topic | `stock-market-data` |
| Bootstrap server | `localhost:9092` |
| Partitions | 1 |
| Replication factor | 1 |
| Producer send rate | 10 records/sec (0.1s sleep between sends) |
| Consumer group ID | `stock-consumer-group` |
| Consumer offset reset | `earliest` (replays all messages if restarted) |

---

## Phase Summary

### Phase 1 — Data Collection, Spark Profiling & Kafka Setup
**Members:** Muthu Nageswaran, Anusha Venkatesh

- Downloaded ~6,285 rows of historical OHLCV data (2020–2024) for 5 symbols via yfinance; saved as CSVs in `data/`
- Ran Apache Spark (PySpark) data profiling in the same script (`download_data.py`): validated 0 nulls, computed descriptive statistics per symbol and combined; report saved to `data/profile_report.txt`
- Set up Kafka and Zookeeper using Docker Compose; created Kafka topic `stock-market-data`
- Wrote `producer.py` — reads CSVs, serializes rows as JSON, streams to Kafka at 10 records/sec

### Phase 2 — Kafka Consumer, DynamoDB Storage & Benchmarking
**Members:** Shiva Raghav Rajasekar, Arya Shetty

- Created AWS DynamoDB table `StockData` (partition key: `symbol`, sort key: `date`, On-Demand capacity)
- Wrote `consumer.py` — reads from Kafka and computes four derived fields: `pct_change`, `moving_avg_7`, `is_anomaly`, `volume_spike`
- Used `Decimal` type for all numeric DynamoDB writes to avoid float precision issues
- On startup, auto-runs five DynamoDB benchmark suites (individual insert, bulk insert, update, delete, query) and writes results to `storage/dynamodb_benchmarks.md`
- On startup, auto-generates `storage/dynamodb_data_model.md` documenting the logical and physical data model

### Phase 3 — Analysis, Charts & Documentation
**Members:** Om Ashokkumar Patel, Souptik Sinha

- Wrote `analysis.py` — paginated DynamoDB Scan, pandas DataFrame construction, 5 matplotlib charts
- Generated charts: stock price history, AAPL moving average, anomaly detection, volume spikes, anomaly count per stock
- Completed README and all project documentation

---

## Shutting Down

Stop Kafka and Zookeeper:

```bash
docker compose down
```

---

## References

- Apache Kafka Documentation: https://kafka.apache.org/documentation/
- yfinance Python Library: https://pypi.org/project/yfinance/
- AWS DynamoDB Developer Guide: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/
- Apache Spark / PySpark: https://spark.apache.org/docs/latest/api/python/
- boto3 (AWS SDK for Python): https://boto3.amazonaws.com/v1/documentation/api/latest/index.html
- pandas Documentation: https://pandas.pydata.org/docs/
- matplotlib Documentation: https://matplotlib.org/stable/contents.html
