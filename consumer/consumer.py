import json
import os
import random
import time
from decimal import Decimal
from collections import defaultdict, deque

import boto3
from kafka import KafkaConsumer

TOPIC_NAME = "stock-market-data"
BOOTSTRAP_SERVER = "localhost:9092"
TABLE_NAME = "StockData"
AWS_REGION = "us-east-2"

STORAGE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "storage")
os.makedirs(STORAGE_DIR, exist_ok=True)

BENCHMARKS_PATH = os.path.join(STORAGE_DIR, "dynamodb_benchmarks.md")
DATA_MODEL_PATH = os.path.join(STORAGE_DIR, "dynamodb_data_model.md")

recent_prices = defaultdict(lambda: deque(maxlen=7))
recent_volumes = defaultdict(lambda: deque(maxlen=7))

dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
table = dynamodb.Table(TABLE_NAME)


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_value(data, lower_key, upper_key):
    return data.get(lower_key, data.get(upper_key))


def to_decimal(value):
    if value is None:
        return None
    return Decimal(str(value))


def process_record(data):
    symbol = get_value(data, "symbol", "Symbol")
    date = get_value(data, "date", "Date")

    open_price = float(get_value(data, "open", "Open"))
    high_price = float(get_value(data, "high", "High"))
    low_price = float(get_value(data, "low", "Low"))
    close_price = float(get_value(data, "close", "Close"))
    volume = int(get_value(data, "volume", "Volume"))

    pct_change = round(((close_price - open_price) / open_price) * 100, 2)

    recent_prices[symbol].append(close_price)
    recent_volumes[symbol].append(volume)

    moving_avg_7 = None
    if len(recent_prices[symbol]) == 7:
        moving_avg_7 = round(sum(recent_prices[symbol]) / 7, 2)

    avg_volume = sum(recent_volumes[symbol]) / len(recent_volumes[symbol])

    is_anomaly = abs(pct_change) > 3
    volume_spike = volume > 2 * avg_volume

    return {
        "symbol": symbol,
        "date": str(date),
        "open": to_decimal(open_price),
        "high": to_decimal(high_price),
        "low": to_decimal(low_price),
        "close": to_decimal(close_price),
        "volume": volume,
        "pct_change": to_decimal(pct_change),
        "moving_avg_7": to_decimal(moving_avg_7) if moving_avg_7 is not None else None,
        "is_anomaly": is_anomaly,
        "volume_spike": volume_spike,
    }


# ── DynamoDB Benchmarks ───────────────────────────────────────────────────────

def _make_bench_item(symbol, date_str):
    return {
        "symbol": symbol,
        "date": "BENCH-" + date_str,
        "open": to_decimal(round(random.uniform(100, 400), 2)),
        "high": to_decimal(round(random.uniform(100, 400), 2)),
        "low": to_decimal(round(random.uniform(100, 400), 2)),
        "close": to_decimal(round(random.uniform(100, 400), 2)),
        "volume": random.randint(1_000_000, 100_000_000),
        "pct_change": to_decimal(round(random.uniform(-5, 5), 2)),
        "moving_avg_7": to_decimal(round(random.uniform(100, 400), 2)),
        "is_anomaly": random.choice([True, False]),
        "volume_spike": random.choice([True, False]),
    }


def _bench_individual_insert(n=100):
    print(f"  [1] Individual insert ({n} records)...")
    symbols = ["AAPL", "AMZN", "GOOGL", "MSFT", "TSLA"]
    latencies = []
    for i in range(n):
        item = _make_bench_item(random.choice(symbols), f"2019-{i//30+1:02d}-{i%28+1:02d}")
        t0 = time.perf_counter()
        table.put_item(Item=item)
        latencies.append((time.perf_counter() - t0) * 1000)
    avg = sum(latencies) / len(latencies)
    return {"avg_ms": round(avg, 1), "min_ms": round(min(latencies), 1),
            "max_ms": round(max(latencies), 1), "throughput": round(1000 / avg, 1)}


def _bench_bulk_insert(total=250, batch=25):
    print(f"  [2] Bulk insert ({total} records, batches of {batch})...")
    symbols = ["AAPL", "AMZN", "GOOGL", "MSFT", "TSLA"]
    items = [_make_bench_item(random.choice(symbols), f"2018-{(i//28)+1:02d}-{i%28+1:02d}") for i in range(total)]
    latencies = []
    for start in range(0, total, batch):
        chunk = items[start:start + batch]
        t0 = time.perf_counter()
        with table.batch_writer() as bw:
            for item in chunk:
                bw.put_item(Item=item)
        latencies.append((time.perf_counter() - t0) * 1000)
    total_ms = sum(latencies)
    return {"total_ms": round(total_ms, 0), "avg_batch_ms": round(total_ms / len(latencies), 0),
            "records_per_sec": round(total / (total_ms / 1000), 1)}


def _bench_update(n=100):
    print(f"  [3] Update ({n} update_item calls)...")
    latencies = []
    for i in range(n):
        t0 = time.perf_counter()
        try:
            table.update_item(
                Key={"symbol": "AAPL", "date": f"BENCH-2019-{i//30+1:02d}-{i%28+1:02d}"},
                UpdateExpression="SET pct_change = :v",
                ExpressionAttributeValues={":v": to_decimal(round(random.uniform(-5, 5), 2))},
            )
        except Exception:
            pass
        latencies.append((time.perf_counter() - t0) * 1000)
    avg = sum(latencies) / len(latencies)
    return {"avg_ms": round(avg, 1), "min_ms": round(min(latencies), 1), "max_ms": round(max(latencies), 1)}


def _bench_delete(n=100):
    print(f"  [4] Delete ({n} delete_item calls)...")
    latencies = []
    for i in range(n):
        t0 = time.perf_counter()
        try:
            table.delete_item(Key={"symbol": "AAPL",
                                   "date": f"BENCH-2019-{i//30+1:02d}-{i%28+1:02d}"})
        except Exception:
            pass
        latencies.append((time.perf_counter() - t0) * 1000)
    avg = sum(latencies) / len(latencies)
    return {"avg_ms": round(avg, 1), "min_ms": round(min(latencies), 1), "max_ms": round(max(latencies), 1)}


def _clean_bench_records(total=350):
    """Delete all BENCH- prefixed records inserted during benchmarking."""
    symbols = ["AAPL", "AMZN", "GOOGL", "MSFT", "TSLA"]
    for sym in symbols:
        from boto3.dynamodb.conditions import Key
        resp = table.query(
            KeyConditionExpression=Key("symbol").eq(sym) & Key("date").begins_with("BENCH-")
        )
        for item in resp.get("Items", []):
            table.delete_item(Key={"symbol": item["symbol"], "date": item["date"]})


def _bench_query():
    print("  [5] Query performance...")
    from boto3.dynamodb.conditions import Key
    results = {}
    for symbol in ["AAPL", "AMZN", "GOOGL", "MSFT", "TSLA"]:
        t0 = time.perf_counter()
        resp = table.query(KeyConditionExpression=Key("symbol").eq(symbol))
        elapsed = round((time.perf_counter() - t0) * 1000, 1)
        results[symbol] = {"ms": elapsed, "count": resp.get("Count", 0)}
        print(f"      {symbol}: {elapsed}ms  ({results[symbol]['count']} items)")

    t0 = time.perf_counter()
    resp = table.query(
        KeyConditionExpression=Key("symbol").eq("AAPL") & Key("date").between("2023-01-01", "2023-12-31")
    )
    elapsed = round((time.perf_counter() - t0) * 1000, 1)
    results["AAPL_2023"] = {"ms": elapsed, "count": resp.get("Count", 0)}
    print(f"      AAPL(2023 range): {elapsed}ms  ({results['AAPL_2023']['count']} items)")
    return results


def run_benchmarks():
    print("\n" + "=" * 55)
    print("  DynamoDB Performance Benchmarks")
    print(f"  Table: {TABLE_NAME}  |  Region: {AWS_REGION}")
    print("=" * 55)

    r1 = _bench_individual_insert(100)
    r2 = _bench_bulk_insert(250, 25)
    r3 = _bench_update(100)
    r4 = _bench_delete(100)
    r5 = _bench_query()

    print("  Cleaning up benchmark test records...")
    _clean_bench_records()

    # Write results to file
    lines = []
    lines.append(f"# DynamoDB Performance Benchmarks")
    lines.append(f"")
    lines.append(f"**Table:** `{TABLE_NAME}` | **Region:** `{AWS_REGION}` | **Capacity mode:** On-Demand  ")
    lines.append(f"**Generated by:** `consumer/consumer.py` on startup")
    lines.append(f"")
    lines.append(f"---")
    lines.append(f"")
    lines.append(f"## 1. Individual Record Insert (100 calls)")
    lines.append(f"")
    lines.append(f"| Metric | Value |")
    lines.append(f"|--------|-------|")
    lines.append(f"| Average latency | {r1['avg_ms']} ms |")
    lines.append(f"| Min latency | {r1['min_ms']} ms |")
    lines.append(f"| Max latency | {r1['max_ms']} ms |")
    lines.append(f"| Throughput | {r1['throughput']} ops/sec |")
    lines.append(f"")
    lines.append(f"## 2. Bulk Insert / Batch Write (250 records, batches of 25)")
    lines.append(f"")
    lines.append(f"| Metric | Value |")
    lines.append(f"|--------|-------|")
    lines.append(f"| Total time | {r2['total_ms']} ms |")
    lines.append(f"| Avg per batch (25 items) | {r2['avg_batch_ms']} ms |")
    lines.append(f"| Throughput | {r2['records_per_sec']} records/sec |")
    lines.append(f"")
    lines.append(f"## 3. Update Operation (100 calls)")
    lines.append(f"")
    lines.append(f"| Metric | Value |")
    lines.append(f"|--------|-------|")
    lines.append(f"| Average latency | {r3['avg_ms']} ms |")
    lines.append(f"| Min latency | {r3['min_ms']} ms |")
    lines.append(f"| Max latency | {r3['max_ms']} ms |")
    lines.append(f"")
    lines.append(f"## 4. Delete Operation (100 calls)")
    lines.append(f"")
    lines.append(f"| Metric | Value |")
    lines.append(f"|--------|-------|")
    lines.append(f"| Average latency | {r4['avg_ms']} ms |")
    lines.append(f"| Min latency | {r4['min_ms']} ms |")
    lines.append(f"| Max latency | {r4['max_ms']} ms |")
    lines.append(f"")
    lines.append(f"## 5. Query Performance")
    lines.append(f"")
    lines.append(f"| Query | Latency | Records returned |")
    lines.append(f"|-------|---------|-----------------|")
    for sym in ["AAPL", "AMZN", "GOOGL", "MSFT", "TSLA"]:
        lines.append(f"| All {sym} records | {r5[sym]['ms']} ms | {r5[sym]['count']} |")
    lines.append(f"| AAPL records, 2023 only | {r5['AAPL_2023']['ms']} ms | {r5['AAPL_2023']['count']} |")
    lines.append(f"")
    lines.append(f"## Summary")
    lines.append(f"")
    lines.append(f"| Operation | Avg Latency | Throughput |")
    lines.append(f"|-----------|-------------|-----------|")
    lines.append(f"| Individual insert | {r1['avg_ms']} ms | {r1['throughput']} ops/sec |")
    lines.append(f"| Bulk insert (batch of 25) | {round(r2['avg_batch_ms']/25, 1)} ms/record | {r2['records_per_sec']} records/sec |")
    lines.append(f"| Update | {r3['avg_ms']} ms | — |")
    lines.append(f"| Delete | {r4['avg_ms']} ms | — |")
    lines.append(f"| Query (full symbol) | ~{sum(r5[s]['ms'] for s in ['AAPL','AMZN','GOOGL','MSFT','TSLA'])//5} ms | — |")

    with open(BENCHMARKS_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"\n  Benchmark results saved -> {BENCHMARKS_PATH}")


# ── Data Model Documentation ──────────────────────────────────────────────────

def write_data_model():
    lines = []
    lines.append("# DynamoDB Data Model")
    lines.append("")
    lines.append(f"**Table:** `{TABLE_NAME}` | **Region:** `{AWS_REGION}`  ")
    lines.append("**Generated by:** `consumer/consumer.py` on startup")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## Logical Data Model")
    lines.append("")
    lines.append("### Entity: StockRecord")
    lines.append("")
    lines.append("One `StockRecord` represents one trading day for one stock symbol.")
    lines.append("It contains raw OHLCV data from yfinance plus four computed fields")
    lines.append("added by the Kafka consumer.")
    lines.append("")
    lines.append("```")
    lines.append("┌─────────────────────────────────────────────────────────┐")
    lines.append("│                      StockRecord                        │")
    lines.append("├─────────────────┬───────────┬──────────────────────────┤")
    lines.append("│ Attribute       │ Type      │ Description               │")
    lines.append("├─────────────────┼───────────┼──────────────────────────┤")
    lines.append("│ symbol  (PK)    │ String    │ Stock ticker (AAPL etc.)  │")
    lines.append("│ date    (SK)    │ String    │ Trading date YYYY-MM-DD   │")
    lines.append("│ open            │ Decimal   │ Opening price ($)         │")
    lines.append("│ high            │ Decimal   │ Day's high price ($)      │")
    lines.append("│ low             │ Decimal   │ Day's low price ($)       │")
    lines.append("│ close           │ Decimal   │ Closing price ($)         │")
    lines.append("│ volume          │ Integer   │ Shares traded             │")
    lines.append("│ pct_change      │ Decimal   │ (close-open)/open × 100   │")
    lines.append("│ moving_avg_7    │ Decimal   │ 7-day rolling avg close   │")
    lines.append("│ is_anomaly      │ Boolean   │ |pct_change| > 3%         │")
    lines.append("│ volume_spike    │ Boolean   │ volume > 2× 7-day avg vol │")
    lines.append("└─────────────────┴───────────┴──────────────────────────┘")
    lines.append("```")
    lines.append("")
    lines.append("### Relationship")
    lines.append("")
    lines.append("```")
    lines.append("Symbol (1) ──── (M) StockRecord")
    lines.append("  AAPL              AAPL / 2020-01-02")
    lines.append("  AMZN              AAPL / 2020-01-03")
    lines.append("  GOOGL             ...")
    lines.append("  MSFT              AAPL / 2024-12-30")
    lines.append("  TSLA")
    lines.append("```")
    lines.append("")
    lines.append("Each symbol has one record per trading day (~1,258 per symbol, ~6,290 total).")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## Physical Data Model")
    lines.append("")
    lines.append("### Table Configuration")
    lines.append("")
    lines.append("| Property | Value |")
    lines.append("|----------|-------|")
    lines.append(f"| Table name | `{TABLE_NAME}` |")
    lines.append("| Partition key (PK) | `symbol` (String) |")
    lines.append("| Sort key (SK) | `date` (String, `YYYY-MM-DD`) |")
    lines.append("| Capacity mode | On-Demand |")
    lines.append(f"| AWS Region | `{AWS_REGION}` |")
    lines.append("| Billing | PAY_PER_REQUEST (free-tier compatible) |")
    lines.append("")
    lines.append("### Attribute Types as Stored")
    lines.append("")
    lines.append("| Attribute | DynamoDB type | Python type | Notes |")
    lines.append("|-----------|---------------|-------------|-------|")
    lines.append("| symbol | S (String) | str | Partition key |")
    lines.append("| date | S (String) | str | Sort key, ISO 8601 |")
    lines.append("| open | N (Number) | Decimal | Decimal avoids float precision errors |")
    lines.append("| high | N (Number) | Decimal | Same as above |")
    lines.append("| low | N (Number) | Decimal | Same as above |")
    lines.append("| close | N (Number) | Decimal | Same as above |")
    lines.append("| volume | N (Number) | int | Whole number |")
    lines.append("| pct_change | N (Number) | Decimal | Rounded to 2 dp |")
    lines.append("| moving_avg_7 | N (Number) | Decimal | NULL for first 6 records per symbol |")
    lines.append("| is_anomaly | BOOL | bool | True if |pct_change| > 3% |")
    lines.append("| volume_spike | BOOL | bool | True if volume > 2× 7-day avg |")
    lines.append("")
    lines.append("### Why This Key Design")
    lines.append("")
    lines.append("- **PK = `symbol`**: Co-locates all records for one stock on one partition.")
    lines.append("  A query like 'all AAPL records' is a single-partition read.")
    lines.append("- **SK = `date`**: ISO 8601 format means lexicographic = chronological order.")
    lines.append("  Date-range queries (`BETWEEN '2023-01-01' AND '2023-12-31'`) are efficient.")
    lines.append("- **`(symbol, date)` is unique**: One trading day per stock, enforced by DynamoDB.")
    lines.append("")
    lines.append("### Access Patterns")
    lines.append("")
    lines.append("| Access pattern | Operation | Key condition |")
    lines.append("|---------------|-----------|---------------|")
    lines.append("| All records for a symbol | Query | PK = symbol |")
    lines.append("| Symbol + date range | Query | PK = symbol AND SK BETWEEN d1 AND d2 |")
    lines.append("| Full table scan (analysis) | Scan | — (paginated) |")
    lines.append("| Single record | GetItem | PK = symbol, SK = date |")
    lines.append("")
    lines.append("### Schema Diagram")
    lines.append("")
    lines.append("```")
    lines.append("DynamoDB Table: StockData")
    lines.append("─────────────────────────────────────────────────────────")
    lines.append("  PK (symbol)  │  SK (date)    │  open  │ close  │ is_anomaly")
    lines.append("─────────────────────────────────────────────────────────")
    lines.append("  AAPL         │  2020-01-02   │ 297.15 │ 300.35 │ False")
    lines.append("  AAPL         │  2020-01-03   │ 297.15 │ 297.43 │ False")
    lines.append("  ...          │  ...          │  ...   │  ...   │ ...")
    lines.append("  AMZN         │  2020-01-02   │  93.75 │  94.90 │ False")
    lines.append("  ...          │  ...          │  ...   │  ...   │ ...")
    lines.append("  TSLA         │  2024-12-30   │ 421.06 │ 403.84 │ True")
    lines.append("─────────────────────────────────────────────────────────")
    lines.append("  Total: ~6,290 items across 5 partitions")
    lines.append("```")

    with open(DATA_MODEL_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"  Data model saved -> {DATA_MODEL_PATH}")


# ── Main consumer loop ────────────────────────────────────────────────────────

def main():
    print("=" * 55)
    print("  PHASE 2: Kafka Consumer + DynamoDB Writer")
    print(f"  Table: {TABLE_NAME}  |  Topic: {TOPIC_NAME}")
    print("=" * 55)

    # Run benchmarks and write data model docs before consuming
    write_data_model()
    run_benchmarks()

    print("\nConsumer started. Waiting for records from Kafka...")

    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVER,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="stock-consumer-group",
    )

    for message in consumer:
        try:
            processed = process_record(message.value)
            table.put_item(Item=processed)
            print("Saved:", processed)
        except Exception as e:
            print("Error processing record:", e)
            print("Bad message:", message.value)


if __name__ == "__main__":
    main()
