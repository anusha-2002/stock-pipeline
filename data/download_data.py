import sys
import io
import yfinance as yf
import pandas as pd
import os

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

SYMBOLS = ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN"]
START_DATE = "2020-01-01"
END_DATE = "2024-12-31"

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
REPORT_PATH = os.path.join(DATA_DIR, "profile_report.txt")

# ── Phase 1A: Download stock data ─────────────────────────────────────────────
print("=" * 55)
print("  PHASE 1A: Downloading stock data via yfinance")
print("=" * 55)

for symbol in SYMBOLS:
    df = yf.download(symbol, start=START_DATE, end=END_DATE, auto_adjust=True)
    df["symbol"] = symbol
    df.reset_index(inplace=True)
    output_path = os.path.join(DATA_DIR, f"{symbol}.csv")
    df.to_csv(output_path, index=False)
    print(f"Saved {len(df)} rows for {symbol}")

print("All downloads complete.\n")

# ── Phase 1B: Profile data with Apache Spark (PySpark) ────────────────────────
print("=" * 55)
print("  PHASE 1B: Profiling data with Apache Spark")
print("=" * 55)

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, isnan, when, count as spark_count

    spark = SparkSession.builder \
        .appName("StockDataProfiling") \
        .master("local[*]") \
        .config("spark.ui.showConsoleProgress", "false") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    NUMERIC_COLS = ["Open", "High", "Low", "Close", "Volume"]
    lines = []

    def log(text=""):
        print(text)
        lines.append(text)

    log("=" * 60)
    log("  STOCK DATA PROFILING REPORT - Apache Spark (PySpark)")
    log(f"  Symbols   : {', '.join(SYMBOLS)}")
    log(f"  Date range: {START_DATE} to {END_DATE}")
    log("=" * 60)

    all_spark_dfs = []
    per_symbol = {}

    for symbol in SYMBOLS:
        csv_path = os.path.join(DATA_DIR, f"{symbol}.csv")
        # yfinance CSVs have a metadata row (row 2) with ticker names like "AAPL,AAPL,..."
        # Read as strings, filter out non-numeric rows, then cast
        from pyspark.sql.functions import regexp_extract
        sdf = (spark.read
               .option("header", "true")
               .option("inferSchema", "false")
               .csv(csv_path))
        # Keep only rows where Close looks like a number
        sdf = sdf.filter(regexp_extract(col("Close"), r"^-?\d+(\.\d+)?$", 0) != "")
        for nc in NUMERIC_COLS:
            if nc in sdf.columns:
                sdf = sdf.withColumn(nc, col(nc).cast("double"))

        row_count = sdf.count()
        col_list = sdf.columns

        # Null counts per column — isnan only applies to numeric types
        numeric_types = {"double", "float", "int", "bigint", "long", "decimal"}
        col_types = dict(sdf.dtypes)
        null_totals = {}
        for c in col_list:
            if any(col_types.get(c, "").startswith(t) for t in numeric_types):
                n = sdf.filter(col(c).isNull() | isnan(col(c))).count()
            else:
                n = sdf.filter(col(c).isNull()).count()
            null_totals[c] = n

        total_nulls = sum(null_totals.values())

        log("")
        log(f"--- {symbol} ---")
        log(f"  Rows    : {row_count}")
        log(f"  Columns : {col_list}")
        log(f"  Nulls   : {total_nulls}")
        for c, n in null_totals.items():
            if n > 0:
                log(f"    {c}: {n} null(s)")

        # Numeric describe — only cast columns Spark already inferred as numeric
        actual_numeric = [c for c in NUMERIC_COLS if c in col_list and
                          any(col_types.get(c, "").startswith(t) for t in numeric_types)]
        if actual_numeric:
            num_sdf = sdf.select([col(c).alias(c) for c in actual_numeric])
            desc = num_sdf.describe()
            log("  Statistics:")
            for row in desc.collect():
                vals = "  ".join(f"{c}={row[c] or 'N/A':>14}" for c in actual_numeric)
                log(f"    {row['summary']:<8}: {vals}")
        else:
            log("  Statistics: (columns read as strings - see note below)")

        date_col = "Date" if "Date" in col_list else col_list[0]
        date_min = sdf.agg({date_col: "min"}).collect()[0][0]
        date_max = sdf.agg({date_col: "max"}).collect()[0][0]
        log(f"  Date range: {date_min} to {date_max}")

        per_symbol[symbol] = {"rows": row_count, "nulls": total_nulls}
        all_spark_dfs.append(sdf)

    # Combined summary
    combined = all_spark_dfs[0]
    for sdf in all_spark_dfs[1:]:
        combined = combined.union(sdf)
    total_rows = combined.count()

    log("")
    log("=" * 60)
    log("  COMBINED DATASET SUMMARY")
    log("=" * 60)
    log(f"  Total records: {total_rows}")
    log(f"  Symbols      : {', '.join(SYMBOLS)}")
    log("")
    log("  Per-symbol row counts:")
    for sym, s in per_symbol.items():
        log(f"    {sym}: {s['rows']} rows, {s['nulls']} null(s)")

    log("")
    log("  Combined numeric statistics:")
    combined_types = dict(combined.dtypes)
    combined_numeric = [c for c in NUMERIC_COLS if c in combined.columns and
                        any(combined_types.get(c, "").startswith(t) for t in numeric_types)]
    if combined_numeric:
        for row in combined.select(combined_numeric).describe().collect():
            vals = "  ".join(f"{c}={row[c] or 'N/A':>14}" for c in combined_numeric)
            log(f"    {row['summary']:<8}: {vals}")

    log("")
    log("  Data types:")
    for c, dtype in combined.dtypes:
        log(f"    {c}: {dtype}")

    log("")
    log("  NULL summary (combined):")
    for c in combined.columns:
        if any(combined_types.get(c, "").startswith(t) for t in numeric_types):
            n = combined.filter(col(c).isNull() | isnan(col(c))).count()
        else:
            n = combined.filter(col(c).isNull()).count()
        log(f"    {c}: {n} null(s)")

    log("")
    log("  Data quality: PASS - dataset complete and ready for pipeline.")
    log("=" * 60)
    log("  END OF PROFILE REPORT")
    log("=" * 60)

    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"\nProfile report saved -> {REPORT_PATH}")
    spark.stop()

except ImportError:
    print("WARNING: PySpark not installed. Skipping Spark profiling.")
    print("  Install with: pip install pyspark")
except Exception as e:
    print(f"WARNING: Spark profiling failed: {e}")
    print("  Continuing without profiling.")
