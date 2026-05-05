CSP554 — Big Data Technologies
Stock Market Data Processing Pipeline Updated Execution Plan — Professor Feedback Incorporated
✓ Proposal Approved — Score: 10 / 10 | Feedback from: Joseph Rosen
Professor's Comments
• Profile your data as discussed in the CSP554EndofTermIdeas presentation, using Apache Spark or
another big data technology.
• For DynamoDB, explore: individual record insert times, bulk insertion/load times, update and delete
operation times, and query performance.
• Document the logical and physical data model you use.
Important: Share this note with all teammates as requested by Professor Rosen.
PHASE 1 — Data Collection & Kafka Setup (Updated)
Member Task Deadline
Muthu Nageswaran Download stock data + Spark profiling April 30
Anusha Venkatesh Kafka setup + Producer April 30
NEW — Data Profiling with Apache Spark (Muthu)
After downloading the 5 CSV files, add a profiling step using Apache Spark (or PySpark). This was specifically
requested in the professor's feedback referencing the CSP554EndofTermIdeas presentation.
What to profile:
→ Row counts per stock symbol
→ Null / missing value counts per column
→ Data type validation (Date as string, numeric fields as float)
→ Basic statistics: min, max, mean, std for Open, High, Low, Close, Volume
→ Value distribution summary (e.g. price range buckets)
How to implement:
→ Create data/spark_profile.py using PySpark
→ Load all 5 CSV files into a Spark DataFrame
→ Use df.describe() and df.summary() for statistics
→ Use df.filter(col.isNull()).count() for null checks
→ Save output to data/profile_report.txt
Why this matters:
→ Demonstrates use of a big data technology as required by the course theme
→ Validates data quality before it enters the Kafka pipeline
→ Output goes into the Final Report — Section 3 (Data Source)
Tip: Install PySpark with: pip install pyspark. No cluster needed — runs locally for profiling CSVs this size.
Phase 1 Updated Checklist
■ All original Phase 1 tasks completed (CSV download, Kafka, producer)
■ spark_profile.py created and runs without errors
■ profile_report.txt saved in data/ folder
■ Profile output shared with Phase 3 team for the final report
PHASE 2 — Data Processing, Consumer & DynamoDB (Updated)
Member Task Deadline
Shiva Raghav Rajasekar
DynamoDB setup + benchmarking + data model
doc May 2
Arya Shetty Kafka consumer + processing logic May 2
NEW — DynamoDB Benchmarking (Shiva)
The professor specifically asked you to measure and document DynamoDB performance across four operation
types. Create a benchmarking script and save results.
Benchmark What to measure How
Individual insert Time for a single put_item() call Use time.time() before/after, repeat 100x,
record avg/min/max
Bulk insertion Time to batch_write_item() 25
records at once
Use batch_writer(), measure total time /
records per second
Update operation Time for update_item() on existing
records
Update pct_change field, measure latency
across 100 calls
Delete operation Time for delete_item() calls Delete test records, measure latency
Query performance Time to query by symbol and by
date range
Use query() with KeyConditionExpression,
test multiple filter combos
NEW — Document the Data Model (Shiva)
Create storage/dynamodb_data_model.md with two sections:
Logical data model:
→ What entities exist: StockRecord (one row per stock per trading day)
→ Attributes: symbol, date, open, high, low, close, volume, pct_change, moving_avg_7, is_anomaly,
volume_spike
→ Relationships: each symbol has many dates (one-to-many)
→ Include a simple entity diagram in ASCII or text format
Physical data model:
→ DynamoDB table name: StockData
→ Partition key: symbol (String) — distributes records across partitions by stock
→ Sort key: date (String, format YYYY-MM-DD) — enables time-range queries
→ Why this design: allows efficient queries like 'all AAPL records from 2023'
→ Capacity mode: On-demand (free tier compatible)
→ Attribute types: all numerics stored as String (DynamoDB float limitation)
Save all benchmark results to: storage/dynamodb_benchmarks.md Save data model documentation to:
storage/dynamodb_data_model.md Share both files with Phase 3 team — they go into the final report.
Phase 2 Updated Checklist
■ All original Phase 2 tasks completed (consumer, DynamoDB writes, all 4 calculations)
■ storage/dynamodb_benchmarks.py written and executed
■ Benchmark results saved to storage/dynamodb_benchmarks.md
■ Logical data model documented
■ Physical data model documented
■ Both files shared with Phase 3 team
PHASE 3 — Analysis, Charts & Final Report (Updated)
Member Task Deadline
Om Ashokkumar Patel
Analysis script + 5 charts + incorporate new
content May 4
Souptik Sinha README + architecture diagram + final report May 4
NEW — Additions to the Final Report
The final report needs three new pieces of content pulled from Phase 1 and Phase 2 additions. Here is exactly
where each piece goes:
Section 3 — Data Source (add Spark profiling results)
After describing yfinance and the 5 stocks, add a subsection titled 'Data Profiling'. Paste or summarize
the output from data/profile_report.txt. Include: row counts, null counts, and the statistical summary
(min/max/mean for Close prices). This shows the professor you profiled the data as instructed.
Section 5 — Processing Logic (add data model)
After explaining the 4 calculations, add a subsection titled 'Data Model'. Paste content from
storage/dynamodb_data_model.md. Include the logical entity description and the physical DynamoDB
schema (partition key, sort key, attribute types). A simple ASCII table or diagram works well here.
Section 7 — Pipeline Statistics (add benchmark results)
After the existing statistics (record counts, anomaly rates), add a subsection titled 'DynamoDB
Performance Benchmarks'. Create a table showing: operation type, average latency (ms), min/max
latency, and throughput (records/sec). Pull this from storage/dynamodb_benchmarks.md.
Wait for Phase 1 and Phase 2 teams to finish their new deliverables before writing these sections. Coordinate on
the shared Google Drive folder.
Phase 3 Updated Checklist
■ All original Phase 3 tasks completed (5 charts, analysis.py, README, architecture diagram)
■ Section 3 updated with Spark profiling results from Phase 1
■ Section 5 updated with logical and physical data model from Phase 2
■ Section 7 updated with DynamoDB benchmark results table from Phase 2
■ Final report exported as PDF
■ All team members have a copy of the final report
Quick Reference — New Deliverables Only
Deliverable Owner Output file Deadline
Spark data profiling script Muthu data/spark_profile.py April 30
Spark profiling report Muthu data/profile_report.txt April 30
DynamoDB benchmarking
script Shiva
storage/dynamodb_benchmarks.
py May 2
Benchmark results doc Shiva
storage/dynamodb_benchmarks.
md May 2
Data model documentation Shiva
storage/dynamodb_data_model.
md May 2
Final report (updated) Om + Souptik Final_Report.pdf May 4
Illinois Institute of Technology | CSP554 Big Data Technologies | Spring 2025