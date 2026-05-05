import boto3
import pandas as pd
import matplotlib.pyplot as plt
import os
from decimal import Decimal

# 1. SETUP & CONNECTION
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CHARTS_DIR = os.path.join(SCRIPT_DIR, 'charts')
os.makedirs(CHARTS_DIR, exist_ok=True)

dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
table = dynamodb.Table('StockData')

def get_all_data():
    all_items = []
    
    print("Connecting to DynamoDB...")
    # DynamoDB scan with pagination to handle 6,000+ records
    response = table.scan()
    all_items.extend(response.get('Items', []))
    
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        all_items.extend(response.get('Items', []))
    
    # Convert to Pandas DataFrame
    df = pd.DataFrame(all_items)
    
    # Convert numeric columns from Decimal/String to float
    numeric_cols = ['close', 'open', 'high', 'low', 'volume', 'pct_change', 'moving_avg_7']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Format dates and sort
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(['symbol', 'date'])
    
    return df

# 2. LOAD DATA
df = get_all_data()
print(f"Successfully loaded {len(df)} records.")

# 3. VISUALIZATIONS

# CHART 1: Stock Price Over Time
plt.figure(figsize=(12, 6))
for symbol in df['symbol'].unique():
    stock_df = df[df['symbol'] == symbol]
    plt.plot(stock_df['date'], stock_df['close'], label=symbol)
plt.title('Stock Price Over Time (2020-2024)')
plt.xlabel('Date')
plt.ylabel('Closing Price ($)')
plt.legend()
plt.grid(True)
plt.savefig(os.path.join(CHARTS_DIR, 'chart1_stock_prices.png'), dpi=150)
plt.close() # Prevents the "one chart at a time" blocking issue

# CHART 2: 7-Day Moving Average (AAPL Example)
plt.figure(figsize=(12, 6))
aapl_df = df[df['symbol'] == 'AAPL']
plt.plot(aapl_df['date'], aapl_df['close'], label='Actual Close', alpha=0.4)
plt.plot(aapl_df['date'], aapl_df['moving_avg_7'], label='7-Day MA', color='red', linewidth=2)
plt.title('AAPL: Price vs 7-Day Moving Average')
plt.legend()
plt.grid(True)
plt.savefig(os.path.join(CHARTS_DIR, 'chart2_moving_average.png'), dpi=150)
plt.close()

# CHART 3: Anomaly Detection
plt.figure(figsize=(12, 6))
for symbol in df['symbol'].unique():
    subset = df[df['symbol'] == symbol]
    plt.plot(subset['date'], subset['close'], alpha=0.2, color='gray')
    anomalies = subset[subset['is_anomaly'] == True]
    plt.scatter(anomalies['date'], anomalies['close'], label=f'{symbol} Anomaly', s=15)
plt.title('Stock Price Anomalies Detected')
plt.legend(loc='upper left', bbox_to_anchor=(1, 1))
plt.savefig(os.path.join(CHARTS_DIR, 'chart3_anomalies.png'), dpi=150, bbox_inches='tight')
plt.close()

# CHART 4: Volume Spikes
plt.figure(figsize=(12, 6))
for symbol in df['symbol'].unique():
    subset = df[df['symbol'] == symbol]
    spikes = subset[subset['volume_spike'] == True]
    plt.scatter(subset['date'], subset['volume'], alpha=0.1)
    plt.scatter(spikes['date'], spikes['volume'], s=20, label=f'{symbol} Spike')
plt.title('Trading Volume Spikes')
plt.ylabel('Volume')
plt.legend(loc='upper left', bbox_to_anchor=(1, 1))
plt.savefig(os.path.join(CHARTS_DIR, 'chart4_volume_spikes.png'), dpi=150, bbox_inches='tight')
plt.close()

# CHART 5: Anomaly Count by Stock
plt.figure(figsize=(10, 6))
anomaly_counts = df[df['is_anomaly'] == True]['symbol'].value_counts()
anomaly_counts.plot(kind='bar', color='skyblue', edgecolor='black')
plt.title('Total Anomalies Detected per Stock')
plt.ylabel('Number of Anomalies')
plt.xticks(rotation=45)
plt.savefig(os.path.join(CHARTS_DIR, 'chart5_anomaly_counts.png'), dpi=150, bbox_inches='tight')
plt.close()

# 4. FINAL STATISTICS SUMMARY
print("\n" + "="*35)
print("   PHASE 3: ANALYSIS SUMMARY")
print("="*35)
print(f"Total Records Processed: {len(df)}")
print(f"Total Price Anomalies:   {df['is_anomaly'].sum()}")
print(f"Total Volume Spikes:     {df['volume_spike'].sum()}")

if not anomaly_counts.empty:
    most_volatile = anomaly_counts.idxmax()
    print(f"Most Volatile Stock:     {most_volatile} ({anomaly_counts.max()} anomalies)")

print("="*35)
print(f"Charts saved successfully in {CHARTS_DIR}")