import json
from decimal import Decimal
from collections import defaultdict, deque

import boto3
from kafka import KafkaConsumer


TOPIC_NAME = "stock-market-data"
BOOTSTRAP_SERVER = "localhost:9092"
TABLE_NAME = "StockData"
AWS_REGION = "us-east-2"

recent_prices = defaultdict(lambda: deque(maxlen=7))
recent_volumes = defaultdict(lambda: deque(maxlen=7))

dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
table = dynamodb.Table(TABLE_NAME)


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


def main():
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVER,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="stock-consumer-group",
    )

    print("Consumer started. Waiting for records...")

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