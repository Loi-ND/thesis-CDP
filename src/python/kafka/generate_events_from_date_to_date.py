from typing import Dict, Any
import argparse
import json
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer

from utils import (
    import_initial_data_to_pos,
    import_initial_data_to_ops,
    process_ops_records,
    process_pos_records
)

import_initial_data_to_pos()
import_initial_data_to_ops()

SOURCE_TOPIC = "raw-events"
TARGET_TOPIC = "customer-events"
BATCH_SIZE = 20_000

TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

parser = argparse.ArgumentParser()
parser.add_argument(
    "--start-time",
    default="2019-10-01 00:00:00",
)
parser.add_argument(
    "--end-time",
    default="2019-11-30 23:59:59",
)
args = parser.parse_args()

start_time = datetime.strptime(args.start_time, TIME_FORMAT)
end_time = datetime.strptime(args.end_time, TIME_FORMAT)

consumer = KafkaConsumer(
    SOURCE_TOPIC,
    bootstrap_servers=["localhost:9091"],
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id="event-generator-group",
    value_deserializer=lambda value: json.loads(value),
    max_poll_records=5000,
)

producer = KafkaProducer(
    bootstrap_servers=["localhost:9091"],
    acks="all",
    retries=10,
    linger_ms=10,
    batch_size=128 * 1024,
    max_in_flight_requests_per_connection=5,
    value_serializer=lambda data: json.dumps(
        data, ensure_ascii=False
    ).encode("utf-8"),
)

batch = []
total_read = 0
total_matched = 0
total_sent = 0

def send_batch(events):
    global total_sent

    if not events:
        return

    # Submit all events asynchronously first.
    futures = [
        producer.send(TARGET_TOPIC, value=event)
        for event in events
    ]

    # Wait for broker acknowledgements.
    for future in futures:
        future.get(timeout=120)

    total_sent += len(events)
    print(
        f"Sent {len(events):,} events | "
        f"Total sent={total_sent:,}"
    )

try:
    for message in consumer:
        total_read += 1
        data: Dict = message.value
        if (data.get("event_type") == "join") | (data.get("event_type") == "purchase"):
            if data.get("system") == "pos":
                process_pos_records(data)
            elif data.get("system") == "ops":
                process_ops_records(data)


        event_time_str = data["event_time"]
        event_time = datetime.strptime(
            event_time_str, TIME_FORMAT
        )

        if start_time <= event_time <= end_time:
            batch.append(data)
            total_matched += 1

        if len(batch) >= BATCH_SIZE:
            send_batch(batch)

            # Ensure pending producer requests are completed.
            producer.flush()

            # Commit only after the batch is acknowledged.
            consumer.commit()

            batch.clear()

        if total_read % 100_000 == 0:
            print(
                f"Read={total_read:,} | "
                f"Matched={total_matched:,} | "
                f"Sent={total_sent:,}"
            )

    if batch:
        send_batch(batch)
        producer.flush()
        consumer.commit()
        batch.clear()

except KeyboardInterrupt:
    print("\nStopping consumer...")

finally:
    consumer.close()
    producer.close()

    print(
        f"Finished | Read={total_read:,} | "
        f"Matched={total_matched:,} | "
        f"Sent={total_sent:,}"
    )
