import json
import time
from datetime import datetime, timedelta
from kafka import KafkaConsumer, KafkaProducer

from utils import (
    import_initial_data_to_pos,
    import_initial_data_to_ops
)

import_initial_data_to_pos()
import_initial_data_to_ops()

START_TIME = datetime(2019, 10, 1, 0, 0, 0)
CURRENT_TIME = datetime.now()

SOURCE_TOPIC = "raw-events"
TARGET_TOPIC = "customer-events"

TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

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
    buffer_memory=64 * 1024 * 1024,
    max_in_flight_requests_per_connection=5,
    value_serializer=lambda data: json.dumps(
        data, ensure_ascii=False
    ).encode("utf-8"),
)

total_read = 0
total_sent = 0

try:
    for message in consumer:
        event = message.value
        event_time = datetime.strptime(
            event["event_time"], TIME_FORMAT
        )

        while event_time > START_TIME + timedelta(
            seconds=(datetime.now() - CURRENT_TIME).total_seconds() * 1000
        ):
            time.sleep(0.05)

        producer.send(TARGET_TOPIC, value=event)
        producer.flush()
        total_sent += 1
        total_read += 1
        if total_sent % 100 == 0:
            print(
                f"Sent {total_sent:,} events | "
                f"Read {total_read:,} events"
            )



except KeyboardInterrupt:
    print("Stopping event generation...")
finally:
    consumer.close()
    producer.close()

    print(
        f"Finished | Read={total_read:,} | "
        f"Sent={total_sent:,}"
    )