
import json
import argparse
from pathlib import Path
from decimal import Decimal

import ijson
from kafka import KafkaProducer


def read_json_messages(file_path):
    """Đọc JSONL hoặc JSON array theo streaming."""
    path = Path(file_path)

    if not path.is_file():
        raise FileNotFoundError(f"File not found: {path}")

    # Xác định định dạng file
    with path.open("rb") as f:
        first_char = b""
        while True:
            char = f.read(1)
            if not char:
                break
            if not char.isspace():
                first_char = char
                break

    if first_char == b"[":
        # JSON array: đọc từng phần tử
        with path.open("rb") as f:
            yield from ijson.items(f, "item")
    else:
        # JSONL: mỗi dòng là một JSON object
        with path.open("r", encoding="utf-8") as f:
            for line_number, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue

                try:
                    yield json.loads(
                        line,
                        parse_float=Decimal,
                    )
                except json.JSONDecodeError as exc:
                    raise ValueError(
                        f"Invalid JSON at line {line_number}: {exc}"
                    ) from exc


def json_default(obj):
    """Chuyển Decimal thành kiểu JSON có thể serialize."""
    if isinstance(obj, Decimal):
        # Lưu ý: chuyển thành float có thể mất độ chính xác
        return float(obj)

    raise TypeError(
        f"Object of type {type(obj).__name__} "
        "is not JSON serializable"
    )


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--file",
        default="/workspace/kafka/data/all_events_sorted.json",
    )
    parser.add_argument("--topic", default="raw-events")
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9091",
    )
    parser.add_argument("--batch-size", type=int, default=1000_000)

    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        acks="all",
        retries=10,
        linger_ms=20,
        batch_size=65536,
        max_in_flight_requests_per_connection=5,
    )

    sent = 0
    pending = []

    try:
        for record in read_json_messages(args.file):
            # Serialize thủ công trước khi gửi
            value = json.dumps(
                record,
                ensure_ascii=False,
                default=json_default,
            ).encode("utf-8")

            pending.append(
                producer.send(args.topic, value=value)
            )

            if len(pending) >= args.batch_size:
                for future in pending:
                    future.get(timeout=120)

                sent += len(pending)
                pending.clear()

                print(f"Successfully sent {sent} messages")

        # Gửi và xác nhận các message còn lại
        for future in pending:
            future.get(timeout=120)

        sent += len(pending)
        producer.flush()

        print(f"Completed. Successfully sent {sent} messages.")

    finally:
        producer.close()


if __name__ == "__main__":
    main()
