import argparse
import csv
import json
import time
from pathlib import Path

from kafka import KafkaProducer


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-server", default="localhost:9092")
    parser.add_argument("--topic", default="rides-live")
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--delay", type=float, default=0.01)
    return parser.parse_args()


def main():
    args = parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_server,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    path = Path("data/rides.csv")
    sent = 0

    with path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)

        for row in reader:
            if sent >= args.limit:
                break

            producer.send(
                args.topic,
                key=row["ride_id"],
                value=row,
            )

            sent += 1

            if sent % 100 == 0:
                print(f"sent {sent} events")

            time.sleep(args.delay)

    producer.flush()
    producer.close()

    print(f"finished sending {sent} events")


if __name__ == "__main__":
    main()