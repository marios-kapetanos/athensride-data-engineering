import csv
import random
from datetime import datetime, timedelta
from pathlib import Path


DATA_DIR = Path("data")
OUTPUT = DATA_DIR / "rides.csv"
STATIONS = [
    "Syntagma",
    "Monastiraki",
    "Omonia",
    "Panepistimio",
    "Akropoli",
    "Gazi",
    "Piraeus",
    "Kifisia",
]
USER_TYPES = ["subscriber", "casual"]


def random_timestamp(start):
    return start + timedelta(minutes=random.randint(0, 60 * 24 * 30))


def main():
    random.seed(42)
    DATA_DIR.mkdir(exist_ok=True)
    start = datetime(2026, 1, 1, 8, 0, 0)

    with OUTPUT.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "ride_id",
                "bike_id",
                "station_start",
                "station_end",
                "started_at",
                "ended_at",
                "user_type",
            ],
        )
        writer.writeheader()

        for i in range(1, 5001):
            started_at = random_timestamp(start)
            duration = random.randint(3, 90)
            ended_at = started_at + timedelta(minutes=duration)
            station_start = random.choice(STATIONS)
            station_end = random.choice([s for s in STATIONS if s != station_start])

            writer.writerow(
                {
                    "ride_id": f"r-{i:05d}",
                    "bike_id": f"b-{random.randint(1, 300):03d}",
                    "station_start": station_start,
                    "station_end": station_end,
                    "started_at": started_at.strftime("%Y-%m-%d %H:%M:%S"),
                    "ended_at": ended_at.strftime("%Y-%m-%d %H:%M:%S"),
                    "user_type": random.choice(USER_TYPES),
                }
            )

    print(f"Wrote {OUTPUT}")


if __name__ == "__main__":
    main()

