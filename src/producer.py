import argparse
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from config import LOCAL_BOOTSTRAP_SERVERS, RAW_TOPIC


def iter_rows(data_dir, year=None, station=None):
    years = [Path(data_dir) / year] if year else sorted(Path(data_dir).iterdir())
    for year_dir in years:
        if not year_dir.is_dir():
            continue
        files = [year_dir / station] if station else sorted(year_dir.glob("*.csv"))
        for path in files:
            if not path.exists():
                continue
            with path.open(newline="", encoding="utf-8") as f:
                next(f, None)
                for row_number, line in enumerate(f, start=2):
                    yield {
                        "source_file": path.as_posix(),
                        "year": year_dir.name,
                        "station_file": path.name,
                        "row_number": row_number,
                        "raw_line": line.rstrip("\r\n"),
                        "ingested_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                    }


def ensure_topic(bootstrap_servers, topic):
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers, client_id="noaa-producer-admin")
    try:
        admin.create_topics([NewTopic(name=topic, num_partitions=1, replication_factor=1)])
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--bootstrap-servers", default=LOCAL_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=RAW_TOPIC)
    parser.add_argument("--delay-ms", type=int, default=0)
    parser.add_argument("--max-records", type=int)
    parser.add_argument("--year")
    parser.add_argument("--station")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("kafka").setLevel(logging.WARNING)
    ensure_topic(args.bootstrap_servers, args.topic)
    producer = KafkaProducer(bootstrap_servers=args.bootstrap_servers)

    sent = 0
    for event in iter_rows(args.data_dir, args.year, args.station):
        producer.send(args.topic, json.dumps(event, separators=(",", ":")).encode("utf-8"))
        sent += 1
        if sent % 1000 == 0:
            logging.info("published %s records", sent)
        if args.max_records and sent >= args.max_records:
            break
        if args.delay_ms:
            time.sleep(args.delay_ms / 1000)

    producer.flush()
    producer.close()
    logging.info("published %s records total", sent)


if __name__ == "__main__":
    main()
