"""Alert writer: reads alerts from the Kafka topic `alerts` and stores each one
in two HBase tables (alerts_by_account and alerts_recent).

Runs inside the hadoop-master container.
Formats and row keys follow docs/contracts.md.

Usage:
    python3 alert_writer.py
    python3 alert_writer.py --idle-timeout 10      # stop after 10 s without messages
"""
import argparse
import calendar
import json
import time
from datetime import datetime

import happybase
from kafka import KafkaConsumer

MAX_LONG = 9223372036854775807

# Format of the contract: yyyy-MM-dd'T'HH:mm:ss.SSS'Z' (UTC).
# The second one covers a timestamp without milliseconds.
EVENT_TIME_FORMATS = ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"]

# Columns of family d. Values that are None are NOT written (see build_columns).
COLUMNS = ["rule", "nameDest", "type", "amount", "step",
           "event_time", "isFraud", "tx_count", "counted_by"]


def parse_event_time(value):
    """Convert the alert event_time (ISO 8601, UTC, 'Z') to epoch milliseconds."""
    text = value.strip()
    # fake_alerts.py still writes "+00:00": treat it as UTC 'Z'.
    if text.endswith("+00:00"):
        text = text[:-6] + "Z"
    for fmt in EVENT_TIME_FORMATS:
        try:
            parsed = datetime.strptime(text, fmt)
        except ValueError:
            continue
        return calendar.timegm(parsed.timetuple()) * 1000 + parsed.microsecond // 1000
    raise ValueError("Unsupported event_time format: {!r}".format(value))


def reversed_timestamp(epoch_ms):
    """Newest alert -> smallest number -> sorts first in a scan. 19-digit string."""
    return "{:019d}".format(MAX_LONG - epoch_ms)


def build_row_keys(alert):
    """Return (row key for alerts_by_account, row key for alerts_recent).

    The rule is part of both keys (contract, section 5): one transaction that
    matches two rules gives two alerts with the same account and timestamp, and
    they must not overwrite each other.
    """
    rev = reversed_timestamp(parse_event_time(alert["event_time"]))
    account = alert["nameOrig"]
    rule = alert["rule"]

    key_by_account = "{}_{}_{}".format(account, rev, rule)
    key_recent = "{}_{}_{}".format(rev, account, rule)
    return key_by_account.encode("utf-8"), key_recent.encode("utf-8")


def build_columns(alert):
    """Build the HBase cells. Null fields are skipped: a missing column means
    'no value' (VELOCITY alerts have no nameDest, type or step)."""
    columns = {}
    for name in COLUMNS:
        value = alert.get(name)
        if value is not None:
            columns[("d:" + name).encode("utf-8")] = str(value).encode("utf-8")
    return columns


def parse_record(raw_value):
    """Decode one Kafka message into (key1, key2, columns). Raises on bad input."""
    alert = json.loads(raw_value.decode("utf-8"))
    if not isinstance(alert, dict):
        raise ValueError("Alert is not a JSON object")
    for field in ("rule", "nameOrig", "event_time"):
        if not alert.get(field):
            raise ValueError("Missing required field: {}".format(field))
    key_by_account, key_recent = build_row_keys(alert)
    return key_by_account, key_recent, build_columns(alert)


def parse_args():
    parser = argparse.ArgumentParser(description="Kafka alerts -> HBase writer")
    parser.add_argument("--topic", default="alerts")
    parser.add_argument("--bootstrap", default="kafka:29092")
    parser.add_argument("--group-id", default="alert-writer")
    parser.add_argument("--hbase-host", default="hadoop-master")
    parser.add_argument("--hbase-port", type=int, default=9090)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--idle-timeout", type=int, default=0,
                        help="stop after N seconds without messages (0 = run forever)")
    return parser.parse_args()


def main():
    args = parse_args()

    connection = happybase.Connection(args.hbase_host, port=args.hbase_port)
    connection.open()
    table_by_account = connection.table("alerts_by_account")
    table_recent = connection.table("alerts_recent")
    print("Connected to HBase at {}:{}".format(args.hbase_host, args.hbase_port))

    # Offsets are committed manually, only after the rows are written to HBase.
    # After a crash, the writer resumes from the last committed offset.
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap,
        group_id=args.group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    print("Listening on topic '{}' ({})".format(args.topic, args.bootstrap))

    written = 0
    skipped = 0
    last_activity = time.time()

    try:
        while True:
            polled = consumer.poll(timeout_ms=1000, max_records=args.batch_size)
            if not polled:
                if args.idle_timeout and time.time() - last_activity > args.idle_timeout:
                    print("No message for {} s, stopping.".format(args.idle_timeout))
                    break
                continue
            last_activity = time.time()

            batch_written = 0
            # transaction=True: if something fails, nothing is sent, and since the
            # offsets are not committed the messages will be read again.
            with table_by_account.batch(transaction=True) as batch_a, \
                    table_recent.batch(transaction=True) as batch_r:
                for records in polled.values():
                    for record in records:
                        try:
                            key_a, key_r, columns = parse_record(record.value)
                        except (ValueError, KeyError, TypeError) as error:
                            skipped += 1
                            print("Skipped offset {}: {}".format(record.offset, error))
                            continue
                        batch_a.put(key_a, columns)
                        batch_r.put(key_r, columns)
                        batch_written += 1

            consumer.commit()
            written += batch_written
            print("Batch: {} written | total written: {} | skipped: {}".format(
                batch_written, written, skipped))
    except KeyboardInterrupt:
        print("Stopped by user.")
    finally:
        consumer.close()
        connection.close()
        print("Done. Total written: {} | skipped: {}".format(written, skipped))


if __name__ == "__main__":
    main()