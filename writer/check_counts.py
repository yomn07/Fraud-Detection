"""Compare the number of alerts in Kafka with the number of rows in HBase.
Used for the end-to-end test and for A's resilience test.

Usage (inside hadoop-master):
    python3 check_counts.py
"""
import argparse

import happybase
from kafka import KafkaConsumer, TopicPartition


def count_rows(table):
    """Count rows with a scan that returns only the row keys."""
    count = 0
    for _ in table.scan(filter=b"FirstKeyOnlyFilter() AND KeyOnlyFilter()"):
        count += 1
    return count


def count_kafka_messages(bootstrap, topic):
    consumer = KafkaConsumer(bootstrap_servers=bootstrap)
    partitions = [TopicPartition(topic, p) for p in consumer.partitions_for_topic(topic)]
    begin = consumer.beginning_offsets(partitions)
    end = consumer.end_offsets(partitions)
    consumer.close()
    return sum(end[p] - begin[p] for p in partitions)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default="alerts")
    parser.add_argument("--bootstrap", default="kafka:29092")
    parser.add_argument("--hbase-host", default="hadoop-master")
    parser.add_argument("--hbase-port", type=int, default=9090)
    args = parser.parse_args()

    kafka_count = count_kafka_messages(args.bootstrap, args.topic)

    connection = happybase.Connection(args.hbase_host, port=args.hbase_port)
    connection.open()
    by_account = count_rows(connection.table("alerts_by_account"))
    recent = count_rows(connection.table("alerts_recent"))
    connection.close()

    print("Kafka messages in '{}': {}".format(args.topic, kafka_count))
    print("HBase rows alerts_by_account : {}".format(by_account))
    print("HBase rows alerts_recent     : {}".format(recent))

    if kafka_count == by_account == recent:
        print("OK: counts match.")
    elif kafka_count == recent and by_account < recent:
        print("alerts_by_account has fewer rows than alerts_recent: it probably holds "
              "rows written with the old key (without the rule). Drop both tables "
              "(hbase/create_tables.txt), create them again and replay with a new --group-id.")
    else:
        print("MISMATCH: check skipped messages or writer lag.")


if __name__ == "__main__":
    main()