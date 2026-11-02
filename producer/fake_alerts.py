"""Send sample alerts to the Kafka topic `alerts`, in the format of docs/contracts.md.

Lets C build and test the HBase writer without waiting for B's rules job.

Usage (PowerShell, on the host):
    python fake_alerts.py --count 50
    python fake_alerts.py --count 200 --delay 0.05
"""
import argparse
import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

parser = argparse.ArgumentParser()
parser.add_argument("--count", type=int, default=20, help="number of alerts")
parser.add_argument("--delay", type=float, default=0.2, help="seconds between alerts")
parser.add_argument("--bootstrap", default="localhost:9092")
args = parser.parse_args()

producer = KafkaProducer(
    bootstrap_servers=args.bootstrap,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

RULES = ["ACCOUNT_DRAIN", "LARGE_AMOUNT", "VELOCITY"]
ACCOUNTS = ["C100000001", "C100000002", "C100000003", "C100000004", "C100000005"]


def now_iso():
    # ISO 8601 with milliseconds and UTC offset. The real format comes from
    # Spark's to_json: confirm it from a real alert and update this function.
    t = datetime.now(timezone.utc)
    return t.strftime("%Y-%m-%dT%H:%M:%S.") + "{:03d}".format(t.microsecond // 1000) + "+00:00"


def make_alert():
    rule = random.choice(RULES)
    account = random.choice(ACCOUNTS)
    if rule == "VELOCITY":
        # windowed alert: no single transaction behind it
        return {
            "rule": rule,
            "nameOrig": account,
            "nameDest": None,
            "type": None,
            "amount": round(random.uniform(500, 50000), 2),
            "step": None,
            "event_time": now_iso(),
            "isFraud": random.choice([0, 1]),
            "tx_count": random.randint(4, 12),
        }
    amount = round(random.uniform(1000, 900000), 2)
    if rule == "LARGE_AMOUNT":
        amount = round(random.uniform(200001, 900000), 2)
    return {
        "rule": rule,
        "nameOrig": account,
        "nameDest": "C2" + str(random.randint(10000000, 99999999)),
        "type": random.choice(["TRANSFER", "CASH_OUT"]),
        "amount": amount,
        "step": random.randint(1, 743),
        "event_time": now_iso(),
        "isFraud": random.choice([0, 1]),
    }


for i in range(args.count):
    alert = make_alert()
    producer.send("alerts", key=alert["nameOrig"], value=alert)
    time.sleep(args.delay)

producer.flush()
print("sent", args.count, "fake alerts to topic 'alerts'")
