import argparse
import csv
import json
import time
from datetime import datetime,timezone
from kafka import KafkaProducer

parser = argparse.ArgumentParser()
parser.add_argument("--file", default="PS_20174392719_1491204439457_log.csv")
parser.add_argument("--rate", type=int, default=100, help="events per second")
parser.add_argument("--limit", type=int, default=0, help="max events (0 = all)")
args = parser.parse_args()

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

INT_FIELDS = ["step", "isFraud", "isFlaggedFraud"]
FLOAT_FIELDS = ["amount", "oldbalanceOrg", "newbalanceOrig",
                "oldbalanceDest", "newbalanceDest"]

sent = 0
start = time.time()

with open(args.file, newline="") as f:
    for row in csv.DictReader(f):
        for k in INT_FIELDS:
            row[k] = int(row[k])
        for k in FLOAT_FIELDS:
            row[k] = float(row[k])
        row["event_time"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")

        producer.send("transactions", key=row["nameOrig"], value=row)
        sent += 1

        if args.limit and sent >= args.limit:
            break

        # simple rate control: stay on schedule
        ahead = sent / float(args.rate) - (time.time() - start)
        if ahead > 0:
            time.sleep(ahead)

        if sent % 1000 == 0:
            print("sent", sent)

producer.flush()
print("done:", sent, "events in", round(time.time() - start, 1), "s")