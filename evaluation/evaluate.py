"""Evaluation of the fraud rules: precision and recall per rule (Person C).

Truth comes from the Parquet archive written by B (hdfs://hadoop-master:9000/fraud/raw).
The alerts come from HBase (table alerts_recent) or from a JSON-lines file.

Matching of an alert with a transaction: (nameOrig, step, type, amount).

Levels of evaluation (see docs/evaluation.md):
  - ACCOUNT_DRAIN, LARGE_AMOUNT: one alert = one transaction (transaction level).
  - VELOCITY: one alert = one destination account in a window, no step or type, so
    it is evaluated at account level (destination accounts that received a fraud).
  - Baseline: the isFlaggedFraud column of PaySim, at transaction level.

Usage (inside hadoop-master, Python 3.5):
    export PYSPARK_PYTHON=python3
    spark-submit /root/evaluation/evaluate.py --output-dir /root/results
    spark-submit /root/evaluation/evaluate.py --sweep          # threshold study of LARGE_AMOUNT
    spark-submit /root/evaluation/evaluate.py --alerts-file alerts.jsonl
"""
import argparse
import csv
import json
import os

ARCHIVE_PATH = "hdfs://hadoop-master:9000/fraud/raw"
TX_RULES = ["ACCOUNT_DRAIN", "LARGE_AMOUNT"]
DEFAULT_THRESHOLDS = [50000, 100000, 150000, 200000, 300000, 500000, 1000000]
MAX_FRAUD_ROWS = 1000000  # safety limit for the rows collected on the driver


# ------------------------------------------------------------------ metrics
def tx_key(name_orig, step, tx_type, amount):
    """Identity of a transaction, built the same way for archive rows and alerts."""
    return (str(name_orig), int(step), str(tx_type), round(float(amount), 2))


def ratio(numerator, denominator):
    return float(numerator) / denominator if denominator else None


def f1(precision, recall):
    if precision is None or recall is None:
        return None
    if precision + recall == 0:
        return 0.0
    return 2.0 * precision * recall / (precision + recall)


def make_row(name, level, alerts_count, flagged, true_positive, positives):
    precision = ratio(true_positive, flagged)
    recall = ratio(true_positive, positives)
    return {
        "name": name,
        "level": level,
        "alerts": alerts_count,
        "flagged": flagged,
        "true_positive": true_positive,
        "positives": positives,
        "precision": precision,
        "recall": recall,
        "f1": f1(precision, recall),
    }


def evaluate_transaction_rules(alerts, fraud_keys):
    """One row per transaction rule, plus the union of the transaction rules."""
    rows = []
    union_labels = {}  # transaction key -> isFraud label carried by the alert
    for rule in TX_RULES:
        labels = {}
        count = 0
        for alert in alerts:
            if alert.get("rule") != rule or alert.get("step") is None:
                continue
            count += 1
            key = tx_key(alert["nameOrig"], alert["step"], alert["type"], alert["amount"])
            labels[key] = max(labels.get(key, 0), int(alert["isFraud"]))
        true_positive = sum(1 for label in labels.values() if label == 1)
        rows.append(make_row(rule, "transaction", count, len(labels),
                             true_positive, len(fraud_keys)))
        for key, label in labels.items():
            union_labels[key] = max(union_labels.get(key, 0), label)
    true_positive = sum(1 for label in union_labels.values() if label == 1)
    alerts_count = sum(row["alerts"] for row in rows)
    rows.append(make_row("ACCOUNT_DRAIN + LARGE_AMOUNT", "transaction", alerts_count,
                         len(union_labels), true_positive, len(fraud_keys)))
    return rows


def evaluate_velocity(alerts, fraud_destinations):
    """VELOCITY at account level. The counted account is stored in nameOrig."""
    velocity = [a for a in alerts if a.get("rule") == "VELOCITY"]
    if not velocity:
        return []
    flagged_accounts = set(a["nameOrig"] for a in velocity)
    true_positive = len(flagged_accounts & fraud_destinations)
    return [make_row("VELOCITY", "destination account", len(velocity),
                     len(flagged_accounts), true_positive, len(fraud_destinations))]


def evaluate_baseline(flagged_keys, flagged_fraud_keys, fraud_keys):
    """isFlaggedFraud of PaySim, used as a reference."""
    return make_row("isFlaggedFraud (baseline)", "transaction", len(flagged_keys),
                    len(flagged_keys), len(flagged_fraud_keys), len(fraud_keys))


# ------------------------------------------------------------------ loading
def load_archive(spark, path):
    """Read the archive and collect only what the evaluation needs (fraud rows)."""
    from pyspark.sql import functions as F

    data = spark.read.parquet(path)
    total_rows = data.count()

    columns = ["nameOrig", "step", "type", "amount", "nameDest", "isFraud", "isFlaggedFraud"]
    interesting = (data.filter((F.col("isFraud") == 1) | (F.col("isFlaggedFraud") == 1))
                   .select(*columns).limit(MAX_FRAUD_ROWS).collect())

    fraud_keys = set()
    fraud_destinations = set()
    flagged_keys = set()
    flagged_fraud_keys = set()
    for row in interesting:
        key = tx_key(row["nameOrig"], row["step"], row["type"], row["amount"])
        if row["isFraud"] == 1:
            fraud_keys.add(key)
            fraud_destinations.add(row["nameDest"])
        if row["isFlaggedFraud"] == 1:
            flagged_keys.add(key)
            if row["isFraud"] == 1:
                flagged_fraud_keys.add(key)
    return {
        "total_rows": total_rows,
        "fraud_keys": fraud_keys,
        "fraud_destinations": fraud_destinations,
        "flagged_keys": flagged_keys,
        "flagged_fraud_keys": flagged_fraud_keys,
    }


def threshold_sweep(spark, path, thresholds):
    """What LARGE_AMOUNT would give for several thresholds, computed on the archive."""
    from pyspark.sql import functions as F

    data = spark.read.parquet(path)
    candidate = F.col("type").isin("TRANSFER", "CASH_OUT")
    expressions = [F.sum(F.when(F.col("isFraud") == 1, 1).otherwise(0)).alias("fraud")]
    for threshold in thresholds:
        above = candidate & (F.col("amount") > threshold)
        expressions.append(F.sum(F.when(above, 1).otherwise(0)).alias("flag_{}".format(threshold)))
        expressions.append(F.sum(F.when(above & (F.col("isFraud") == 1), 1).otherwise(0))
                           .alias("tp_{}".format(threshold)))
    result = data.agg(*expressions).collect()[0]

    fraud = result["fraud"] or 0
    rows = []
    for threshold in thresholds:
        flagged = result["flag_{}".format(threshold)] or 0
        true_positive = result["tp_{}".format(threshold)] or 0
        rows.append(make_row("LARGE_AMOUNT > {}".format(threshold), "transaction",
                             flagged, flagged, true_positive, fraud))
    return rows


def load_alerts_from_hbase(host, port):
    import happybase

    connection = happybase.Connection(host, port=port)
    connection.open()
    alerts = []
    try:
        for _, data in connection.table("alerts_recent").scan():
            alert = dict((column.decode("utf-8")[2:], value.decode("utf-8"))
                         for column, value in data.items())
            alerts.append(normalize_alert(alert))
    finally:
        connection.close()
    return alerts


def load_alerts_from_file(path):
    alerts = []
    with open(path) as handle:
        for line in handle:
            line = line.strip()
            if line:
                alerts.append(normalize_alert(json.loads(line)))
    return alerts


def normalize_alert(alert):
    """HBase stores strings: convert the numeric fields back."""
    result = dict(alert)
    for field, cast in (("amount", float), ("step", int), ("isFraud", int), ("tx_count", int)):
        value = result.get(field)
        result[field] = cast(float(value)) if value not in (None, "") else None
    if result.get("isFraud") is None:
        result["isFraud"] = 0
    return result


# ------------------------------------------------------------------ output
def percent(value):
    return "n/a" if value is None else "{:.2f}%".format(100.0 * value)


def to_markdown(rows):
    lines = ["| Rule | Level | Alerts | Flagged | True positives | Fraud to find | Precision | Recall | F1 |",
             "|---|---|---|---|---|---|---|---|---|"]
    for row in rows:
        lines.append("| {name} | {level} | {alerts} | {flagged} | {true_positive} | {positives} "
                     "| {p} | {r} | {f} |".format(p=percent(row["precision"]),
                                                  r=percent(row["recall"]),
                                                  f=percent(row["f1"]), **row))
    return "\n".join(lines)


def write_outputs(output_dir, name, rows):
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)
    with open(os.path.join(output_dir, name + ".md"), "w") as handle:
        handle.write(to_markdown(rows) + "\n")
    fields = ["name", "level", "alerts", "flagged", "true_positive", "positives",
              "precision", "recall", "f1"]
    with open(os.path.join(output_dir, name + ".csv"), "w") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def parse_args():
    parser = argparse.ArgumentParser(description="Precision and recall of the fraud rules")
    parser.add_argument("--archive", default=ARCHIVE_PATH)
    parser.add_argument("--hbase-host", default="hadoop-master")
    parser.add_argument("--hbase-port", type=int, default=9090)
    parser.add_argument("--alerts-file", help="JSON lines file, instead of reading HBase")
    parser.add_argument("--output-dir", help="write evaluation_results.md/.csv here")
    parser.add_argument("--sweep", action="store_true",
                        help="also study the LARGE_AMOUNT threshold on the archive")
    parser.add_argument("--thresholds", default=",".join(str(t) for t in DEFAULT_THRESHOLDS))
    return parser.parse_args()


def main():
    from pyspark.sql import SparkSession

    args = parse_args()
    spark = SparkSession.builder.appName("FraudEvaluation").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    archive = load_archive(spark, args.archive)
    if args.alerts_file:
        alerts = load_alerts_from_file(args.alerts_file)
    else:
        alerts = load_alerts_from_hbase(args.hbase_host, args.hbase_port)

    print("Transactions in the archive : {}".format(archive["total_rows"]))
    print("Frauds in the archive       : {}  (compare with MapReduce job 1: 8213 for a full replay)"
          .format(len(archive["fraud_keys"])))
    print("Alerts read                 : {}".format(len(alerts)))

    rows = evaluate_transaction_rules(alerts, archive["fraud_keys"])
    rows += evaluate_velocity(alerts, archive["fraud_destinations"])
    rows.append(evaluate_baseline(archive["flagged_keys"], archive["flagged_fraud_keys"],
                                  archive["fraud_keys"]))

    for row in rows:
        if row["true_positive"] > row["positives"] and row["level"] == "transaction":
            print("WARNING: {} has more true positives than frauds in the archive: "
                  "the archive job is probably behind the rules job.".format(row["name"]))

    print("\n" + to_markdown(rows))
    if args.output_dir:
        write_outputs(args.output_dir, "evaluation_results", rows)

    if args.sweep:
        thresholds = [int(t) for t in args.thresholds.split(",")]
        sweep_rows = threshold_sweep(spark, args.archive, thresholds)
        print("\nLARGE_AMOUNT threshold study (computed on the archive):\n")
        print(to_markdown(sweep_rows))
        if args.output_dir:
            write_outputs(args.output_dir, "threshold_sweep", sweep_rows)

    spark.stop()


if __name__ == "__main__":
    main()
