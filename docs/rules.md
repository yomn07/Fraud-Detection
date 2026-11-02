# Rules and Spark jobs 

This file documents the three Spark Structured Streaming jobs and the fraud rules.
The message formats are defined in `contracts.md`, which stays the single source of truth.

## Jobs

All jobs run inside `hadoop-master`, one at a time because the machine is small.

    export PYSPARK_PYTHON=python3
    spark-submit --master local[2] \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 \
      JOB_FILE.py 2>&1 | grep -v INFO

| Application | File | Reads | Writes | Checkpoint |
|---|---|---|---|---|
| FraudArchive | spark/archive_job.py | topic `transactions` | Parquet in `hdfs://hadoop-master:9000/fraud/raw` | `/fraud/checkpoints/archive` |
| FraudRules | spark/rules_job.py | topic `transactions` | topic `alerts` | `/fraud/checkpoints/rules` |
| FraudVelocity | spark/velocity_job.py | topic `transactions` (from latest) | topic `alerts` | `/fraud/checkpoints/velocity` |

Each job writes a micro-batch every 5 to 10 seconds and resumes from its checkpoint after a stop or a crash.

## Rules

Every rule only looks at TRANSFER and CASH_OUT, because fraud only appears in these two types.

| Rule | Condition | Parameter | Why |
|---|---|---|---|
| ACCOUNT_DRAIN | newbalanceOrig = 0 and amount = oldbalanceOrg | none | Typical PaySim fraud signature: the sender account is emptied |
| LARGE_AMOUNT | amount above the threshold | 200000 (constant at the top of rules_job.py) | Large transfers are risky; shows the precision and recall trade-off |
| VELOCITY | more than N transactions for one destination account in one window | N = 5 then changed intp N=10, window 1 minute, watermark 30 seconds (constants at the top of velocity_job.py) | Many receivers repeat heavily |

A transaction matching two rules produces two alerts, one per rule.

### Why velocity counts by destination

MapReduce job 2 on the full file (6,362,620 rows) found 2,722,362 distinct destination accounts, 459,658 of them with at least 2 transactions, and a maximum of 113 for one account. The busiest destinations in that result are the same ones that raise velocity alerts. The result is in `top_destinations.txt`.

### Velocity alert layout

See section 4 of `contracts.md`. In short: the field `nameOrig` holds the counted destination account, `amount` is the sum over the window, `event_time` is the end of the window, `isFraud` is the maximum in the window, `tx_count` is added, and `nameDest`, `type` and `step` are absent.

## Why alerts go through Kafka

Spark 2.2 has no foreachBatch, so the jobs cannot write to HBase directly. They publish alerts to the `alerts` topic and a separate writer stores them in HBase. This also decouples processing from storage: if the writer stops, alerts wait in Kafka.

## First observations (to be replaced by the evaluation )

Sample: first 2000 PaySim rows, replayed several times, so some rows are counted more than once.

- ACCOUNT_DRAIN: the alerts read all had isFraud = 1 (high precision, but it cannot catch frauds that do not empty the account).
- LARGE_AMOUNT: almost all alerts have isFraud = 0, so there are many false positives at 200000.
- VELOCITY: 37 alerts from one minute window, only 1 with isFraud = 1. N = 5 is low and will be raised after the evaluation. N was raised to for now 10

## Limits

- event_time is the producer sending time, not a PaySim date. A one-minute window therefore counts rows replayed in one minute, and velocity results depend on the producer `--rate`.
- A closed velocity window is written only when a later micro-batch with new data runs, so the last window of a test needs one more small batch of data to appear.
- The system is rule-based: thresholds are tuned by hand and it cannot learn new patterns.