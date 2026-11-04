# Real-Time Fraud Detection on a Big Data Architecture

Big Data mini-project (II3). A pipeline that replays mobile-payment transactions as a live stream, archives them, flags suspicious ones in real time with rules, stores the alerts in a NoSQL database and shows them on a dashboard. Batch statistics on the full history are computed with MapReduce, and the rules are evaluated with precision and recall.

**Repository:** https://github.com/yomn07/Fraud-Detection
**Team:** [member A: yomn07], [member B: aicha], [member C: ghassen]

## Goal

A payment system receives thousands of transactions per second, and fraud must be flagged within seconds. The pipeline:

1. receives a continuous stream of transactions,
2. archives every transaction permanently,
3. analyzes each transaction in real time with fraud rules (no machine learning in this phase),
4. stores alerts in a fast NoSQL database and shows them on a dashboard,
5. runs batch statistics on the full history,
6. evaluates the rules (precision and recall).

## Data

[PaySim](https://www.kaggle.com/datasets/ealaxi/paysim1) (Kaggle, *Synthetic Financial Datasets For Fraud Detection*): 6,362,620 simulated mobile-money transactions with an `isFraud` label. The CSV (about 470 MB) is not stored in this repository. Download it from Kaggle and place it in the project folder.

## Architecture

```
CSV -> producer.py -> KAFKA [transactions] -> SPARK STREAMING -+-> HDFS archive (Parquet)
                                                               +-> KAFKA [alerts] -> alert writer -> HBASE -> dashboard

CSV in HDFS -> MAPREDUCE -> statistics that guide the rules
HDFS archive + alerts -> evaluation (Spark) -> precision and recall per rule
```

A Lambda-style design: Kafka and Spark Structured Streaming form the speed layer, HDFS and MapReduce the batch layer, HBase the serving layer.

| Layer | Technology | Role |
|---|---|---|
| Ingestion | Kafka 3.7.0 (KRaft mode) | Durable, replayable message log between components |
| Stream processing | Spark Structured Streaming 2.2.0 | Parsing, archiving, fraud rules |
| Raw storage | HDFS (Hadoop 2.7.2) | Parquet archive of every transaction |
| Batch processing | MapReduce (Hadoop Streaming, Python) | Statistics on the full history |
| NoSQL | HBase 1.4.9 | Alerts, queried by account and by time |
| Alert writer | Python (`kafka-python`, `happybase`) | Copies the `alerts` topic into HBase through the Thrift server |
| Dashboard | Streamlit (Docker) | Displays the alerts stored in HBase |
| Evaluation | Spark (`spark-submit`) | Precision and recall per rule |

## Environment

- Course image `liliasfaxi/spark-hadoop:hv-2.7.2`: `hadoop-master`, `hadoop-slave1`, `hadoop-slave2` on the Docker network `hadoop`.
- Kafka and the dashboard run as separate containers on the same network, defined in `docker-compose.yml`.
- Versions in the cluster: Spark 2.2.0 (Scala 2.11), Hadoop 2.7.2, Java 8, Python 3.5 (`python3`, no f-strings in cluster code).
- Spark 2.2 has no `foreachBatch`, so alerts are published to a Kafka topic and written to HBase by a separate script (the alert writer).
- The alert writer talks to HBase through the HBase Thrift server, which must be started in `hadoop-master` (see section 10).
- Use `hdfs://hadoop-master:9000/...` for every HDFS path in Spark. `hdfs:///...` fails with this version.

| Kafka address | Used by |
|---|---|
| `kafka:29092` | Containers on the `hadoop` network (Spark and the alert writer in `hadoop-master`) |
| `localhost:9092` | Scripts on the host machine (`producer.py`) |

The interfaces between the three team members (topics, JSON formats, HBase row keys, HDFS paths) are in [`docs/contracts.md`](docs/contracts.md).

## Repository structure

```
.
├── docker-compose.yml          Kafka container (joins the existing `hadoop` network)
├── requirements.txt            Python libraries for the ML work (streamlit, scikit-learn, lightgbm, shap, ...)
├── producer/
│   ├── producer.py             Replays the PaySim CSV into Kafka
│   └── fake_alerts.py          Sends sample alerts to the `alerts` topic for testing
├── spark/
│   ├── archive_job.py          Kafka -> Parquet on HDFS (checkpointed)
│   ├── rules_job.py            ACCOUNT_DRAIN and LARGE_AMOUNT alerts
│   ├── velocity_job.py         VELOCITY alerts (window and watermark)
│   └── test_tx.py              Test transaction that matches two rules
├── mapreduce/
│   ├── mapper_type.py, reducer_type.py          Job 1: fraud statistics per type
│   └── mapper_account.py, reducer_account.py    Job 2: transactions per account
├── hbase/
│   └── create_tables.txt       HBase shell commands (tables, family d)
├── writer/
│   ├── alert_writer.py         Kafka -> HBase
│   └── check_counts.py         Compares Kafka messages with HBase rows
├── dashboard/
│   ├── app.py                  Streamlit app
│   ├── Dockerfile              Image of the dashboard (python:3.11-slim, port 8501)
│   └── requirements.txt        streamlit, happybase, pandas
├── evaluation/
│   └── evaluate.py             Precision and recall per rule
├── scripts/
│   └── setup_hadoop_master.sh  Installs kafka-python and happybase in hadoop-master
├── docs/
│   ├── contracts.md            Interfaces between A, B and C
│   ├── rules.md                Fraud rules: details and rationale
│   ├── evaluation.md           Precision and recall results
│   └── top_destinations.txt    Destination accounts with the most transactions (MapReduce job 2)
├── .gitignore                  Excludes the CSV (*.csv)
└── README.md
```

## Progress

| Component | Status |
|---|---|
| Kafka container, topics `transactions` (3 partitions) and `alerts` (1 partition) | Done |
| Hadoop, HDFS (2 datanodes), HBase, folders under `/fraud`, HBase tables `alerts_by_account` and `alerts_recent` | Done |
| Producer replaying PaySim into Kafka | Done |
| Archive job (Kafka to Parquet on HDFS) | Done: Parquet files written to `/fraud/raw` |
| MapReduce job 1 (fraud per transaction type) | Done |
| MapReduce job 2 (transactions per sender and per destination) | Done |
| Contracts between the team members | Done, all points agreed (see `docs/contracts.md`) |
| Fraud rules job (Spark) | Done |
| Alert writer to HBase | Done |
| Dashboard | Done |
| Evaluation (precision and recall) | Done (results in `docs/evaluation.md`) |

## Results so far

### Fraud per transaction type (MapReduce job 1)

| Type | Transactions | Frauds | Fraud rate in the type |
|---|---|---|---|
| CASH_IN | 1,399,284 | 0 | 0% |
| CASH_OUT | 2,237,500 | 4,116 | 0.18% |
| DEBIT | 41,432 | 0 | 0% |
| PAYMENT | 2,151,495 | 0 | 0% |
| TRANSFER | 532,909 | 4,097 | 0.77% |
| **Total** | **6,362,620** | **8,213** | **0.13%** |

Fraud only occurs in `TRANSFER` and `CASH_OUT`, so every rule starts by filtering on these two types. Fraud is rare, so the rules are evaluated with precision and recall, not accuracy.

### How often an account repeats (MapReduce job 2)

| | Distinct accounts | Accounts seen 2+ times | Maximum per account |
|---|---|---|---|
| Sender (`nameOrig`) | 6,353,307 | 9,298 | 3 |
| Destination (`nameDest`) | 2,722,362 | 459,658 | 113 |

Senders almost never repeat, so a velocity rule counting per sender would never fire. Destinations do repeat, so the velocity rule counts transactions per destination account.

### Producer throughput

50,000 events in 16.8 s, about 2,976 events per second, from a single Python process on one machine to one broker. The target rate was higher, so this is the ceiling of the Python producer, not of Kafka. 

### Rule evaluation

Precision and recall of each rule against `isFraud` are in [`docs/evaluation.md`](docs/evaluation.md). They are produced by `evaluation/evaluate.py` (see section 10).

## How to run

Run `docker` commands and the producer in **Windows PowerShell**. Run `spark-submit`, `hdfs`, `hbase` and `hadoop` commands **inside** `hadoop-master`.

### 1. Start Kafka and create the topics (PowerShell)

```powershell
docker compose up -d kafka
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic transactions --bootstrap-server kafka:29092 --partitions 3 --replication-factor 1
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic alerts --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
docker exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server kafka:29092
```

### 2. Start the cluster (inside `hadoop-master`)

```powershell
docker start hadoop-master hadoop-slave1 hadoop-slave2
docker exec -it hadoop-master bash
```

```bash
./start-hadoop.sh
start-hbase.sh
jps
hdfs dfs -mkdir -p /fraud/raw /fraud/checkpoints /fraud/csv /fraud/output
```

Create the HBase tables (first time only). The same commands are in `hbase/create_tables.txt`:

```bash
hbase shell
create 'alerts_by_account', 'd'
create 'alerts_recent', 'd'
list
exit
```

Check that the cluster reaches Kafka:

```bash
(echo > /dev/tcp/kafka/29092) && echo "Kafka reachable"
```

### 3. Send transactions to Kafka (PowerShell)

```powershell
pip install kafka-python
python producer\producer.py --rate 50 --limit 200
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --topic transactions --bootstrap-server kafka:29092 --from-beginning --max-messages 3
```

Options: `--file` (CSV path), `--rate` (events per second), `--limit` (maximum events, 0 for all). Each message is the PaySim row as JSON plus `event_time`. The sender account (`nameOrig`) is the Kafka key, so all transactions of one account go to the same partition.

### 4. Archive the stream to HDFS (inside `hadoop-master`)

```powershell
docker cp spark\archive_job.py hadoop-master:/root/archive_job.py
```

```bash
export PYSPARK_PYTHON=python3
spark-submit --master local[2] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 \
  archive_job.py
```

From a second terminal, check the result:

```bash
hdfs dfs -ls /fraud/raw
```

The job reads the `transactions` topic, parses the JSON with an explicit schema, and writes Parquet files every 10 seconds. The checkpoint in `/fraud/checkpoints/archive` stores the Kafka offsets, so a restarted job resumes where it stopped.

### 5. Batch statistics with MapReduce

Upload the CSV and the scripts (PowerShell, then inside `hadoop-master`):

```powershell
docker cp PS_20174392719_1491204439457_log.csv hadoop-master:/root/
docker exec hadoop-master mkdir -p /root/mapreduce
docker cp mapreduce\mapper_type.py hadoop-master:/root/mapreduce/mapper_type.py
docker cp mapreduce\reducer_type.py hadoop-master:/root/mapreduce/reducer_type.py
docker cp mapreduce\mapper_account.py hadoop-master:/root/mapreduce/mapper_account.py
docker cp mapreduce\reducer_account.py hadoop-master:/root/mapreduce/reducer_account.py
```

```bash
hdfs dfs -put /root/PS_20174392719_1491204439457_log.csv /fraud/csv/
cd /root/mapreduce

# Job 1: fraud per transaction type
hdfs dfs -rm -r -f /fraud/output/type_stats
hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar \
  -files mapper_type.py,reducer_type.py \
  -mapper "python3 mapper_type.py" -reducer "python3 reducer_type.py" \
  -input /fraud/csv/PS_20174392719_1491204439457_log.csv \
  -output /fraud/output/type_stats
hdfs dfs -cat /fraud/output/type_stats/part-00000

# Job 2: transactions per sender (column 3) and per destination (column 6)
hdfs dfs -rm -r -f /fraud/output/orig_counts /fraud/output/dest_counts
hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar \
  -D mapreduce.job.reduces=1 \
  -files mapper_account.py,reducer_account.py \
  -mapper "python3 mapper_account.py 3" -reducer "python3 reducer_account.py" \
  -input /fraud/csv/PS_20174392719_1491204439457_log.csv \
  -output /fraud/output/orig_counts
hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar \
  -D mapreduce.job.reduces=1 \
  -files mapper_account.py,reducer_account.py \
  -mapper "python3 mapper_account.py 6" -reducer "python3 reducer_account.py" \
  -input /fraud/csv/PS_20174392719_1491204439457_log.csv \
  -output /fraud/output/dest_counts
hdfs dfs -cat /fraud/output/orig_counts/part-00000 | tail -1
hdfs dfs -cat /fraud/output/dest_counts/part-00000 | tail -1
```

Output folders must not exist before a run, which is why each job starts with `rm`.

### 6. Test the alert path without the rules job (PowerShell)

```powershell
python producer\fake_alerts.py --count 10
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --topic alerts --bootstrap-server kafka:29092 --from-beginning --max-messages 5
```

### 7. Reset before a measured run

PowerShell:

```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh --delete --topic transactions --bootstrap-server kafka:29092
docker exec kafka /opt/kafka/bin/kafka-topics.sh --delete --topic alerts --bootstrap-server kafka:29092
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic transactions --bootstrap-server kafka:29092 --partitions 3 --replication-factor 1
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic alerts --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
```

Inside `hadoop-master`, with the Spark jobs stopped:

```bash
hdfs dfs -rm -r -f /fraud/raw /fraud/checkpoints/archive /fraud/checkpoints/rules
hdfs dfs -mkdir -p /fraud/raw /fraud/checkpoints
```

### 8. Run the Spark jobs

Three Spark Structured Streaming jobs read the `transactions` topic. Details and rationale are in `docs/rules.md`.

| Application | File | Reads | Writes | Checkpoint |
|---|---|---|---|---|
| FraudArchive | `spark/archive_job.py` | topic `transactions` | Parquet in `hdfs://hadoop-master:9000/fraud/raw` | `/fraud/checkpoints/archive` |
| FraudRules | `spark/rules_job.py` | topic `transactions` (from earliest) | topic `alerts` | `/fraud/checkpoints/rules` |
| FraudVelocity | `spark/velocity_job.py` | topic `transactions` (from latest) | topic `alerts` | `/fraud/checkpoints/velocity` |

Copy the scripts into the container (PowerShell):

```powershell
docker cp spark\rules_job.py hadoop-master:/root/rules_job.py
docker cp spark\velocity_job.py hadoop-master:/root/velocity_job.py
```

Run **one job at a time** inside `hadoop-master` (two Spark jobs together overload Docker on a small machine):

```bash
cd /root
export PYSPARK_PYTHON=python3
spark-submit --master local[2] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 \
  rules_job.py 2>&1 | grep -v INFO
```

Replace `rules_job.py` with `velocity_job.py` or `archive_job.py` for the other jobs. The job stays silent while it runs, and a "batch is falling behind" warning is harmless. Stop it with Ctrl+C: it resumes from its checkpoint when restarted. Each job starts a micro-batch every 5 to 10 seconds.

### 9. Test the rules and check the results

Read the alerts (PowerShell). The `TimeoutException` printed at the end of a `--timeout-ms` command is normal.

```powershell
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --topic alerts --bootstrap-server kafka:29092 --from-beginning --max-messages 5
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --topic alerts --bootstrap-server kafka:29092 --from-beginning --timeout-ms 10000 | findstr LARGE_AMOUNT
```

Test the two-alerts behavior with a transaction built to match both rules (account `CTEST000001`, amount 500000, sender emptied). With the rules job running, two alerts appear, one `ACCOUNT_DRAIN` and one `LARGE_AMOUNT`:

```powershell
python spark\test_tx.py
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --topic alerts --bootstrap-server kafka:29092 --from-beginning --timeout-ms 10000 | findstr CTEST000001
```

Test velocity. The job reads only data sent after it started. A window is written only when a later micro-batch with new data runs, so send a small second batch and wait before reading:

```powershell
python producer\producer.py --rate 50 --limit 2000
python producer\producer.py --rate 50 --limit 200
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --topic alerts --bootstrap-server kafka:29092 --from-beginning --timeout-ms 10000 | findstr VELOCITY
```

After changing a constant in `velocity_job.py` (COUNT_BY, WINDOW, WATERMARK or N), delete its checkpoint, then copy the file again. Do the same after a reset of the topics (step 7):

```powershell
docker exec hadoop-master hdfs dfs -rm -r /fraud/checkpoints/velocity
```

Check that nothing is lost. Add the three partitions of the Kafka count, then count the archived rows (inside `hadoop-master`, always with `--master local[2]`, because plain `pyspark` goes through YARN and can freeze). The two totals must be equal once the archive job has caught up:

```powershell
docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:29092 --topic transactions --time -1
```

```bash
pyspark --master local[2]
>>> spark.read.parquet("hdfs://hadoop-master:9000/fraud/raw").count()
```

### 10. Store the alerts and show them

Start the HBase Thrift server and install the Python libraries (inside `hadoop-master`, first time only):

```bash
hbase-daemon.sh start thrift
jps                                  # ThriftServer must appear
bash /root/setup_hadoop_master.sh    # pip install kafka-python and happybase
```

Copy the files (PowerShell):

```powershell
docker cp writer hadoop-master:/root/
docker cp evaluation hadoop-master:/root/
docker cp scripts\setup_hadoop_master.sh hadoop-master:/root/
```

Run the writer (inside `hadoop-master`). It stays running; `--idle-timeout 30` stops it when no alert arrives for 30 seconds:

```bash
python3 /root/writer/alert_writer.py --idle-timeout 30
python3 /root/writer/check_counts.py     # Kafka messages must equal the rows of both tables
```

Start the dashboard (PowerShell), then open http://localhost:8501:

```powershell
docker compose up -d --build dashboard
```

Evaluate the rules (inside `hadoop-master`):

```bash
export PYSPARK_PYTHON=python3
spark-submit --master local[2] /root/evaluation/evaluate.py --sweep --output-dir /root/results
```

```powershell
docker cp hadoop-master:/root/results .\results
```

### 11. If Docker stops answering

An error `500 Internal Server Error` on every `docker` command means the Docker engine is overloaded. Quit Docker Desktop, run `wsl --shutdown` in PowerShell, reopen Docker Desktop, then restart everything. Never use "Clean / Purge data" or "Reset to factory defaults": they delete the containers and the HDFS data.

```powershell
docker start kafka
docker start hadoop-master hadoop-slave1 hadoop-slave2
docker exec -it hadoop-master bash
```

```bash
./start-hadoop.sh
jps
```

## Fraud rules

Every rule only looks at TRANSFER and CASH_OUT, because fraud only appears in these two types (MapReduce job 1).

| Rule | Condition | Parameter | Why |
|---|---|---|---|
| ACCOUNT_DRAIN | `newbalanceOrig` = 0 and `amount` = `oldbalanceOrg` | none | Typical PaySim fraud signature: the sender account is emptied |
| LARGE_AMOUNT | `amount` above the threshold | 200000 (constant at the top of `rules_job.py`) | Large transfers are risky; shows the precision and recall trade-off |
| VELOCITY | more than N transactions to one destination account in one window | N, window 1 minute, watermark 30 seconds (constants at the top of `velocity_job.py`) | Senders almost never repeat (maximum 3), destinations do (maximum 113) |

A transaction matching two rules produces two alerts, one per rule. Each alert records the rule that fired, and the rules are evaluated with precision and recall against `isFraud`.

**Velocity alert layout.** The field `nameOrig` holds the counted destination account, `amount` is the sum over the window, `event_time` is the end of the window, `isFraud` is the maximum in the window and `tx_count` is the number of transactions. The keys `nameDest`, `type`, `step` and `counted_by` are absent from the JSON (not null), so readers must use `.get()`; the alert writer does not write the missing columns to HBase. See section 4 of `docs/contracts.md`.

## HBase design

| Table | Row key | Purpose |
|---|---|---|
| `alerts_by_account` | `nameOrig` + `_` + reversed timestamp + `_` + `rule` | Latest alerts of one account |
| `alerts_recent` | reversed timestamp + `_` + `nameOrig` + `_` + `rule` | Latest alerts overall |

The reversed timestamp makes the newest rows sort first, so a range scan returns the latest alerts directly. The rule is part of both keys so that two alerts of one transaction do not overwrite each other. Both tables have a single column family `d`.

## Division of work

| Member | Area |
|---|---|
| A: yomn07 | Kafka, producer, HDFS, MapReduce, contracts |
| B: aicha | Spark Structured Streaming: archive job and rules |
| C: ghassen | HBase, alert writer, dashboard, evaluation |

Work is done on one Git branch per person and merged into `main` through pull requests reviewed by a teammate.

## Use of generative AI

Generative AI was used as technical assistance for development and debugging, in line with the course charter. The signed declarations are in `AI_declaration/`.
