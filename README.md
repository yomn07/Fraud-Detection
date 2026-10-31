# Real-Time Fraud Detection on a Big Data Architecture

Big Data mini-project (II3). A pipeline that replays mobile-payment transactions as a live stream, archives them, flags suspicious ones in real time with rules, and stores the alerts in a NoSQL database.

**Team:** [member A], [member B], [member C]

## Project goal

A payment system receives thousands of transactions per second, and fraud must be flagged within seconds. This project builds a Big Data pipeline that:

1. receives a continuous stream of transactions,
2. archives every transaction permanently,
3. analyzes each transaction in real time with fraud rules (no machine learning in this phase),
4. stores alerts in a fast NoSQL database,
5. runs batch statistics on the full history.

## Data

[PaySim](https://www.kaggle.com/datasets/ealaxi/paysim1) (Kaggle, *Synthetic Financial Datasets For Fraud Detection*): about 6.3 million simulated mobile-money transactions with an `isFraud` label. Fraud only appears in `TRANSFER` and `CASH_OUT`, and only about 0.13% of rows are fraud.

The CSV is not stored in this repository (it is about 470 MB). Download it from Kaggle and place it in the project folder.

## Architecture

```
CSV -> producer.py -> KAFKA [transactions] -> SPARK STREAMING -+-> HDFS archive -> MAPREDUCE (batch stats)
                                                               +-> KAFKA [alerts] -> alert writer -> HBASE -> dashboard
```

A Lambda-style design: Kafka and Spark Structured Streaming form the speed layer, HDFS and MapReduce the batch layer, and HBase the serving layer.

| Layer | Technology | Role |
|---|---|---|
| Ingestion | Kafka 3.7.0 (KRaft mode) | Durable, replayable message log between components |
| Stream processing | Spark Structured Streaming 2.2.0 | Parsing, archiving, fraud rules |
| Raw storage | HDFS (Hadoop 2.7.2) | Parquet archive of every transaction |
| Batch processing | MapReduce (Hadoop Streaming) | Statistics on the full history |
| NoSQL | HBase 1.4.9 | Alerts, queried by account and by time |

## Environment

- Course image `liliasfaxi/spark-hadoop:hv-2.7.2`: `hadoop-master`, `hadoop-slave1`, `hadoop-slave2` on the Docker network `hadoop`.
- Kafka runs as a separate container on the same network, defined in `docker-compose.yml`.
- Versions inside the cluster: Spark 2.2.0 (Scala 2.11), Hadoop 2.7.2, Java 8, Python 3.5.
- Spark 2.2 has no `foreachBatch`, so alerts are published to a Kafka topic and written to HBase by a separate script.

Kafka exposes two listeners:

| Address | Used by |
|---|---|
| `kafka:29092` | Containers on the `hadoop` network (Spark in `hadoop-master`) |
| `localhost:9092` | Scripts on the host machine (`producer.py`) |

## Progress so far

| Component | Status |
|---|---|
| Kafka container and compose file | Done |
| Topics `transactions` (3 partitions) and `alerts` (1 partition) | Done |
| Connectivity from `hadoop-master` to Kafka | Done |
| Hadoop and HDFS (2 live datanodes), folders `/fraud/raw`, `/fraud/checkpoints`, `/fraud/csv` | Done |
| HBase running, tables `alerts_by_account` and `alerts_recent` (column family `d`) | Done |
| Producer replaying PaySim into Kafka | Done |
| Messages checked in Kafka with the console consumer | Done |
| Archive job (Kafka to Parquet on HDFS) | Written, to run and verify |
| Fraud rules job | Planned |
| Alert writer to HBase | Planned |
| MapReduce batch statistics | Planned |
| Dashboard | Planned |
| Evaluation (precision and recall), throughput, resilience test | Planned |

## Repository structure

```
.
├── docker-compose.yml      Kafka container (joins the existing `hadoop` network)
├── producer.py             Replays the PaySim CSV into Kafka
├── spark/
│   └── archive_job.py      Kafka -> Parquet on HDFS (checkpointed)
├── .gitignore              Excludes the CSV
└── README.md
```

## How to run

Run `docker` commands and the producer in **Windows PowerShell**. Run `spark-submit`, `hdfs` and `hbase` commands **inside** `hadoop-master`.

### 1. Start Kafka and create the topics (PowerShell)

```powershell
docker compose up -d
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic transactions --bootstrap-server kafka:29092 --partitions 3 --replication-factor 1
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic alerts --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
docker exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server kafka:29092
```

### 2. Start Hadoop and HBase (inside `hadoop-master`)

```powershell
docker start hadoop-master hadoop-slave1 hadoop-slave2
docker exec -it hadoop-master bash
```

```bash
./start-hadoop.sh
start-hbase.sh
jps
hdfs dfs -mkdir -p /fraud/raw /fraud/checkpoints /fraud/csv
```

Create the HBase tables (first time only):

```bash
hbase shell
create 'alerts_by_account', 'd'
create 'alerts_recent', 'd'
list
exit
```

### 3. Check that the cluster reaches Kafka (inside `hadoop-master`)

```bash
(echo > /dev/tcp/kafka/29092) && echo "Kafka reachable"
```

### 4. Send transactions to Kafka (PowerShell)

```powershell
pip install kafka-python
python producer.py --rate 50 --limit 200
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --topic transactions --bootstrap-server kafka:29092 --from-beginning --max-messages 3
```

Options: `--file` (CSV path), `--rate` (events per second), `--limit` (maximum events, 0 for all). Each message is the PaySim row as JSON, plus `event_time`. The sender account (`nameOrig`) is the Kafka key, so all transactions of one account go to the same partition.

### 5. Archive the stream to HDFS (inside `hadoop-master`)

Copy the job into the container from PowerShell:

```powershell
docker cp spark\archive_job.py hadoop-master:/root/archive_job.py
```

Then run it:

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

## Planned fraud rules

Every rule looks only at `TRANSFER` and `CASH_OUT` transactions.

| Rule | Condition |
|---|---|
| Account drain | The amount equals the old balance and the new balance is 0 |
| Large amount | Amount above a tunable threshold (start at 200,000) |
| Velocity | More than N transactions for the same account in a short window (window and watermark) |
| Empty destination (optional) | Large transfer to an account whose balance was 0 |
| Amount anomaly (optional) | Amount above mean plus 3 standard deviations of its transaction type, with the profile computed in batch |

Each alert records the rule that fired. Rules will be evaluated with precision and recall against `isFraud`, not accuracy, because fraud is only about 0.13% of the data.

## Planned HBase design

| Table | Row key | Purpose |
|---|---|---|
| `alerts_by_account` | `nameOrig` + `_` + reversed timestamp | Latest alerts of one account |
| `alerts_recent` | reversed timestamp + `_` + id | Latest alerts overall |

The reversed timestamp makes the newest rows sort first, so a range scan returns the latest alerts directly.

## Division of work

| Member | Area |
|---|---|
| A: [name] | Kafka, producer, HDFS, MapReduce, benchmarks |
| B: [name] | Spark Structured Streaming: archive job and rules |
| C: [name] | HBase, alert writer, dashboard, evaluation |

## Use of generative AI

Generative AI was used as technical assistance for development and debugging, in line with the course charter. The signed declarations are in `AI_declaration/`.
