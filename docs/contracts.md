# Contracts

Single source of truth for the interfaces between A (ingestion and batch), B (stream processing) and C (storage and serving). Nobody changes a field name or type alone: open a pull request on this file and get the affected members to approve it.

## 1. Addresses

| What | Value |
|---|---|
| Kafka from containers (Spark, writer inside `hadoop-master`) | `kafka:29092` |
| Kafka from the host (producer on Windows) | `localhost:9092` |
| HDFS | `hdfs://hadoop-master:9000` (never `hdfs:///`, it fails with Spark 2.2) |
| HBase Thrift (for Python) | `hadoop-master:9090` |

## 2. Kafka topics

| Topic | Partitions | Key | Value |
|---|---|---|---|
| `transactions` | 3 | `nameOrig` | Transaction JSON (section 3) |
| `alerts` | 1 | `nameOrig` | Alert JSON (section 4) |

## 3. Transaction JSON (topic `transactions`)

Written by `producer/producer.py`, read by B.

| Field | Type | Example |
|---|---|---|
| `step` | integer | 1 |
| `type` | string | `PAYMENT` |
| `amount` | double | 9839.64 |
| `nameOrig` | string | `C1231006815` |
| `oldbalanceOrg` | double | 170136.0 |
| `newbalanceOrig` | double | 160296.36 |
| `nameDest` | string | `M1979787155` |
| `oldbalanceDest` | double | 0.0 |
| `newbalanceDest` | double | 0.0 |
| `isFraud` | integer (0 or 1) | 0 |
| `isFlaggedFraud` | integer (0 or 1) | 0 |
| `event_time` | string `yyyy-MM-dd HH:mm:ss.SSSSSS`, UTC | `2026-10-25 13:05:18.099506` |

`event_time` is the moment the producer sent the row, not a PaySim date. Windows and watermarks use `event_time`.

## 4. Alert JSON (topic `alerts`)

Written by B's Spark jobs, read by C's writer.

| Field | Type | Meaning |
|---|---|---|
| `rule` | string | `ACCOUNT_DRAIN`, `LARGE_AMOUNT` or `VELOCITY` |
| `nameOrig` | string | Sender account (for `VELOCITY`: the account that was counted) |
| `nameDest` | string or null | Receiver account |
| `type` | string or null | `TRANSFER` or `CASH_OUT` |
| `amount` | double | Transaction amount (for `VELOCITY`: sum of amounts in the window) |
| `step` | integer or null | PaySim time step |
| `event_time` | string `yyyy-MM-dd'T'HH:mm:ss.SSS'Z'`, UTC | Timestamp written by Spark, for example `2026-10-31T17:10:21.181Z` (for `VELOCITY`: end of the window) |

| `isFraud` | integer | Real label, kept only for evaluation (for `VELOCITY`: maximum in the window) |
| `tx_count` | integer, only in `VELOCITY` | Number of transactions in the window |
| `counted_by` | string, only in `VELOCITY` (proposed) | Which column was counted: `nameDest` |

Rules:

| `rule` | Condition |
|---|---|
| `ACCOUNT_DRAIN` | `type` is `TRANSFER` or `CASH_OUT`, `newbalanceOrig` = 0 and `amount` = `oldbalanceOrg` |
| `LARGE_AMOUNT` | `type` is `TRANSFER` or `CASH_OUT` and `amount` above the threshold (start at 200000, tune with the evaluation) |
| `VELOCITY` | More than N transactions to the same destination account inside a window (start at 1 minute) |

A transaction matching two rules produces two alerts.

Example:

```json
{"rule":"ACCOUNT_DRAIN","nameOrig":"C1305486145","nameDest":"C553264065","type":"TRANSFER","amount":181.0,"step":1,"event_time":"2026-10-31T15:10:21.181Z","isFraud":1}
```

**To confirm:** the exact `event_time` format that Spark's `to_json` produces. Read it from the first real alert, then replace the example above and tell C. 
--> **Confirmed:** `event_time` in alerts is the ISO 8601 UTC format produced by Spark's `to_json`, with milliseconds and a `Z` suffix (`2026-10-31T17:10:21.181Z`), read from the first real alerts on 2026-10-31. It is different from the transaction `event_time` (section 3), which has a space and microseconds. C parses it with the pattern `yyyy-MM-dd'T'HH:mm:ss.SSS'Z'` before computing the reversed timestamp. To be re-checked on the first `VELOCITY` alert.


**Why velocity counts by destination:** MapReduce job 2 on the full file shows that senders almost never repeat (6,353,307 distinct senders, maximum 3 transactions each), while destinations do (2,722,362 distinct, maximum 113). A rule counting per sender would never fire.

**To agree between B and C:** the `VELOCITY` layout. Proposal: the counted destination account goes in `nameOrig` (so the alert key and the HBase keys work unchanged), `counted_by` is `nameDest`, `nameDest`, `type` and `step` are null, and the writer accepts nulls.

## 5. HBase

Both tables have one column family `d`. All values are stored as strings.

| Table | Row key |
|---|---|
| `alerts_by_account` | `nameOrig` + `_` + reversed timestamp + `_` + `rule` (proposed, see below) |
| `alerts_recent` | reversed timestamp + `_` + `nameOrig` + `_` + `rule` |

Reversed timestamp = `9223372036854775807 - epoch_milliseconds`, written as a 19-digit string. The newest alert gets the smallest number, so it sorts first in a range scan.

**Proposed change for `alerts_by_account`:** one transaction can match two rules and produce two alerts with the same `nameOrig` and the same `event_time`. With the key `nameOrig_reversedTimestamp`, the second write would overwrite the first. Adding `rule` at the end of the key avoids this, and a prefix scan on the account still works. To be approved by C.

Python helper for the reversed timestamp (compatible with Python 3.5):

```python
import calendar
from datetime import datetime

def to_epoch_ms(s):
    dt = datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ")
    return calendar.timegm(dt.timetuple()) * 1000 + dt.microsecond // 1000

def reversed_ts(s):
    return str(9223372036854775807 - to_epoch_ms(s)).zfill(19)
```

Columns in family `d`: `rule`, `nameDest`, `type`, `amount`, `step`, `event_time`, `isFraud`, and for velocity alerts `tx_count` and `counted_by`.

## 6. HDFS paths

| Path | Content | Owner |
|---|---|---|
| `/fraud/csv` | Original PaySim CSV (input of MapReduce) | A |
| `/fraud/raw` | Parquet archive of all transactions | B |
| `/fraud/checkpoints/archive` | Checkpoint of the archive job | B |
| `/fraud/checkpoints/rules` | Checkpoint of the rules job | B |
| `/fraud/output/type_stats` | MapReduce job 1: count, frauds and amounts per transaction type | A |
| `/fraud/output/orig_counts` | MapReduce job 2: transactions per sender account | A |
| `/fraud/output/dest_counts` | MapReduce job 2: transactions per destination account | A |

## 7. Open points

| Point | Owners | Status |
|---|---|---|
| `VELOCITY` alert layout | B and C | Proposed above |
| `rule` added to the `alerts_by_account` row key | C | Proposed above |
| Threshold of `LARGE_AMOUNT` | B and C | To tune from the evaluation, 200000 is only a start |