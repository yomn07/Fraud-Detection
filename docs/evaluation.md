# Evaluation of the fraud rules

## Method

- **Truth:** the Parquet archive in `hdfs://hadoop-master:9000/fraud/raw` (column `isFraud`). Only the transactions that were really streamed are evaluated, so the number of frauds must match MapReduce job 1 (8,213) after a full replay.
- **Alerts:** table `alerts_recent` in HBase.
- **Metrics:** precision = true positives / flagged, recall = true positives / frauds to find. Accuracy is not used: with 0.13 % of frauds, a system that never raises an alert would reach 99.87 %.
- **Transaction level** (`ACCOUNT_DRAIN`, `LARGE_AMOUNT`): an alert is matched with a transaction by `(nameOrig, step, type, amount)`. A transaction flagged by two rules counts once in the combined line.
- **Account level** (`VELOCITY`): the alert has no `step` or `type`, so it is evaluated per destination account. A flagged account is a true positive if it received at least one fraudulent transaction.
- **Baseline:** PaySim's own `isFlaggedFraud` (transfers above 200,000).
- **Threshold study:** `--sweep` computes, on the archive, precision and recall of `LARGE_AMOUNT` for several thresholds.

## Results

Run on: <date>, <number of transactions replayed>.

<paste evaluation_results.md here>

### Threshold study of LARGE_AMOUNT

<paste threshold_sweep.md here>

## Analysis

- Which rule has the best precision, which one the best recall?
- How many frauds does no rule find? (recall of the combined line)
- Cost of the false positives of each rule.
- Does the combination beat `isFlaggedFraud`?

## Recommendations for B

- Threshold of `LARGE_AMOUNT`: <value and reason, from the threshold study>.
- Threshold N and window of `VELOCITY`: <value and reason>.
