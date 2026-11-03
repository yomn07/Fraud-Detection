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


| Rule | Level | Alerts | Flagged | True positives | Fraud to find | Precision | Recall | F1 |
|---|---|---|---|---|---|---|---|---|
| ACCOUNT_DRAIN | transaction | 216 | 214 | 214 | 233 | 100.00% | 91.85% | 95.75% |
| LARGE_AMOUNT | transaction | 102737 | 102705 | 103 | 233 | 0.10% | 44.21% | 0.20% |
| ACCOUNT_DRAIN + LARGE_AMOUNT | transaction | 102953 | 102827 | 225 | 233 | 0.22% | 96.57% | 0.44% |
| VELOCITY | destination account | 450 | 435 | 13 | 230 | 2.99% | 5.65% | 3.91% |
| isFlaggedFraud (baseline) | transaction | 0 | 0 | 0 | 233 | n/a | 0.00% | n/a |


### Threshold study of LARGE_AMOUNT

| Rule | Level | Alerts | Flagged | True positives | Fraud to find | Precision | Recall | F1 |
|---|---|---|---|---|---|---|---|---|
| LARGE_AMOUNT > 50000 | transaction | 191933 | 191933 | 143 | 235 | 0.07% | 60.85% | 0.15% |
| LARGE_AMOUNT > 100000 | transaction | 159854 | 159854 | 126 | 235 | 0.08% | 53.62% | 0.16% |
| LARGE_AMOUNT > 150000 | transaction | 129408 | 129408 | 112 | 235 | 0.09% | 47.66% | 0.17% |
| LARGE_AMOUNT > 200000 | transaction | 102735 | 102735 | 103 | 235 | 0.10% | 43.83% | 0.20% |
| LARGE_AMOUNT > 300000 | transaction | 63240 | 63240 | 87 | 235 | 0.14% | 37.02% | 0.27% |
| LARGE_AMOUNT > 500000 | transaction | 27071 | 27071 | 72 | 235 | 0.27% | 30.64% | 0.53% |
| LARGE_AMOUNT > 1000000 | transaction | 9500 | 9500 | 58 | 235 | 0.61% | 24.68% | 1.19% |

## Analysis

All figures come from the tables above. The replay holds 233 distinct frauds out of the 8,213 of the full file, so the rates below describe this sample, not the full history. The threshold study shows 235 frauds because it counts archive rows, and a few transactions were replayed twice.

### Best precision and best recall

- **Best precision: `ACCOUNT_DRAIN`.** All 214 transactions it flagged are frauds (100.00 %).
- **Best recall: `LARGE_AMOUNT` at 44.21 % among the single rules, and the combination at 96.57 % overall.** But `LARGE_AMOUNT` buys that recall with a precision of 0.10 %.
- **`ACCOUNT_DRAIN` is the best rule overall**: it finds 91.85 % of the frauds (214 of 233) with no false positive, F1 95.75 %. `LARGE_AMOUNT` and `VELOCITY` have F1 of 0.20 % and 3.91 %.

### Frauds that no rule finds

- The two transaction rules together miss **8 frauds out of 233 (3.43 %)**: recall of the combined line is 96.57 %.
- `ACCOUNT_DRAIN` alone misses 19 frauds. `LARGE_AMOUNT` finds 103 frauds, but 92 of them (214 + 103 - 225) were already flagged by `ACCOUNT_DRAIN`. It adds only **11 frauds**.
- `VELOCITY` is evaluated per destination account, so it cannot be added to the transaction line. It reaches 13 of the 230 fraud destination accounts (5.65 %).

### Cost of the false positives

| Rule | Items to review | False positives | Frauds found | False positives per fraud found |
|---|---|---|---|---|
| `ACCOUNT_DRAIN` | 214 transactions | 0 | 214 | 0 |
| `LARGE_AMOUNT` | 102,705 transactions | 102,602 | 103 | about 996 |
| `ACCOUNT_DRAIN` + `LARGE_AMOUNT` | 102,827 transactions | 102,602 | 225 | about 456 |
| `VELOCITY` | 435 destination accounts | 422 | 13 | about 32 |

- `LARGE_AMOUNT` is the problem: about 1 alert in 1,000 is a real fraud, and it brings 11 extra frauds for about 102,600 extra alerts. No analyst team can review that.
- The threshold study confirms that the amount alone does not separate frauds from normal large transfers. From 50,000 to 1,000,000, recall falls from 60.85 % to 24.68 % (÷ 2.5) while precision rises from 0.07 % to 0.61 % (× 8.7) and stays under 1 %. The best F1 is 1.19 %, at the highest threshold. No threshold gives a usable rule.
- `VELOCITY` is much cheaper to review (435 accounts, about 32 false positives per fraud found) but finds few frauds. Its result depends on `N`, the window and the producer rate, so it is not directly comparable with the transaction rules.

### Comparison with `isFlaggedFraud`

- The baseline flagged **no transaction** in this replay: recall 0.00 % and no precision (n/a). Any rule that raises alerts beats it on recall, and the combination finds 225 of 233 frauds.
- On the full PaySim file, `isFlaggedFraud` is known to flag only a handful of transactions (16, to confirm with a count on the CSV), so this is expected: it is a business-control flag, not a fraud detector.
- The fair comparison is therefore between our rules: `ACCOUNT_DRAIN` alone (214 alerts, 91.85 % recall) is better than the combination (102,827 alerts, 96.57 % recall).

### What this means

- Keep `ACCOUNT_DRAIN` as the main alert rule. Treat `LARGE_AMOUNT` at 200,000 as a weak signal to combine with another condition, not as an alert by itself; raising the threshold alone does not fix it.
- The 8 frauds missed by both rules should be listed and inspected: they show which pattern a new rule would have to cover.

### Limits of this evaluation

- **Partial replay:** 233 frauds instead of 8,213. Run the full file for the final numbers.
- **Balance columns:** the PaySim description says that fraudulent transactions are cancelled and that the balance columns must not be used for fraud detection. `ACCOUNT_DRAIN` uses `newbalanceOrig` and `oldbalanceOrg`, so its 100 % precision partly reflects how the simulator writes these columns. On real data, this rule could behave differently.
- **Account level:** `VELOCITY` counts a destination account as a true positive if it received at least one fraud, at any time, which is a looser definition than the transaction level.
- **Replays:** transactions replayed twice create duplicate alerts (102,737 alerts for 102,705 distinct transactions), which the evaluation removes by matching on `(nameOrig, step, type, amount)`.