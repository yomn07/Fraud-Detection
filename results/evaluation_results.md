| Rule | Level | Alerts | Flagged | True positives | Fraud to find | Precision | Recall | F1 |
|---|---|---|---|---|---|---|---|---|
| ACCOUNT_DRAIN | transaction | 216 | 214 | 214 | 233 | 100.00% | 91.85% | 95.75% |
| LARGE_AMOUNT | transaction | 102737 | 102705 | 103 | 233 | 0.10% | 44.21% | 0.20% |
| ACCOUNT_DRAIN + LARGE_AMOUNT | transaction | 102953 | 102827 | 225 | 233 | 0.22% | 96.57% | 0.44% |
| VELOCITY | destination account | 450 | 435 | 13 | 230 | 2.99% | 5.65% | 3.91% |
| isFlaggedFraud (baseline) | transaction | 0 | 0 | 0 | 233 | n/a | 0.00% | n/a |
