#!/usr/bin/env python3
"""MapReduce job 1, reducer.

Input : lines sorted by key: type <TAB> 1 <TAB> isFraud <TAB> amount
Output: type <TAB> count <TAB> fraud_count <TAB> total_amount <TAB> fraud_amount
"""
import sys

current = None
count = 0
fraud_count = 0
total_amount = 0.0
fraud_amount = 0.0


def emit(key):
    print("{}\t{}\t{}\t{:.2f}\t{:.2f}".format(
        key, count, fraud_count, total_amount, fraud_amount))


for line in sys.stdin:
    parts = line.strip().split("\t")
    if len(parts) != 4:
        continue
    key = parts[0]
    n = int(parts[1])
    is_fraud = int(parts[2])
    amount = float(parts[3])

    if current is not None and key != current:
        emit(current)
        count = 0
        fraud_count = 0
        total_amount = 0.0
        fraud_amount = 0.0

    current = key
    count += n
    total_amount += amount
    if is_fraud == 1:
        fraud_count += 1
        fraud_amount += amount

if current is not None:
    emit(current)
