#!/usr/bin/env python3
"""MapReduce job 1, mapper.

Input : PaySim CSV lines
        step,type,amount,nameOrig,oldbalanceOrg,newbalanceOrig,nameDest,
        oldbalanceDest,newbalanceDest,isFraud,isFlaggedFraud
Output: type <TAB> 1 <TAB> isFraud <TAB> amount
"""
import sys

for line in sys.stdin:
    line = line.strip()
    if not line or line.startswith("step"):  # skip empty lines and the header
        continue
    fields = line.split(",")
    if len(fields) < 11:
        continue
    tx_type = fields[1]
    amount = fields[2]
    is_fraud = fields[9]
    print("{}\t1\t{}\t{}".format(tx_type, is_fraud, amount))
