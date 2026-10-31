#!/usr/bin/env python3
import sys

# column 3 = nameOrig (sender), column 6 = nameDest (receiver)
col = int(sys.argv[1]) if len(sys.argv) > 1 else 3

for line in sys.stdin:
    line = line.strip()
    if not line or line.startswith("step"):
        continue
    fields = line.split(",")
    if len(fields) < 11:
        continue
    print("{}\t1".format(fields[col]))