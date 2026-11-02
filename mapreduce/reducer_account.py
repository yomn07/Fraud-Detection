#!/usr/bin/env python3
import sys

MIN_COUNT = 2  # only accounts seen at least twice are printed

stats = {"distinct": 0, "repeaters": 0, "max": 0, "total": 0}


def close(key, c):
    stats["distinct"] += 1
    stats["total"] += c
    if c > stats["max"]:
        stats["max"] = c
    if c >= MIN_COUNT:
        stats["repeaters"] += 1
        print("{}\t{}".format(key, c))


current = None
count = 0

for line in sys.stdin:
    parts = line.strip().split("\t")
    if len(parts) != 2:
        continue
    key = parts[0]
    if current is not None and key != current:
        close(current, count)
        count = 0
    current = key
    count += int(parts[1])

if current is not None:
    close(current, count)

print("#SUMMARY\tdistinct={}\trepeaters={}\tmax={}\ttotal={}".format(
    stats["distinct"], stats["repeaters"], stats["max"], stats["total"]))