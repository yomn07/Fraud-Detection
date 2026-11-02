"""Streamlit dashboard for the fraud alerts stored in HBase (Person C).

Three zones:
  1. the latest alerts (scan of alerts_recent, newest first);
  2. the number of alerts per rule (key-only scan of alerts_recent);
  3. a search by account (prefix scan of alerts_by_account).

Configuration (environment variables): HBASE_HOST (default hadoop-master) and
HBASE_PORT (default 9090). The container must be on the Docker network `hadoop`.
"""
import os
import time

import happybase
import pandas as pd
import streamlit as st

HBASE_HOST = os.environ.get("HBASE_HOST", "hadoop-master")
HBASE_PORT = int(os.environ.get("HBASE_PORT", "9090"))
RULES = ["ACCOUNT_DRAIN", "LARGE_AMOUNT", "VELOCITY"]
DISPLAY_COLUMNS = ["event_time", "rule", "account", "nameDest", "type", "amount",
                   "step", "tx_count", "isFraud"]

st.set_page_config(page_title="Fraud alerts", layout="wide")


def open_connection():
    return happybase.Connection(HBASE_HOST, port=HBASE_PORT, timeout=10000)


def rows_to_frame(rows):
    """Turn HBase (key, data) pairs into a DataFrame. A missing column is NaN."""
    records = []
    for key, data in rows:
        record = {}
        for column, value in data.items():
            record[column.decode("utf-8").split(":", 1)[1]] = value.decode("utf-8")
        record["row_key"] = key.decode("utf-8")
        records.append(record)
    frame = pd.DataFrame(records)
    if frame.empty:
        return frame
    for column in DISPLAY_COLUMNS:
        if column not in frame.columns:
            frame[column] = None
    for column in ("amount", "step", "tx_count", "isFraud"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def add_account_column(frame, account_from_key):
    """Row keys are rebuilt as (account, rule) so the account column is reliable."""
    if frame.empty:
        return frame
    frame["account"] = frame["row_key"].map(account_from_key)
    return frame


def account_from_recent_key(key):
    # reversed_ts _ account _ rule   (the rule can contain '_', the account cannot)
    return key.split("_", 2)[1]


def account_from_by_account_key(key):
    # account _ reversed_ts _ rule
    return key.split("_", 1)[0]


def fetch_recent(connection, limit):
    table = connection.table("alerts_recent")
    frame = rows_to_frame(table.scan(limit=limit))
    return add_account_column(frame, account_from_recent_key)


def fetch_rule_counts(connection, max_rows):
    """Count alerts per rule from the row keys only (no cell values are read)."""
    table = connection.table("alerts_recent")
    counts = dict((rule, 0) for rule in RULES)
    total = 0
    for key, _ in table.scan(filter=b"KeyOnlyFilter()", limit=max_rows):
        rule = key.decode("utf-8").split("_", 2)[2]
        counts[rule] = counts.get(rule, 0) + 1
        total += 1
    return counts, total


def fetch_account(connection, account, limit):
    table = connection.table("alerts_by_account")
    prefix = (account.strip() + "_").encode("utf-8")
    frame = rows_to_frame(table.scan(row_prefix=prefix, limit=limit))
    return add_account_column(frame, account_from_by_account_key)


def show_table(frame):
    if frame.empty:
        st.info("No alert yet.")
        return
    view = frame[DISPLAY_COLUMNS].copy()
    st.dataframe(view, width="stretch", hide_index=True)


# ---------------------------------------------------------------- sidebar
st.sidebar.header("Settings")
recent_limit = st.sidebar.slider("Latest alerts to show", 10, 200, 50, step=10)
max_count_rows = st.sidebar.number_input(
    "Max rows scanned for the counters", 1000, 1000000, 200000, step=10000)
auto_refresh = st.sidebar.checkbox("Auto refresh", value=False)
refresh_seconds = st.sidebar.slider("Refresh every (s)", 2, 30, 5)
st.sidebar.caption("HBase Thrift: {}:{}".format(HBASE_HOST, HBASE_PORT))

st.title("Real-time fraud alerts")

connection = None
try:
    connection = open_connection()
    connection.tables()  # forces the connection, so a failure shows up here

    # ---------------------------------------------------- zone 2: counters
    counts, total = fetch_rule_counts(connection, int(max_count_rows))
    columns = st.columns(len(RULES) + 1)
    columns[0].metric("Total alerts", total)
    for column, rule in zip(columns[1:], RULES):
        column.metric(rule, counts.get(rule, 0))
    if total >= int(max_count_rows):
        st.caption("Counters limited to the first {} rows.".format(int(max_count_rows)))

    left, right = st.columns([2, 1])

    # ---------------------------------------------------- zone 1: latest
    with left:
        st.subheader("Latest alerts")
        show_table(fetch_recent(connection, recent_limit))

    with right:
        st.subheader("Alerts per rule")
        if total:
            chart = pd.DataFrame({"alerts": [counts.get(r, 0) for r in RULES]}, index=RULES)
            st.bar_chart(chart)
        else:
            st.info("No alert yet.")

    # ---------------------------------------------------- zone 3: account
    st.subheader("Search by account")
    st.caption("For VELOCITY alerts the account is the destination account that "
               "received the burst of transactions (see contracts.md).")
    account = st.text_input("Account (for example C1305486145)")
    if account.strip():
        found = fetch_account(connection, account, 200)
        if found.empty:
            st.warning("No alert for account {}.".format(account.strip()))
        else:
            st.write("{} alert(s), newest first".format(len(found)))
            show_table(found)
except Exception as error:  # connection refused, Thrift down, table missing...
    st.error("Cannot read HBase at {}:{}. Is the Thrift server running "
             "(hbase-daemon.sh start thrift) and are the tables created?\n\n{}".format(
                 HBASE_HOST, HBASE_PORT, error))
finally:
    if connection is not None:
        connection.close()

st.caption("Last update: {}".format(time.strftime("%Y-%m-%d %H:%M:%S")))
if auto_refresh:
    time.sleep(refresh_seconds)
    st.rerun()
