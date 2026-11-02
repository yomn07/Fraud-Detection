"""
Dark Streamlit dashboard for fraud alerts stored in HBase.

Preserves the existing HBase schema:
  - alerts_recent row key: reversed_timestamp_nameOrig_rule
  - alerts_by_account row key: nameOrig_reversed_timestamp_rule
  - column family: d
  - existing columns are read as-is; this app does not write to HBase.

Environment:
  HBASE_HOST (default: hadoop-master)
  HBASE_PORT (default: 9090)
  DASHBOARD_SCAN_LIMIT (default: 20000; maximum recent rows loaded per refresh)
"""

import os
import time
from datetime import date, datetime, timedelta

import happybase
import pandas as pd
import streamlit as st

HBASE_HOST = os.environ.get("HBASE_HOST", "hadoop-master")
HBASE_PORT = int(os.environ.get("HBASE_PORT", "9090"))
DEFAULT_SCAN_LIMIT = int(os.environ.get("DASHBOARD_SCAN_LIMIT", "20000"))

RULES = ["ACCOUNT_DRAIN", "LARGE_AMOUNT", "VELOCITY"]
DISPLAY_COLUMNS = [
    "event_time", "rule", "account", "nameDest", "type",
    "amount", "step", "tx_count", "isFraud"
]

st.set_page_config(
    page_title="Fraud Intelligence | Monitoring",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Styling
# ---------------------------------------------------------------------------
st.markdown(
    """
    <style>
    :root { color-scheme: dark; }
    .stApp {
        background:
          radial-gradient(circle at 12% 0%, rgba(26, 76, 115, .24), transparent 34%),
          linear-gradient(145deg, #080d18 0%, #0d1422 52%, #101a2b 100%);
        color: #e8eef8;
    }
    [data-testid="stHeader"] { background: rgba(8, 13, 24, .88); }
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #101a2b 0%, #0b1220 100%);
        border-right: 1px solid rgba(148, 163, 184, .15);
    }
    [data-testid="stMetric"] {
        background: linear-gradient(145deg, rgba(24, 39, 61, .96), rgba(15, 26, 43, .96));
        border: 1px solid rgba(100, 150, 200, .22);
        padding: 16px 18px;
        border-radius: 14px;
        box-shadow: 0 8px 24px rgba(0, 0, 0, .18);
    }
    [data-testid="stMetricLabel"] { color: #9fb2cc; }
    [data-testid="stMetricValue"] { color: #f3f7ff; }
    h1, h2, h3 { color: #f3f7ff !important; }
    .hero {
        padding: 22px 26px;
        border: 1px solid rgba(88, 166, 255, .24);
        border-radius: 18px;
        background: linear-gradient(110deg, rgba(22, 45, 72, .95), rgba(15, 26, 43, .88));
        margin-bottom: 18px;
    }
    .hero-kicker {
        color: #69b7ff; text-transform: uppercase; letter-spacing: .16em;
        font-size: .72rem; font-weight: 700;
    }
    .hero-title { font-size: 1.85rem; font-weight: 800; margin: 4px 0; }
    .hero-subtitle { color: #aabbd1; margin: 0; }
    div[data-testid="stDataFrame"] {
        border: 1px solid rgba(148, 163, 184, .18);
        border-radius: 12px;
        overflow: hidden;
    }
    div.stButton > button, div.stDownloadButton > button {
        border-radius: 10px; border: 1px solid rgba(88, 166, 255, .35);
    }
    .muted { color: #9fb2cc; font-size: .86rem; }
    </style>
    """,
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------------
# HBase access and data conversion
# ---------------------------------------------------------------------------
@st.cache_data(ttl=5, show_spinner=False)
def load_recent_alerts(host, port, limit):
    """Load a bounded newest-first sample from alerts_recent."""
    connection = happybase.Connection(host, port=port, timeout=10000)
    try:
        connection.open()
        table = connection.table("alerts_recent")
        records = []

        for row_key, data in table.scan(limit=int(limit)):
            record = {}
            for column, value in data.items():
                column_name = column.decode("utf-8").split(":", 1)[-1]
                record[column_name] = value.decode("utf-8")

            key = row_key.decode("utf-8")
            parts = key.split("_", 2)
            # Contract: reversed_timestamp_nameOrig_rule.
            if len(parts) == 3:
                record["account"] = parts[1]
                record["rule"] = record.get("rule") or parts[2]

            record["row_key"] = key
            records.append(record)

        frame = pd.DataFrame(records)
        if frame.empty:
            return frame

        for column in DISPLAY_COLUMNS + ["row_key"]:
            if column not in frame.columns:
                frame[column] = None

        for column in ("amount", "step", "tx_count", "isFraud"):
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

        if "event_time" in frame.columns:
            frame["event_time"] = pd.to_datetime(
                frame["event_time"], errors="coerce", utc=True
            )

        return frame
    finally:
        connection.close()


@st.cache_data(ttl=5, show_spinner=False)
def load_account_alerts(host, port, account, limit=500):
    """Prefix scan in alerts_by_account, preserving the existing row-key format."""
    connection = happybase.Connection(host, port=port, timeout=10000)
    try:
        connection.open()
        table = connection.table("alerts_by_account")
        prefix = (account.strip() + "_").encode("utf-8")
        records = []

        for row_key, data in table.scan(row_prefix=prefix, limit=int(limit)):
            record = {}
            for column, value in data.items():
                column_name = column.decode("utf-8").split(":", 1)[-1]
                record[column_name] = value.decode("utf-8")

            key = row_key.decode("utf-8")
            parts = key.split("_", 2)
            # Contract: nameOrig_reversed_timestamp_rule.
            if len(parts) == 3:
                record["account"] = parts[0]
                record["rule"] = record.get("rule") or parts[2]
            record["row_key"] = key
            records.append(record)

        frame = pd.DataFrame(records)
        if frame.empty:
            return frame

        for column in DISPLAY_COLUMNS + ["row_key"]:
            if column not in frame.columns:
                frame[column] = None

        for column in ("amount", "step", "tx_count", "isFraud"):
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

        if "event_time" in frame.columns:
            frame["event_time"] = pd.to_datetime(
                frame["event_time"], errors="coerce", utc=True
            )
        return frame
    finally:
        connection.close()


def filtered_alerts(frame, rules, tx_types, account_query,
                    min_amount, max_amount, fraud_filter, date_range):
    if frame.empty:
        return frame

    result = frame.copy()

    if rules:
        result = result[result["rule"].isin(rules)]
    if tx_types:
        result = result[result["type"].isin(tx_types)]
    if account_query.strip():
        needle = account_query.strip().lower()
        result = result[result["account"].fillna("").str.lower().str.contains(
            needle, regex=False
        )]
    if min_amount is not None:
        result = result[result["amount"].fillna(-1) >= min_amount]
    if max_amount is not None:
        result = result[result["amount"].fillna(float("inf")) <= max_amount]
    if fraud_filter == "Dataset fraud label = 1":
        result = result[result["isFraud"] == 1]
    elif fraud_filter == "Dataset fraud label = 0":
        result = result[result["isFraud"] == 0]

    if date_range and len(date_range) == 2 and "event_time" in result.columns:
        start_date, end_date = date_range
        if start_date and end_date:
            start = pd.Timestamp(start_date, tz="UTC")
            end = pd.Timestamp(end_date + timedelta(days=1), tz="UTC")
            result = result[
                (result["event_time"] >= start) & (result["event_time"] < end)
            ]

    return result


def display_frame(frame):
    columns = [column for column in DISPLAY_COLUMNS if column in frame.columns]
    view = frame[columns].copy()
    if "event_time" in view.columns:
        view["event_time"] = view["event_time"].dt.strftime(
            "%Y-%m-%d %H:%M:%S UTC"
        )
    st.dataframe(view, use_container_width=True, hide_index=True)


# ---------------------------------------------------------------------------
# Evaluation results (CSV reports generated by the evaluation job)
# ---------------------------------------------------------------------------
def find_results_file(filename):
    """Find a report in common project/container locations."""
    candidates = [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "results", filename),
        os.path.join(os.getcwd(), "results", filename),
        os.path.join("/app", "results", filename),
        os.path.join("/root", "results", filename),
    ]
    for candidate in candidates:
        if os.path.isfile(candidate):
            return candidate
    return None


@st.cache_data(ttl=30, show_spinner=False)
def read_evaluation_csv(path, modified_time):
    # modified_time is included in the cache key so replacing a CSV reloads it.
    return pd.read_csv(path)


def load_evaluation_data(filename, uploaded_file):
    if uploaded_file is not None:
        uploaded_file.seek(0)
        return pd.read_csv(uploaded_file), "Uploaded file"
    path = find_results_file(filename)
    if path:
        return read_evaluation_csv(path, os.path.getmtime(path)), path
    return None, None


def render_evaluation_section():
    st.divider()
    st.header("🧪 Model Performance & Evaluation")
    st.markdown(
        "Evaluate detection quality using the CSV reports generated by your "
        "Spark evaluation job. These results are separate from the live HBase alert sample."
    )

    with st.expander("Load evaluation CSV reports", expanded=False):
        st.caption(
            "The app first looks for files in the project's results/ folder. "
            "If the files are not packaged into the dashboard container, upload them here."
        )
        upload_main = st.file_uploader(
            "evaluation_results.csv", type=["csv"], key="evaluation_main_upload"
        )
        upload_sweep = st.file_uploader(
            "threshold_sweep.csv", type=["csv"], key="evaluation_sweep_upload"
        )

    main_df, main_source = load_evaluation_data(
        "evaluation_results.csv", upload_main
    )
    sweep_df, sweep_source = load_evaluation_data(
        "threshold_sweep.csv", upload_sweep
    )

    if main_df is None:
        st.warning(
            "evaluation_results.csv was not found. Put it in the project's "
            "results/ folder and rebuild the dashboard, or upload it above."
        )
    else:
        required = {"name", "alerts", "true_positive", "positives",
                    "precision", "recall", "f1"}
        missing = required.difference(main_df.columns)
        if missing:
            st.error(
                "evaluation_results.csv is missing columns: "
                + ", ".join(sorted(missing))
            )
        else:
            main_df = main_df.copy()
            numeric_cols = [
                "alerts", "flagged", "true_positive", "positives",
                "precision", "recall", "f1"
            ]
            for column in numeric_cols:
                if column in main_df.columns:
                    main_df[column] = pd.to_numeric(
                        main_df[column], errors="coerce"
                    )

            st.caption("Main evaluation source: {}".format(main_source))
            metric_rows = main_df[
                main_df["name"].astype(str).str.lower()
                != "isflaggedfraud (baseline)"
            ].copy()
            if not metric_rows.empty:
                best_f1_row = metric_rows.loc[metric_rows["f1"].idxmax()]
                best_recall_row = metric_rows.loc[metric_rows["recall"].idxmax()]
                fraud_cases = (
                    int(metric_rows["positives"].max())
                    if metric_rows["positives"].notna().any() else 0
                )
                m1, m2, m3, m4 = st.columns(4)
                m1.metric("Rules evaluated", str(len(metric_rows)))
                m2.metric("Fraud cases in evaluation", "{:,}".format(fraud_cases))
                m3.metric(
                    "Best F1 rule",
                    str(best_f1_row["name"]),
                    "{:.2%}".format(best_f1_row["f1"])
                    if pd.notna(best_f1_row["f1"]) else None
                )
                m4.metric(
                    "Best recall rule",
                    str(best_recall_row["name"]),
                    "{:.2%}".format(best_recall_row["recall"])
                    if pd.notna(best_recall_row["recall"]) else None
                )

            chartable = main_df.dropna(subset=["name"]).copy()
            chartable = chartable[
                chartable["name"].astype(str).str.lower()
                != "isflaggedfraud (baseline)"
            ]
            chart_left, chart_right = st.columns(2)
            with chart_left:
                st.markdown("**Precision, recall and F1 by rule**")
                if not chartable.empty:
                    score_cols = [
                        column for column in ["precision", "recall", "f1"]
                        if column in chartable.columns
                    ]
                    score_chart = chartable.set_index("name")[score_cols] * 100
                    score_chart.columns = [
                        "{} (%)".format(column.title()) for column in score_chart.columns
                    ]
                    st.bar_chart(score_chart)
            with chart_right:
                st.markdown("**Alerts generated by rule**")
                if not chartable.empty:
                    alert_chart = chartable.set_index("name")[["alerts"]]
                    st.bar_chart(alert_chart)

            st.markdown("**Detailed rule evaluation**")
            st.dataframe(main_df, use_container_width=True, hide_index=True)
            st.download_button(
                "⬇️ Download main evaluation CSV",
                data=main_df.to_csv(index=False).encode("utf-8"),
                file_name="evaluation_results.csv",
                mime="text/csv",
                key="download_main_evaluation",
            )

    if sweep_df is None:
        st.warning(
            "threshold_sweep.csv was not found. Put it in results/ or upload it above."
        )
    else:
        required_sweep = {"name", "alerts", "precision", "recall", "f1"}
        missing_sweep = required_sweep.difference(sweep_df.columns)
        if missing_sweep:
            st.error(
                "threshold_sweep.csv is missing columns: "
                + ", ".join(sorted(missing_sweep))
            )
        else:
            sweep_df = sweep_df.copy()
            for column in ["alerts", "flagged", "true_positive", "positives",
                           "precision", "recall", "f1"]:
                if column in sweep_df.columns:
                    sweep_df[column] = pd.to_numeric(
                        sweep_df[column], errors="coerce"
                    )

            st.subheader("LARGE_AMOUNT threshold analysis")
            st.caption("Threshold sweep source: {}".format(sweep_source))
            sweep_best = sweep_df.dropna(subset=["f1"])
            if not sweep_best.empty:
                best = sweep_best.loc[sweep_best["f1"].idxmax()]
                a, b, c = st.columns(3)
                a.metric("Best tested threshold", str(best["name"]))
                b.metric("Best tested F1", "{:.2%}".format(best["f1"]))
                c.metric("Alerts at best F1", "{:,.0f}".format(best["alerts"]))

            st.markdown("**Precision and recall across tested thresholds**")
            plot_df = sweep_df.copy()
            plot_df["threshold"] = plot_df["name"].astype(str)
            plot_df = plot_df.set_index("threshold")
            score_cols = [
                column for column in ["precision", "recall", "f1"]
                if column in plot_df.columns
            ]
            score_plot = plot_df[score_cols] * 100
            score_plot.columns = [
                "{} (%)".format(column.title()) for column in score_plot.columns
            ]
            st.line_chart(score_plot)

            st.markdown("**Threshold sweep results**")
            st.dataframe(sweep_df, use_container_width=True, hide_index=True)
            st.download_button(
                "⬇️ Download threshold sweep CSV",
                data=sweep_df.to_csv(index=False).encode("utf-8"),
                file_name="threshold_sweep.csv",
                mime="text/csv",
                key="download_threshold_sweep",
            )


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
st.sidebar.title("🛡️ Control Center")
st.sidebar.caption("Fraud monitoring configuration")
scan_limit = st.sidebar.slider(
    "Recent alerts to load", 500, 50000,
    min(max(DEFAULT_SCAN_LIMIT, 500), 50000), step=500
)
auto_refresh = st.sidebar.checkbox("Automatic refresh", value=False)
refresh_seconds = st.sidebar.slider(
    "Refresh interval (seconds)", 5, 60, 10, step=5,
    disabled=not auto_refresh
)
st.sidebar.divider()
st.sidebar.caption("HBase connection")
st.sidebar.code("{}:{}".format(HBASE_HOST, HBASE_PORT))
if st.sidebar.button("↻ Refresh data now", use_container_width=True):
    load_recent_alerts.clear()
    load_account_alerts.clear()
    st.rerun()


# ---------------------------------------------------------------------------
# Header and data load
# ---------------------------------------------------------------------------
st.markdown(
    """
    <div class="hero">
      <div class="hero-kicker">Fraud intelligence platform</div>
      <div class="hero-title">Real-Time Fraud Monitoring</div>
      <p class="hero-subtitle">Live alert visibility across your Kafka · Spark · HBase pipeline</p>
    </div>
    """,
    unsafe_allow_html=True,
)

try:
    with st.spinner("Connecting to HBase and loading recent alerts..."):
        alerts = load_recent_alerts(HBASE_HOST, HBASE_PORT, scan_limit)

    if alerts.empty:
        st.info("No alerts are currently stored in alerts_recent.")
    else:
        st.caption(
            "Showing a newest-first sample of up to {:,} rows from alerts_recent. "
            "KPI totals and charts below describe this loaded sample, not necessarily "
            "the entire HBase table.".format(scan_limit)
        )

        # -------------------------------------------------------------------
        # Filters
        # -------------------------------------------------------------------
        st.subheader("🔎 Investigation filters")
        available_rules = sorted(
            value for value in alerts["rule"].dropna().unique().tolist()
        )
        available_types = sorted(
            value for value in alerts["type"].dropna().unique().tolist()
        )

        f1, f2, f3 = st.columns([1, 1, 1.4])
        with f1:
            selected_rules = st.multiselect(
                "Detection rules", available_rules, default=available_rules
            )
        with f2:
            selected_types = st.multiselect(
                "Transaction types", available_types, default=available_types
            )
        with f3:
            account_query = st.text_input(
                "Account contains", placeholder="e.g. C1305486145"
            )

        f4, f5, f6 = st.columns([1, 1, 1.4])
        valid_amounts = alerts["amount"].dropna()
        data_min = float(valid_amounts.min()) if not valid_amounts.empty else 0.0
        data_max = float(valid_amounts.max()) if not valid_amounts.empty else 0.0
        with f4:
            min_amount = st.number_input(
                "Minimum amount", min_value=0.0, value=0.0, step=100.0
            )
        with f5:
            max_amount = st.number_input(
                "Maximum amount", min_value=0.0,
                value=max(data_max, 0.0), step=100.0
            )
        with f6:
            fraud_filter = st.selectbox(
                "Dataset fraud label",
                ["All labels", "Dataset fraud label = 1", "Dataset fraud label = 0"]
            )

        valid_dates = alerts["event_time"].dropna()
        if not valid_dates.empty:
            min_day = valid_dates.min().date()
            max_day = valid_dates.max().date()
        else:
            min_day = date.today() - timedelta(days=7)
            max_day = date.today()

        date_range = st.date_input(
            "Event date range",
            value=(min_day, max_day),
            min_value=min_day,
            max_value=max_day,
        )

        filtered = filtered_alerts(
            alerts, selected_rules, selected_types, account_query,
            min_amount, max_amount, fraud_filter, date_range
        )

        st.divider()

        # -------------------------------------------------------------------
        # KPI cards
        # -------------------------------------------------------------------
        st.subheader("📊 Monitoring overview")
        total_filtered = len(filtered)
        fraud_labelled = int((filtered["isFraud"] == 1).sum()) if total_filtered else 0
        avg_amount = float(filtered["amount"].mean()) if total_filtered and filtered["amount"].notna().any() else 0.0
        rules_count = filtered["rule"].value_counts() if total_filtered else {}

        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Alerts in view", "{:,}".format(total_filtered))
        k2.metric("Account drain", "{:,}".format(int(rules_count.get("ACCOUNT_DRAIN", 0))))
        k3.metric("Large amount", "{:,}".format(int(rules_count.get("LARGE_AMOUNT", 0))))
        k4.metric("Velocity", "{:,}".format(int(rules_count.get("VELOCITY", 0))))
        k5.metric("Dataset fraud labels", "{:,}".format(fraud_labelled))
        st.caption("Average alert amount in current view: {:,.2f}".format(avg_amount))

        # -------------------------------------------------------------------
        # Analytics
        # -------------------------------------------------------------------
        st.subheader("📈 Alert analytics")
        chart_left, chart_right = st.columns(2)

        with chart_left:
            st.markdown("**Alerts by detection rule**")
            if total_filtered:
                rule_chart = (
                    filtered["rule"].fillna("UNKNOWN")
                    .value_counts().rename_axis("Rule").to_frame("Alerts")
                )
                st.bar_chart(rule_chart)
            else:
                st.info("No alerts match the selected filters.")

        with chart_right:
            st.markdown("**Alerts over time**")
            time_data = filtered.dropna(subset=["event_time"]).copy()
            if not time_data.empty:
                time_data["time_bucket"] = (
                    time_data["event_time"].dt.floor("h")
                )
                timeline = (
                    time_data.groupby("time_bucket").size()
                    .rename("Alerts").to_frame()
                )
                st.line_chart(timeline)
            else:
                st.info("No timestamp data available for this selection.")

        st.markdown("**Transaction amount distribution**")
        amount_data = filtered["amount"].dropna() if total_filtered else pd.Series(dtype=float)
        if not amount_data.empty:
            histogram = amount_data.value_counts(bins=20).sort_index().to_frame("Transactions")
            histogram.index = histogram.index.astype(str)
            st.bar_chart(histogram)
        else:
            st.info("No transaction amounts available for this selection.")

        # -------------------------------------------------------------------
        # Alerts table and CSV export
        # -------------------------------------------------------------------
        st.divider()
        st.subheader("🚨 Alert records")
        st.write("{:,} alert(s) match your filters.".format(total_filtered))

        if total_filtered:
            display_frame(filtered.sort_values(
                "event_time", ascending=False, na_position="last"
            ))
            export_columns = [
                column for column in DISPLAY_COLUMNS if column in filtered.columns
            ]
            export = filtered[export_columns].copy()
            if "event_time" in export.columns:
                export["event_time"] = export["event_time"].dt.strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                )
            st.download_button(
                "⬇️ Export filtered alerts as CSV",
                data=export.to_csv(index=False).encode("utf-8"),
                file_name="fraud_alerts_filtered.csv",
                mime="text/csv",
                use_container_width=True,
            )

            # ----------------------------------------------------------------
            # Alert details
            # ----------------------------------------------------------------
            st.subheader("🔬 Alert investigation")
            detail_options = filtered.sort_values(
                "event_time", ascending=False, na_position="last"
            )
            labels = []
            for _, row in detail_options.iterrows():
                labels.append(
                    "{} | {} | {} | {}".format(
                        row.get("event_time").strftime("%Y-%m-%d %H:%M:%S UTC")
                        if pd.notna(row.get("event_time")) else "Unknown time",
                        row.get("rule", "Unknown rule"),
                        row.get("account", "Unknown account"),
                        row.get("row_key", "")
                    )
                )
            selected_label = st.selectbox("Select an alert to inspect", labels)
            selected_index = labels.index(selected_label)
            selected = detail_options.iloc[selected_index]

            d1, d2, d3 = st.columns(3)
            d1.metric("Detection rule", str(selected.get("rule") or "N/A"))
            d2.metric("Transaction amount", (
                "{:,.2f}".format(selected["amount"])
                if pd.notna(selected.get("amount")) else "N/A"
            ))
            d3.metric("Dataset fraud label", (
                str(int(selected["isFraud"]))
                if pd.notna(selected.get("isFraud")) else "Not recorded"
            ))

            detail_left, detail_right = st.columns(2)
            with detail_left:
                st.markdown("**Account and transaction**")
                st.write("Origin account: `{}`".format(selected.get("account", "N/A")))
                st.write("Destination account: `{}`".format(selected.get("nameDest", "N/A")))
                st.write("Transaction type: `{}`".format(selected.get("type", "N/A")))
                st.write("Step: `{}`".format(selected.get("step", "N/A")))
            with detail_right:
                st.markdown("**Detection metadata**")
                st.write("Event time: `{}`".format(selected.get("event_time", "N/A")))
                st.write("Velocity transaction count: `{}`".format(selected.get("tx_count", "N/A")))
                st.write("HBase row key: `{}`".format(selected.get("row_key", "N/A")))
        else:
            st.info("No alerts match the current filters.")

        # -------------------------------------------------------------------
        # Account lookup in alerts_by_account
        # -------------------------------------------------------------------
        st.divider()
        st.subheader("👤 Account history")
        account_lookup = st.text_input(
            "Search exact account in alerts_by_account",
            placeholder="C1305486145",
            key="account_history_search",
        )
        if account_lookup.strip():
            with st.spinner("Searching account history in HBase..."):
                account_history = load_account_alerts(
                    HBASE_HOST, HBASE_PORT, account_lookup.strip(), 500
                )
            if account_history.empty:
                st.warning("No alert history found for account {}.".format(account_lookup))
            else:
                st.success(
                    "{} alert(s) found for account {}.".format(
                        len(account_history), account_lookup.strip()
                    )
                )
                display_frame(account_history.sort_values(
                    "event_time", ascending=False, na_position="last"
                ))

except Exception as error:
    st.error(
        "Cannot read HBase at {}:{}.\n\n"
        "Check that HBase Thrift is running, the tables exist, and this "
        "dashboard container is on the Hadoop Docker network.\n\n"
        "Technical detail: {}".format(HBASE_HOST, HBASE_PORT, error)
    )
    st.code(
        "docker exec -it hadoop-master bash\n"
        "hbase-daemon.sh start thrift"
    )

# Evaluation reports can be viewed even if the HBase live-alert section is unavailable.
render_evaluation_section()

st.caption("Last dashboard render: {}".format(
    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
))

# Streamlit rerun-based refresh; no extra dependency required.
if auto_refresh:
    time.sleep(refresh_seconds)
    st.rerun()
