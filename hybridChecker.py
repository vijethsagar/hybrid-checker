import streamlit as st
import requests
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

# Set page layout
st.set_page_config(layout="wide", page_title="StarTree Time Boundary Inspector", page_icon="📊")
st.markdown("""
    <style>
        .stTextInput > div > div > input {
            background-color: #f7f9fa;
            border-radius: 8px;
            padding: 10px;
        }
        .stSelectbox > div > div > div {
            background-color: #f7f9fa;
            border-radius: 8px;
        }
        .stButton button {
            background-color: #4CAF50;
            color: white;
            font-weight: bold;
            padding: 10px 20px;
            border-radius: 8px;
        }
    </style>
""", unsafe_allow_html=True)

st.title("📊 StarTree Time Boundary Inspector")
st.caption("Visually analyze REALTIME vs OFFLINE data boundaries for Pinot hybrid tables")

# Sidebar Inputs
st.sidebar.header("🔧 Configuration")
broker_url = st.sidebar.text_input("Pinot Broker URL", placeholder="e.g., https://broker.pinot.xxxxxx.cp.s7e.startree.cloud")
bearer_token = st.sidebar.text_input("Bearer Token", type="password")

# Fetch table list as soon as broker URL and token are available
table_list = []
table_name = None
headers = {}

if broker_url and bearer_token:
    headers = {"Authorization": f"Bearer {bearer_token}"}
    try:
        table_list_url = f"{broker_url.replace('://broker.', '://', 1).rstrip('/')}/tables"
        response = requests.get(table_list_url, headers=headers)
        if response.status_code == 200:
            table_list = response.json().get("tables", [])
            if table_list:
                table_name = st.selectbox("📂 Select Table", sorted(table_list))
        else:
            st.error("Failed to fetch table list.")
    except Exception as e:
        st.error(f"Error fetching tables: {e}")
else:
    st.info("Enter Broker URL and Bearer Token to load table list.")

def epoch_to_datetime_str(epoch_ms):
    try:
        return datetime.fromtimestamp(int(epoch_ms) / 1000.0).strftime('%Y-%m-%d %I:%M:%S %p')
    except:
        return "Invalid timestamp"

# Fetch and visualize when button clicked
if table_name and st.button("Analyze Time Boundaries"):
    try:
        headers["Content-Type"] = "application/json"

        boundary_url = f"{broker_url.rstrip('/')}/debug/timeBoundary/{table_name}"
        boundary_response = requests.get(boundary_url, headers=headers)

        if boundary_response.status_code != 200:
            st.error(f"Failed to get time boundary: {boundary_response.status_code}")
            if boundary_response.status_code == 404:
                st.warning("⚠️ Please pick a Hybrid table")
        else:
            boundary_json = boundary_response.json()
            time_column = boundary_json.get("timeColumn")
            time_value = boundary_json.get("timeValue")

            st.markdown("---")
            st.subheader("🔍 Time Boundary")
            st.markdown(f"""
            <div style='background-color:#eef8f2;padding:1em;border-radius:10px'>
                <strong>⏱ Time Column:</strong> `{time_column}`<br>
                <strong>📌 Boundary Value:</strong> `{epoch_to_datetime_str(time_value)}`
            </div>
            """, unsafe_allow_html=True)

            query_url = f"{broker_url.rstrip('/')}/query/sql"
            def run_query(sql): return requests.post(query_url, headers=headers, json={"sql": sql})

            queries = {
                "min_rt": f"SELECT MIN({time_column}) AS min_time FROM {table_name}_REALTIME",
                "max_rt": f"SELECT MAX({time_column}) AS max_time FROM {table_name}_REALTIME",
                "min_off": f"SELECT MIN({time_column}) AS min_time FROM {table_name}_OFFLINE",
                "max_off": f"SELECT MAX({time_column}) AS max_time FROM {table_name}_OFFLINE"
            }

            resp = {k: run_query(q) for k, q in queries.items()}

            st.subheader("📊 Component Table Stats")

            if resp["min_rt"].status_code == 200 and resp["max_off"].status_code == 200:
                min_rt_val = float(resp["min_rt"].json()["resultTable"]["rows"][0][0])
                max_off_val = float(resp["max_off"].json()["resultTable"]["rows"][0][0])
                if max_off_val < min_rt_val:
                    st.markdown("⚠️ Data loss detected: Max of OFFLINE is less than Min of REALTIME")
                if min_rt_val > float(time_value):
                    st.markdown("⚠️ Data loss detected: Min of REALTIME is greater than Time Boundary")
            else:
                st.markdown("❌ Could not load dates for validation")

            def safe_epoch(resp_obj):
                return float(resp_obj.json()["resultTable"]["rows"][0][0]) if resp_obj.status_code == 200 else None

            min_time = safe_epoch(resp["min_rt"])
            realtimemax_time = safe_epoch(resp["max_rt"])
            min_time_off = safe_epoch(resp["min_off"])
            max_time = safe_epoch(resp["max_off"])

            # Print stats
            st.markdown("**REALTIME TABLE**")
            if min_time: st.markdown(f"MIN ({time_column}): `{epoch_to_datetime_str(min_time)}`")
            if realtimemax_time: st.markdown(f"MAX ({time_column}): `{epoch_to_datetime_str(realtimemax_time)}`")
            st.markdown("**OFFLINE TABLE**")
            if min_time_off: st.markdown(f"MIN ({time_column}): `{epoch_to_datetime_str(min_time_off)}`")
            if max_time: st.markdown(f"MAX ({time_column}): `{epoch_to_datetime_str(max_time)}`")

            # Visualization continues here...
            if min_time and realtimemax_time and max_time and min_time_off and time_value:
                try:
                    dt_min = datetime.fromtimestamp(min_time / 1000.0)
                    dt_rt_max = datetime.fromtimestamp(realtimemax_time / 1000.0)
                    dt_off_max = datetime.fromtimestamp(max_time / 1000.0)
                    dt_boundary = datetime.fromtimestamp(float(time_value) / 1000.0)
                    difference = (realtimemax_time - min_time) / 1000
                    buffer = datetime.fromtimestamp(max_time / 1000.0 - difference)
                    dt_off_min_raw = datetime.fromtimestamp(min_time_off / 1000.0)
                    dt_off_min = max(buffer, dt_off_min_raw)

                    st.markdown("---")
                    st.subheader("📈 Timeline Visualization")
                    fig, ax = plt.subplots(figsize=(12, 3))
                    ax.set_title("Time Range Overview")
                    ax.set_xlabel("Time")
                    ax.set_yticks([0, 1])
                    ax.set_yticklabels(["OFFLINE", "REALTIME"])

                    ax.hlines(y=1, xmin=dt_min, xmax=dt_rt_max, color='limegreen', linewidth=12, label='REALTIME Range')
                    ax.hlines(y=0, xmin=dt_off_min, xmax=dt_off_max, color='indianred', linewidth=12, label='OFFLINE Range')
                    ax.axvline(dt_boundary, color='gold', linestyle='--', linewidth=2, label='Time Boundary')

                    current_date = dt_min.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
                    while current_date < max(dt_rt_max, dt_off_max):
                        ax.axvline(current_date, color='gray', linestyle=':', linewidth=1)
                        current_date += timedelta(days=1)

                    fig.autofmt_xdate()
                    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.25), ncol=3)
                    st.pyplot(fig)

                except Exception as e:
                    st.error(f"Failed to render timeline plot: {str(e)}")

    except Exception as e:
        st.error(f"An unexpected error occurred: {str(e)}")
