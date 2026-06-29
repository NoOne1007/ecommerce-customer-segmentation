import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "processed"

st.set_page_config(
    page_title="Customer Segmentation | Olist",
    page_icon="🛒",
    layout="wide"
)


@st.cache_data
def load_data():
    df = pd.read_csv(
        DATA_DIR / "final_dataset.csv",
        parse_dates=["order_purchase_timestamp"]
    )
    clusters = pd.read_csv(DATA_DIR / "customer_clusters.csv")
    return df, clusters


df, clusters = load_data()

st.title("E-Commerce Customer Segmentation")
st.caption("Brazilian E-Commerce (Olist) Dataset · 2016–2018")
st.divider()

# --- KPIs ---
total_revenue   = df["payment_value"].sum()
total_customers = clusters["customer_unique_id"].nunique()
total_orders    = df["order_id"].nunique()
aov             = total_revenue / total_orders

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Revenue",    f"BRL {total_revenue:,.0f}")
col2.metric("Total Customers",  f"{total_customers:,}")
col3.metric("Total Orders",     f"{total_orders:,}")
col4.metric("Avg Order Value",  f"BRL {aov:.2f}")

st.divider()

# --- Monthly Revenue Trend ---
st.subheader("Monthly Revenue Trend")

monthly = (
    df
    .groupby(df["order_purchase_timestamp"].dt.to_period("M"))["payment_value"]
    .sum()
    .reset_index()
)
monthly.columns = ["month", "revenue"]
monthly["month"] = monthly["month"].astype(str)

fig = px.line(
    monthly,
    x="month",
    y="revenue",
    markers=True,
    labels={"month": "Month", "revenue": "Revenue (BRL)"}
)
fig.update_layout(xaxis_tickangle=-45)
st.plotly_chart(fig, use_container_width=True)