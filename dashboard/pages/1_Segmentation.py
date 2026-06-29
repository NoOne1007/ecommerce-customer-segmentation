import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "processed"

st.set_page_config(page_title="Segmentation", layout="wide")


@st.cache_data
def load_data():
    return pd.read_csv(DATA_DIR / "customer_clusters.csv")


df = load_data()

st.title("Customer Segmentation")
st.caption("Rule-based segments derived from RFM scores")
st.divider()

segment_summary = (
    df
    .groupby("segment", as_index=False)
    .agg(
        customers     = ("customer_unique_id", "count"),
        total_revenue = ("monetary", "sum"),
        avg_revenue   = ("monetary", "mean")
    )
    .sort_values("total_revenue", ascending=False)
    .round(2)
)

col1, col2 = st.columns(2)

with col1:
    st.subheader("Revenue by Segment")
    fig1 = px.bar(
        segment_summary,
        x="segment",
        y="total_revenue",
        color="segment",
        labels={"segment": "Segment", "total_revenue": "Total Revenue (BRL)"},
        color_discrete_sequence=px.colors.qualitative.Set2
    )
    fig1.update_layout(showlegend=False)
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    st.subheader("Customer Count by Segment")
    fig2 = px.bar(
        segment_summary,
        x="segment",
        y="customers",
        color="segment",
        labels={"segment": "Segment", "customers": "Customers"},
        color_discrete_sequence=px.colors.qualitative.Set2
    )
    fig2.update_layout(showlegend=False)
    st.plotly_chart(fig2, use_container_width=True)

st.divider()
st.subheader("Segment Summary")
st.dataframe(
    segment_summary.rename(columns={
        "segment":      "Segment",
        "customers":    "Customers",
        "total_revenue":"Total Revenue (BRL)",
        "avg_revenue":  "Avg Revenue / Customer (BRL)"
    }),
    use_container_width=True,
    hide_index=True
)