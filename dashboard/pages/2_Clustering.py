import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "processed"

st.set_page_config(page_title="Clustering", layout="wide")


def assign_labels(profiles: pd.DataFrame) -> dict:
    """
    Assign cluster labels based on actual cluster characteristics.
    Never relies on cluster number — stable across pipeline re-runs.
    """
    labels = {}
    remaining = profiles.copy()

    # Repeat Buyers: highest avg frequency
    idx = remaining["avg_frequency"].idxmax()
    labels[remaining.loc[idx, "cluster"]] = "Repeat Buyers"
    remaining = remaining.drop(idx)

    # Lapsed Low-Value: highest avg recency among the rest
    idx = remaining["avg_recency"].idxmax()
    labels[remaining.loc[idx, "cluster"]] = "Lapsed Low-Value"
    remaining = remaining.drop(idx)

    # Recent Mid-Spenders: higher monetary among the two remaining
    idx = remaining["avg_monetary"].idxmax()
    labels[remaining.loc[idx, "cluster"]] = "Recent Mid-Spenders"
    remaining = remaining.drop(idx)

    # Recent Low-Spenders: whatever is left
    labels[remaining.iloc[0]["cluster"]] = "Recent Low-Spenders"

    return labels


@st.cache_data
def load_data():
    df = pd.read_csv(DATA_DIR / "customer_clusters.csv")

    profiles = (
        df
        .groupby("cluster", as_index=False)
        .agg(
            customers     = ("customer_unique_id", "count"),
            avg_recency   = ("recency",   "mean"),
            avg_frequency = ("frequency", "mean"),
            avg_monetary  = ("monetary",  "mean")
        )
        .round(2)
    )

    label_map = assign_labels(profiles)
    profiles["label"] = profiles["cluster"].map(label_map)
    df["cluster_label"] = df["cluster"].map(label_map)

    return df, profiles


df, profiles = load_data()

st.title("K-Means Clustering")
st.caption("Unsupervised clustering on log-transformed, scaled RFM features · K=4")
st.divider()

st.subheader("Cluster Profiles")
st.dataframe(
    profiles[["cluster", "label", "customers",
              "avg_recency", "avg_frequency", "avg_monetary"]]
    .rename(columns={
        "cluster":       "Cluster",
        "label":         "Label",
        "customers":     "Customers",
        "avg_recency":   "Avg Recency (days)",
        "avg_frequency": "Avg Frequency",
        "avg_monetary":  "Avg Monetary (BRL)"
    }),
    use_container_width=True,
    hide_index=True
)

st.divider()

col1, col2 = st.columns(2)

with col1:
    st.subheader("Customer Count by Cluster")
    fig1 = px.bar(
        profiles,
        x="label",
        y="customers",
        color="label",
        labels={"label": "Cluster", "customers": "Customers"},
        color_discrete_sequence=px.colors.qualitative.Set1
    )
    fig1.update_layout(showlegend=False, xaxis_tickangle=-20)
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    st.subheader("Avg Monetary Value by Cluster")
    fig2 = px.bar(
        profiles,
        x="label",
        y="avg_monetary",
        color="label",
        labels={"label": "Cluster", "avg_monetary": "Avg Monetary (BRL)"},
        color_discrete_sequence=px.colors.qualitative.Set1
    )
    fig2.update_layout(showlegend=False, xaxis_tickangle=-20)
    st.plotly_chart(fig2, use_container_width=True)