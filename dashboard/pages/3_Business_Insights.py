import streamlit as st
import pandas as pd
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "processed"

st.set_page_config(page_title="Business Insights", layout="wide")


@st.cache_data
def load_data():
    clusters = pd.read_csv(DATA_DIR / "customer_clusters.csv")
    df       = pd.read_csv(DATA_DIR / "final_dataset.csv")
    return clusters, df


clusters, df = load_data()

st.title("Business Insights")
st.caption("Recommendations derived from RFM segmentation and K-Means clustering")
st.divider()

# --- Compute numbers dynamically ---
at_risk      = clusters[clusters["segment"] == "At Risk"]
high_value   = clusters[clusters["segment"] == "High Value"]
total_rev    = clusters["monetary"].sum()
hv_rev       = high_value["monetary"].sum()
ar_rev       = at_risk["monetary"].sum()
ar_customers = len(at_risk)
ar_avg_rev   = at_risk["monetary"].mean()

st.subheader("🔴 Priority: Win Back At-Risk Customers")
st.error(
    f"**{ar_customers:,} customers** have not purchased in ~364 days on average "
    f"but previously spent an average of **BRL {ar_avg_rev:.0f}** each. "
    f"This segment represents **BRL {ar_rev:,.0f}** in at-risk revenue "
    f"({ar_rev / total_rev * 100:.1f}% of total). "
    f"A targeted reactivation campaign recovering even 10% of this segment "
    f"would recover approximately **BRL {ar_rev * 0.1:,.0f}**."
)

st.subheader("🟡 Opportunity: Protect High-Value Customers")
st.warning(
    f"**{len(high_value):,} High Value customers** account for "
    f"**BRL {hv_rev:,.0f}** ({hv_rev / total_rev * 100:.1f}% of total revenue). "
    f"These customers are active and frequent. Retention investment here has the "
    f"highest revenue-per-customer return at **BRL {high_value['monetary'].mean():.0f}** average spend."
)

st.subheader("🟢 Opportunity: Convert One-Time Buyers")
one_time = clusters[clusters["frequency"] == 1]
st.success(
    f"**{len(one_time):,} customers** ({len(one_time) / len(clusters) * 100:.1f}%) "
    f"made exactly one purchase. Even a modest improvement in repeat purchase rate "
    f"through post-purchase engagement (review follow-ups, personalised recommendations) "
    f"would meaningfully shift the frequency distribution."
)

st.subheader("🔵 Insight: Clustering Reveals Repeat Buyers Across Segments")
# Identify repeat buyers cluster dynamically — the one with avg frequency > 1
repeat_cluster_id = (
    clusters.groupby("cluster")["frequency"]
    .mean()
    .idxmax()
)
repeat = clusters[clusters["cluster"] == repeat_cluster_id]
st.info(
    f"K-Means identified **{len(repeat):,} Repeat Buyers** (avg {repeat['frequency'].mean():.1f} orders) "
    f"that the rule-based segmentation splits across High Value and At Risk depending on recency. "
    f"These customers have demonstrated willingness to repurchase — they are the highest-priority "
    f"cohort for loyalty program targeting regardless of their current recency status."
)

st.divider()
st.subheader("Seasonal Observation")
st.markdown(
    "Revenue peaked in **November 2017 (BRL 1,153,528)**, coinciding with Black Friday — "
    "a major retail event in Brazil. This suggests the platform is sensitive to promotional "
    "calendar events. Proactive segment-targeted campaigns ahead of Q4 would likely amplify this effect."
)