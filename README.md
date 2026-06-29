# E-Commerce Customer Segmentation

A end-to-end data analytics and machine learning project built on the Brazilian E-Commerce (Olist) public dataset. The goal is to help an e-commerce platform understand its customer base well enough to act on it — not just describe it.

**Live Dashboard:** [ecommerce-customer-segmentation-noone.streamlit.app](https://ecommerce-customer-segmentation-noone.streamlit.app)

---

## Business Problem

An e-commerce platform with ~93,000 customers cannot treat every customer the same way. A blanket discount campaign wastes budget on customers who would have purchased anyway, while doing nothing for the ones who are quietly lapsing.

The core questions this project answers:

- Who are our most valuable customers, and are they at risk of leaving?
- Which customers have already lapsed, and is it worth trying to win them back?
- What does our repeat purchase rate look like, and what would improving it mean for revenue?

---

## Dataset

**Source:** [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — available on Kaggle.

The dataset covers ~100,000 orders placed on the Olist marketplace between 2016 and 2018. It includes order status, payment values, customer identifiers, and timestamps across multiple relational tables.

**Scope:** Only delivered orders are included in this analysis. Cancelled, unavailable, and in-transit orders are excluded — they represent transactions where payment was not collected, and including them would inflate both revenue figures and customer frequency counts.

After filtering: **96,478 delivered orders** across **93,358 unique customers**.

---

## Project Structure

```
ecommerce-customer-segmentation/
│
├── data/
│   ├── raw/                        ← Kaggle source files (not committed)
│   └── processed/
│       ├── final_dataset.csv       ← Cleaned, merged transaction data
│       ├── rfm_segments.csv        ← RFM scores + rule-based segments
│       └── customer_clusters.csv   ← K-Means cluster assignments
│
├── models/
│   ├── scaler.pkl                  ← Fitted StandardScaler
│   └── kmeans_model.pkl            ← Fitted KMeans model
│
├── notebooks/
│   ├── 01_eda.ipynb                ← Exploratory data analysis
│   ├── 02_rfm_feature_engineering.ipynb  ← RFM metrics + segmentation
│   └── 03_clustering.ipynb         ← K-Means clustering + comparison
│
├── src/
│   ├── data_loader.py              ← CSV loading functions
│   ├── preprocessing.py            ← Merging, filtering, date conversion
│   ├── rfm.py                      ← RFM computation and scoring
│   ├── segmentation.py             ← Rule-based segment assignment
│   ├── clustering.py               ← K-Means training and model persistence
│   └── pipeline.py                 ← End-to-end pipeline orchestration
│
├── dashboard/
│   ├── app.py                      ← Executive Overview (entry point)
│   └── pages/
│       ├── 1_Segmentation.py
│       ├── 2_Clustering.py
│       └── 3_Business_Insights.py
│
├── .python-version                 ← Pins Python 3.11 for deployment
├── requirements.txt
└── README.md
```

---

## Methodology

### 1. Data Preparation

Raw Olist data arrives as eight separate CSV files. The pipeline joins orders, customers, order items, and payments into a single flat table, filters to delivered orders only, and converts timestamps for downstream use.

### 2. RFM Feature Engineering

RFM is a behaviour-based customer scoring framework derived entirely from transaction history:

| Metric | Definition | Why it matters |
|---|---|---|
| **Recency** | Days since last purchase | Recent customers are more likely to respond to outreach |
| **Frequency** | Number of unique orders | Repeat buyers signal loyalty and lower acquisition cost |
| **Monetary** | Total payment value (BRL) | Captures revenue contribution independent of order count |

Each metric is scored 1–4 using quartile binning. Score 4 is always best. Recency labels are reversed (lower recency = more recent = better score).

**Frequency scoring note:** 97% of customers made exactly one purchase, making direct quantile binning on raw frequency impossible — the value 1 spans three quartile boundaries simultaneously. `rank(method="first")` assigns unique ranks before binning, resolving the tie issue without altering the underlying data.

### 3. Rule-Based Segmentation

Customers are assigned to one of six segments using explicit R and F score rules:

| Segment | Rule | Business interpretation |
|---|---|---|
| **High Value** | R ≥ 3 and F ≥ 3 | Active and frequent — protect and reward |
| **Loyal** | R ≥ 3 and F = 2 | Active, moderate frequency — nurture |
| **Recent** | R ≥ 3 and F = 1 | New or returned — onboard and engage |
| **At Risk** | R ≤ 2 and F ≥ 3 | Previously frequent, now inactive — win back |
| **Average** | R ≤ 2 and F = 2 | Low engagement, moderate history |
| **Lost** | R ≤ 2 and F = 1 | Inactive one-time buyers — deprioritise |

Monetary score is computed but not used in segment assignment. The segments are defined by purchase behaviour — recency and frequency — and monetary value is used afterward to measure the revenue consequence of each segment.  
This keeps the segmentation logic clean and the business interpretation 
straightforward.

### 4. K-Means Clustering

K-Means clustering provides a second, data-driven view of the customer base that does not rely on predefined rules.

**Feature engineering:**
- Frequency and monetary are log-transformed (`log1p`) to reduce the influence of high-spend outliers on distance calculations
- All three features are standardised using `StandardScaler` before clustering

**Why K = 4:**
Elbow method analysis showed diminishing returns in inertia reduction beyond K = 4. Four clusters also produces interpretable, actionable groups at this data scale.

**Model persistence:** Both the fitted `StandardScaler` and `KMeans` model are serialised to `models/` using `joblib`. This ensures consistent cluster assignment without retraining and supports future inference on new customers.

**Cluster labels are assigned dynamically** based on actual cluster characteristics (highest avg frequency → Repeat Buyers, highest avg recency → Lapsed Low-Value, etc.) rather than hardcoded cluster numbers. K-Means does not guarantee stable numbering across runs, so hardcoding is a silent failure mode.

---

## Pipeline

The full pipeline runs with a single command from the project root:

```bash
python -m src.pipeline
```

This executes the following steps in sequence:

```
Raw CSVs → data_loader.py → preprocessing.py → rfm.py → segmentation.py → clustering.py → pipeline.py → Processed CSVs + Model Artifacts
```

Outputs written to `data/processed/` and `models/`. The dashboard reads only from these outputs — it does not rerun the pipeline.

---

## Key Findings

**Revenue concentration**
The top 20% of customers account for 53.5% of total revenue (BRL 15.4M total). Segment-targeted strategies have disproportionate impact relative to blanket campaigns.

**One-time buyer dominance**
97% of customers (90,557 of 93,358) made exactly one purchase. Frequency is the most significant challenge for this platform — improving repeat purchase rate even modestly would materially shift the revenue profile.

**November 2017 revenue peak**
Monthly revenue peaked at BRL 1,153,528 in November 2017, consistent with Black Friday promotional activity — a major retail event in Brazil.

**At Risk is the primary actionable segment**
23,188 customers have not purchased in ~364 days on average but previously spent BRL 166 per customer on average. This segment represents BRL 3.86M in at-risk revenue. A targeted reactivation campaign recovering 10% of this segment recovers approximately BRL 386K.

**K-Means reveals what rules miss**
The Repeat Buyers cluster (2,801 customers, avg 2.1 orders) cuts across rule-based segment boundaries. Rule-based segmentation splits these customers into High Value and At Risk depending on recency. K-Means keeps them together by purchase behaviour — they have demonstrated willingness to repurchase regardless of when they last did so, making them the highest-priority cohort for loyalty program investment.

---

## Dashboard

Four-page Streamlit dashboard deployed at [ecommerce-customer-segmentation-noone.streamlit.app](https://ecommerce-customer-segmentation-noone.streamlit.app):

| Page | Contents |
|---|---|
| **Executive Overview** | Total revenue, customers, orders, AOV, monthly revenue trend |
| **Segmentation** | Revenue and customer count by RFM segment, segment summary table |
| **Clustering** | K-Means cluster profiles, customer count and monetary charts |
| **Business Insights** | Quantified recommendations derived from segmentation and clustering |

The dashboard reads only from `data/processed/` — no pipeline recomputation at runtime.

---

## How to Run

**Prerequisites:** Python 3.11, pip

```bash
# 1. Clone the repository
git clone https://github.com/NoOne1007/ecommerce-customer-segmentation.git
cd ecommerce-customer-segmentation

# 2. Create and activate virtual environment
python -m venv venv
source venv/bin/activate        # Mac/Linux
venv\Scripts\activate           # Windows

# 3. Install dependencies
pip install -r requirements.txt

# 4. Download raw data
# Download the Olist dataset from:
# https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
# Extract all CSV files into data/raw/

# 5. Run the pipeline
python -m src.pipeline

# 6. Launch the dashboard
streamlit run dashboard/app.py
```

The pipeline generates all processed files and model artifacts. The dashboard is then available at `http://localhost:8501`.

---

## Tech Stack

| Category | Tools |
|---|---|
| Language | Python 3.11 |
| Data manipulation | pandas, numpy |
| Machine learning | scikit-learn (KMeans, StandardScaler) |
| Model persistence | joblib |
| Visualisation | matplotlib, seaborn (notebooks), plotly (dashboard) |
| Dashboard | Streamlit |
| Version control | Git, GitHub |
| Deployment | Streamlit Community Cloud |

---

## Limitations

- **Dataset is historical (2016–2018).** Recency scores and business insights reflect a fixed snapshot, not live customer behaviour.
- **No customer demographics.** Segmentation is based purely on transaction history. Demographic or product category data would enable richer profiling.
- **Frequency range is narrow.** With 97% of customers at frequency = 1, frequency contributes limited differentiation to both rule-based segments and clustering. This is a genuine platform characteristic, not a modelling artefact.
- **Single-market context.** Olist operates in Brazil. Currency (BRL), seasonal patterns (Black Friday timing, local holidays), and consumer behaviour may not generalise to other markets.

---

## Future Improvements

- **Cohort analysis** — track retention curves by acquisition month to understand 
  whether repeat purchase rates improved over the platform's growth period
- **Product category segmentation** — layer in what customers bought, not just how 
  often and how much, to enable category-level targeting
- **Churn prediction model** — convert the At Risk segment finding into a binary 
  classification problem with a probability score per customer
- **CLV estimation** — extend RFM into a forward-looking Customer Lifetime Value 
  model using probabilistic frameworks (BG/NBD)