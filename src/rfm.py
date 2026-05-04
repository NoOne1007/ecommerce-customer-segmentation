import pandas as pd


def compute_rfm(df: pd.DataFrame) -> pd.DataFrame:
    reference_date = df["order_purchase_timestamp"].max() + pd.Timedelta(days=1)

    rfm = (
        df
        .groupby("customer_unique_id", as_index=False)
        .agg({
            "order_purchase_timestamp": "max",
            "order_id": "nunique",
            "payment_value": "sum"
        })
    )

    rfm.columns = [
        "customer_unique_id",
        "last_purchase",
        "frequency",
        "monetary"
    ]

    # Recency
    rfm["recency"] = (reference_date - rfm["last_purchase"]).dt.days

    return rfm.drop(columns=["last_purchase"])


def score_rfm(rfm: pd.DataFrame) -> pd.DataFrame:

    # Recency: lower is better → reverse labels
    rfm["R_score"] = pd.qcut(
        rfm["recency"],
        4,
        labels=[4, 3, 2, 1]
    )

    # Frequency: handle duplicates using rank
    rfm["F_score"] = pd.qcut(
        rfm["frequency"].rank(method="first"),
        4,
        labels=[1, 2, 3, 4]
    )

    # Monetary
    rfm["M_score"] = pd.qcut(
        rfm["monetary"],
        4,
        labels=[1, 2, 3, 4]
    )

    return rfm


def add_rfm_score(rfm: pd.DataFrame) -> pd.DataFrame:
    rfm["RFM_score"] = (
        rfm["R_score"].astype(str) +
        rfm["F_score"].astype(str) +
        rfm["M_score"].astype(str)
    )

    return rfm


def build_rfm(df: pd.DataFrame) -> pd.DataFrame:
    rfm = compute_rfm(df)
    rfm = score_rfm(rfm)
    rfm = add_rfm_score(rfm)

    return rfm


if __name__ == "__main__":
    from src.data_loader import load_all_data
    from src.preprocessing import preprocess_data

    data = load_all_data()
    df = preprocess_data(data)

    rfm = build_rfm(df)

    print(rfm.head())
    print(rfm.shape)