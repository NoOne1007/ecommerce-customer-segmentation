import pandas as pd


def assign_segment(row):
    r = int(row["R_score"])
    f = int(row["F_score"])

    if r >= 3 and f >= 3:
        return "High Value"

    elif r >= 3 and f >= 2:
        return "Loyal"

    elif r >= 3 and f == 1:
        return "Recent"

    elif r <= 2 and f >= 3:
        return "At Risk"

    elif r <= 2 and f == 1:
        return "Lost"

    else:
        return "Average"    # r <= 2, f == 2


def segment_customers(rfm: pd.DataFrame) -> pd.DataFrame:
    rfm["segment"] = rfm.apply(assign_segment, axis=1)
    return rfm


if __name__ == "__main__":
    from src.data_loader import load_all_data
    from src.preprocessing import preprocess_data
    from src.rfm import build_rfm

    data = load_all_data()
    df = preprocess_data(data)
    rfm = build_rfm(df)
    rfm = segment_customers(rfm)

    print(rfm["segment"].value_counts())