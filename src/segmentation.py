import pandas as pd


def assign_segment(row):

    if row["R_score"] >= 3 and row["F_score"] >= 3:
        return "High Value"

    elif row["R_score"] >= 3 and row["F_score"] >= 2:
        return "Loyal"

    elif row["R_score"] >= 3:
        return "Recent"

    elif row["R_score"] <= 2 and row["F_score"] >= 3:
        return "At Risk"

    elif row["R_score"] <= 2:
        return "Lost"

    else:
        return "Average"
    

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