import pandas as pd


def aggregate_order_items(order_items: pd.DataFrame) -> pd.DataFrame:
    df = (
        order_items
        .groupby("order_id", as_index=False)
        .agg({
            "order_item_id": "count",
            "price": "sum",
            "freight_value": "sum"
        })
        .rename(columns={
            "order_item_id": "num_items",
            "price": "total_price",
            "freight_value": "total_freight"
        })
    )

    return df

def aggregate_payments(payments: pd.DataFrame) -> pd.DataFrame:
    df = (
        payments
        .groupby("order_id", as_index=False)
        .agg({
            "payment_value": "sum"
        })
    )

    return df

def build_dataset(
    orders: pd.DataFrame,
    customers: pd.DataFrame,
    order_items: pd.DataFrame,
    payments: pd.DataFrame
) -> pd.DataFrame:

    order_items_agg = aggregate_order_items(order_items)
    payments_agg = aggregate_payments(payments)

    df = (
        orders
        .merge(customers, on="customer_id", how="left")
        .merge(payments_agg, on="order_id", how="left")
        .merge(order_items_agg, on="order_id", how="left")
    )

    return df

def convert_dates(df: pd.DataFrame) -> pd.DataFrame:
    df["order_purchase_timestamp"] = pd.to_datetime(
        df["order_purchase_timestamp"]
    )
    return df


def preprocess_data(data: dict) -> pd.DataFrame:
    df = build_dataset(
        data["orders"],
        data["customers"],
        data["order_items"],
        data["payments"]
    )

    df = convert_dates(df)

    return df

if __name__ == "__main__":
    from src.data_loader import load_all_data

    data = load_all_data()
    df = preprocess_data(data)

    print(df.shape)
    print(df.head())