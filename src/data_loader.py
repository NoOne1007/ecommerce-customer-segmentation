import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).parent.resolve().parent
DATA_DIR = BASE_DIR / "data" / "raw"
print(f"Data directory: {DATA_DIR}")

def load_orders():
    return pd.read_csv(DATA_DIR / "olist_orders_dataset.csv")


def load_customers():
    return pd.read_csv(DATA_DIR / "olist_customers_dataset.csv")


def load_order_items():
    return pd.read_csv(DATA_DIR / "olist_order_items_dataset.csv")


def load_payments():
    return pd.read_csv(DATA_DIR / "olist_order_payments_dataset.csv")


def load_all_data():
    return {
        "orders": load_orders(),
        "customers": load_customers(),
        "order_items": load_order_items(),
        "payments": load_payments(),
    }



if __name__ == "__main__":
    data = load_all_data()
    for key, df in data.items():
        print(f"{key} shape: {df.shape}")