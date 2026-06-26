from pathlib import Path

from src.data_loader import load_all_data
from src.preprocessing import preprocess_data
from src.rfm import build_rfm
from src.segmentation import segment_customers
from src.clustering import build_clusters

BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "data" / "processed"


def run_pipeline():
    # Load
    data = load_all_data()

    # Preprocess
    df = preprocess_data(data)

    # RFM
    rfm = build_rfm(df)

    # Segmentation
    rfm = segment_customers(rfm)

    # Clustering (also saves models to models/)
    clustered = build_clusters(rfm)

    return df, rfm, clustered


def save_outputs(df, rfm, clustered):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_DIR / "final_dataset.csv", index=False)
    rfm.to_csv(OUTPUT_DIR / "rfm_segments.csv", index=False)
    clustered.to_csv(OUTPUT_DIR / "customer_clusters.csv", index=False)


if __name__ == "__main__":
    df, rfm, clustered = run_pipeline()
    save_outputs(df, rfm, clustered)

    print("Pipeline complete.")
    print(f"  final_dataset:     {df.shape}")
    print(f"  rfm_segments:      {rfm.shape}")
    print(f"  customer_clusters: {clustered.shape}")