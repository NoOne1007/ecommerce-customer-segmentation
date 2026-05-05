from src.data_loader import load_all_data
from src.preprocessing import preprocess_data
from src.rfm import build_rfm
from src.segmentation import segment_customers


def run_pipeline():

    # Load
    data = load_all_data()

    # Preprocess
    df = preprocess_data(data)

    # RFM
    rfm = build_rfm(df)

    # Segmentation
    rfm = segment_customers(rfm)

    return df, rfm


from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "data" / "processed"


def save_outputs(df, rfm):

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df.to_csv(OUTPUT_DIR / "final_dataset.csv", index=False)
    rfm.to_csv(OUTPUT_DIR / "rfm_segments.csv", index=False)


if __name__ == "__main__":

    df, rfm = run_pipeline()

    save_outputs(df, rfm)

    print("Pipeline executed successfully")
    print("Final dataset shape:", df.shape)
    print("RFM shape:", rfm.shape)