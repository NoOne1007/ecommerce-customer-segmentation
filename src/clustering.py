import joblib
import numpy as np
from pathlib import Path
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

BASE_DIR = Path(__file__).resolve().parent.parent
MODELS_DIR = BASE_DIR / "models"


def prepare_features(rfm):
    """
    Log-transform frequency and monetary to reduce
    the effect of skewed distributions on distance-based clustering.
    Returns the modified dataframe, scaled features, and the fitted scaler.
    """
    rfm_cluster = rfm.copy()

    rfm_cluster["frequency_log"] = np.log1p(rfm_cluster["frequency"])
    rfm_cluster["monetary_log"] = np.log1p(rfm_cluster["monetary"])

    features = rfm_cluster[["recency", "frequency_log", "monetary_log"]]

    scaler = StandardScaler()
    scaled_features = scaler.fit_transform(features)

    return rfm_cluster, scaled_features, scaler


def run_kmeans(rfm_cluster, scaled_features, n_clusters=4):
    """
    Fit KMeans and assign cluster labels.
    Returns the dataframe with cluster column and the fitted model.
    """
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    rfm_cluster["cluster"] = kmeans.fit_predict(scaled_features)
    return rfm_cluster, kmeans


def save_models(scaler, kmeans):
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump(scaler, MODELS_DIR / "scaler.pkl")
    joblib.dump(kmeans, MODELS_DIR / "kmeans_model.pkl")
    print(f"Models saved to {MODELS_DIR}")


def load_models():
    scaler = joblib.load(MODELS_DIR / "scaler.pkl")
    kmeans = joblib.load(MODELS_DIR / "kmeans_model.pkl")
    return scaler, kmeans


def build_clusters(rfm, n_clusters=4):
    """
    Full clustering pipeline: feature prep → scaling → KMeans → save models.
    """
    rfm_cluster, scaled_features, scaler = prepare_features(rfm)
    rfm_cluster, kmeans = run_kmeans(rfm_cluster, scaled_features, n_clusters)
    save_models(scaler, kmeans)

    return rfm_cluster.drop(columns=["frequency_log", "monetary_log"])


if __name__ == "__main__":
    from src.data_loader import load_all_data
    from src.preprocessing import preprocess_data
    from src.rfm import build_rfm
    from src.segmentation import segment_customers

    data = load_all_data()
    df = preprocess_data(data)
    rfm = build_rfm(df)
    rfm = segment_customers(rfm)
    clustered = build_clusters(rfm)

    print(clustered.head())
    print(clustered["cluster"].value_counts())