from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import numpy as np


def prepare_features(rfm):
    """
    Create clustering features using
    log-transformed frequency and monetary.
    """

    rfm_cluster = rfm.copy()

    rfm_cluster["frequency_log"] = np.log1p(
        rfm_cluster["frequency"]
    )

    rfm_cluster["monetary_log"] = np.log1p(
        rfm_cluster["monetary"]
    )

    features = rfm_cluster[
        [
            "recency",
            "frequency_log",
            "monetary_log"
        ]
    ]

    scaler = StandardScaler()

    scaled_features = scaler.fit_transform(
        features
    )

    return rfm_cluster, scaled_features


def run_kmeans(
    rfm_cluster,
    scaled_features,
    n_clusters=4
):
    """
    Run final KMeans clustering.
    """

    kmeans = KMeans(
        n_clusters = n_clusters,
        random_state = 42,
        n_init = 10
    )

    rfm_cluster["cluster"] = (
        kmeans.fit_predict(
            scaled_features
        )
    )

    return rfm_cluster


def build_clusters(rfm, n_clusters = 4):
    """
    Complete clustering pipeline.
    """

    rfm_cluster, scaled_features = (
        prepare_features(rfm)
    )

    rfm_cluster = run_kmeans(
        rfm_cluster,
        scaled_features,
        n_clusters
    )

    return rfm_cluster.drop(
        columns=[
            "frequency_log",
            "monetary_log"
        ]
    )


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

    print(
        clustered["cluster"]
        .value_counts()
    )