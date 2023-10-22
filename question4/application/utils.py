import matplotlib.pyplot as plt
import numpy as np
from sklearn.cluster import KMeans


def plot_wss(
    points: np.ndarray,
    kmax: int,
):
    result = []
    for k in range(1, kmax+1):
        kmeans = KMeans(n_clusters=k, n_init=20).fit(points)
        centroids = kmeans.cluster_centers_
        predicted_clusters = kmeans.predict(points)
        current_sse = 0

        for i in range(len(points)):
            current_center = centroids[predicted_clusters[i]]
            current_sse += (
                (points[i, 0] - current_center[0]) ** 2 +
                (points[i, 1] - current_center[1]) ** 2
            )

        result.append(current_sse)

    plt.figure(figsize=(10, 7))
    plt.plot(
        list(range(1, kmax + 1)),
        result,
        label="wss",
    )
    plt.xlabel("K")
    plt.ylabel("WSS")
    plt.title("WSS per K")
    plt.xticks(list(range(1, kmax + 1)))
    plt.grid(alpha=0.3)
    plt.show()
