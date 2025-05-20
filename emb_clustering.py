from sentence_transformers import SentenceTransformer
from sklearn.cluster import AgglomerativeClustering

def generate_embeddings(sentences, model_name):
    model = SentenceTransformer(model_name)
    print(f"Loaded Model {model_name}")
    print(f"Encoding {len(sentences)} sentences...")
    return model.encode(sentences, show_progress_bar=True)


def perform_clustering(embeddings, distance_threshold):
    clustering_model = AgglomerativeClustering(n_clusters=None, distance_threshold=distance_threshold)
    return clustering_model.fit_predict(embeddings)
