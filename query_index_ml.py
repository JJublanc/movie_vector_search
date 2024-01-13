import logging

from config import (
    MODEL_ID,
    elastic_target_index_id,
)
from connect import get_elasticsearch_session

if __name__ == "__main__":
    es = get_elasticsearch_session()
    input_description_search = "Top scientists want to build a nuclear bomb-powered spaceship to visit Mars and the planets." # Movie : To Mars by A-Bomb: The Secret History of Project Orion"
    knn_description = {
        "field": "overviews_embedded.predicted_value",
        "query_vector_builder": {"text_embedding": {"model_id": MODEL_ID,
                                                    "model_text": input_description_search}},
        "k": 100,
        "num_candidates": 1000,
    }

    query = {
        "bool": {
            "must": [
                {"exists": {"field": "overview"}},
            ]
        }
    }

    source = ["id", "original_title", "overview"]
    results = es.search(index=elastic_target_index_id, knn=knn_description, _source=source)

    for res in results["hits"]["hits"]:
        print(res["_source"]["original_title"])
    logging.info("\n ---------------- \n")
