from connect import get_elasticsearch_session
from mapping import mapping_movies
from utils import delete_index
from config import (pipeline_id,
                    target_field_descriptions_embedded,
                    elastic_origin_index_id,
                    elastic_target_index_id,
                    indice_settings)

es = get_elasticsearch_session()

mapping = mapping_movies["mappings"]
mapping["properties"][target_field_descriptions_embedded] = {
	"properties": {
		"predicted_value": {"type": "dense_vector", "dims": 384, "index": True,
		                    "similarity": "cosine"}}
}
body = {"mappings": mapping,
        "settings": indice_settings}

if __name__ == "__main__":
	delete_index(es, elastic_target_index_id)
	delete_index(es, "failed-movies_with_embeddings")
	es.indices.create(index=elastic_target_index_id, body=body)
	es.reindex(
		source={"index": elastic_origin_index_id},
		dest={"index": elastic_target_index_id, "pipeline": pipeline_id}
	)
