import dotenv
import os

dotenv.load_dotenv()

pipeline_id = 'overview'
MODEL = "sentence-transformers/all-minilm-l12-v2"
MODEL_ID = "sentence-transformers__all-minilm-l12-v2"

elastic_cloud_id=os.getenv('ELASTIC_CLOUD_ID')
elastic_password= os.getenv('ELASTIC_PASSWORD')
elastic_user= os.getenv('ELASTIC_USER')
target_field_descriptions_embedded = "overview_embedded"
elastic_origin_index_id = "movies"
elastic_target_index_id = "movies_with_embeddings"
indice_settings = {"number_of_shards": 5, "number_of_replicas": 0}