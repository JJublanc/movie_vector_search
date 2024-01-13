from config import elastic_cloud_id, elastic_password, elastic_user
from elasticsearch import Elasticsearch


def get_elasticsearch_session():
    """Retrieve an Elasticsearch session."""
    es = Elasticsearch(cloud_id=elastic_cloud_id,
                       basic_auth=(elastic_user, elastic_password),
                       request_timeout=1000)
    return es


if __name__ == "__main__":
    es = get_elasticsearch_session()
    print(es.info())