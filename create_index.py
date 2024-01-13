import logging

from connect import get_elasticsearch_session
from mapping import mapping_movies
from config import elastic_origin_index_id, indice_settings


def create_es_index(index_name: str, mapping: dict, indice_settings: dict):
    """Create the Elasticsearch index with prepared column names and types.

    If the index already exists, a warning will be printed.

    Args:
                    index_name (str): name of the Elasticsearch index
                    mapping (dict): mapping of the ES index (column names and types)
    """
    es = get_elasticsearch_session()

    # Check if index already exists
    if es.indices.exists(index=index_name):
        logging.info(f"Index '{index_name}' already exists.")
        return

    es.indices.create(index=index_name, body=mapping, settings=indice_settings)


def reset_es_index(index_name: str,
                   mapping: dict,
                   indice_settings: dict):
    """Reset the Elasticsearch index. If the index exists, delete it and then recreate it.

    Args:
                    index_name (str): name of the Elasticsearch index
                    mapping (dict): mapping of the ES index (column names and types)
    """
    es = get_elasticsearch_session()

    # Delete the index if it already exists
    if not es.indices.exists(index=index_name):
        logging.info(f"Index '{index_name}' does not exist.")
        return

    es.indices.delete(index=index_name)
    logging.info(f"Index '{index_name}' deleted.")
    create_es_index(index_name, mapping, indice_settings)
    logging.info(f"Index '{index_name}' recreated.")


if __name__ == "__main__":
    create_es_index(
        index_name=elastic_origin_index_id,
        mapping=mapping_movies,
        indice_settings=indice_settings
    )
