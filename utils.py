import logging

from connect import get_elasticsearch_session
from elasticsearch import Elasticsearch, NotFoundError

logging.basicConfig(level=logging.INFO)


def create_index(host, username, password, ssl_assert_fingerprint, index_name, mapping):
    """Create an es instance, basic authentication and the index name and mapping."""
    es = Elasticsearch(hosts=[host], basic_auth=(username, password), ssl_assert_fingerprint=ssl_assert_fingerprint)
    es.indices.create(index=index_name, body=mapping)


def reindex_and_delete_old(es, source_index, dest_index, slices=5):
    """
    Reindex data from source index to destination index and then deletes the source index.

    Parameters:
    - es (Elasticsearch): Elasticsearch client instance.
    - source_index (str): The name of the source index.
    - dest_index (str): The name of the destination index.
    - slices (int): The number of slices for parallel processing. Default is 5.
    """
    # First, perform the reindexing with slices
    for i in range(slices):
        es.reindex(
            body={"source": {"index": source_index, "slice": {"id": i, "max": slices}}, "dest": {"index": dest_index}}
        )
        logging.info(f"Reindexed slice {i + 1}/{slices} from {source_index} to {dest_index}")

    # After successful reindex, delete the source index
    es.indices.delete(index=source_index)
    logging.info(f"Deleted source index: {source_index}")


def add_element_to_mapping(es, source_index):
    """
    Reindex data from source index to destination index and then deletes the source index.

    Parameters:
    - es (Elasticsearch): Elasticsearch client instance.
    """
    properties = {
        "educations": {"type": "nested", "properties": {"edu_details": {"type": "text"}}},
        "experiences": {"type": "nested", "properties": {"employment_contract": {"type": "text"}}},
    }

    es.indices.put_mapping(index=source_index, properties=properties)


def delete_index(es, index_name):
    """Delete a specified index.

    Parameters:
    - es (Elasticsearch): Elasticsearch client instance.
    - index_name (str): The name of the index to delete.
    """
    try:
        es.indices.delete(index=index_name)
        logging.info(f"Deleted index: {index_name}")
    except NotFoundError:
        logging.warning(f"Index: {index_name} not found. No deletion performed.")
    except Exception as e:
        logging.error(f"Error occurred while deleting {index_name}: {e}")


def count_id_occurence(id, index_name, es):
    """Count the number of occurence of an id in an index.

    Args:
        id: id to count
        index_name: name of the index
        es: Elasticsearch instance

    Returns:
        count: number of occurence of the id in the index
    """
    # Construct the search query
    body = {"query": {"term": {"_id": id}}, "size": 0}

    response = es.search(index=index_name, body=body)

    # Return the count
    return response["hits"]["total"]["value"]


def get_candidates(es, index_name):
    """Get all the candidates from an index.

    Args:
        es: Elasticsearch instance
        index_name: name of the index

    Returns:
        candidates: list of candidates
    """
    return es.search(index=index_name, query={"match_all": {}})["hits"]["hits"]


# function to count the number of doc in oindex
def count_id(es, index_name):
    """Count the number of documents in an index.

    Args:
        es: Elasticsearch instance
        index_name: name of the index

    Returns:
        count: number of documents in the index
    """
    return es.count(index=index_name)["count"]


def try_model(model, text, es):
    """Try a model on a text."""
    result = es.ml.infer_trained_model(model_id=model, body={"docs": {"text_field": text}})
    return result


if __name__ == "__main__":
    es = get_elasticsearch_session()
    source_index = "test_candidates_embedded"
    add_element_to_mapping(es, source_index)
    logging.info("changes done")

    current_mapping = es.indices.get_mapping(index=source_index)
    logging.info(current_mapping)
