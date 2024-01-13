import logging
from datetime import datetime
from typing import List

import pandas as pd
from connect import get_elasticsearch_session
from config import elastic_origin_index_id, pipeline_id

logger = logging.getLogger(__name__)

movies = pd.read_csv("data/archive/movies_metadata.csv")
movies.columns

def populate_index(df: pd.DataFrame, index_name: str):
    """Populate and index from candidate informations.

    Args:
        rows (List[dict]): list of scrapped candidate information
        index_name (str): name of the index to which documents should be written
    """
    es = get_elasticsearch_session()
    if df is None or df.empty:
        raise ValueError("Empty rows")
    bulk_body = []
    for i in range(len(df)):
        doc = {
            "id": str(df.loc[i, "id"]),
            "original_title": df.loc[i, "original_title"],
            "overview": df.loc[i, "overview"],
        }
        bulk_body.append({"index": {"_index": index_name, "_id": doc["id"]}})
        bulk_body.append(doc)
    response = es.bulk(operations=bulk_body, index=index_name, refresh=True)
    handle_errors_indexing(response)


# def populate_index_with_embeddings(rows: List[dict], index_name: str):
#     """Populate and index from candidate information with embedded information.
#
#     Args:
#         rows (List[dict]): list of scrapped candidate information
#         index_name (str): name of the index to which documents should be written
#     """
#     es = get_elasticsearch_session()
#     if not rows:
#         raise ValueError("Empty rows")
#     profiles_info_df = pd.DataFrame(rows)
#     processed_infos = process_cassandra_outputs(profiles_info_df)
#     bulk_body = []
#     for doc in processed_infos:
#         bulk_body.append({"index": {"_index": index_name, "_id": doc["id"], "pipeline": pipeline_id}})
#         bulk_body.append(doc)
#     response = es.bulk(operations=bulk_body, index=index_name, refresh=True)
#     handle_errors_indexing(response)
#     logger.info(f"Indexed {len(processed_infos)} documents")


def handle_errors_indexing(response: dict):
    """Get errors from bulk indexing response."""
    if response.get("errors"):
        for item in response.get("items"):
            if item.get("index").get("error"):
                error = item.get("index").get("error")
                doc_id = item.get("index").get("_id")
                logger.warning(f"Error when indexing {doc_id}: {error}")


if __name__ == "__main__":
    df = pd.read_csv("data/archive/movies_metadata.csv")
    populate_index(df, elastic_origin_index_id)
