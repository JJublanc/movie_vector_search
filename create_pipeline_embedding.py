from connect import get_elasticsearch_session
from config import pipeline_id, MODEL_ID
es = get_elasticsearch_session()
def create_pipeline_flatten_overviews(es):
    """Create the pipeline to flatten the descriptions and embed them."""
    es.ingest.put_pipeline(
        id=pipeline_id,
        description="Aggregated text embedding pipeline",
        processors=[
            {
                "inference": {
                    "model_id": MODEL_ID,
                    "target_field": "overviews_embedded",
                    "field_map": {"overview": "text_field"},
                }
            },
        ],
        on_failure=[
            {"set": {"field": "_index", "value": "failed-{{{_index}}}"}},
            {"set": {"field": "ingest.failure", "value": "{{_ingest.on_failure_message}}"}},
        ],
    )


if __name__ == "__main__":
    create_pipeline_flatten_overviews(es)
