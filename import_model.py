import logging
from pathlib import Path

import elasticsearch
from connect import get_elasticsearch_session
from config import MODEL
from eland.common import es_version
from eland.ml.pytorch import PyTorchModel
from eland.ml.pytorch.transformers import TransformerModel

es = get_elasticsearch_session()

if __name__ == "__main__":
    es_cluster_version = es_version(es)

    # Load a Hugging Face transformers model directly from the model hub
    tm = TransformerModel(model_id=MODEL, task_type="text_embedding")
    tmp_path = "models"
    Path(tmp_path).mkdir(parents=True, exist_ok=True)
    model_path, config, vocab_path = tm.save(tmp_path)
    ptm = PyTorchModel(es, tm.elasticsearch_model_id())
    try:
        ptm.import_model(model_path=model_path, config_path=None, vocab_path=vocab_path, config=config)
    except elasticsearch.BadRequestError as e:
        logging.error(e)
        pass
    es.ml.start_trained_model_deployment(model_id=ptm.model_id)
