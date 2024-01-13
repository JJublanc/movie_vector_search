# Get your data from Kaggle
https://www.kaggle.com/datasets/rounakbanik/the-movies-dataset?resource=download

# Setup
```
pyenv local 3.9.7
pip install poetry==1.4.2
poetry config virtualenvs.in-project true
poetry shell
poetry install
```

# Get your credentials and put them in a file called .env
You can use the .env.example as a template
```
ELASTIC_USER="elastic"
ELASTIC_PASSWORD=< your password >
ELASTIC_CLOUD_ID= < your cloud id >
```

# How to use this project
* Create your basic index : python `create_index.py`
* add some movies : python `add_movies.py`
* Create your pipeline : python `create_pipeline.py`
* Import your model : python `import_model.py`
* Create the index with embeddings : python `create_index_with_embedding.py`
* Query the index : python `query_index.py`
 


