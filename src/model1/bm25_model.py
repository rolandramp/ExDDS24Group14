from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import pandas as pd
import json

es = Elasticsearch(
    ["https://localhost:9200"],
    http_auth=("elastic", "p7JoJTsIivJUCbN1Cu8H"),
    timeout=120,
    max_retries=10,
    retry_on_timeout=True,
    verify_certs=False,
    ssl_show_warn=False,
)

index_name = "lawyer_profiles"
index_mapping = {
    "mappings": {
        "properties": {
            "lawyer_id": {"type": "keyword"},
            "answers": {"type": "text"},  # BM25 applies to text fields by default
            "stars": {"type": "float"},
            "rating": {"type": "float"},
            "helpful": {"type": "integer"}
        }
    }
}

def generate_documents(profiles):
    for profile in profiles:
        yield {
            "_index": index_name,
            "_source": {
                "lawyer_id": profile["lawyers"],
                "answers": profile["answers"],
                "stars": profile["stars"],
                "rating": profile["rating"],
                "helpful": profile["helpful"],
            }
        }

def train():
    """
    Indexes lawyer profiles into Elasticsearch.
    """
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name, body=index_mapping)

    data = pd.read_parquet(
        "D:\\fax\\master_tuw\\1_semester\\ed\\project\\ExDDS24Group14\\src\\model_1\\github_solution\\all_questions_and_answer_new.parquet"
    )
    data_agg = data.groupby("lawyers").agg({
        'answers': ' '.join,  
        'stars': 'mean',      
        'rating': 'mean',
        'helpful': 'sum'
    }).reset_index()
    profiles = data_agg.to_dict(orient="records")
    bulk(es, generate_documents(profiles))
    print("Documents indexed successfully")

def check_index_health(index_name):
    try:
        if es.indices.exists(index=index_name):
            print(f"Index '{index_name}' exists.")
            health = es.cluster.health(index=index_name)
            print(f"Index '{index_name}' health: {health['status']}")
        else:
            print(f"Index '{index_name}' does not exist.")
    except Exception as e:
        print(f"An error occurred: {e}")

def test_model():
    """
    Tests the model using provided queries and ranks experts using BM25.
    """
    queries_path = "queries_bankruptcy.csv"
    queries = open(queries_path, "r").read().splitlines()
    candidates_scores = {}

    for query in queries:
        print(f"Processing query: {query}")
        query_text = query.lower().split(",")[1]

        
        body = {
            "size": 100,
            "query": {
                "match": {
                    "answers": query_text
                }
            }
        }
        response = es.search(index=index_name, body=body)

        # Store ranked results
        candidates_scores[query] = [
            {
                "expert_id": hit["_source"]["lawyer_id"],
                "score": hit["_score"]
            }
            for hit in response["hits"]["hits"]
        ]
    
    
    with open("final_version_model_1_bm25.json", "w") as f:
        json.dump(candidates_scores, f, indent=4)
    print("Results saved to final_version_model_1_bm25.json")

if __name__ == "__main__":
    
    # train()
    
    # check_index_health(index_name)
    test_model()
