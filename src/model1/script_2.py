from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import pandas as pd

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
            "answers": {"type": "text"},
            "stars": {"type": "float"},
            "rating": {"type": "float"},
            "helpful": {"type": "integer"}
        }
    }
}

def generate_documents(profiles):
    for profile in profiles:
        yield {
            "_index":index_name,
            "_source":{
                "lawyer_id":profile["lawyers"],
                "answers":profile["answers"],
                "stars":profile["stars"],
                "rating":profile["rating"],
                "helpful":profile["helpful"],

            }
        }

def train():
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name, body=index_mapping)

    data = pd.read_parquet("D:\\fax\\master_tuw\\1_semester\\ed\\project\\ExDDS24Group14\\src\\model_1\\github_solution\\all_questions_and_answer_new.parquet")
    data_agg = data.groupby("lawyers").agg({
        'answers': ' '.join,  
        'stars': 'mean',      
        'rating': 'mean',
        'helpful': 'sum'
    }).reset_index()
    profiles = data_agg.to_dict(orient="records")
    bulk(es,generate_documents(profiles))
    print("Documents indexed successfully")


def check_index_health(index_name):
    try:
        # Check if the index exists
        if es.indices.exists(index=index_name):
            print(f"Index '{index_name}' exists.")

            # Get the index health
            health = es.cluster.health(index=index_name)
            print(f"Index '{index_name}' health: {health['status']}")

            # Get index settings
            settings = es.indices.get_settings(index=index_name)
            print(f"Settings for index '{index_name}': {settings[index_name]['settings']}")

            # Get index mapping
            mapping = es.indices.get_mapping(index=index_name)
            print(f"Mapping for index '{index_name}': {mapping[index_name]['mappings']}")

        else:
            print(f"Index '{index_name}' does not exist.")
    except Exception as e:
        print(f"An error occurred: {e}")



CF_ALL_TERMS_EXPERT_LEVEL = 3443922
CANDIDATE_LEVEL_FIELD = "answers"



def get_all_experts():
    """
    Retrieves all lawyer IDs from the Elasticsearch index.
    """
    try:
        body = {
            "_source": ["lawyer_id"],
            "query": {"match_all": {}}
        }
        response = es.search(index=index_name, body=body, scroll='2m')
        lawyer_ids = [hit['_source']['lawyer_id'] for hit in response['hits']['hits']]
        
        # Scroll for more results
        scroll_id = response['_scroll_id']
        while len(response['hits']['hits']) > 0:
            response = es.scroll(scroll_id=scroll_id, scroll='2m')
            lawyer_ids.extend([hit['_source']['lawyer_id'] for hit in response['hits']['hits']])
            scroll_id = response['_scroll_id']
        
        es.clear_scroll(scroll_id=scroll_id)
        return lawyer_ids
    except Exception as e:
        # print(f"Error retrieving lawyer IDs: {e}")
        return []

def get_p_tc(query_input_term):
    """
    Calculates the collection-wide probability of a term (p(t)).
    """
    try:
        res = es.termvectors(
            index=index_name,
            id=get_all_experts()[0],  # Use any document to fetch term statistics
            fields=[CANDIDATE_LEVEL_FIELD],
            term_statistics=True
        )
        total_terms_fre_in_collection = CF_ALL_TERMS_EXPERT_LEVEL
        query_term_freq_in_collection = res['term_vectors'][CANDIDATE_LEVEL_FIELD]['terms'].get(query_input_term, {}).get('ttf', 0)
        return query_term_freq_in_collection / total_terms_fre_in_collection
    except Exception as e:
        # print(f"Error calculating p_tc for term '{query_input_term}': {e}")
        return 1e-10

def get_lambda_expert(expert_id):
    """
    Calculates the smoothing parameter (lambda) for a lawyer.
    """
    try:
        res = es.get(index=index_name, id=expert_id)
        answers = res['_source']['answers']
        doc_len = len(answers.split())  # Total words in all answers
        total_terms_fre_in_collection = CF_ALL_TERMS_EXPERT_LEVEL
        beta = total_terms_fre_in_collection
        return beta / (doc_len + beta)
    except Exception as e:
        # print(f"Error calculating lambda for expert '{expert_id}': {e}")
        return 0.5

def get_expert_score_per_term(query_term, expert_id):
    """
    Calculates the score for a single query term for a given expert.
    """
    try:
        query_term_p_tc = get_p_tc(query_term)
        lambda_expert = get_lambda_expert(expert_id)
        res = es.get(index=index_name, id=expert_id)
        answers = res['_source']['answers']
        
        # Foreground score: term frequency in the expert's answers
        answer_words = answers.split()
        doc_len = len(answer_words)
        term_frequency = answer_words.count(query_term)
        p_td = term_frequency / doc_len if doc_len > 0 else 0
        
        # Final score
        return (1 - lambda_expert) * p_td + lambda_expert * query_term_p_tc
    except Exception as e:
        # print(f"Error calculating expert score for term '{query_term}' and expert '{expert_id}': {e}")
        return 1e-10

def test_model():
    """
    Tests the model using provided queries and ranks experts.
    """
    queries_path = "queries_bankruptcy.csv"
    queries = open(queries_path, "r").read().splitlines()
    candidates_scores = {}
    expert_list = get_all_experts()
    
    for query in queries:
        print(f"Processing query: {query}")
        query_terms = query.lower().split(",")[1].split(" ")
        
        for expert_id in expert_list:
            total_score = 1
            for term in query_terms:
                term_score = get_expert_score_per_term(term, expert_id)
                total_score *= term_score
            
            if query not in candidates_scores:
                candidates_scores[query] = []
            candidates_scores[query].append((expert_id, total_score))
        
        # Sort experts for the query by their scores
        candidates_scores[query] = sorted(candidates_scores[query], key=lambda x: x[1], reverse=True)
    
    
    # Save results to file
    import json
    with open("final_version_model_1_lm.json", "w") as f:
        json.dump(candidates_scores, f, indent=4)
    print("Results saved to model_1_ranking.json")


if __name__=="__main__":
    # train()
    # check_index_health(index_name)
    test_model()
#     query = {
#     "query": {
#         "match": {
#             "answers": "bankruptcy"
#         }
#     }
# }
#     response = es.search(index=index_name, body=query)
#     print(response)
