# This script is used to rank the answers based on the BM25 score for each query.
import pandas as pd
import json
from rank_bm25 import BM25Okapi

# Load the data
data = pd.read_parquet("data\\all_questions_and_answer_new.parquet")
queries_path = "data\\own_files\\queries_bankruptcy_own.csv"
queries = open(queries_path, "r").read().splitlines()
queries = queries[1:len(queries)]  # remove header
df_lawyer_ids = pd.read_csv("data\\own_files\\lawyerid_to_lawyerurl_own.csv")
data_wid = pd.merge(data, df_lawyer_ids, left_on='lawyers', right_on='lawyer_url', how='left')
data_wid = data_wid.drop(columns=['lawyer_url']) # data with lawyer id

# Function to find the top 1000 answers for a given query
def find_top_1000_answers_for_query(query):
    query = query.lower().split()  # Tokenize query into words
    data_wid["answers"] = data_wid["answers"].fillna("").str.lower()

    tokenized_corpus = data_wid["answers"].apply(str.split).tolist()
    bm25 = BM25Okapi(tokenized_corpus)

    scores = bm25.get_scores(query)
    data_wid["bm25_score"] = scores

    return data_wid.sort_values(by="bm25_score", ascending=False).head(1000)

# Structure to store the scores of the candidates at the document level
candidates_scores_doclevel = (
    {}
)  # structure: {"expert_id": {"query":[{answerIDXofExpert:score}, ..., {answeridN:score}]} }
docs_score_with_owner_candidate_id = (
    {}
)  # structure: {"query":[(doc_id, doc_score, expert_owner_id]}

for query in queries:
    print("query: ", query)
    query_text = query.lower().split(",")[1] # remove the number in the query
    top_1000_answers_for_query = find_top_1000_answers_for_query(query_text)
    for answer in top_1000_answers_for_query.itertuples():
        owner_incremental_id = int(answer.lawyer_id) if not pd.isna(answer.lawyer_id) else None
        answer_id = answer.Index # I'm assuming that the index is the answer id
        # create the structure for the scores, if not exists
        if query_text not in candidates_scores_doclevel:
            candidates_scores_doclevel[query_text] = {}
            docs_score_with_owner_candidate_id[query_text] = []
            candidates_scores_doclevel[query_text][
                owner_incremental_id
            ] = []  # list of tuple (answer_id, score)
        if (
            query_text in candidates_scores_doclevel
            and owner_incremental_id not in candidates_scores_doclevel[query_text]
        ):
            candidates_scores_doclevel[query_text][
                owner_incremental_id
            ] = []  # list of tuple (answer_id, score)
        candidates_scores_doclevel[query_text][owner_incremental_id].append(
            (answer_id, answer.bm25_score)
        )
        docs_score_with_owner_candidate_id[query_text].append(
            (
                answer_id,
                answer.bm25_score,
                owner_incremental_id,
            )
        )

# Save the results
model2_doclevel_ranking_path = "src\\model2\\model_two_doclevel_bm25_ranking.dict"
with open(model2_doclevel_ranking_path, "w") as f:
    json.dump(candidates_scores_doclevel, f, indent=4)

model2_doclevel_scoreperdoc_ranking_path = (
    "src\\model2\\model_two_doclevel_score_perdoc_bm25_ranking.dict"
)
with open(model2_doclevel_scoreperdoc_ranking_path, "w") as f:
    json.dump(docs_score_with_owner_candidate_id, f, indent=4)