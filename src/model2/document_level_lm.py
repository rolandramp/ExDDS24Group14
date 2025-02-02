# Description: This file contains the code to rank the answers based on the document level language model.
import pandas as pd
from collections import Counter
import json
from rank_bm25 import BM25Okapi

# Load the data
data = pd.read_parquet("data\\all_questions_and_answer_new.parquet")

all_words = [answers.split(" ") for answers in data["answers"] if isinstance(answers, str)]

# Flatten the list of lists into a single list
flat_all_words = [word for sublist in all_words for word in sublist]

# Count word frequencies
word_counts = Counter(flat_all_words)
total_words = sum(word_counts.values())
CF_ALL_TERMS_EXPERT_LEVEL = total_words
Count_of_all_answers = data["answers"].notna().sum()

# Function to get the probability of a term, p(t)
def get_p_t(term):
    term_probabilities = {word: count / total_words for word, count in word_counts.items()}
    return term_probabilities.get(term, 0)

# Calculate beta_doc_level
beta_doc_level = CF_ALL_TERMS_EXPERT_LEVEL / Count_of_all_answers
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
    # I'm using BM25 to rank the answers, this was implicit in the original (standard for elastic search),
    # but I don't use the bm25_score (probabilist approach here), just the ranking

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
    query_words = query_text.split(" ")
    top_1000_answers_for_query = find_top_1000_answers_for_query(query_text)
    # for every answer
    for answer in top_1000_answers_for_query.itertuples():
        owner_incremental_id = int(answer.lawyer_id) if not pd.isna(answer.lawyer_id) else None
        answer_id = answer.Index # I'm assuming that the index is the answer id
        answer_text_list = answer.answers.split(" ")
        answer_len = len(answer_text_list)
        n_d = answer_len # number of words in the document
        # calculate lambda_doc_level, penalizing for the document length
        lambda_doc_level = beta_doc_level / (beta_doc_level + n_d)
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
        # calculate the probability (the product for every term in the query)
        total_score_for_this_query_term_to_this_doc = 1
        for query_term in query_words:
            p_tc = get_p_t(query_term)
            p_td = answer_text_list.count(query_term) / answer_len # p(t|d)
            foreground_score = (1 - lambda_doc_level) * p_td
            background_score = lambda_doc_level * p_tc
            final_score_per_term = foreground_score + background_score
            final_score_per_term = final_score_per_term + 0.0000000001 # to avoid 0
            total_score_for_this_query_term_to_this_doc *= final_score_per_term
        candidates_scores_doclevel[query_text][owner_incremental_id].append(
            (answer_id, total_score_for_this_query_term_to_this_doc)
        )
        docs_score_with_owner_candidate_id[query_text].append(
            (
                answer_id,
                total_score_for_this_query_term_to_this_doc,
                owner_incremental_id,
            )
        )

# Save the results
model2_doclevel_ranking_path = "src\\model2\\model_two_doclevel_lm_ranking.dict"
with open(model2_doclevel_ranking_path, "w") as f:
    json.dump(candidates_scores_doclevel, f, indent=4)

model2_doclevel_scoreperdoc_ranking_path = (
    "src\\model2\\model_two_doclevel_score_perdoc_lm_ranking.dict"
)
with open(model2_doclevel_scoreperdoc_ranking_path, "w") as f:
    json.dump(docs_score_with_owner_candidate_id, f, indent=4)