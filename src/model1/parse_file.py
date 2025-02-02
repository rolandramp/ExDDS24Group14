import json 
import elasticsearch
import warnings
warnings.filterwarnings('ignore')

def transform_to_txt_1(input_file, output_file):
    with open(input_file,"r") as f:
        data_res = json.load(f)

    with open("D:\\fax\\master_tuw\\1_semester\\ed\\project\\ExDDS24Group14\\src\\model_1\\github_solution\\lawyerid_to_lawyerurl.json","r") as f:
        data_law = json.load(f)

    lookup_dict = {}
    for k,v in data_law.items():
        lookup_dict[v]  =int(k)
    # print(len(lookup_dict))
    res = []
    
    for k in data_res.keys():
        query_id = int(k.split(",")[0])
        
        
        for el in data_res[k]:
            
            if len(el)>0:
                if el[0] in lookup_dict:
                    res.append((query_id,lookup_dict[el[0]],el[1]))    
                

    res.sort(key = lambda x:x[1])
    expert_lawyers = []
    with open("D:\\fax\\master_tuw\\1_semester\\ed\\project\\ExDDS24Group14\\src\\model_1\\labels.qrel","r") as f:
        lines = f.readlines()
        for line in lines:
            lawyer_id = line.split(" ")[2]
            if lawyer_id not in expert_lawyers:
                expert_lawyers.append(lawyer_id)
    
    with open(output_file,"w") as f:
        for el in res:
            # if el[1] in expert_lawyers:
                f.write(f"{el[0]} 0 {el[1]} {el[2]}\n")

def transform_to_txt_2(input_file, output_file):#transform json to result txt file
    with open(input_file,"r") as f:
        data_res = json.load(f)

    with open("D:\\fax\\master_tuw\\1_semester\\ed\\project\\ExDDS24Group14\\src\\model_1\\github_solution\\lawyerid_to_lawyerurl.json","r") as f:
        data_law = json.load(f)

    lookup_dict = {}
    for k,v in data_law.items():
        lookup_dict[v]  =int(k)
    
    res = []
    
    for k in data_res.keys():
        query_id = int(k.split(",")[0])
        
        
        for el in data_res[k]:
            if el["expert_id"] in lookup_dict:
                res.append((query_id,lookup_dict[el["expert_id"]],el["score"]))    
            

    res.sort(key = lambda x:x[1])
    expert_lawyers = []
    with open("D:\\fax\\master_tuw\\1_semester\\ed\\project\\ExDDS24Group14\\src\\model_1\\labels.qrel","r") as f:
        lines = f.readlines()
        for line in lines:
            lawyer_id = line.split(" ")[2]
            if lawyer_id not in expert_lawyers:
                expert_lawyers.append(lawyer_id)
    
    with open(output_file,"w") as f:
        for el in res:
        
                f.write(f"{el[0]} 0 {el[1]} {el[2]}\n")


    
def filter_fun(x,interseption_list):
    x = x.split(" ")
    if x[2]  in interseption_list:
        return True
    return False

def files_interseption(file1,file2,file3):#function to find interseption between labeled and non-labeled lawyers
    expert_lawyers = set()
    with open(file2,"r") as f:
        lines = f.readlines()
        for line in lines:
            lawyer_id = line.split(" ")[2]
            if lawyer_id not in expert_lawyers:
                expert_lawyers.add(lawyer_id)
    

    expert_lawyers_2 = set()
    with open(file1,"r") as f:
        lines = f.readlines()
        for line in lines:
            lawyer_id = line.split(" ")[2]
            if lawyer_id not in expert_lawyers_2:
                expert_lawyers_2.add(lawyer_id)
    
    expert_lawyers_interseption = expert_lawyers_2.intersection(expert_lawyers)
    
    result_file = list(filter(lambda x: x.split(" ")[2] in expert_lawyers_interseption,lines))
    
    with open(file3,"w+") as f:
        for line in result_file:
            f.write(line)
            
def calculate_metrics(labels_file, predictions_file, k_values=[1, 2, 5]):
    
    import pandas as pd
    
    labels = pd.read_csv(labels_file, delim_whitespace=True, header=None, names=["query_id", "iteration", "lawyer_id", "expert"])
    predictions = pd.read_csv(predictions_file, delim_whitespace=True, header=None, names=["query_id", "iteration", "lawyer_id", "score"])

    
    labels = labels[labels["expert"] == 1]

    
    ground_truth = labels.groupby("query_id")["lawyer_id"].apply(list).to_dict()
    predictions = predictions.groupby("query_id").apply(
        lambda group: group.sort_values("score", ascending=False)["lawyer_id"].tolist()
    ).to_dict()

    
    metrics = {
        "Precision@k": {k: [] for k in k_values},
        "MRR": [],
        "MAP": []
    }

    
    for query_id, predicted_lawyers in predictions.items():
        if query_id not in ground_truth:
            continue  # Skip queries not in ground truth

        relevant_lawyers = set(ground_truth[query_id])
        num_relevant = len(relevant_lawyers)

        # Precision@k
        for k in k_values:
            top_k = predicted_lawyers[:k]
            num_relevant_in_k = len(set(top_k) & relevant_lawyers)
            precision_at_k = num_relevant_in_k / k
            metrics["Precision@k"][k].append(precision_at_k)

        # MRR
        reciprocal_rank = 0
        for rank, lawyer_id in enumerate(predicted_lawyers, start=1):
            if lawyer_id in relevant_lawyers:
                reciprocal_rank = 1 / rank
                break
        metrics["MRR"].append(reciprocal_rank)

        # MAP
        precision_sum = 0
        num_found = 0
        for rank, lawyer_id in enumerate(predicted_lawyers, start=1):
            if lawyer_id in relevant_lawyers:
                num_found += 1
                precision_sum += num_found / rank
        average_precision = precision_sum / num_relevant if num_relevant > 0 else 0
        metrics["MAP"].append(average_precision)

    
    metrics["MRR"] = sum(metrics["MRR"]) / len(metrics["MRR"]) if metrics["MRR"] else 0
    metrics["MAP"] = sum(metrics["MAP"]) / len(metrics["MAP"]) if metrics["MAP"] else 0
    for k in k_values:
        metrics["Precision@k"][k] = sum(metrics["Precision@k"][k]) / len(metrics["Precision@k"][k]) if metrics["Precision@k"][k] else 0

    return metrics
    
    
    

if __name__=="__main__":
    transform_to_txt_1("D:\\fax\\master_tuw\\1_semester\\ed\\project\\ExDDS24Group14\\src\\model1\\final_version_model_1_lm.json","D:\\fax\\master_tuw\\1_semester\\ed\\project\\ExDDS24Group14\\src\\model1\\tmp_version_model_1_lm.txt")
    transform_to_txt_2("D:\\fax\\master_tuw\\1_semester\\ed\\project\\ExDDS24Group14\\src\\model1\\final_version_model_1_bm25.json","D:\\fax\\master_tuw\\1_semester\\ed\\project\\ExDDS24Group14\\src\\model1\\tmp_version_model_1_bm25.txt")
    files_interseption("D:\\fax\\master_tuw\\1_semester\\ed\\project\\ExDDS24Group14\\src\\model1\\tmp_version_model_1_lm.txt","D:\\fax\\master_tuw\\1_semester\\ed\\project\\ExDDS24Group14\\src\\model1\\labels.qrel","D:\\fax\\master_tuw\\1_semester\\ed\\project\\ExDDS24Group14\\src\\model1\\final_version_model_1_lm.txt")
    files_interseption("D:\\fax\\master_tuw\\1_semester\\ed\\project\\ExDDS24Group14\\src\\model1\\tmp_version_model_1_bm25.txt","D:\\fax\\master_tuw\\1_semester\\ed\\project\\ExDDS24Group14\\src\\model1\\labels.qrel","D:\\fax\\master_tuw\\1_semester\\ed\\project\\ExDDS24Group14\\src\\model1\\final_version_model_1_bm25.txt")

    metrics_lm = calculate_metrics("D:\\fax\\master_tuw\\1_semester\\ed\\project\\ExDDS24Group14\\src\\model1\\labels.qrel","D:\\fax\\master_tuw\\1_semester\\ed\\project\\ExDDS24Group14\\src\\model1\\final_version_model_1_lm.txt")
    print("results for LM model:")
    print(metrics_lm)
    print("results for BM25 model:")
    metrics_bm25 = calculate_metrics("D:\\fax\\master_tuw\\1_semester\\ed\\project\\ExDDS24Group14\\src\\model1\\labels.qrel","D:\\fax\\master_tuw\\1_semester\\ed\\project\\ExDDS24Group14\\src\\model1\\final_version_model_1_bm25.txt")
    print(metrics_bm25)