# ExDDS24Group14

## Expert Finding in Legal Community Question Answering

This project tries to reproduce the paper [Expert Finding in Legal Community Question Answering. 
Arian Askari, Suzan Verberne, and Gabriella Pasi (ECIR 2022)](https://doi.org/10.1007/978-3-030-99739-7_3)


This paper reveres also to the following GitHub repository [EF_in_Legal_CQA-ECIR2022](https://github.com/arian-askari/EF_in_Legal_CQA-ECIR2022.git)

# Scraping

The following section describes how to scrape the data from the websites. The scraping is done with the help of Selenium and ChromeDriver. The scraping script is located in the **main.py** file.

## Setup

Create a conda environment with `conda create -n ExDDsEx2 python=3.12` and activate it `conda activate ExDDsEx2`.
With `pip install -r requirements.txt` all necessary libraries will be installed.

### Selenium and Chrome Setup

This project requires Selenium for web scraping. You will also need to have Google Chrome installed on your system, as well as the ChromeDriver that matches your Chrome version.

1. **Install Google Chrome**: Download and install Google Chrome from [here](https://www.google.com/chrome/).

2. **Install Selenium**: Selenium is included in the `requirements.txt` file, so it will be installed when you run `pip install -r requirements.txt`.


## Runtime Parameters

The command line options for the script in **main.py** are as follows:  
- *--scrape*: This flag indicates that the script should perform the scraping of websites. When this flag is provided, the script will scrape the websites specified in the *question_links_bankruptcy.json* file.
- *--rescrape*: This flag indicates that the script should rescrape the websites. It will use the existing data to determine which websites need to be rescraped.
- *--rescrapemissing*: This flag indicates that the script should rescrape the missing websites. It will compare the existing data with new data to find and rescrape the missing websites.
- *--transform*: This flag indicates that the script should transform the JSON files into a DataFrame. When this flag is provided, the script will read the JSON files from the specified directory, process them, and save the resulting DataFrame as a Parquet file.  
- *--start*: This option specifies the start index for scraping. It is an integer value and defaults to 0 if not provided. It is used to determine the starting point in the list of URLs to scrape.  
- *--end*: This option specifies the end index for scraping. It is an integer value and defaults to None if not provided. It is used to determine the ending point in the list of URLs to scrape.  
- *--directory*: This option specifies the directory path where the JSON files are located. It is a string value and defaults to ../data/scraped if not provided. This directory is used when transforming JSON files to a DataFrame.
- *--previous*: This option specifies the path to the previous Parquet file. It is used as a source for rescraping websites.
- *--actual*: This option specifies the path to the actual Parquet file. It is used as a source for rescraping missing websites.

## Example Usage:
start main.py from inside the src folder

- `python main.py --scrape --start 0 --end 10`
- `python main.py --transform --directory '../data/scraped'`

## Extracted date format

The produced parquet file has the following columns:

| **Field**           | **Description**                                                                                     |
|---------------------|-----------------------------------------------------------------------------------------------------|
| number              | A unique identifier assigned to each entry or record                                                |
| url                 | The web address where the entry or record can be accessed                                           |
| title               | The title or headline of the entry or record                                                        |
| question            | The question of a person                                                                            |
| question\_tags      | Keywords or tags associated with the question to categorize and identify its main topics            |
| answers             | Responses provided by a lawyer                                                                      |
| lawyers             | The anonymized lawyer who provided answer to the question                                           |
| posted\_times       | The timestamp indicating when the answers were posted                                               |
| answer\_card\_text  | The full raw scraped raw answer                                                                     |
| stars               | A rating between 0 and 5 to indicate the quality of the lawyer                                      |
| reviews             | A numerical value indicating the amount of reviews done                                             |
| rating              | A numerical value representing the overall evaluation of the answers or the lawyer                  |
| helpful             | An indicator of how many users found the answers helpful                                            |
| lawyers\_agree      | The number of lawyers who agree with a particular answer or response                                |
| best\_answer        | The answer that has been marked as the most useful or accurate by the question poster or the community |


# Model 1
- elasticSearch - can be installed from https://www.elastic.co/guide/en/elasticsearch/reference/current/install-elasticsearch.html and run by startig .bat file
- requirements.txt - python virtual environment requirements, can be installed in started enviroment with *pip install -r requirement.txt*
- lm_model.py - script implements model 1 in lm variant; it has train and test function that should be called in main function; it is necessary to have elasticsearch installed and run, available on port 9200, with http authentication username and password specified inside elasticsearch object
- bm25_model.py - script implements model 1 in bm25 variant; it has train and test function that should be called in main function; it is necessary to have elasticsearch installed and run, available on port 9200, with http authentication username and password specified inside elasticsearch object
- parse_file.py - script parses output of model1 structurated in .json file; inside main function file paths of models' results should be specified, as first arguments of functions tranfrom_to_txt_1 and tranfrom_to_txt_2; second arguments are txt format of results; according to resulting files, final version are created, after performing interseption function, where only labeled lawyers are extracted; according to them metrics are calculated with function calculate_metrics

# Model 2

This folder contains the files needed to perform the document-based models. There are two .py files to create each model, one should simply run it, as the files it uses are already on this repository, only make sure to have all the needed python libraries installed. It will then create 4 json files, which are already in this folder (if you run again, change their names, unless you want to overwrite the previous files).
The last file is a .ipynb (a Jupyter Notebook, using python). It can be used to calculate the evaluation metrics for the models. It's also simple to run, once you have connected to the repository.
This model was run on VSCode, in a Windows 11 Home.

# BERT Baseline
The final results are present in the executed jupyter notebook "bert_baseline.ipynb". To run the code yourself, simply execute the entire notebook. Note that training BERT with this setup using a GPU T4 x2 takes about 1h 36mins. 
If this is not desired, simply exclude the code for training and run the cells for evaluation that load the trained model bert_ranker.pth. The model is linked to the Zenodo project - download it there and place it into the same folder as the bert_baseline notebook.

