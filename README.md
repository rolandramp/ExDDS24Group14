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

- `python main.py --scrape --start 0 --end 10`
- `python main.py --transform --directory '../data/scraped'`

# BERT Baseline
The final results are present in the executed jupyter notebook "bert_baseline.ipynb". To run the code yourself, simply execute the entire notebook. Note that training BERT with this setup using a GPU T4 x2 takes about 1h 36mins. If this is not necessary, simply execute the cells for evaluation that load the trained model bert_ranker.pth.