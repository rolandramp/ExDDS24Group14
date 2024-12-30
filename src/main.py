from avvo_downloader import AvvoDownloader
import json
from bs4 import BeautifulSoup

if __name__ == '__main__':
    loader = AvvoDownloader()

    with open('../data/question_links_bankruptcy.json', 'r') as file:
        data = json.load(file)

    q_n_a_urls = []

    for key in data.keys():
        for url in data.get(key):
            q_n_a_urls.append(url)

    loader.scrape_website(q_n_a_urls[0])
