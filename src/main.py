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

    results = []

    for url in q_n_a_urls[:10]:
        (title, question, answers, lawyers, posted_times, answer_card_text) = loader.scrape_website(url)
        result = {
            'title': title,
            'question': question,
            'answers': answers,
            'lawyers': lawyers,
            'posted_times': [str(time) for time in posted_times],
            'answer_card_text': answer_card_text
        }
        results.append(result)

    with open('results.json', 'w', encoding='utf-8') as json_file:
        json.dump(results, json_file, ensure_ascii=False, indent=4)
