from avvo_downloader import AvvoDownloader
import json
from pathlib import Path

if __name__ == '__main__':
    loader = AvvoDownloader()

    data_path =  Path('../data/scraped/')

    with open('../data/question_links_bankruptcy.json', 'r') as file:
        data = json.load(file)

    q_n_a_urls = []

    # try to scrape all files
    for key in data.keys():
        for url in data.get(key):
            q_n_a_urls.append(url)

    results = []
    # if scraping fails go on from here
    start:int = 454
    end:int = 500
    for url in q_n_a_urls[start:end]:
        (title, question, answers, lawyers, posted_times, answer_card_text) = loader.scrape_website(url)
        result = {
            'url': url,
            'title': title,
            'question': question,
            'answers': answers,
            'lawyers': lawyers,
            'posted_times': [str(time) for time in posted_times],
            'answer_card_text': answer_card_text
        }
        filename = f'results_n_{start}.json'
        with open(data_path.joinpath(filename), 'w', encoding='utf-8') as json_file:
            json.dump(result, json_file, ensure_ascii=False, indent=4)
        start += 1
        results.append(result)

    with open('results2.json', 'w', encoding='utf-8') as json_file:
        json.dump(results, json_file, ensure_ascii=False, indent=4)
