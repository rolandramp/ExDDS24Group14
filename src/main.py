
from avvo_downloader import AvvoDownloader
import json
from pathlib import Path


def scrape_websites(start:int, end:int, urls: list):
    for url in urls[start:end]:
        (title, question, answers, lawyers, posted_times, answer_card_text) = loader.scrape_website(url)
        if title is None and question is None and answers is None and lawyers is None and posted_times is None and answer_card_text is None:
            result = {
                'url': url,
                'title': 'Not Found',
                'question': 'Not Found',
                'answers': [],
                'lawyers': [],
                'posted_times': [],
                'answer_card_text': []
            }
        else:
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

    print(f'urls to scrape {len(q_n_a_urls)}')

    # if scraping fails go on from here (start end)
    scrape_websites(8746, len(q_n_a_urls), q_n_a_urls)
