import re
import os
from avvo_downloader import AvvoDownloader
import json
from pathlib import Path
import polars as pl


def scrape_websites(start: int, end: int, urls: list):
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


def anonymize_lawyer_name_from_url(input_url: str):
    match = re.search(r'(\d+).html', input_url)
    if match:
        unique_id = match.group(1)
        # Construct the anonymized URL
        anonymized_url = f"https://www.avvo.com/attorneys/{unique_id}.html"
        return anonymized_url
    else:
        return input_url  # Return the original URL if the pattern does not match


def extract_details(answer_card_texts: list) -> dict:
    details = {}
    stars = []
    reviews = []
    rating = []
    helpful = []
    lawyers_agree = []
    for answer_card_text in answer_card_texts:
        details = {}

        # Extract stars
        stars_match = re.search(r'(\d+(\.\d+)?) stars', answer_card_text)
        stars.append(float(stars_match.group(1)) if stars_match else None)

        # Extract reviews
        reviews_match = re.search(r'(\d+) reviews', answer_card_text)
        reviews.append(int(reviews_match.group(1)) if reviews_match else None)

        # Extract rating
        rating_match = re.search(r'Rating: \n(\d+(\.\d+)?)', answer_card_text)
        rating.append(float(rating_match.group(1)) if rating_match else None)

        # Extract helpful count
        helpful_match = re.search(r'Helpful \((\d+)\)', answer_card_text)
        helpful.append(int(helpful_match.group(1)) if helpful_match else None)

        # Extract number of lawyers who agree
        lawyers_agree_match = re.search(r'(\d+) lawyer(s)? agree(s)?', answer_card_text)
        lawyers_agree.append(int(lawyers_agree_match.group(1)) if lawyers_agree_match else None)

    details['stars'] = stars
    details['reviews'] = reviews
    details['rating'] = rating
    details['helpful'] = helpful
    details['lawyers_agree'] = lawyers_agree
    return details


def transform_files_to_data_frame(directory: str) -> pl.DataFrame:
    data_frame_list = []
    for filename in os.listdir(directory):
        match = re.search(r'_n_(\d+)', filename)
        if match:
            question_number =  int(match.group(1))
            with open(Path.joinpath(Path(directory),filename), 'r', encoding='utf-8') as actual_file:
                data = json.load(actual_file)
            lawyers = [anonymize_lawyer_name_from_url(lawyer) for lawyer in data['lawyers']]
            details = extract_details(data['answer_card_text'])
            df = pl.DataFrame({
                'number': [question_number] * len(data['answers']),
                'url': [data['url']] * len(data['answers']),
                'title': [data['title']] * len(data['answers']),
                'question': [data['question']] * len(data['answers']),
                'answers': data['answers'],
                'lawyers': lawyers,
                'posted_times': data['posted_times'],
                'answer_card_text': data['answer_card_text'],
                'stars': details['stars'],
                'reviews': details['reviews'],
                'rating': details['rating'],
                'helpful': details['helpful'],
                'lawyers_agree': details['lawyers_agree']
            })
            data_frame_list.append(df)
    return pl.concat(data_frame_list)

def create_result_data_frame(directory_path: str= '../data/scraped'):
    df = transform_files_to_data_frame(directory_path)
    print(df.head(5))

if __name__ == '__main__':
    loader = AvvoDownloader()

    data_path = Path('../data/scraped/')

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