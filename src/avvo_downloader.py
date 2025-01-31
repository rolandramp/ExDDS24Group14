import json
import os
import re
import time
from datetime import datetime
from pathlib import Path

import polars as pl
from pydantic import BaseModel
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager


class AvvoDownloader(BaseModel):

    def _scrape_website(self, url):
        # Initialize the Chrome driver
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

        try:
            # Open the URL
            driver.get(url)
            time.sleep(5)  # Wait for Cloudflare to process the request

            # Extract the page page_content
            questions = driver.find_elements(By.CLASS_NAME, 'content-question')
            if (questions is None) or (len(questions) == 0):
                title = 'Not Found'
                question = 'Not Found'
                question_tags = 'Not Found'
            else:
                title = (questions[0].find_element(By.TAG_NAME, 'div')
                         .find_element(By.TAG_NAME, 'h1')
                         .text)
                print(title)
                try:
                    topic_expander = questions[0].find_element(By.ID,'topic-expander-text')
                    topic_expander.click()
                except NoSuchElementException:
                    print("Element 'topic-expander-text' not found")
                except Exception as e:
                    print(e)
                question_tags = ','.join([element.text for element in questions[0].find_elements(By.CLASS_NAME, 'related-topic-advice-tag')])
                question = questions[0].find_element(By.XPATH, './/div//div//div//div//p').text

            answers = driver.find_elements(By.CLASS_NAME, 'answer-body')
            answers_text = []
            for answer in answers:
                answers_text.append(answer.find_element(By.TAG_NAME, 'p').text)

            answer_container = driver.find_element(By.ID, 'answers_container')
            answer_cards = answer_container.find_elements(By.CLASS_NAME, 'qa-lawyer-card')

            lawyers = []
            posted_times = []
            answer_card_text = []
            for answer_card in answer_cards:
                lawyers.append(answer_card.find_element(By.CLASS_NAME, 'name-specialty').find_element(By.TAG_NAME, 'a').get_attribute('href'))
                posted_times.append(answer_card.find_element(By.TAG_NAME, 'time').text)
                answer_card_text.append(answer_card.text)

            return (title, question, question_tags, answers_text, lawyers, posted_times, answer_card_text)
        except Exception as e:
            print(e)
            return (None, None, None, None, None, None, None)
        finally:
            driver.quit()


    def extract_date(self, date_string: str) -> datetime | None:
        # Define the regex pattern to match the date
        date_pattern = r'on (\w+ \d{1,2}, \d{4})'

        # Search for the date in the string
        match = re.search(date_pattern, date_string)

        if match:
            # Extract the date string
            date_str = match.group(1)

            # Parse the date string into a datetime object
            date_obj = datetime.strptime(date_str, '%b %d, %Y')

            return date_obj
        else:
            print(f'Date not found in {date_string}')
            return None


    def transform_files_to_data_frame(self, directory_path: str = '../data/scraped') -> pl.DataFrame:
        data_frame_list = []
        json_files = [filename for filename in os.listdir(directory_path) if filename.endswith('.json')]
        brocken_files = []
        for filename in json_files:
            match = re.search(r'_n_(\d+)', filename)
            if match:
                question_number = int(match.group(1))
                with open(Path.joinpath(Path(directory_path), filename), 'r', encoding='utf-8') as actual_file:
                    data = json.load(actual_file)
                if data['title'] == 'Not Found' and data['question'] == 'Not Found':
                    df = pl.DataFrame({
                        'number': question_number,
                        'url': data['url'],
                        'title': data['title'],
                        'question': data['question'],
                        'question_tags': None,
                        'answers': None,
                        'lawyers': None,
                        'posted_times': None,
                        'answer_card_text': None,
                        'stars': None,
                        'reviews': None,
                        'rating': None,
                        'helpful': None,
                        'lawyers_agree': None,
                        'best_answer': None
                    })
                else:
                    try:
                        lawyers = [self.anonymize_lawyer_name_from_url(lawyer) for lawyer in data['lawyers']]
                        details = self.extract_details(data['answer_card_text'])
                        posted_times = [self.extract_date(time) for time in data['posted_times']]
                        df = pl.DataFrame({
                            'number': [question_number] * len(data['answers']),
                            'url': [data['url']] * len(data['answers']),
                            'title': [data['title']] * len(data['answers']),
                            'question': [data['question']] * len(data['answers']),
                            'question_tags': [data['question_tags']] * len(data['answers']),
                            'answers': data['answers'],
                            'lawyers': lawyers,
                            'posted_times': posted_times,
                            'answer_card_text': data['answer_card_text'],
                            'stars': details['stars'],
                            'reviews': details['reviews'],
                            'rating': details['rating'],
                            'helpful': details['helpful'],
                            'lawyers_agree': details['lawyers_agree'],
                            'best_answer': details['best_answer']
                        })
                    except Exception as e:
                        brocken_files.append(question_number)
                        continue
                data_frame_list.append(df)
        print(brocken_files)
        return pl.concat(data_frame_list)


    def extract_details(self, answer_card_texts: list) -> dict:
        details = {}
        stars = []
        reviews = []
        rating = []
        helpful = []
        lawyers_agree = []
        best_answer = []
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

            # Extract "Selected as best answer"
            best_answer_match = re.search(r'Selected as best answer', answer_card_text)
            best_answer.append(True if best_answer_match else False)

        details['stars'] = stars
        details['reviews'] = reviews
        details['rating'] = rating
        details['helpful'] = helpful
        details['lawyers_agree'] = lawyers_agree
        details['best_answer'] = best_answer
        return details


    def anonymize_lawyer_name_from_url(self, input_url: str):
        match = re.search(r'(\d+).html', input_url)
        if match:
            unique_id = match.group(1)
            # Construct the anonymized URL
            anonymized_url = f"https://www.avvo.com/attorneys/{unique_id}.html"
            return anonymized_url
        else:
            return input_url  # Return the original URL if the pattern does not match


    def scrape_websites(self, start: int, end: int, urls: list, data_path: Path):
        for url in urls[start:end]:
            if isinstance(url, tuple):
                number = url[0]
                url = url[1]
            else:
                number = -1
            (title, question, question_raw, answers, lawyers, posted_times, answer_card_text) = self._scrape_website(url)
            if title is None and question is None and answers is None and lawyers is None and posted_times is None and answer_card_text is None:
                result = {
                    'url': url,
                    'title': 'Not Found',
                    'question': 'Not Found',
                    'quesiton_tags': 'Not Found',
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
                    'question_tags': question_raw,
                    'answers': answers,
                    'lawyers': lawyers,
                    'posted_times': [str(time) for time in posted_times],
                    'answer_card_text': answer_card_text
                }
            if number != -1:
                filename = f'results_n_{number}.json'
                print(f'writing {number}')
            else:
                filename = f'results_n_{start}.json'
                print(f'writing {start}')
            with open(data_path.joinpath(filename), 'w', encoding='utf-8') as json_file:
                json.dump(result, json_file, ensure_ascii=False, indent=4)
            start += 1
