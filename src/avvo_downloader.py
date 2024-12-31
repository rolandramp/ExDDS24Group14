from pydantic import BaseModel

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from bs4 import BeautifulSoup


class AvvoDownloader(BaseModel):

    def scrape_website(self, url):
        # Initialize the Chrome driver
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

        try:
            # Open the URL
            driver.get(url)
            time.sleep(2)  # Wait for Cloudflare to process the request

            # Extract the page page_content
            questions = driver.find_elements(By.CLASS_NAME, 'content-question')
            title = (questions[0].find_element(By.TAG_NAME, 'div')
                     .find_element(By.TAG_NAME, 'h1')
                     .text)
            print(title)
            question = questions[0].find_element(By.XPATH, './/div//div//div//div//p').text
            print(question)
            answers = driver.find_elements(By.CLASS_NAME, 'answer-body')
            answers_text = []
            for answer in answers:
                answers_text.append(answer.find_element(By.TAG_NAME, 'p').text)
            print(answers_text)
            answer_container = driver.find_element(By.ID, 'answers_container')
            answer_cards = answer_container.find_elements(By.CLASS_NAME, 'qa-lawyer-card')

            loyers = []
            posted_times = []
            helpfullness = []
            for answer_card in answer_cards:
                loyers.append(answer_card.find_element(By.CLASS_NAME, 'name-specialty').find_element(By.TAG_NAME, 'a').get_attribute('href'))
                posted_times.append(answer_card.find_element(By.TAG_NAME, 'time').text)
            print(loyers)
            print(posted_times)

            return (title, question, answers, loyers, posted_times)
        finally:
            driver.quit()