from pydantic import BaseModel
from bs4 import BeautifulSoup
import requests

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time

class AvvoDownloader(BaseModel):


    def scrape_website(self, url):
        # Set up Chrome options
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # Run in headless mode
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")

        # Initialize the Chrome driver
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

        try:
            # Open the URL
            driver.get(url)
            time.sleep(20)  # Wait for Cloudflare to process the request

            # Extract the page page_content
            page_content = driver.page_source
            print(f'-------{page_content}')
            return page_content
        finally:
            driver.quit()

        #headers = {
        #    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
        #    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        #    'Accept-Language': 'en-US,en;q=0.5',
        #    'Referer': 'https://www.google.com/',
        #    'Connection': 'keep-alive',
        #    'Cache-Control': 'max-age=0'
        #}

        #cookies = {
        #    'avvo-login': 'avvo-login=BAh7BkkiD3Nlc3Npb25faWQGOgZFVEkiKWIwNzFmODU0LTA2NTEtNDVlOS04%0AN2IxLTBjZTA1MTUzNmMxZQY7AFQ%3D%0A--3899b838c470e840935af56151133384249492dc; _persistent_session_id=BAh7BkkiD3Nlc3Npb25faWQGOgZFVEkiKTEzMDBjOTgxLWI3MTEtNGIzOC04%0AMDM0LTAxZjNhYjI0N2FmMAY7AFQ%3D%0A; avvo_cache_context=logged_out; _session_id=76e51546427eab937feefbae43b99bd1; aa_session_count=1; aa_persistent_session_id=1300c981-b711-4b38-8034-01f3ab247af0; laravel_session=eyJpdiI6IlFjM0l0VnFETzg0VHFaVUZvS3Z4WlE9PSIsInZhbHVlIjoiUWFwWXBPMkhzNDMxeEVXK0xYdjdqTjZobmdtdUdJbzdQZTZrQjA2RjhJRU1jTUo0UVN4aW52SU9ablJEdHNaZTNvVk1OdUNqRFJiNEZHcTYwZGp3ZU85SWdpMzRhZHQ4dVhFbGVkVjVPbWJ4cDd1cnl0TXlBV0RYZ3drdmRISm8iLCJtYWMiOiI3NmE1MTBmNGFhM2I5MDIyMjFhYzQ4MzkxMTU4NTU0MWQ3ODQ4MzFlNTNmMzI0MWI0OGNiMGVhNmU3Y2E3MTgzIiwidGFnIjoiIn0%3D; BIGipServerlegal-avvo-k8sw_20080_POOL=1926415626.28750.0000'
        #}

        #session = requests.Session()
        #session.headers.update(headers)
        #session.cookies.update(cookies)

        #try:
        #    response = session.get(url=url)
        #    response.raise_for_status()  # Check if the request was successful
        #    soup = BeautifulSoup(response.text, 'html.parser')
        #    page_content = soup.page_content.string if soup.page_content else 'No page_content found'
        #    return page_content
        #except requests.exceptions.RequestException as e:
        #    return f"Error: {e}"