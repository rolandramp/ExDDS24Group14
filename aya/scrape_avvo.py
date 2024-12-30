from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
import csv

# Path to your ChromeDriver
chrome_driver_path = "C:\\Users\\z004mcpx\\Downloads\\chromedriver-win64\\chromedriver-win64\\chromedriver.exe"

# Set up the WebDriver service
service = Service(chrome_driver_path)
driver = webdriver.Chrome(service=service)

# URL of the Avvo bankruptcy advice section for California
url = "https://www.avvo.com/topics/bankruptcy/advice?utf8=%E2%9C%93&search_topic_advice_search%5Bstate%5D=CA&search_topic_advice_search%5Bcontent_type%5D=question"

# Open the URL
driver.get(url)

# Function to scrape a single page
def scrape_page():
    questions_data = []
    questions = driver.find_elements(By.CLASS_NAME, 'topic-advice-question-card')
    for question in questions:
        title_element = question.find_element(By.CSS_SELECTOR, 'a.block-link')
        title = title_element.text
        link = title_element.get_attribute('href')
        metadata = question.find_element(By.CLASS_NAME, 'text-muted').text
        
        # Extracting category tags (assuming they are part of the metadata)
        category_tags = metadata.split('\n')[0].split(', ')
        
        questions_data.append({
            'Question': title,
            'Link': link,
            'Date': ', '.join(category_tags)
        })
    return questions_data

# Function to scrape details from a question link
def scrape_question_details(link):
    driver.get(link)
    time.sleep(10)  # Wait for the page to fully load
    
    # Get the page source and parse it with BeautifulSoup
    page_source = driver.page_source
    soup = BeautifulSoup(page_source, 'html.parser')
    
    # Extract question content
    question_content = soup.find('div', id='question-body').get_text(strip=True)
    
    # Extract answers and lawyer info
    answers_data = []
    answers = soup.find_all('div', class_='qa-answer')
    for answer in answers:
        if 'data-answer-id' in answer.attrs:
            answer_id = answer['data-answer-id']
        else:
            answer_id = None
        answer_content = answer.find('div', class_='answer-body').get_text(strip=True)
        lawyer_info = answer.find('div', class_='answer-professional')
        lawyer_id = lawyer_info.find('a')['href'].split('/')[-1]
        is_best_answer = 'Best Answer' in answer.get_text()
        
        answers_data.append({
            'answer_id': answer_id,
            'answer_content': answer_content,
            'lawyer_id': lawyer_id,
            'is_best_answer': is_best_answer
        })
    
    # Extract category tags
    tags = soup.find('div', class_='content-topic-tags').find_all('a')
    category_tags = [tag.get_text(strip=True) for tag in tags]
    
    # Compile the data
    question_data = {
        'question_content': question_content,
        'category': 'bankruptcy',
        'tags': category_tags,
        'answers': answers_data
    }
    
    return question_data

# Scrape multiple pages
all_data = []
for page in range(1, 3):  # Adjust the range for more pages
    page_data = scrape_page()
    
    for entry in page_data:
        details = scrape_question_details(entry['Link'])
        entry['Question Content'] = details['question_content']
        entry['Tags'] = ', '.join(details['tags'])
        
        for ans in details['answers']:
            entry['Answer ID'] = ans['answer_id']
            entry['Answer Content'] = ans['answer_content']
            entry['Lawyer ID'] = ans['lawyer_id']
            entry['Is Best Answer'] = ans['is_best_answer']
            
            all_data.append(entry.copy())
    
    try:
        # Scroll to the bottom of the page to ensure all content is loaded
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        
        # Wait for the next button to be clickable
        next_button = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.CLASS_NAME, 'pagination-next'))
        )
        
        # Use JavaScript to click the next button
        driver.execute_script("arguments[0].click();", next_button)
    except Exception as e:
        print(f"Error on page {page}: {e}")
        break

# Save the data to a CSV file
with open('avvo_data.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(['Question', 'Link', 'Date', 'Question Content', 'Tags', 'Answer ID', 'Answer Content', 'Lawyer ID', 'Is Best Answer'])
    
    for entry in all_data:
        writer.writerow([
            entry['Question'],
            entry['Link'],
            entry['Date'],
            entry.get('Question Content', ''),
            entry.get('Tags', ''),
            entry.get('Answer ID', ''),
            entry.get('Answer Content', ''),
            entry.get('Lawyer ID', ''),
            entry.get('Is Best Answer', '')
        ])

# Close the WebDriver
driver.quit()