from avvo_downloader import AvvoDownloader
import json


if __name__ == '__main__':
    loader = AvvoDownloader()

    with open('../data/question_links_bankruptcy.json', 'r') as file:
        data = json.load(file)

    print(loader.scrape_website('https://www.avvo.com/attorneys/285119.html'))