import argparse

from avvo_downloader import AvvoDownloader
import json
from pathlib import Path
import polars as pl

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scrape websites and transform JSON files to DataFrame.')
    parser.add_argument('--scrape', action='store_true', help='Scrape websites')
    parser.add_argument('--rescrape', action='store_true', help='Rescrape websites using parquet file as source')
    parser.add_argument('--rescrapemissing', action='store_true',
                        help='Rescrape missing websites two parquet files as source on with a previous scrape and one with the actual scrape')
    parser.add_argument('--transform', action='store_true', help='Transform JSON files to DataFrame')
    parser.add_argument('--start', type=int, default=0, help='Start index for scraping')
    parser.add_argument('--end', type=int, help='End index for scraping')
    parser.add_argument('--directory', type=str, default='../data/scraped', help='Directory path for JSON files')
    parser.add_argument('--previous', type=str, default='../data/all_questions_and_answer.parquet',
                        help='Path to the previous parquet file')
    parser.add_argument('--actual', type=str, default='../data/all_questions_and_answer_new.parquet',
                        help='Path to the actual parquet file')
    args = parser.parse_args()

    loader = AvvoDownloader()

    data_path = Path(args.directory)

    if args.scrape:
        with open('../data/question_links_bankruptcy.json', 'r') as file:
            data = json.load(file)
        q_n_a_urls = [url for key in data.keys() for url in data.get(key)]

        # q_n_a_urls= ["https://www.avvo.com/legal-answers/how-does-husband-filing-for-bankruptcy-impact-my-d-5023546.html"]

        print(f'urls to scrape {len(q_n_a_urls)}')
        loader.scrape_websites(args.start, args.end if args.end else len(q_n_a_urls), q_n_a_urls, data_path)

    if args.rescrape:
        scrape_questions_and_answers_df = pl.read_parquet(args.previous)
        to_scrape_df = scrape_questions_and_answers_df.filter(pl.col('title') != 'Not Found').select('number',
                                                                                                     'url').unique().sort(
            'number')
        to_scrape_list_of_tuples = list(map(tuple, to_scrape_df.to_numpy()))
        start = to_scrape_df.with_row_index().filter(pl.col('number') == args.start).select('index')[0, 0]
        if args.end:
            end = to_scrape_df.with_row_index().filter(pl.col('number') == args.end).select('index')[0, 0]
        else:
            end = len(to_scrape_list_of_tuples)
        loader.scrape_websites(start, end, to_scrape_list_of_tuples, data_path)

    if args.rescrapemissing:
        scrape_questions_and_answers_df = pl.read_parquet(args.previous)
        scrape_questions_and_answers_new_df = pl.read_parquet(args.actual)
        truth_df = scrape_questions_and_answers_df.filter(pl.col('title') != 'Not Found').select('number',
                                                                                                 'url').unique().sort(
            'number')
        new_df = scrape_questions_and_answers_new_df.filter(pl.col('title') != 'Not Found').select('number',
                                                                                                   'url').unique().sort(
            'number')
        to_scrape_df = truth_df.select('number', 'url').join(new_df.select('number', 'url'), on=['number', 'url'],
                                                             how='anti')
        dict = to_scrape_df.to_dict(as_series=False)
        to_scrape_list_of_tuples = list(zip(dict['number'], dict['url']))
        if args.start == 0:
            start = 0
        else:
            start = to_scrape_df.with_row_index().filter(pl.col('number') == args.start).select('index')[0, 0]
        if args.end:
            end = to_scrape_df.with_row_index().filter(pl.col('number') == args.end).select('index')[0, 0]
        else:
            end = len(to_scrape_list_of_tuples)
        loader.scrape_websites(start, end, to_scrape_list_of_tuples, data_path)

    if args.transform:
        df = loader.transform_files_to_data_frame(args.directory)
        df = df.sort(by='number')
        df.write_parquet('../data/all_questions_and_answer_new.parquet')
