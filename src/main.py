import argparse
from datetime import datetime
import snscrape.modules.twitter as twt
from typing import Callable
from collections import namedtuple

import writer
import classify

def parse_arguments():
    p = argparse.ArgumentParser()
    p.add_argument("-s", "--start", help="YYYY-MM-DD date to collect tweets from", type=str, default="now")
    p.add_argument("-e", "--end", help="YYYY-MM-DD date to collect tweets until (only valid if using '--start'; must be earlier than the current time)", type=str)
    p.add_argument("-l", "--limit", help="Maximum number of tweets to scrape", type=int, default=5000)
    p.add_argument("-o", "--output", help="Places the output in to <file>") 
    p.add_argument(
        "-t", "--terms",
        nargs="+",
        help="(Required) List of terms to use when searching twitter",
        required=True)

    # Can't write to both CSV and Postgres 
    group = p.add_mutually_exclusive_group()
    group.add_argument(
        "-db", 
        "--database-config",
        help="JSON file containing connection details for a Postgres database. See `writer.py` for required keys", 
        type=str)
    group.add_argument(
        "-f",
        "--file",
        help="File to append tweets to. Will be created if doesn't already exist",
        type=str)

    return p.parse_args()

def parse_date(date: str):
    """
    Converts a string in format YYYY-MM-DD to a datetime object
    """
    
    try:
        return datetime.strptime(date, "%Y-%m-%d")
    except ValueError as error:
        print(f"Error parsing the date {date}: \n{error}")
        raise

def scrape_tweets(*, start: str, end: str, num_tweets: int, terms: list[str], target: str):
    scraper = twt.TwitterSearchScraper(
        " ".join(terms),
        f" lang: en since:{start} until:{end} -filter:replies")

    Row = namedtuple("Row", ["tweetId", "date", "user", "url", "contents", "weight", "pos", "neu", "neg"])
    
    # Represents the output file we're writing to
    output = None
    if target[0] == "psql":
        output = writer.PSQLWriter(
                db_config=target[1])
    else:
        output = csv_file = writer.CSVWriter(
            file_name=target[1],
            out_directory=target[2],
            column_names=Row._fields)

    for i, tweet in enumerate(scraper.get_items()):
        if i > MAX_TWEETS:
            break

        weight = classify.naive_weight(tweet.content)
        pos, neu, neg = classify.vader(tweet.content)

        output.append(Row(
            tweet.id, tweet.date, tweet.user, tweet.url, tweet.content, weight, pos, neu, neg
            ))

    output.stop()

def main():
    args = parse_arguments()
    
    MAX_TWEETS = args.limit - 1
    SEARCH_TERMS = args.terms
    
    if MAX_TWEETS < 1:
        print("--limit must be greater than 1. Got:", MAX_TWEETS)
        raise

    if not args.start:
        args.start = "2006-04-21" # Date twitter was created
    if not args.end:
        args.end = datetime.now().strftime("%Y-%m-%d")

    start = parse_date(args.start)
    end = parse_date(args.end) 

    # `end` can't be in the future
    if end > datetime.now():
        print(f"Error: `start` can't be in the future: {args.start}")
        raise
    
    # `start` can't be after `end`
    if start > end:
        print(f"Error: `start` can't be after `end` \nstart: {args.start} \t end: {args.end}")
        raise

    target = None
    if args.database-config:
        target = ["psql", args.database-config]
    else:
        target = ["csv", args.file, args.output]


    scrape(
        start=args.start,
        end=args.end,
        num_tweets=MAX_TWEETS,
        terms=SEARCH_TERMS,
        target=target)    

if __name__ == "__main__":
    main()
