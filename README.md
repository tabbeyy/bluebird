# `bluebird`

A script to scrape tweets by term, and perform some light analysis on the sentiment of the tweet (using a pretrained VADER model), and the prevalance of the defined terms in the tweet (useful for identifying keyword spam).  
The script writes to either CSV or a PostgreSQL database. 

## Structure
Dependencies are listed in `requirements.txt`.

### `src`
- `main.py` - Entry point of the script. Parses arguments and handles the bulk of the scraping
- `writer.py` - Defines a couple of classes to handle I/O to file and database. Both present the same interface
- `classify.py` - Functions that perform the analysis of tweet content