import os
import time

import ipdb
import numpy as np
import pandas as pd
import pprint
import requests

def main():
    """
    Dowload training data from stackoverflow using the stackexchange api.

    Data scraped:
        question title
        question text
        question tags
        question accepted answer
    """
    now = time.strftime('%Y%m%d%H%M%S')

    info = get_info(now)
    info_filename = 'info_' + now + '.csv'
    info.to_csv(os.path.join('..', '..', 'data', 'raw', info_filename), index=False)
    # make sure to add a custom filter to get .body
    # don't talk about all this detail in the talk

def get_info(now):
    """
    Returns a dataframe of statistics about the site.
    """
    url = 'https://api.stackexchange.com/2.2/info'
    payload = {
                    'site': 'stackoverflow'
              }
    result = requests.get(url, params=payload)
    # print(result.url) # https://api.stackexchange.com/2.2/info?site=stackoverflow
    result = result.json()
    items = result['items'][0]
    items_seq = {k:[v] for k, v in items.items()}
    df = pd.DataFrame(items_seq)
    df['time_requested'] = now
    return df

def get_questions(now):
    """
    Returns a dataframe of questions.
    """
    url = 'https://api.stackexchange.com/2.2/questions'
    payload = {
                    'site': 'stackoverflow',

    pass

def get_accepted_answers(now):
    """
    Returns a dataframe of accepted answers to the questions,
    if the questions have one.
    """
    pass

if __name__ == '__main__':
    main()
