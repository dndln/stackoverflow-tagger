import pandas as pd
import numpy as np
import ipdb
import os


def main():
    raw_data_path = os.path.join('..', '..', 'data', 'raw')
    processed_data_path = os.path.join('..', '..', 'data', 'processed')
    interim_data_path = os.path.join('..', '..', 'data', 'interim')

    tags = pd.read_csv(os.path.join(interim_data_path, 'tags.csv'))
    # TODO: Filter out tags below count threshold?
    # count_threshold = int
    # tags = tags['Count'] >= count_threshold


    # open a new file to write cleaned_posts to

    # post_tags can be multiple times the chunksize, don't set chunksize too big
    chunksize = 5
    posts = pd.read_csv(os.path.join(interim_data_path, 'posts.csv'), chunksize=chunksize)
    # each chunk keeps the header columns
    for chunk in posts:
        cleaned_posts = encode_tags(chunk, tags)

        # write the cleaned_posts df (not the headers) to the opened file, don't do IO in encode_tags






def encode_tags(chunk, tags):

    # reluctant/non-greedy match
    post_tags = chunk['Tags'].str.extractall('(<.+?>)').reset_index()
    post_tags.columns = ['posts_row', 'match', 'tag_text']
    post_tags['tag_text'] = post_tags['tag_text'].str[1:-1]
    post_tags = pd.merge(post_tags, tags, how='left', left_on='tag_text', right_on='TagName')

    # how do I encode each chunk row with the tag 'Id's?
    ipdb.set_trace()


if __name__ == '__main__':
    main()
