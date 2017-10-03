
#!/usr/bin/env python3
# -*- python -*-
# adapted from https://meta.stackexchange.com/questions/28221/scripts-to-convert-data-dump-to-other-formats
import xml.etree.ElementTree as ET
import csv
import ipdb
import tqdm
import os
import sys


# Copy/paste from the help section on SEDE
# http://data.stackexchange.com/stackoverflow/query/new
posts_schema = [
    'Id',                    # int
    'PostTypeId',            # tinyint
    'AcceptedAnswerId',      # int
    'ParentId',              # int
    'CreationDate',          # datetime
    'DeletionDate',          # datetime
    'Score',                 # int
    'ViewCount',             # int
    'Body',                  # nvarchar(max)
    'OwnerUserId',           # int
    'OwnerDisplayName',      # nvarchar(40)
    'LastEditorUserId',      # int
    'LastEditorDisplayName', # nvarchar(40)
    'LastEditDate',          # datetime
    'LastActivityDate',      # datetime
    'Title',                 # nvarchar(250)
    'Tags',                  # nvarchar(250)
    'AnswerCount',           # int
    'CommentCount',          # int
    'FavoriteCount',         # int
    'ClosedDate',            # datetime
    'CommunityOwnedDate'     # datetime
    ]

tags_schema = [
    'Id',               # int
    'TagName',          # nvarchar (35)
    'Count',            # int
    'ExcerptPostId',    # int
    'WikiPostId'        # int
    ]

posthistory_schema = [
    'Id', # int
    'PostHistoryTypeId', # tinyint
    'PostId', # int
    'RevisionGUID', # uniqueidentifier
    'CreationDate', # datetime
    'UserId', # int
    'UserDisplayName', # nvarchar (40)
    'Comment', # nvarchar (400)
    'Text', # nvarchar (max)
]

def dump(fn, schema):
    """
    Read a stream from xml filename, yield next row
    """
    tree = ET.iterparse(fn)
    for event, elem in tqdm.tqdm(tree):
        if elem.tag != 'row':
            continue
        result = []
        for key in schema:
            try:
                result.append(elem.attrib[key])
            except KeyError:
                result.append('')
        yield result
        elem.clear()

def convert_to_csv(xml_filepath, csv_filepath, schema):
    # newline arg stops Windows converting \r\n into \r\r\n
    # https://stackoverflow.com/questions/3348460/csv-file-written-with-python-has-blank-lines-between-each-row
    writer = csv.writer(open(csv_filepath, 'w', newline=''))

    # write the column titles
    writer.writerow(schema)

    for line in dump(xml_filepath, schema):
        writer.writerow(line)

def main():
    raw_data_path = os.path.join('..', '..', 'data', 'raw')
    processed_data_path = os.path.join('..', '..', 'data', 'processed')
    interim_data_path = os.path.join('..', '..', 'data', 'interim')
    
    convert_to_csv(
        os.path.join(raw_data_path, 'Tags.xml'),
        os.path.join(processed_data_path, 'tags.csv'),
        tags_schema
    )



if __name__ == '__main__':
    main()
