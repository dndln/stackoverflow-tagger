
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

def dump(fp, schema):
    """
    Read a stream from xml filepath, yield next row
    """
    tree = ET.iterparse(fp)
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

    schema_dict = {
        'Posts.xml': posts_schema,
        'Tags.xml': tags_schema,
        'PostHistory.xml': posthistory_schema
    }

    xml_filename = sys.argv[1]
    csv_filename = sys.argv[2]
    schema = schema_dict[xml_filename]

    # sample use: 'python ./xml2csv.py Tags.xml tags.csv'
    convert_to_csv(
        os.path.join(raw_data_path, xml_filename),
        os.path.join(interim_data_path, csv_filename),
        schema
    )
    

if __name__ == '__main__':
    # sample use: 'python ./xml2csv.py Tags.xml tags.csv'
    main()
