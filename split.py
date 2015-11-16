#!/usr/bin/env python

import os

def split(filepath, delimiter=',', row_limit=100000, 
    output_name_template='output_%s.csv', output_path="../dedupe/data/split/", keep_headers=True):
    """
    Splits a CSV file into multiple pieces.
    Arguments:
        `row_limit`: The number of rows you want in each output file. 10,000 by default.
        `output_name_template`: A %s-style template for the numbered output files.
        `output_path`: Where to stick the output files.
        `keep_headers`: Whether or not to print the headers in each output file.
    """
    import csv
    reader = csv.reader(filepath, delimiter=delimiter)
    current_piece = 1
    current_out_path = os.path.join(
         output_path,
         output_name_template  % current_piece
    )
    current_out_writer = csv.writer(open(current_out_path, 'wb'), delimiter=delimiter)
    current_limit = row_limit
    if keep_headers:
        headers = reader.next()
        current_out_writer.writerow(headers)
    for i, row in enumerate(reader):
        if i + 1 > current_limit:
            current_piece += 1
            current_limit = row_limit * current_piece
            current_out_path = os.path.join(
               output_path,
               output_name_template  % current_piece
            )
            current_out_writer = csv.writer(open(current_out_path, 'wb'), delimiter=delimiter)
            if keep_headers:
                current_out_writer.writerow(headers)
        current_out_writer.writerow(row)


if __name__ == "__main__":
    myfilepath = "osha_inspection.csv"
    split(open(myfilepath, 'rb'),output_name_template="osha_insp_%s.csv")