

    # Multi-file data reader
    # Describe: Script for download the data from multiple files.
    #           It can be helpful for concat & analizing the data from a lot of files from one folder.
    # Author: Tomasz Łabędzki


import pandas as p
import glob as g


# Settings and paths
path_to_files = './../data/mag*.txt'
files = g.glob(path_to_files)

# CSV read settings
separator = '|'
encoding_type = 'ISO-8859-1'

# Exceptions in read files, fe. '.\\folder\\name_of_file.txt'
exceptions = [
    './../data/mag_def.txt'
]


# Method for load data from one file
def load_csv_file(file, separator, encoding_type):

    result_data = p.read_csv(file, sep=separator, header='infer', encoding=encoding_type)
    print(result_data)
    return result_data


# Method for load data from multiple files and then concat this data
def concat_data_from_path(separator):
    all_data = []
    for file in files:
        if file not in exceptions:
            file_with_data = p.read_csv(file, sep=separator, header='infer', encoding=encoding_type)
            all_data.append(file_with_data)

    concated_data = p.concat(all_data)
    print(concated_data)
    return concated_data


def process_the_data():
    concat_data_from_path(separator)


# Run scripts
process_the_data()





