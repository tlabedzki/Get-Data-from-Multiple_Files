
    # Multi-file data reader
    # Describe: Script for download the data from multiple files. It can be helpful for concat & analizing the data from a lot of files from one folder.
    # Author: Tomasz Łabędzki

    # Script Structure:
    # 1. Main function with settings.
    # 2. Standard importrange function replacement (data_importer) with smaller methods for remove headers from souce data & text formatting.


import pandas as p
import glob as g
import matplotlib.pyplot as plt


# Settings and paths
path_to_files = '.\\data\\mag*.txt'
path_to_file_with_cities = '.\\data\\mag_def.txt'
files = g.glob(path_to_files)

# CSV read settings
separator = '|'
encoding_type = 'ISO-8859-1'

# Filtering settings
filterby_col_name = 'my_date'
from_date = '2020-01-01'
to_date = '2021-01-01'

# Sorting settings
groupby_col_name = 'mag'
sortby_col_name = 'netto'

# Join settings
column_name_for_join = 'mag'

# Chart settings
date_column = 'my_date'
values_columns = 'netto'

# Exceptions in read files, fe. '.\\folder\\name_of_file.txt'
exceptions = [
    '.\\data\\mag_def.txt'
]


def print_info_about_data(concated_data):

    print(concated_data.info())


def load_csv_file(file, separator, encoding_type):

    result_data = p.read_csv(file, sep=separator, header='infer', encoding=encoding_type)
    print(result_data)
    return result_data


def concat_data_from_path(separator):

    all_data = []
    for file in files:
        if file not in exceptions:
            file_with_data = p.read_csv(file, sep=separator, header='infer', encoding=encoding_type)
            all_data.append(file_with_data)

    concated_data = p.concat(all_data)
    print(concated_data)
    return concated_data


def filtering_by_data(concated_data, filterby_col_name, from_date, to_date):

    result_data = concated_data.loc[(concated_data[filterby_col_name] >= from_date) & (concated_data[filterby_col_name] < to_date)]
    print(result_data)
    return result_data


def groupedby_and_sorting_by_data(concated_data, groupby_col_name, sortby_col_name):

    result_data = concated_data.groupby(by=groupby_col_name).sum()
    result_data = result_data.sort_values(by=sortby_col_name, ascending=False).reset_index()
    result_data = result_data
    print(result_data)
    return result_data


def join_two_tables(concated_data, path_to_file_with_cities, column_name_for_join, separator, encoding_type):

    file_with_cities = load_csv_file(path_to_file_with_cities, separator, encoding_type)

    result_data = concated_data.set_index(column_name_for_join).join(file_with_cities.set_index(column_name_for_join), how='inner')
    result_data = result_data.groupby(by='miasto').count()
    result_data = result_data.sort_values(by='netto', ascending=False).reset_index()
    print(result_data)
    return result_data


def make_and_show_chart(concated_data, date_column):

    concated_data[date_column] = p.to_datetime(concated_data[date_column])
    ax = concated_data.plot(x=date_column, y=values_columns, figsize=(12,8))
    plt.show()


def check_version_of_installed_packages():
    import pandas
    import numpy
    print('pd:{}'.format(pandas.__version__))
    print('pd:{}'.format(numpy.__version__))


def process_the_data():
    concated_data = concat_data_from_path(separator)
    # filtering_by_data(concated_data, filterby_col_name, from_date, to_date)
    # groupedby_and_sorting_by_data(concated_data, groupby_col_name, sortby_col_name)
    # join_two_tables(concated_data, path_to_file_with_cities, column_name_for_join, separator, encoding_type)
    make_and_show_chart(concated_data, date_column)


# Run scripts
process_the_data()





