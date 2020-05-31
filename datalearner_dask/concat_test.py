"""
Function:
Author: Du Fei
Create Time: 2020/5/31 11:15
"""

import csv
import os
import random

import dask.dataframe as dd

from data.generate_table_data import generate_data
from datalearner_dask.client_test import init_local_cluster
from util.dl_file_util import clear_folder, check_dir_exist


def create_test_data(base_dir, dataset_count, field_count, line_count, field_type):
    for dataset_id in range(dataset_count):
        save_dir = os.path.join(base_dir, str(dataset_id))
        clear_folder(save_dir)
        check_dir_exist(save_dir, create=True)

        id_list = random.sample(range(int(line_count * 1.2)), line_count)

        generate_data(1, field_count, line_count, save_dir, field_type, id_list=id_list)


def concat_one_bye_one(input_dir, save_dir):
    clear_folder(save_dir)

    df = None
    for data_set_index in os.listdir(input_dir):
        filepath = os.path.join(input_dir, data_set_index)
        df_tmp = dd.read_csv(filepath + "/*").set_index("id")
        if df is None:
            df = df_tmp
        else:
            df = dd.concat([df, df_tmp], axis=1)

        # import dask
        # print(df.head())
        # print(df_tmp.head())
        # print(dask.compute(df_tmp.shape))

    df = df.repartition(partition_size="32MB")

    df.to_csv(save_dir + "/concat_res_*.csv", line_terminator="\n", quoting=csv.QUOTE_NONE)


if __name__ == '__main__':

    init_local_cluster(2)

    base_data_dir = r"F:\data\python_test\concat_test\input"
    output_dir = r"F:\data\python_test\concat_test\output"
    num_of_dataset = 30
    num_of_fields = 1
    field_value_type = "int"
    line_number = 5000000

    # create_test_data(base_data_dir, num_of_dataset, num_of_fields, line_number, field_value_type)
    concat_one_bye_one(base_data_dir, output_dir)
