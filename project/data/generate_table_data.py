"""
Function:
Author: Du Fei
Create Time: 2020/5/16 22:00
"""

import os
import random
import shutil
import numpy as np


def generate_data(num_of_files, num_of_fields, number_of_lines_each_file, save_dir,
                  add_head=True,
                  add_id=True,
                  id_type="str",
                  shuffle_id=True,
                  id_list=None):
    if id_list is None:
        total_lines = num_of_files * number_of_lines_each_file
        id_list = list(range(total_lines))

    if shuffle_id:
        random.shuffle(id_list)

    if id_type == "str":
        id_prefix = "id_"
    else:
        id_prefix = ""

    for file_index in range(num_of_files):
        filepath = os.path.join(save_dir, f"data_{file_index}.csv")

        if os.path.exists(filepath):
            os.remove(filepath)

        out_writer = open(filepath, "w")

        field_value_number = np.random.randint(0, 100, (number_of_lines_each_file, num_of_fields))

        res_list = []

        if add_head:
            if add_id:
                res_list.append(f"id,{','.join('field_' + str(field_index) for field_index in range(num_of_fields))}")
            else:
                res_list.append(f"{','.join('field_' + str(field_index) for field_index in range(num_of_fields))}")

        for line_index in range(number_of_lines_each_file):
            line_str = ",".join(f"field_{file_index}_{str(field_value).zfill(3)}" for field_index, field_value in
                                enumerate(field_value_number[line_index]))

            if add_id:
                id_index = file_index * number_of_lines_each_file + line_index
                id_index = id_list[id_index]
                res_list.append(f"{id_prefix}{id_index},{line_str}")
            else:
                res_list.append(line_str)

        out_writer.write("\n".join(res_list))
        print(f"file {file_index} has been generated...")


if __name__ == '__main__':
    output_dir = r"F:\data\python_test\dask"

    input_1 = f"{output_dir}/input_1"
    input_2 = f"{output_dir}/input_2"

    if os.path.exists(input_1):
        shutil.rmtree(input_1)

    if os.path.exists(input_2):
        shutil.rmtree(input_2)

    if not os.path.exists(input_1):
        os.makedirs(input_1)

    if not os.path.exists(input_2):
        os.makedirs(input_2)

    file_number = 50
    field_number = 10
    line_number = 1000000

    generate_data(file_number, field_number, line_number, input_1, id_type="str")

    file_number_2 = 2
    field_number_2 = 5
    line_number_2 = 1000000

    sample_ids = np.random.randint(0, file_number * line_number, file_number_2 * line_number_2)
    generate_data(file_number_2, field_number_2, line_number_2, input_2, id_list=sample_ids, id_type="str")
