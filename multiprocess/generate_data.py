"""
Function: This script will generate sample data for our tests. Each field value will be a random integer between
          100 and 100000.
Author: Du Fei
Create Time: 2020/3/22 17:38
"""

import os
import numpy as np

num_of_instances = 1000000  # total number of lines of the data
num_of_fields = 5  # how many fields each line will have
num_of_files = 10  # how many files will be generated in total

save_path = "f:/data/python_test/input"  # save path
save_file_name = "data_"  # save file name, we will and file index in the end of this variable for multiple files.
file_encoding = "utf-8"  # file encoding
field_delimiter = "|"  # delimiter of the fields

# create the output path if exists
if not os.path.exists(save_path):
    os.makedirs(save_path)

num_of_instances_each_file = num_of_instances / num_of_files
for file_index in range(num_of_files):

    file_path = os.path.join(save_path, f"{save_file_name}{file_index}")

    # remove the file if exists
    if os.path.exists(file_path):
        os.remove(file_path)

    writer = open(file_path, "a+", encoding=file_encoding)

    instance_index_start = num_of_instances * file_index
    instance_index_end = num_of_instances * (file_index + 1)

    for i in range(instance_index_start, instance_index_end):
        field_values = np.random.randint(100, 100000, size=num_of_fields)
        writer.write(field_delimiter.join(str(x) for x in field_values) + "\n")
