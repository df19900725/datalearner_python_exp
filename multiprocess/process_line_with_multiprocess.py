"""
Function: This is our first try for python multiprocess. However, this is a incorrect way.
Author: Du Fei
Create Time: 2020/3/22 17:38
"""

import os
import datetime
import multiprocessing as mp

input_path = "f:/data/python_test/input"  # input path
save_path_no_multiprocess = "f:/data/python_test/output_0"  # save path
save_path_multiprocess_file = "f:/data/python_test/output_1"  # save path
save_path_multiprocess_line = "f:/data/python_test/output_2"  # save path
save_file_name = "data_"  # save file name, we will and file index in the end of this variable for multiple files.
file_encoding = "utf-8"  # file encoding
field_delimiter = "|"  # delimiter of the fields


def process_line(input_line):
    """
    process one line for input data. we will add 1 for each field value
    :param input_line: input line
    :return new_line: new line which each field value will be add 1
    """
    line_array = input_line.split(field_delimiter)
    new_line = []
    for array in line_array:
        new_line.append(str(int(array) + 1))

    return field_delimiter.join(new_line)


def process_file(input_file_path, output_file_path):
    """
    This method is used to process one file.
    :param input_file_path:
    :param output_file_path:
    :return:
    """

    if not os.path.exists(input_file_path):
        print(f"no input file found:{input_file_path}")

    res = []
    for line in open(input_file_path, "r"):
        res.append(process_line(line))

    open(output_file_path, "a+").write("\n".join(new_line for new_line in res))


def process_with_multiprocess_file_level(input_data_path, output_data_path, process_number):
    """
    This method process all data with multiprocess module. Each sub process will handle one file
    :param input_data_path: input data path
    :param output_data_path: output data path
    :param process_number:  number of processed that will be generated
    :return None: no return
    """

    if not os.path.exists(output_data_path):
        os.makedirs(output_data_path)

    pool = mp.Pool(processes=process_number)
    for file_name in os.listdir(input_data_path):
        input_file_path = os.path.join(input_data_path, file_name)
        output_file_path = os.path.join(output_data_path, file_name)

        pool.apply_async(process_file, args=(input_file_path, output_file_path))

    pool.close()
    pool.join()


def process_with_multiprocess_line_level(input_data_path, output_data_path, process_number):
    """
    This method process all data with multiprocess module. Each sub process will one line
    :param input_data_path: input data path
    :param output_data_path: output data path
    :param process_number:  number of processed that will be generated
    :return None: no return
    """

    if not os.path.exists(output_data_path):
        os.makedirs(output_data_path)

    pool = mp.Pool(processes=process_number)
    for file_name in os.listdir(input_data_path):
        input_file_path = os.path.join(input_data_path, file_name)
        output_file_path = os.path.join(output_data_path, file_name)

        res = [pool.apply_async(process_line, args=(line,)) for line in open(input_file_path, "r")]

        # res = []
        # for line in open(input_file_path, "r"):
        #     res.append(pool.apply_async(process_line, args=(line,)).get())
        #
        open(output_file_path, "a+").write("\n".join(new_line.get() for new_line in res))

    pool.close()
    pool.join()


def process_with_no_multiprocess(input_data_path, output_data_path):
    """
    This method will process one input data with no multiprocess module.
    :param input_data_path: input data path
    :param output_data_path: output data path
    :return None: no return
    """
    if not os.path.exists(output_data_path):
        os.makedirs(output_data_path)

    for file_name in os.listdir(input_data_path):
        input_file_path = os.path.join(input_data_path, file_name)
        output_file_path = os.path.join(output_data_path, file_name)

        process_file(input_file_path, output_file_path)


if __name__ == '__main__':
    t0 = datetime.datetime.now()
    process_with_no_multiprocess(input_path, save_path_no_multiprocess)
    print(f"elapsed time with no multiprocess:{(datetime.datetime.now() - t0).seconds}")

    cpu_number = mp.cpu_count()

    t1 = datetime.datetime.now()
    process_with_multiprocess_line_level(input_path, save_path_multiprocess_line, cpu_number)
    print(f"elapsed time with {cpu_number} processes for line level:{(datetime.datetime.now() - t1).seconds}")

    t2 = datetime.datetime.now()
    process_with_multiprocess_file_level(input_path, save_path_multiprocess_file, cpu_number)
    print(f"elapsed time with {cpu_number} processes for file level:{(datetime.datetime.now() - t2).seconds}")

    t3 = datetime.datetime.now()
    process_with_multiprocess_file_level(input_path, save_path_multiprocess_file, cpu_number * 2)
    print(f"elapsed time with {cpu_number * 2} processes for file level:{(datetime.datetime.now() - t2).seconds}")
