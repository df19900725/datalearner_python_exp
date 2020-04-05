"""
Function: This script will provide compares results of different ways for loading and writing large data
Author: Du Fei
Create Time: 2020/4/5 21:46
"""

import os
import pickle
import numpy as np
import pandas as pd
import datetime

data_dir = "f:/data/python_test/big_data_comparison"


def generate_data(n_rows, n_columns):
    sample_values = np.random.randint(100, 1000000, (n_rows, n_columns))
    row_names = ["row_" + str(x).zfill(10) for x in range(n_rows)]
    column_names = ["column_" + str(x).zfill(3) for x in range(n_columns)]

    return sample_values, pd.DataFrame(sample_values, index=row_names, columns=column_names)


def np_save_text(file_name, input_data: np.ndarray):
    file_path = os.path.join(data_dir, file_name)
    if os.path.exists(file_path):
        os.remove(file_path)

    np.savetxt(file_path, input_data)


def np_save_npy(file_name, input_data: np.ndarray):
    file_path = os.path.join(data_dir, file_name)
    if os.path.exists(file_path):
        os.remove(file_path)

    np.save(file_path, input_data)


def pd_to_csv(file_name, input_data: pd.DataFrame):
    file_path = os.path.join(data_dir, file_name)
    if os.path.exists(file_path):
        os.remove(file_path)

    input_data.to_csv(file_path)


def pd_to_hdf5(file_name, input_data: pd.DataFrame):
    file_path = os.path.join(data_dir, file_name)
    if os.path.exists(file_path):
        os.remove(file_path)

    input_data.to_hdf(file_path, key='df', mode='w')


def pd_to_parquet(file_name, input_data: pd.DataFrame, engine):
    file_path = os.path.join(data_dir, file_name)
    if os.path.exists(file_path):
        os.remove(file_path)

    input_data.to_parquet(file_path, compression=None, engine=engine)


def pickle_to_file(file_name, input_data):
    file_path = os.path.join(data_dir, file_name)
    if os.path.exists(file_path):
        os.remove(file_path)

    pickle.dump(input_data, open(file_name, "wb"))


def write_comparison(input_numpy_data, input_df):
    t_0 = datetime.datetime.now()
    # np_save_text("np.array.txt", input_numpy_data)
    print(f"numpy save txt:{(datetime.datetime.now() - t_0).seconds} seconds")

    t_0 = datetime.datetime.now()
    np_save_npy("np.array", input_df)
    print(f"numpy save npy:{(datetime.datetime.now() - t_0).seconds} seconds")

    t_0 = datetime.datetime.now()
    # pd_to_csv("pd.data_frame.csv", input_df)
    print(f"pd save csv:{(datetime.datetime.now() - t_0).seconds} seconds")

    t_0 = datetime.datetime.now()
    pd_to_hdf5("pd.data_frame.h5", input_df)
    print(f"pd save hdf5:{(datetime.datetime.now() - t_0).seconds} seconds")

    t_0 = datetime.datetime.now()
    pd_to_parquet("pd.data_frame_fastparquet.parquet", input_df, "fastparquet")
    print(f"pd save parquet with fastparquet engine:{(datetime.datetime.now() - t_0).seconds} seconds")

    t_0 = datetime.datetime.now()
    pd_to_parquet("pd.data_frame_pyarrow.parquet", input_df, "pyarrow")
    print(f"pd save parquet with pyarrow engine:{(datetime.datetime.now() - t_0).seconds} seconds")

    t_0 = datetime.datetime.now()
    pickle_to_file("pickle.pkl", input_df)
    print(f"pickle save pkl:{(datetime.datetime.now() - t_0).seconds} seconds")


if __name__ == '__main__':

    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    raw_data, raw_df = generate_data(10000000, 100)

    write_comparison(raw_data, raw_df)
