"""
Function:
Author: Du Fei
Create Time: 2020/5/16 22:32
"""

from datetime import datetime
from dask.distributed import LocalCluster, Client
import dask.dataframe as dd

if __name__ == '__main__':
    start_time = datetime.now()

    base_dir = r"F:\data\python_test\dask"

    input_1 = f"{base_dir}/input_1"
    input_2 = f"{base_dir}/input_2"

    output_dir = f"{base_dir}/output"

    cluster = LocalCluster(n_workers=4, threads_per_worker=1)
    client = Client(cluster)

    t_0 = datetime.now()
    df1 = dd.read_csv(input_1 + "/data_*.csv").set_index("id")
    df2 = dd.read_csv(input_2 + "/data_*.csv").set_index("id")
    print(f"elapsed time for reading:{(datetime.now() - t_0).seconds}")

    print(df1.divisions)

    print("----------df1 head-----------")
    print(df1.head())
    print("----------df2 head-----------")
    print(df2.head())

    df3 = df1.merge(df2, on="id", how="left")
    df3.to_csv(output_dir)

    print(f"elapsed time:{(datetime.now() - start_time).seconds}")
