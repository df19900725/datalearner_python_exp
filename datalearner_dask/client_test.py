"""
Function: Examples of init client for dask local cluster and distributed clusters.
Author: Du Fei
Create Time: 2020/5/31 17:11
"""

from dask.distributed import LocalCluster, Client


def init_local_cluster(n_workers=6):
    cluster = LocalCluster(n_workers=n_workers, threads_per_worker=1)
    client = Client(cluster)
    return client


def init_distribute_cluster():
    client = Client("127.0.0.1:8786")
    return client


if __name__ == '__main__':
    init_local_cluster()
