"""
Function:
Author: Du Fei
Create Time: 2020/5/31 11:08
"""

import numpy as np
import dask.array as da
from dask.dataframe.utils import make_meta

if __name__ == '__main__':
    source_array = np.random.randint(0, 10, (2, 4))
    index_array = np.asarray([[0, 0], [1, 0], [2, 1], [3, 2]])

    b = np.apply_along_axis(lambda a: a[index_array], 1, source_array)
    print(b)

    source_array = da.from_array(source_array)
    # b = da.apply_along_axis(lambda a: a[index_array], 1, source_array)

    res = da.apply_along_axis(lambda a: a[index_array], 1, source_array,
                              shape=make_meta(source_array).shape,
                              dtype=make_meta(source_array).dtype).compute()

    print(res)
