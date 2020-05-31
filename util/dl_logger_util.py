"""
Function:
Author: Du Fei
Create Time: 2020/5/31 11:50
"""
import logging


def get_logger():
    logger = logging.getLogger()
    logger.setLevel('DEBUG')
    basic_format = "%(asctime)s:%(levelname)s:%(message)s"
    date_format = '%Y-%m-%d %H:%M:%S'
    formatter = logging.Formatter(basic_format, date_format)
    chlr = logging.StreamHandler()  # 输出到控制台的handler
    chlr.setFormatter(formatter)
    chlr.setLevel('DEBUG')  # 也可以不设置，不设置就默认用logger的level
    # fhlr = logging.FileHandler(f"logs/{log_file}", encoding="utf-8")  # 输出到文件的handler
    # fhlr.setFormatter(formatter)
    logger.addHandler(chlr)
    # logger.addHandler(fhlr)
    return logger


if __name__ == '__main__':
    debug_logger = get_logger()
