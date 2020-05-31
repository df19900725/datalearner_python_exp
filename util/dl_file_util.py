"""
Function:
Author: Du Fei
Create Time: 2020/5/31 11:45
"""

import os
import shutil
import logging


def get_logger():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    return logging


def check_dir_exist(input_path, create=False, logger=None):
    if logger is None:
        logger = get_logger()

    if os.path.exists(input_path):
        return True
    elif create:
        os.makedirs(input_path)
        return True
    else:
        logger.info(f"Input path does not exist and param of create is False!")
        return False


def clear_folder(input_path, file_only=False, logger=None):
    if logger is None:
        logger = get_logger()

    if os.path.exists(input_path):
        if os.path.isdir(input_path):
            for filename in os.listdir(input_path):
                filepath = os.path.join(input_path, filename)

                if not os.path.isdir(filepath):
                    os.remove(filepath)
                else:
                    if file_only:
                        logger.info(f"{filepath} is a directory and param of file_only is True!")
                    else:
                        shutil.rmtree(filepath)

        else:
            logger.warning(f"Input path is not a directory! Input path:{input_path}")
    else:
        logger.warning(f"Input directory does not exist! Input path:{input_path}")


if __name__ == '__main__':
    clear_folder_test_input = r"F:\data\python_test\clear_folder_test\test.txt"
    clear_folder(clear_folder_test_input, file_only=True)

    check_dir_exist(clear_folder_test_input, create=False)
