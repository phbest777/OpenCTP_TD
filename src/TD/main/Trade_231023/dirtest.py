import os
import importlib
import cx_Oracle

import os

# 获取当前工作目录的完整路径
current_directory = os.getcwd()

# 使用os.path.basename获取当前工作目录的文件夹名
directory_name = os.path.basename(current_directory)

print(directory_name)


