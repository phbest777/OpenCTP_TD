import os
import importlib
import time

import cx_Oracle
import threading
import queue
import os

'''
# 获取当前工作目录的完整路径
current_directory = os.getcwd()

# 使用os.path.basename获取当前工作目录的文件夹名
directory_name = os.path.basename(current_directory)

print(directory_name)
'''


def producer(q):
    for i in range(5):
        q.put(i)


def consumer(q):
    while not q.empty():
        item = q.get()
        print("Consumed:", item)


# 创建队列
q = queue.Queue()

# 创建生产者和消费者线程
producer_thread = threading.Thread(target=producer, args=(q,))
consumer_thread = threading.Thread(target=consumer, args=(q,))

# 启动线程
producer_thread.start()
consumer_thread.start()

# 等待线程执行完成
producer_thread.join()
consumer_thread.join()
