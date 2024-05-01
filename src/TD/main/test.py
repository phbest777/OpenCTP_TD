import inspect
import queue
import time
import sys


def switch_case(case):
    switch_dict={
        '001':'处理1',
        '002':'处理2',
    }
    return switch_dict.get(case,'默认处理')
if __name__ == "__main__":
    #print(sys.argv[1])
    result=switch_case('002')
    print(result)