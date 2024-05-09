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

def ret_format(retstr:str)->list:
    liststr=retstr.split(',')
    print ("str is:"+retstr)
    return liststr
if __name__ == "__main__":
    #print(sys.argv[1])
    #result=switch_case('002')
    #print(result)
    str='BrokerID=9999,ExchangeID=,InstrumentID=,InvestUnitID=,InvestorID=200231'
    list=ret_format(str)
    #list=[('BrokerID','9999'),('InvestorID','200231')]
    dic={key.strip():value.strip() for key,sep,value in (item.partition('=') for item in list)}
    #print("list is:"+list[0])
    print(dic.get('ExchangeID'))
    exit()