import inspect
import queue
import time
import sys
import datetime
import cx_Oracle

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


def ret_format2(ret_list:list)->dict:
    ret_dict={key.strip():value.strip() for key,sep,value in (item.partition('=') for item in ret_list)}
    return ret_dict

def _get_login_ret_sql(retlist:list)->dict:
    login_ret_dict={}
    retdict=ret_format2(retlist)
    datadate=datetime.datetime.today().strftime("%Y%m%d")
    sql="insert into QUANT_FUTURE_CONFIRM(APPID,AUTHCODE,BROKERID,USERID,TRADINGDAY,CZCETIME,DCETIME,FFEXTIME,GFEXTIME,INETIME," \
        "SHFETIME,LOGINTIME,SESSIONID,SYSTEMNAME,CONFIRMSTATUS,CONFIRMDATE,CONFIRMTIME,DATADATE) values (" \
        "'simnow_client_test','0000000000000000','9999','200231','"+retdict.get('TradingDay')+"','"+retdict.get('CZCETime')+"','"+retdict.get('DCETime')+"','"\
        +retdict.get('FFEXTime')+"','"+retdict.get('GFEXTime')+"','"+retdict.get('INETime')+"','"+retdict.get('SHFETime')+"','"+retdict.get('LoginTime')+"','"+retdict.get('SessionID')+"','"+retdict.get('SystemName')+"','"\
        +""+"','"+""+"','"+""+"','"+datadate+"'"+")"
    login_ret_dict['SQL']=sql
    login_ret_dict['SESSIONID']=retdict.get('SessionID')
    #login_ret_list.append(sql)
    #login_ret_list.append(retdict.get('SessionID'))
    return login_ret_dict

def _get_confirm_ret_sql(retlist:list,sessionid:str,userid:str)->str:
    confirm_ret_dict=ret_format2(retlist)
    sql="update QUANT_FUTURE_CONFIRM set confirmstatus='"+confirm_ret_dict.get('RetCode')+\
        "',confirmdate='"+confirm_ret_dict.get('ConfirmDate')+"',confirmtime='"+confirm_ret_dict.get('ConfirmTime')+\
        "' where sessionid='"+sessionid+"' and userid='"+userid+"'"
    return sql

def _db_execute(sql:str):
    conn = cx_Oracle.connect('user_ph', 'ph', '127.0.1.1:1521/orclpdb')
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()
    print("------完成数据库操作-------")

if __name__ == "__main__":
    #print(sys.argv[1])
    #result=switch_case('002')
    #print(result)
    user_id='200231'
    str='RetCode=000,RetMsg=响应成功,CZCETime=11:23:46,DCETime=11:23:46,FFEXTime=11:23:46,FrontID=1,GFEXTime=11:23:46,INETime=11:23:46,LoginTime=11:23:46,MaxOrderRef=1,SHFETime=11:23:46,SessionID=1103697589,SysVersion=v6.7.3_XC_20240401 10:12:45.6905.tkernel,SystemName=TradingHosting,TradingDay=20240509,UserID=200231'
    list=ret_format(str)
    str2='RetCode=000,RetMsg=响应成功,AccountID=,BrokerID=9999,ConfirmDate=20240509,ConfirmTime=11:23:46,CurrencyID=,InvestorID=200231,SettlementID=0'
    uplist=ret_format(str2)
    #list=[('BrokerID','9999'),('InvestorID','200231')]
    #dic={key.strip():value.strip() for key,sep,value in (item.partition('=') for item in list)}
    retdict=_get_login_ret_sql(retlist=list)
    sql=retdict['SQL']
    sessionid=retdict['SESSIONID']
    print("execute sql is:"+sql)
    _db_execute(sql)
    print("sessionid is:"+sessionid)
    print("更新数据")
    upsql=_get_confirm_ret_sql(retlist=uplist,sessionid=sessionid,userid=user_id)
    _db_execute(upsql)
    #print("list is:"+list[0])
    #print(dic.get('RetCode'))
    exit()