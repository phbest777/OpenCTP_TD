import importlib
import inspect
import queue
import time
import sys
import os
import datetime
import schedule
from src import config
import cx_Oracle
sys.path.append('D:\PythonProject\OpenCTP_TD')
sys.path.append('C:\DEVENV\Anaconda3\envs\CTPAPIDEV')
from openctp_ctp import tdapi
class trade_engin_comm():
    def __init__(self,conn_user: str,
                 conn_pass: str,
                 conn_db: str,):
        print("----------------初始化交易引擎----------- ")
        super().__init__()
        self._starttime1 = datetime.time(hour=20, minute=59, second=6)
        self._endtime1 = datetime.time(hour=23, minute=0, second=0)
        self._starttime2 = datetime.time(hour=8, minute=59, second=6)
        self._endtime2 = datetime.time(hour=10, minute=15, second=0)
        self._starttime3 = datetime.time(hour=10, minute=29, second=6)
        self._endtime3 = datetime.time(hour=11, minute=30, second=0)
        self._starttime4 = datetime.time(hour=13, minute=29, second=6)
        self._endtime4 = datetime.time(hour=14, minute=59, second=0)
        self._job=""
        self._conn_user = conn_user
        self._conn_pass = conn_pass
        self._conn_db = conn_db
        self._conn = cx_Oracle.connect(conn_user, conn_pass, conn_db)
        self._conn_cursor = self._conn.cursor()

    def _db_insert(self, sqlstr: str):
        self._conn_cursor.execute(sqlstr)
        self._conn.commit()
        print("[" + sqlstr + "]" + "写入数据库成功")

    def _db_update(self, sqlstr: str):
        self._conn_cursor.execute(sqlstr)
        self._conn.commit()
        print("[" + sqlstr + "]" + "更新数据库成功")

    def _db_select_rows(self, sqlstr: str) -> dict:
        ret_dict = {}
        self._conn_cursor.execute(sqlstr)
        columns = [col[0] for col in self._conn_cursor.description]
        rows = self._conn_cursor.fetchall()
        ret_dict['col_name'] = columns
        ret_dict['rows'] = rows
        return ret_dict

    def _db_select_cnt(self, sqlstr: str):
        self._conn_cursor.execute(sqlstr)
        rows = self._conn_cursor.fetchall()
        return rows[0][0]

    def _db_select_rows_list(self, sqlstr: str) -> list:
        self._conn_cursor.execute(sqlstr)
        columns = [col[0] for col in self._conn_cursor.description]
        rows = self._conn_cursor.fetchall()
        result_list = [dict(zip(columns, row)) for row in rows]
        # self._conn_cursor.close()
        return result_list
    def GetNextWorkDay(self, workday: str):
        sql = "select * from workday where code='0' and originday>='" + workday + "' and rownum<=2 order by id asc"
        day_list = self._db_select_rows_list(sqlstr=sql)
        workday = day_list[0].get("ORIGINDAY")
        nextworkday = day_list[1].get("ORIGINDAY")
        retdict = {}
        retdict["workday"] = workday
        retdict["nextworkday"] = nextworkday
        return retdict
    def getcurrdate(self):
        now = datetime.datetime.now()
        year = now.year
        month = now.month
        day = now.day
        temptime = datetime.datetime(year, month, day, 15, 00)  ##当天下午三点之后的交易算作第二天
        currenttime = datetime.datetime.today().strftime("%Y%m%d")
        if now > temptime:
            # currenttime=datetime.datetime.today()+datetime.timedelta(days=1)
            currenttime = self.GetNextWorkDay(currenttime).get("nextworkday")
        return currenttime

    def GetModuleName(self,userid:str):
        current_dir = os.getcwd()  # 获取当前目录路径
        parent_dir_1 = os.path.dirname(current_dir)  # 获取当前目录的上级目录路径
        parent_dir_2 = os.path.dirname(parent_dir_1)
        parent_dir_3 = os.path.dirname(parent_dir_2)
        parent_dir_1_name = os.path.basename(parent_dir_1)
        parent_dir_2_name = os.path.basename(parent_dir_2)
        parent_dir_3_name = os.path.basename(parent_dir_3)
        modulename = parent_dir_3_name + "." + parent_dir_2_name + "." + parent_dir_1_name + "." + "Trade_" + userid + ".trade_" + userid
        return modulename
    def Trade_Engin_Test(self):
        ordertime = datetime.datetime.now().strftime("%H:%M:%S")
        ordermin = datetime.datetime.now().strftime("%H:%M")
        tradingday = self.getcurrdate()  # 获取交易日
        sql="select * from QUANT_FUTURE_USER_TRADE where runflag='1' and userid='200231'"
        retdict=self._db_select_rows_list(sqlstr=sql)[0]
        userid=retdict.get("USERID")
        modelcode=retdict.get("MODELCODE")
        current_dir = os.getcwd()  # 获取当前目录路径
        modulename=self.GetModuleName(userid=userid)
        tradebf=importlib.import_module(modulename)
        #current_directory = os.getcwd()
        # 使用os.path.basename获取当前工作目录的文件夹名
        #directory_name = os.path.basename(current_directory)  # directort_name 就是investorid
        tradebf.test()
        #self.tradebf = importlib.import_module("trade_" + directory_name)  # 引入交易模块

    def GetModelSignal(self,modelcode:str,tradate:str,tratime:str):
        tramin=tratime[0:5]
        sql="select * from QUANT_FUTURE_MODEL_ROUTER where modelcode='"+modelcode+"' and orderdate='"+\
            tradate+"' and substr(ordertime,1,5)='"+tramin+"'"
        retlist=self._db_select_rows_list(sqlstr=sql)
        retdict={}
        if(len(retlist)==0):
            return retdict
        else:
            retdict=retlist[0]
            return retdict

    def Gen_Ave_Model_2(self):
        #print("job is runing----")
        for item in self._instrumentlist:
            self.Ave_Model_2_Engine(instrumentid=item.get("INSTRUMENTID"),exchangeid=item.get("EXCHANGEID"))
        time1 = datetime.datetime.now().time()
        print("time1 is：" + time1.strftime(("%H:%M:%S")))
        if (time1 > self._endtime2 and time1 < self._starttime3):
            print("job is stoping")
            schedule.cancel_job(self._job)
        elif (time1 > self._endtime3 and time1 < self._starttime4):
            print("job is stoping")
            schedule.cancel_job(self._job)
        elif (time1>self._endtime4 and time1<self._starttime1):
            self.End_Ave_Model_2()  ##每天下午15点收盘，平掉所有持仓单
            self._seqno = 1
            print("job is stoping")
            schedule.cancel_job(self._job)
        elif (time1 > self._endtime1):
            print("job is stoping")
            schedule.cancel_job(self._job)


    def Control_Ave_Mode_2(self):
        #print("Ave_Mode_1 is starting")
        self._job = schedule.every(1).minutes.do(self.Gen_Ave_Model_2)


if __name__ == "__main__":
    connuser = config.conn_user
    connpass = config.conn_pass
    conndb = config.conn_db
    trade_engin_test=trade_engin_comm(conn_user=connuser,conn_pass=connpass,conn_db=conndb)
    retdict=trade_engin_test.GetModelSignal(modelcode='AVE_MODEL_1',tradate='20240904',tratime='09:01:05')
