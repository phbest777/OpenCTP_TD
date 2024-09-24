import importlib
import inspect
import queue
import time
import threading
import sys
import os
import datetime
import schedule
from src import config
import subprocess
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
        self._starttime1 = datetime.time(hour=22, minute=49, second=6)
        self._endtime1 = datetime.time(hour=23, minute=0, second=0)
        self._starttime2 = datetime.time(hour=8, minute=55, second=6)
        self._endtime2 = datetime.time(hour=10, minute=15, second=0)
        self._starttime3 = datetime.time(hour=10, minute=29, second=6)
        self._endtime3 = datetime.time(hour=11, minute=30, second=0)
        self._starttime4 = datetime.time(hour=17, minute=25, second=6)
        self._endtime4 = datetime.time(hour=17, minute=59, second=0)
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
        modulename = parent_dir_3_name + "." + parent_dir_2_name + "." + parent_dir_1_name + "." + "Trade_" + userid + ".controllerbf_" + userid
        return modulename
    def GetFileName(self,userid:str):
        current_dir = os.getcwd()  # 获取当前目录路径
        parent_dir_1 = os.path.dirname(current_dir)  # 获取当前目录的上级目录路径
        filename = parent_dir_1 + "\\" + "Trade_" + userid + "\\controllerbf_" + userid+".py"
        return filename

    def GetModelSignal(self,modelcode:str,tradate:str,tratime:str):
        tramin=tratime[0:5]
        sql="select * from QUANT_FUTURE_MODEL_ROUTER where modelcode='"+modelcode+"' and orderdate='"+\
            tradate+"' and substr(ordertime,1,5)='"+tramin+"' order by id asc"
        retlist=self._db_select_rows_list(sqlstr=sql)
        retdict={}
        if(len(retlist)==1): #根据订单性质返回交易码,一分钟仅存在一条记录
            retdict=retlist[0]
            #1-101仅开多单，-2-102仅开空单，-1-107仅平多单，2-108仅平空单
            retdict["tradetype"]= {1:"101",-2:"102",-1:"107",2:"108"}.get(retdict.get("ORDERDIRECTION"))
            return retdict
        elif(len(retlist)==2):#根据订单性质返回交易码,一分钟存在两条记录
            retdict=retlist[0]
            #根据ID第一条记录的性质，第一条记录是平仓单，-1-105，顺序为-1，-2表示平多单开空单；2-106，顺序为2，1表示平空单开多单
            retdict["tradetype"]={-1:"105",2:"106"}.get(retdict.get("ORDERDIRECTION"))
            return retdict
        else:
            return retdict
    def Trade_Engin_Test(self):
        #ordertime = datetime.datetime.now().strftime("%H:%M:%S")
        #ordermin = datetime.datetime.now().strftime("%H:%M")
        ordertime="14:52:00"
        #tradingday = self.getcurrdate()  # 获取交易日
        tradingday="20240920"
        sql="select * from QUANT_FUTURE_USER_TRADE where runflag='1' and userid='200231'"
        retdict=self._db_select_rows_list(sqlstr=sql)[0]
        userid=retdict.get("USERID")
        modelcode=retdict.get("MODELCODE")
        tradevol=int(retdict.get("TRADEVOL"))
        current_dir = os.getcwd()  # 获取当前目录路径
        modulename = self.GetModuleName(userid=userid)
        TradeCtl = importlib.import_module(modulename)
        signaldict=self.GetModelSignal(modelcode=modelcode,tradate=tradingday,tratime=ordertime)
        if(len(signaldict)!=0):
            tradedict={}
            tradedict["exchangeid"]=signaldict.get("EXCHANGEID")
            tradedict["instrumentid"]=signaldict.get("INSTRUMENTID")
            tradedict["volume"]=tradevol
            tradetype=signaldict.get("tradetype")
            TradeCtl.MainProc(conn_user=self._conn_user,conn_pass=self._conn_pass,conn_db=self._conn_db,trade_dict=tradedict,trade_type=tradetype)
            #TradeCtl.test()

        #tradebf=importlib.import_module(modulename)
        #current_directory = os.getcwd()
        # 使用os.path.basename获取当前工作目录的文件夹名
        #directory_name = os.path.basename(current_directory)  # directort_name 就是investorid
        #tradebf.test()
        #self.tradebf = importlib.import_module("trade_" + directory_name)  # 引入交易模块

    def Trade_Engine_Main(self,user_trade_dict:dict):
        ordertime = datetime.datetime.now().strftime("%H:%M:%S")
        # ordermin = datetime.datetime.now().strftime("%H:%M")
        #ordertime = "16:36:00"
        #tradingday = self.getcurrdate()  # 获取交易日
        tradingday = "20240923"
        userid = user_trade_dict.get("USERID")
        modelcode = user_trade_dict.get("MODELCODE")
        tradevol = int(user_trade_dict.get("TRADEVOL"))
        filename = self.GetFileName(userid=userid)#找到controller_userid 模块
        #TradeCtl = importlib.import_module(modulename)
        signaldict = self.GetModelSignal(modelcode=modelcode, tradate=tradingday, tratime=ordertime)#查找交易路由表
        if (len(signaldict) != 0):#不空该分钟有交易产生，调用CTP交易下单
            #tradedict = {}
            #tradedict["exchangeid"] = signaldict.get("EXCHANGEID")
            #tradedict["instrumentid"] = signaldict.get("INSTRUMENTID")
            #tradedict["volume"] = tradevol
            tradetype = signaldict.get("tradetype")
            paradictstr=signaldict.get("EXCHANGEID")+","+signaldict.get("INSTRUMENTID")+","+str(tradevol)
            cmd = ['python', filename,self._conn_user, self._conn_pass,self._conn_db, tradetype, paradictstr]
            result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8')
            print("result is:" + result.stdout)
        else:
            print("------无交易路由,无需下单------------")
    def Trade_Engine_Working(self):
        #print("job is runing----")
        #for item in self._instrumentlist:
        #    self.Ave_Model_2_Engine(instrumentid=item.get("INSTRUMENTID"),exchangeid=item.get("EXCHANGEID"))
        time1 = datetime.datetime.now().time()
        print("time1 is：" + time1.strftime(("%H:%M:%S")))
        sql = "select * from QUANT_FUTURE_USER_TRADE where runflag='1'"
        retlist = self._db_select_rows_list(sqlstr=sql)
        for item in retlist:
            self.Trade_Engine_Main(user_trade_dict=item)
            #time.sleep(3)
        if (time1 > self._endtime2 and time1 < self._starttime3):
            print("job is stoping")
            schedule.cancel_job(self._job)
        elif (time1 > self._endtime3 and time1 < self._starttime4):
            print("job is stoping")
            schedule.cancel_job(self._job)
        elif (time1>self._endtime4 and time1<self._starttime1):
            #self.End_Ave_Model_2()  ##每天下午15点收盘，平掉所有持仓单
            #self._seqno = 1
            print("job is stoping")
            schedule.cancel_job(self._job)
        elif (time1 > self._endtime1):
            print("job is stoping")
            schedule.cancel_job(self._job)


    def Trade_Engine_Start(self):
        #print("Ave_Mode_1 is starting")
        self._job = schedule.every(1).minutes.do(self.Trade_Engine_Working)


    def Trade_Engine_Run(self):
        print("--------交易引擎工作开始-----------")
        schedule.every().day.at(self._starttime1.strftime("%H:%M:%S")).do(self.Trade_Engine_Start)
        schedule.every().day.at(self._starttime2.strftime("%H:%M:%S")).do(self.Trade_Engine_Start)
        schedule.every().day.at(self._starttime3.strftime("%H:%M:%S")).do(self.Trade_Engine_Start)
        schedule.every().day.at(self._starttime4.strftime("%H:%M:%S")).do(self.Trade_Engine_Start)
        while True:
            schedule.run_pending()

    def Trade_Engine_Run_First(self):
        t=threading.Thread(target=self.Trade_Engine_Run())
        t.start()
        #while True:
         #schedule.run_pending()
    def Test(self):
        # 获取当前工作目录的完整路径
        #current_directory = os.path.abspath(os.path.dirname(__file__))
        current_directory=os.getcwd()
        # 使用os.path.basename获取当前工作目录的文件夹名
        user_id = os.path.basename(current_directory)  # directort_name 就是investorid
        filename=self.GetFileName("200231")
        print(filename)

if __name__ == "__main__":
    connuser = config.conn_user
    connpass = config.conn_pass
    conndb = config.conn_db
    trade_engin_test=trade_engin_comm(conn_user=connuser,conn_pass=connpass,conn_db=conndb)
    #usertradedict={"MODELCODE":"AVE_MODEL_2","USERCODE":"phbest777","USERID":"200231","TRADEVOL":1}
    #trade_engin_test.Trade_Engine_Main(user_trade_dict=usertradedict)
    #trade_engin_test.Trade_Engine_Working()
    trade_engin_test.Trade_Engine_Run_First()
    #retdict=trade_engin_test.GetModelSignal(modelcode='AVE_MODEL_1',tradate='20240904',tratime='09:01:05')
    #trade_engin_test.Test()
