import inspect
import queue
import time
import sys
import subprocess
import datetime
sys.path.append('D:\PythonProject\OpenCTP_TD')
sys.path.append('C:\DEVENV\Anaconda3\envs\CTPAPIDEV')
from src import config
from src.TD.main import tradebf
from src.TD.main import trade_test
import cx_Oracle

class TradeController():
    def __init__(self,
                 front: str,
                 user: str,
                 usercode:str,
                 passwd: str,
                 authcode: str,
                 appid: str,
                 broker_id: str,
                 conn_user: str,
                 conn_pass: str,
                 conn_db: str,
                 root_path: str,
    ):
        print("----------------初始化交易模块----------- ")
        super().__init__()
        self._front = front
        self._user = user
        self._usercode=usercode
        self._password = passwd
        self._authcode = authcode
        self._appid = appid
        self._broker_id = broker_id
        self._conn_user=conn_user
        self._conn_pass=conn_pass
        self._conn_db=conn_db
        self._root_path = root_path
        self._datadate = datetime.datetime.today().strftime("%Y%m%d")
        self._datatime = datetime.datetime.now().strftime("%H:%M:%S")
        self._conn = cx_Oracle.connect(conn_user, conn_pass, conn_db)
        self._conn_cursor = self._conn.cursor()
        '''初始化交易参数'''


    def Run_Proc(self,pythonfile:str,tradetype:str,rettype:str,parastr:str):
        cmd=['python',pythonfile,self._front,self._user,self._password,
             self._authcode,self._appid,self._broker_id,self._conn_user,self._conn_pass,
             self._conn_db,self._root_path,tradetype,rettype,parastr]
        result=subprocess.run(cmd,capture_output=False,text=True)
        return result.stdout
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

    def getcurrdate(self):
        now=datetime.datetime.now()
        year=now.year
        month=now.month
        day=now.day
        temptime=datetime.datetime(year,month,day,15,00)##当天下午三点之后的交易算作第二天
        currenttime=datetime.datetime.today()
        if now>temptime:
            currenttime=datetime.datetime.today()+datetime.timedelta(days=1)
        return currenttime.strftime("%Y%m%d")

    def Inverstor_Confirm(self):
        self._spi = tradebf.InitProc(frontinfo=self._front, user=self._user, usercode=self._usercode,
                                     password=self._password,authcode=self._authcode, appid=self._appid,
                                     brokerid=self._broker_id,connuser=self._conn_user, connpass=self._conn_pass,
                                     conndb=self._conn_db,tradetype='001',rootpath=self._root_path)
        trandate = self.getcurrdate()
        sqlstr = "select count(*) from QUANT_FUTURE_CONFIRM where tradingday='" + trandate + "'"
        confirm_cnt = self._db_select_cnt(sqlstr=sqlstr)
        if (int(confirm_cnt) > 0):
            print("交易日:[" + trandate + "]确认单已确认")
            exit()
        else:
            ret=tradebf.MainProc(spi=self._spi,TradeType='001',RetType='Y',ParaList=[])
        return ret

    def Position_Update(self):
        self._spi = tradebf.InitProc(frontinfo=self._front, user=self._user, usercode=self._usercode,
                                     password=self._password, authcode=self._authcode, appid=self._appid,
                                     brokerid=self._broker_id, connuser=self._conn_user, connpass=self._conn_pass,
                                     conndb=self._conn_db, tradetype='002', rootpath=self._root_path)
        ret=tradebf.MainProc(spi=self._spi,TradeType='002',RetType='Y',ParaList=[])
        return ret

    def Order_Insert(self,parastr:str):
        self._spi = tradebf.InitProc(frontinfo=self._front, user=self._user, usercode=self._usercode,
                                     password=self._password, authcode=self._authcode, appid=self._appid,
                                     brokerid=self._broker_id, connuser=self._conn_user, connpass=self._conn_pass,
                                     conndb=self._conn_db, tradetype='016', rootpath=self._root_path)
        paralist=parastr.split(',')
        lastprice=tradebf.MainProc(spi=self._spi,TradeType='016',RetType='Y',ParaList=paralist)
        print('last price is')
        print(str(lastprice))
        return lastprice





if __name__ == "__main__":
    frontinfo = config.fronts["电信1"]["td"]
    user = config.user
    password = config.password
    authcode = config.authcode
    appid = config.appid
    brokerid = config.broker_id
    connuser = config.conn_user
    connpass = config.conn_pass
    conndb = config.conn_db
    rootpath = config.rootpath
    traderCtl=TradeController(front=frontinfo,user=user,usercode='phbest',passwd=password,authcode=authcode,
                              appid=appid,broker_id=brokerid,conn_user=connuser,conn_pass=connpass,conn_db=conndb,
                              root_path=rootpath)
    #ret=traderCtl.Inverstor_Confirm()
    #ret=traderCtl.Position_Update()
    ret=traderCtl.Order_Insert('SA409')
    print(ret)
    #trade_test.mainproc()
    #print("ret str is:"+ret)
    #tradetype = "001"
    #rettype = "Y"  ##返回类型：Y返回结果,N 不返回结果
    #paraStr = "CZCE,SA409,0,0,1,1850"
    '''
    ret=tradebf.MainProc(frontinfo=frontinfo,user=user,password=password,authcode=authcode,
                         appid=appid,brokerid=brokerid,connuser=connuser,connpass=connpass,
                         conndb=conndb,rootpath=rootpath,tradertype=tradetype,rettype=rettype,
                         paralist=paraStr)
    '''

    #trade.main(frontinfo,user,password,authcode,appid,brokerid,rootpath,tradetype)