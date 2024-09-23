import inspect
import queue
import time
import sys
import subprocess
import datetime
sys.path.append('D:\PythonProject\OpenCTP_TD')
sys.path.append('C:\DEVENV\Anaconda3\envs\CTPAPIDEV')
from src import config
from src.TD.main import trade
import cx_Oracle

class TradeController():
    def __init__(self,
                 front: str,
                 user: str,
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

    #def Inverstor_Confirm(self):


if __name__ == "__main__":
    frontinfo=config.fronts["电信1"]["td"]
    user=config.user
    password=config.password
    authcode=config.authcode
    appid=config.appid
    brokerid=config.broker_id
    connuser=config.conn_user
    connpass=config.conn_pass
    conndb=config.conn_db
    rootpath=config.rootpath
    tradetype="006"
    rettype="Y"##返回类型：Y返回结果,N 不返回结果
    paraStr="CZCE,SA501,0,0,1,1380"
    cmd=['python','trade.py',frontinfo,user,password,authcode,appid,brokerid,connuser,connpass,conndb,rootpath,tradetype,rettype,paraStr]
    result=subprocess.run(cmd,capture_output=True,text=True,encoding='utf-8')
    print("result is:"+result.stdout)
    print("ddddddddddddddddddddddddddddd")
    #print(result.stdout.decode())
    #if result.stderr:
    #    print(result.stderr.decode())

    #trade.main(frontinfo,user,password,authcode,appid,brokerid,rootpath,tradetype)