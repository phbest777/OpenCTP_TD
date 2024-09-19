import importlib
import inspect
import queue
import time
import sys
import subprocess
import datetime
import os
sys.path.append('D:\PythonProject\OpenCTP_TD')
sys.path.append('C:\DEVENV\Anaconda3\envs\CTPAPIDEV')
from src import config
import cx_Oracle


class TradeController():
    def __init__(self,
                 conn_user: str,
                 conn_pass: str,
                 conn_db: str,
    ):
        print("----------------初始化交易模块----------- ")
        super().__init__()
        self._conn_user = conn_user
        self._conn_pass = conn_pass
        self._conn_db = conn_db
        self._conn = cx_Oracle.connect(conn_user, conn_pass, conn_db)
        self._conn_cursor = self._conn.cursor()
        # 获取当前工作目录的完整路径
        current_directory = os.getcwd()
        # 使用os.path.basename获取当前工作目录的文件夹名
        directory_name = os.path.basename(current_directory).split('_')[1]#directort_name 就是investorid
        self.tradebf = importlib.import_module("trade_"+directory_name)#引入交易模块
        self._paradict=self._db_select_rows_list(sqlstr="select * from QUANT_FUTURE_USERINFO where investorid='"+directory_name+"'")[0]
        front={}
        #front["td"]=self._paradict["TDPROC"]
        #front["md"]=self._paradict["MDPROC"]
        front["td"] = self._paradict["TDTEST"]
        front["md"] = self._paradict["MDTEST"]
        self._front = front["td"]
        self._user = self._paradict["INVESTORID"]
        self._usercode=self._paradict["USERCODE"]
        self._password = self._paradict["INVESTORPASS"]
        self._authcode = self._paradict["AUTHCODE"]
        self._appid = self._paradict["APPID"]
        self._broker_id = self._paradict["BROKERID"]
        self._root_path = self._paradict["DATAPATH"]
        self._datadate = datetime.datetime.today().strftime("%Y%m%d")
        self._datatime = datetime.datetime.now().strftime("%H:%M:%S")


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

    def Inverstor_Confirm(self):#返回_login_session_id
        self._spi = self.tradebf.InitProc(frontinfo=self._front, user=self._user, usercode=self._usercode,
                                     password=self._password,authcode=self._authcode, appid=self._appid,
                                     brokerid=self._broker_id,connuser=self._conn_user, connpass=self._conn_pass,
                                     conndb=self._conn_db,tradetype='001',rootpath=self._root_path)
        ret=self.tradebf.MainProc(spi=self._spi,TradeType='001',RetType='Y',ParaDict={})

    def Position_Update(self):#返回_login_session_id
        self._spi = self.tradebf.InitProc(frontinfo=self._front, user=self._user, usercode=self._usercode,
                                     password=self._password, authcode=self._authcode, appid=self._appid,
                                     brokerid=self._broker_id, connuser=self._conn_user, connpass=self._conn_pass,
                                     conndb=self._conn_db, tradetype='002', rootpath=self._root_path)
        ret=self.tradebf.MainProc(spi=self._spi,TradeType='002',RetType='Y',ParaDict={})
        return ret
    def Trading_Account_Update(self):
        self._spi = self.tradebf.InitProc(frontinfo=self._front, user=self._user, usercode=self._usercode,
                                          password=self._password, authcode=self._authcode, appid=self._appid,
                                          brokerid=self._broker_id, connuser=self._conn_user, connpass=self._conn_pass,
                                          conndb=self._conn_db, tradetype='015', rootpath=self._root_path)
        self.tradebf.MainProc(spi=self._spi, TradeType='015', RetType='Y', ParaDict={})
    def Qry_Instrument(self):
        self._spi = self.tradebf.InitProc(frontinfo=self._front, user=self._user, usercode=self._usercode,
                                     password=self._password, authcode=self._authcode, appid=self._appid,
                                     brokerid=self._broker_id, connuser=self._conn_user, connpass=self._conn_pass,
                                     conndb=self._conn_db, tradetype='003', rootpath=self._root_path)
        self.tradebf.MainProc(spi=self._spi,TradeType='003',RetType='Y',ParaDict=[])

    def Qry_Lastprice(self,paradict:dict):
        self._spi = self.tradebf.InitProc(frontinfo=self._front, user=self._user, usercode=self._usercode,
                                     password=self._password, authcode=self._authcode, appid=self._appid,
                                     brokerid=self._broker_id, connuser=self._conn_user, connpass=self._conn_pass,
                                     conndb=self._conn_db, tradetype='016', rootpath=self._root_path)
        lastprice=self.tradebf.MainProc(spi=self._spi,TradeType='016',RetType='Y',ParaDict=paradict)
        print('last price is')
        print(str(lastprice))
        return lastprice

    def Order_Insert_Market(self,paradict:dict):
        self._spi = self.tradebf.InitProc(frontinfo=self._front, user=self._user, usercode=self._usercode,
                                     password=self._password, authcode=self._authcode, appid=self._appid,
                                     brokerid=self._broker_id, connuser=self._conn_user, connpass=self._conn_pass,
                                     conndb=self._conn_db, tradetype='006', rootpath=self._root_path)
        retdict=self.tradebf.MainProc(spi=self._spi,TradeType='006',RetType='Y',ParaDict=paradict)
        #tradebf.MainProc(spi=self._spi,TradeType='002',RetType='Y',ParaList=[])
        return retdict

    def Order_Cancel(self,paradict:dict):
        self._spi = self.tradebf.InitProc(frontinfo=self._front, user=self._user, usercode=self._usercode,
                                     password=self._password, authcode=self._authcode, appid=self._appid,
                                     brokerid=self._broker_id, connuser=self._conn_user, connpass=self._conn_pass,
                                     conndb=self._conn_db, tradetype='008', rootpath=self._root_path)
        self.tradebf.MainProc(spi=self._spi,TradeType='008',RetType='Y',ParaDict=paradict)



    def Order_Cancel_Batch(self):
        sql="select EXCHANGEID,INSTRUMENTID,ORDERSYSID from " \
            "QUANT_FUTURE_ORDER_REQ where tradestatus='0' and orderdate='"+self._datadate+"' and investorid='"+self._user+"'"
        retlist=self._db_select_rows_list(sqlstr=sql)
        self._spi = self.tradebf.InitProc(frontinfo=self._front, user=self._user, usercode=self._usercode,
                                     password=self._password, authcode=self._authcode, appid=self._appid,
                                     brokerid=self._broker_id, connuser=self._conn_user, connpass=self._conn_pass,
                                     conndb=self._conn_db, tradetype='008', rootpath=self._root_path)
        for item in retlist:
            paradict={}
            paradict["exchangeid"]=item.get("EXCHANGEID")
            paradict["instrumentid"]=item.get("INSTRUMENTID")
            paradict["ordersysid"]=item.get("ORDERSYSID")
            #paralist.append(item[retdict['col_name'].index('EXCHANGEID')])
            #paralist.append(item[retdict['col_name'].index('INSTRUMENTID')])
            #paralist.append(item[retdict['col_name'].index('ORDERSYSID')])
            self.tradebf.MainProc(spi=self._spi,TradeType='008',RetType='Y',ParaDict=paradict)

        ret='000'
        return ret

    def OpenForLongOnly(self,paradict:dict):#开多单
        #paralist=parastr.split(',')
        orderdict={}
        orderdict["exchangeid"]=paradict.get("exchangeid")
        orderdict["instrumentid"]=paradict.get("instrumentid")
        orderdict["volume"]=paradict.get("volume")
        self._spi = self.tradebf.InitProc(frontinfo=self._front, user=self._user, usercode=self._usercode,
                                          password=self._password, authcode=self._authcode, appid=self._appid,
                                          brokerid=self._broker_id, connuser=self._conn_user, connpass=self._conn_pass,
                                          conndb=self._conn_db, tradetype='101', rootpath=self._root_path)
        self.tradebf.MainProc(spi=self._spi, TradeType='101', RetType='Y', ParaDict=orderdict)


    def OpenForShortOnly(self,paradict:dict):
        #trandate = self.getcurrdate()
        orderdict = {}
        orderdict["exchangeid"] = paradict.get("exchangeid")
        orderdict["instrumentid"] = paradict.get("instrumentid")
        orderdict["volume"] = paradict.get("volume")
        self._spi = self.tradebf.InitProc(frontinfo=self._front, user=self._user, usercode=self._usercode,
                                          password=self._password, authcode=self._authcode, appid=self._appid,
                                          brokerid=self._broker_id, connuser=self._conn_user, connpass=self._conn_pass,
                                          conndb=self._conn_db, tradetype='102', rootpath=self._root_path)
        self.tradebf.MainProc(spi=self._spi, TradeType='102', RetType='Y', ParaDict=orderdict)

    def LongToShort(self,paradict:dict):
        orderdict = {}
        orderdict["exchangeid"] = paradict.get("exchangeid")
        orderdict["instrumentid"] = paradict.get("instrumentid")
        orderdict["volume"] = paradict.get("volume")
        self._spi = self.tradebf.InitProc(frontinfo=self._front, user=self._user, usercode=self._usercode,
                                          password=self._password, authcode=self._authcode, appid=self._appid,
                                          brokerid=self._broker_id, connuser=self._conn_user, connpass=self._conn_pass,
                                          conndb=self._conn_db, tradetype='105', rootpath=self._root_path)
        self.tradebf.MainProc(spi=self._spi, TradeType='105', RetType='Y', ParaDict=orderdict)

    def ShortToLong(self,paradict:dict):
        orderdict = {}
        orderdict["exchangeid"] = paradict.get("exchangeid")
        orderdict["instrumentid"] = paradict.get("instrumentid")
        orderdict["volume"] = paradict.get("volume")
        self._spi = self.tradebf.InitProc(frontinfo=self._front, user=self._user, usercode=self._usercode,
                                          password=self._password, authcode=self._authcode, appid=self._appid,
                                          brokerid=self._broker_id, connuser=self._conn_user, connpass=self._conn_pass,
                                          conndb=self._conn_db, tradetype='106', rootpath=self._root_path)
        self.tradebf.MainProc(spi=self._spi, TradeType='106', RetType='Y', ParaDict=orderdict)

    def CloseForLongOnly(self,paradict:dict):
        orderdict = {}
        orderdict["exchangeid"] = paradict.get("exchangeid")
        orderdict["instrumentid"] = paradict.get("instrumentid")
        orderdict["volume"] = paradict.get("volume")
        self._spi = self.tradebf.InitProc(frontinfo=self._front, user=self._user, usercode=self._usercode,
                                          password=self._password, authcode=self._authcode, appid=self._appid,
                                          brokerid=self._broker_id, connuser=self._conn_user, connpass=self._conn_pass,
                                          conndb=self._conn_db, tradetype='107', rootpath=self._root_path)
        self.tradebf.MainProc(spi=self._spi, TradeType='107', RetType='Y', ParaDict=orderdict)

    def CloseForShortOnly(self,paradict:dict):
        orderdict = {}
        orderdict["exchangeid"] = paradict.get("exchangeid")
        orderdict["instrumentid"] = paradict.get("instrumentid")
        orderdict["volume"] = paradict.get("volume")
        self._spi = self.tradebf.InitProc(frontinfo=self._front, user=self._user, usercode=self._usercode,
                                          password=self._password, authcode=self._authcode, appid=self._appid,
                                          brokerid=self._broker_id, connuser=self._conn_user, connpass=self._conn_pass,
                                          conndb=self._conn_db, tradetype='108', rootpath=self._root_path)
        self.tradebf.MainProc(spi=self._spi, TradeType='108', RetType='Y', ParaDict=orderdict)


if __name__ == "__main__":
    connuser = config.conn_user
    connpass = config.conn_pass
    conndb = config.conn_db
    traderCtl=TradeController(conn_user=connuser,conn_pass=connpass,conn_db=conndb)
    tradedict={"exchangeid":"DCE","instrumentid":"p2501","volume":1}
    #tradedict = {"exchangeid": "CZCE", "instrumentid": "SA501", "volume": 5,"buysellflag":"0","trantype":"0","price":1405.0}
    traderCtl.OpenForLongOnly(tradedict)
    #traderCtl.LongToShort(tradedict)
    #traderCtl.CloseForShortOnly(tradedict)
    #traderCtl.Qry_Instrument()
    #ret=traderCtl.Inverstor_Confirm()
    #ret=traderCtl.Position_Update()
    #ret_lastprice=traderCtl.Qry_Lastprice('DCE,p2501')
    #print(ret_lastprice)
    #retdict=traderCtl.Order_Insert_Market(paradict=tradedict)
    #traderCtl.Order_Cancel("DCE,p2409,      649638")
    #traderCtl.Order_Cancel_Batch()
    #traderCtl.Position_Update()
    #traderCtl.Trading_Account_Update()
    #print("sessionid is:"+retdict.get('SESSIONID'))
    #print("ordersysid is:"+retdict.get('ORDERSYSID'))
    #print(ret)
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