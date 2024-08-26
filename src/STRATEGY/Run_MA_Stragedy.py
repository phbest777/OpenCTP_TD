"""
    行情API demo

    注意选择有效合约, 没有行情可能是过期合约或者不再交易时间内导致
"""
import asyncio
import inspect
import os
import sys
from pprint import pprint

import cx_Oracle
import random

# import src.md.test
# curPath = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
import websockets

sys.path.append('D:\PythonProject\OpenCTP_TD')
from openctp_ctp import mdapi

from src import config
from Gen_MA_Stragedy import OneMinuteTick


class CMdSpiImpl(mdapi.CThostFtdcMdSpi):
    bar_cache = {
        "InstrumentID": "",
        "UpdateTime": "99:99:99",
        "LastPrice": 0.00,
        "HighPrice": 0.00,
        "LowPrice": 0.0,
        "OpenPrice": 0.0,
        "BarVolume": 0,
        "BarTurnover": 0.0,
        "BarSettlement": 0.0,
        "BVolume": 0,
        "SVolume": 0,
        "FVolume": 0,
        "DayVolume": 0,
        "DayTurnover": 0.0,
        "DaySettlement": 0.0,
        "OpenInterest": 0.0,
        "TradingDay": "99999999",
    }
    sql2 = ''
    def __init__(self,
                 front: str,
                 user: str,
                 usercode: str,
                 passwd: str,
                 authcode: str,
                 appid: str,
                 broker_id: str,
                 conn_user: str,
                 conn_pass: str,
                 conn_db: str,
                 root_path: str,
                 ):
        print("-------------------------------- 启动 mduser api demo ")
        super().__init__()
        self._front = front
        self._usercode = usercode
        self._password = passwd
        self._authcode = authcode
        self._appid = appid
        self._broker_id = broker_id
        self._conn = cx_Oracle.connect(conn_user, conn_pass, conn_db)
        self._conn_cursor = self._conn.cursor()
        self._api = mdapi.CThostFtdcMdApi.CreateFtdcMdApi(
            #"D:\\PythonProject\\OpenCTP_TD\\src\\MD\\data\\"
            root_path
        )  # type: mdapi.CThostFtdcMdApi
        self._instruments=self._get_instrumnets()
        self.oneminutecls = OneMinuteTick(self._instruments,self._conn,self._conn_cursor)
        print("CTP行情API版本号:", self._api.GetApiVersion())
        print("行情前置:" + self._front)
        # 注册行情前置
        self._api.RegisterFront(self._front)
        # 注册行情回调实例
        self._api.RegisterSpi(self)
        # 初始化行情实例
        self._api.Init()
        print("初始化成功")


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

    def _db_select_rows_list(self,sqlstr:str)->list:
        self._conn_cursor.execute(sqlstr)
        columns = [col[0] for col in self._conn_cursor.description]
        rows = self._conn_cursor.fetchall()
        result_list = [dict(zip(columns, row)) for row in rows]
        #self._conn_cursor.close()
        return result_list
    def _get_list_bycolname(self,retlist:list,colname:str)->list:
        paralist = []
        for item in retlist:
            paralist.append(item.get(colname))
        return paralist
    def _get_instrumnets(self):
        sql="select * from QUANT_FUTURE_MA_INSTRUMNET where useflag='1'"
        ret_list=self._db_select_rows_list(sqlstr=sql)
        ret_instruments=self._get_list_bycolname(retlist=ret_list,colname="STD_INSTRUMENTID")
        return ret_instruments
    def OnFrontConnected(self):
        """行情前置连接成功"""
        print("行情前置连接成功")

        # 登录请求, 行情登录不进行信息校验
        print("登录请求")
        req = mdapi.CThostFtdcReqUserLoginField()
        req.BrokerID=config.broker_id
        req.UserID=config.user
        req.Password=config.password
        print("broke id is:"+req.BrokerID)
        print("broke user is:" +req.UserID)
        self._api.ReqUserLogin(req, 0)

    def OnRspUserLogin(
            self,
            pRspUserLogin: mdapi.CThostFtdcRspUserLoginField,
            pRspInfo: mdapi.CThostFtdcRspInfoField,
            nRequestID: int,
            bIsLast: bool,
    ):
        """登录响应"""
        if pRspInfo and pRspInfo.ErrorID != 0:
            print(f"登录失败: ErrorID={pRspInfo.ErrorID}, ErrorMsg={pRspInfo.ErrorMsg}")
            return

        print("登录成功")

        if len(self._instruments) == 0:
            return

        # 订阅行情
        print("订阅行情请求：", self._instruments)
        self._api.SubscribeMarketData(
            [i.encode("utf-8") for i in self._instruments], len(self._instruments)
        )

    def GetOneMinuteBar(self, pDepthMarketData: mdapi.CThostFtdcDepthMarketDataField):
        """
        self.bar_cache["InstrumentID"] = pDepthMarketData.InstrumentID
        self.bar_cache["UpdateTime"] = pDepthMarketData.UpdateTime
        self.bar_cache["LastPrice"] = pDepthMarketData.LastPrice
        if self.bar_cache["HighPrice"] <= pDepthMarketData.LastPrice and self.bar_cache[
            "InstrumentID"] == pDepthMarketData.InstrumentID:
            self.bar_cache["HighPrice"] = pDepthMarketData.LastPrice

        print("bar_cache is:")
        print(self.bar_cache)
        """
        # return self.oneminutecls.GetOneMinuteTick(pDepthMarketData)
        return self.oneminutecls.GetOneMinute(pDepthMarketData)

    def OnRtnDepthMarketData(
            self, pDepthMarketData: mdapi.CThostFtdcDepthMarketDataField
    ):
        """深度行情通知"""

        '''
        params = []
        for name, value in inspect.getmembers(pDepthMarketData):
            if name[0].isupper():
                params.append(f"{name}={value}")
        print("深度行情通知:", ",".join(params))
        '''
        '''
        print("InstrumentID:", pDepthMarketData.InstrumentID, " LastPrice:", pDepthMarketData.LastPrice,
              " Volume:", pDepthMarketData.Volume, " PreSettlementPrice:", pDepthMarketData.PreSettlementPrice,
              " PreClosePrice:", pDepthMarketData.PreClosePrice, " TradingDay:", pDepthMarketData.TradingDay)
        '''
        '''
        sql = "insert into QUANT_FUTURE_MD_TICKS (TRADINGDAY,INSTRUMENTID,EXCHANGEID,EXCHANGEINSTID,LASTPRICE,PRESETTLEMENTPRICE" \
              ",PRECLOSEPRICE,PREOPENINTEREST,OPENPRICE,HIGHESTPRICE,LOWESTPRICE,VOLUME,TURNOVER,OPENINTEREST,CLOSEPRICE" \
              ",SETTLEMENTPRICE,UPPERLIMITPRICE,LOWERLIMITPRICE,PREDELTA,CURRDELTA,UPDATETIME,UPDATEMILLISEC,BIDPRICE1" \
              ",BIDVOLUME1,ASKVOLUME1,BIDPRICE2,BIDVOLUME2,ASKVOLUME2,BIDPRICE3,BIDVOLUME3,ASKVOLUME3,BIDPRICE4,BIDVOLUME4,ASKVOLUME4" \
              ",BIDPRICE5,BIDVOLUME5,ASKVOLUME5,AVERAGEPRICE,ACTIONDAY,BANDINGUPPERPRICE,BANDINGLOWERPRICE,UPRATIO,INTERESTMINUS,INTERESTRATIO)values(" \
              "'" + pDepthMarketData.TradingDay + "','" + pDepthMarketData.InstrumentID + "','" + pDepthMarketData.ExchangeID + \
              "','" + pDepthMarketData.ExchangeInstID + "'," + str(pDepthMarketData.LastPrice) + "," + str(
            pDepthMarketData.PreSettlementPrice) + \
              "," + str(pDepthMarketData.PreClosePrice) + "," + str(pDepthMarketData.PreOpenInterest) + "," + str(
            pDepthMarketData.OpenPrice) + \
              "," + str(pDepthMarketData.HighestPrice) + "," + str(pDepthMarketData.LowestPrice) + "," + str(
            pDepthMarketData.Volume) + "," + str(pDepthMarketData.Turnover) + \
              "," + str(pDepthMarketData.OpenInterest) + "," + str(pDepthMarketData.ClosePrice)[:7] + "," + str(
            pDepthMarketData.SettlementPrice)[:7] + "," + str(pDepthMarketData.UpperLimitPrice) + \
              "," + str(pDepthMarketData.LowerLimitPrice) + "," + str(pDepthMarketData.PreDelta) + "," + str(
            pDepthMarketData.CurrDelta)[:7] + ",'" + str(pDepthMarketData.UpdateTime) + \
              "'," + str(pDepthMarketData.UpdateMillisec) + "," + str(pDepthMarketData.BidPrice1) + "," + str(
            pDepthMarketData.BidVolume1) + "," + str(pDepthMarketData.AskVolume1) + \
              "," + str(pDepthMarketData.BidPrice2)[:7] + "," + str(pDepthMarketData.BidVolume2) + "," + str(
            pDepthMarketData.AskVolume2) + \
              "," + str(pDepthMarketData.BidPrice3)[:7] + "," + str(pDepthMarketData.BidVolume3) + "," + str(
            pDepthMarketData.AskVolume3) + \
              "," + str(pDepthMarketData.BidPrice4)[:7] + "," + str(pDepthMarketData.BidVolume4) + "," + str(
            pDepthMarketData.AskVolume4) + \
              "," + str(pDepthMarketData.BidPrice5)[:7] + "," + str(pDepthMarketData.BidVolume5) + "," + str(
            pDepthMarketData.AskVolume5) + \
              "," + str(pDepthMarketData.AveragePrice) + ",'" + str(pDepthMarketData.ActionDay) + "'," + str(
            pDepthMarketData.BandingUpperPrice) + \
              "," + str(pDepthMarketData.BandingLowerPrice) + \
              "," + str(
            (pDepthMarketData.LastPrice - pDepthMarketData.PreSettlementPrice) / pDepthMarketData.PreSettlementPrice) + \
              "," + str(pDepthMarketData.OpenInterest - pDepthMarketData.PreOpenInterest) + \
              "," + str(
            (pDepthMarketData.OpenInterest - pDepthMarketData.PreOpenInterest) / pDepthMarketData.PreOpenInterest) + ")"
            '''
        sql2 = self.GetOneMinuteBar(pDepthMarketData)
        #filename = 'test.py'
        #exec(open(filename).read())
        # print("sql2 is:"+sql2)
        if sql2 != "ddd":
            #print("sqlstr is:" + sql2)
            #cursor.execute(sql2)
            #conn.commit()
            self._db_insert(sqlstr=sql2)
        # cursor.execute(sql2["return_str"])
        # conn.commit()

    def OnRspSubMarketData(
            self,
            pSpecificInstrument: mdapi.CThostFtdcSpecificInstrumentField,
            pRspInfo: mdapi.CThostFtdcRspInfoField,
            nRequestID: int,
            bIsLast: bool,
    ):
        """订阅行情响应"""
        if pRspInfo and pRspInfo.ErrorID != 0:
            print(
                f"订阅行情失败:ErrorID={pRspInfo.ErrorID}, ErrorMsg={pRspInfo.ErrorMsg}",
            )
            return

        print("订阅行情成功:", pSpecificInstrument.InstrumentID)

    def wait(self):
        # 阻塞 等待
        input("-------------------------------- 按任意键退出 mduser api demo ")

        self._api.Release()

if __name__ == "__main__":
    #instruments = ("SA409", "SH409", "FG409", "P409")
    user = config.user
    password = config.password
    authcode = config.authcode
    appid = config.appid
    brokerid = config.broker_id
    connuser = config.conn_user
    connpass = config.conn_pass
    conndb = config.conn_db
    rootpath = "D:\\PythonProject\\OpenCTP_TD\\src\\MD\\data\\"
    spi = CMdSpiImpl(config.fronts["电信1"]["md"],user=user,usercode='phbest',passwd=password,authcode=authcode,
                              appid=appid,broker_id=brokerid,conn_user=connuser,conn_pass=connpass,conn_db=conndb,
                              root_path=rootpath)

    # 注意选择有效合约, 没有行情可能是过期合约或者不再交易时间内导致
    spi.wait()

#def Exit_Proc():
#    sys.exit(0)