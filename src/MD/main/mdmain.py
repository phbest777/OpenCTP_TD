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
from src import config
import random

# import src.md.test
# curPath = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
import websockets

sys.path.append('D:\PythonProject\OpenCTP_TD')
from openctp_ctp import mdapi

from src import config
#from TickDataStruct import OneMinuteTick


class CMdSpiImpl(mdapi.CThostFtdcMdSpi):
    def __init__(self,
                 front: str,
                 conn_user: str,
                 conn_pass: str,
                 conn_db: str,):
        print("-------------------------------- 启动 mduser api demo ")
        super().__init__()
        self._conn = cx_Oracle.connect(conn_user, conn_pass, conn_db)
        self._conn_cursor = self._conn.cursor()
        self._front = front

        self._api = mdapi.CThostFtdcMdApi.CreateFtdcMdApi(
            "D:\\PythonProject\\OpenCTP_TD\\src\\MD\\data\\"
        )  # type: mdapi.CThostFtdcMdApi
        print("CTP行情API版本号:", self._api.GetApiVersion())
        print("行情前置:" + self._front)
        # 注册行情前置
        self._api.RegisterFront(self._front)
        # 注册行情回调实例
        self._api.RegisterSpi(self)
        # 初始化行情实例
        self._api.Init()
        print("初始化成功")

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

        if len(instruments) == 0:
            return

        # 订阅行情
        print("订阅行情请求：", instruments)
        self._api.SubscribeMarketData(
            [i.encode("utf-8") for i in instruments], len(instruments)
        )


    def OnRtnDepthMarketData(
            self, pDepthMarketData: mdapi.CThostFtdcDepthMarketDataField
    ):
        """深度行情通知"""


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

    def _db_select_rows(self, sqlstr: str) -> dict:
        ret_dict = {}
        self._conn_cursor.execute(sqlstr)
        columns = [col[0] for col in self._conn_cursor.description]
        rows = self._conn_cursor.fetchall()
        ret_dict['col_name'] = columns
        ret_dict['rows'] = rows
        return ret_dict


if __name__ == "__main__":
    instruments = ("SA409", "SH409", "FG409", "P409")
    spi = CMdSpiImpl(config.fronts["电信2"]["md"],
                     config.conn_user,
                     config.conn_pass,
                     config.conn_db,
                     )
    # 注意选择有效合约, 没有行情可能是过期合约或者不再交易时间内导致

    conn = cx_Oracle.connect('user_ph', 'ph', '127.0.1.1:1521/orclpdb')
    cursor = conn.cursor()
    print('连接数据库成功！')
    spi.wait()


