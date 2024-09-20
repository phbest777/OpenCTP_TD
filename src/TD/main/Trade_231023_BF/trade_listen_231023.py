"""
    交易API demo
"""

import inspect
import queue
import time
import sys
import os
import datetime
import cx_Oracle
sys.path.append('D:\PythonProject\OpenCTP_TD')
sys.path.append('C:\DEVENV\Anaconda3\envs\CTPAPIDEV')
from openctp_ctp import tdapi
#from match import match
from src import config


class CTdSpiImpl(tdapi.CThostFtdcTraderSpi):
    """交易回调实现类"""

    def __init__(
        self,
        conn_user: str,
        conn_pass: str,
        conn_db: str,
    ):
        print("-------------------------------- 启动 trader api demo ")
        super().__init__()
        self._conn_user = conn_user
        self._conn_pass = conn_pass
        self._conn_db = conn_db
        self._conn = cx_Oracle.connect(conn_user, conn_pass, conn_db)
        self._conn_cursor = self._conn.cursor()
        # 获取当前工作目录的完整路径
        current_directory = os.getcwd()
        # 使用os.path.basename获取当前工作目录的文件夹名
        directory_name = os.path.basename(current_directory)  # directort_name 就是investorid
        self._paradict = self._db_select_rows_list(sqlstr="select * from QUANT_FUTURE_USERINFO where investorid='" + directory_name + "'")[0]
        front = {}
        front["td"] = self._paradict["TDPROC"]
        front["md"] = self._paradict["MDPROC"]
        self._front = front["td"]
        self._user = self._paradict["INVESTORID"]
        self._usercode = self._paradict["USERCODE"]
        self._password = self._paradict["INVESTORPASS"]
        self._authcode = self._paradict["AUTHCODE"]
        self._appid = self._paradict["APPID"]
        self._broker_id = self._paradict["BROKERID"]
        self._root_path = self._paradict["DATAPATH"]
        self._trantype="999"
        self._login_session_id = ''
        self._is_authenticate = False
        self._is_login = False
        self._datadate = datetime.datetime.today().strftime("%Y%m%d")
        self._datatime = datetime.datetime.now().strftime("%H:%M:%S")
        self._is_last = True
        self._print_max = 20000
        self._print_count = 0
        self._total = 0
        self._lastprice=0.0
        self._ordersysid=''
        self._wait_queue = queue.Queue(2)
        ####如果当天存在用户目录直接创建实例，如果不存在则创建当天文件目录后再创建实例###############
        save_path=self._root_path+"\\"+self.getcurrdate()+"\\"+self._user
        if(os.path.exists(save_path)):
            self._api: tdapi.CThostFtdcTraderApi = (
                tdapi.CThostFtdcTraderApi.CreateFtdcTraderApi(save_path+"\\"+self._user)
        )
        else:
            os.makedirs(save_path)
            self._api: tdapi.CThostFtdcTraderApi = (
                tdapi.CThostFtdcTraderApi.CreateFtdcTraderApi(save_path+"\\"+self._user)
            )
        self._conn = cx_Oracle.connect(conn_user, conn_pass, conn_db)
        self._conn_cursor = self._conn.cursor()
        print("CTP交易API版本号:", self._api.GetApiVersion())
        print("交易前置:" + self._front)

        # 注册交易前置
        self._api.RegisterFront(self._front)
        # 注册交易回调实例
        self._api.RegisterSpi(self)
        # 订阅私有流
        self._api.SubscribePrivateTopic(tdapi.THOST_TERT_QUICK)
        # 订阅公有流
        self._api.SubscribePublicTopic(tdapi.THOST_TERT_QUICK)
        # 初始化交易实例
        self._api.Init()
        print("初始化成功")

    def ret_format(self, ret_list: list) -> dict:
        ret_dict = {key.strip(): value for key, sep, value in (item.partition('=') for item in ret_list)}
        return ret_dict
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
    @property
    def is_login(self):
        return self._is_login

    @property
    def get_sessionid(self):
        return self._login_session_id
    @property
    def get_lastprice(self):
        return self._lastprice
    def release(self):
        # 释放实例
        self._api.Release()

    def _check_req(self, req, ret: int):
        """检查请求"""

        # 打印请求
        params = []
        for name, value in inspect.getmembers(req):
            if name[0].isupper():
                params.append(f"{name}={value}")
        self.print("发送请求:", ",".join(params))

        # 检查请求结果
        error = {
            0: "",
            -1: "网络连接失败",
            -2: "未处理请求超过许可数",
            -3: "每秒发送请求数超过许可数",
        }.get(ret, "未知错误")
        if ret != 0:
            self.print(f"请求失败: {ret}={error}")


    def _check_rsp(
            self, pRspInfo: tdapi.CThostFtdcRspInfoField, rsp=None, is_last: bool = True
    ) -> bool:
        """检查响应

        True: 成功 False: 失败
        """

        if self._is_last:
            if pRspInfo and pRspInfo.ErrorID != 0:
                self.print(
                    f"响应失败, ErrorID={pRspInfo.ErrorID}, ErrorMsg={pRspInfo.ErrorMsg}"
                )
                return False

            self.print("响应成功")
            if rsp:
                params = []
                for name, value in inspect.getmembers(rsp):
                    if name[0].isupper():
                        params.append(f"{name}={value}")
                self.print("响应内容:", ",".join(params))
            else:
                self.print("响应为空")

            if not is_last:
                self._print_count += 1
                self._total += 1
            else:
                if self._is_login:
                    self._wait_queue.put_nowait(None)

        else:
            if self._print_count < self._print_max:
                if rsp:
                    params = []
                    for name, value in inspect.getmembers(rsp):
                        if name[0].isupper():
                            params.append(f"{name}={value}")
                    self.print("     ", ",".join(params))

                self._print_count += 1

            self._total += 1

            if is_last:
                self.print("总计数量:", self._total, "打印数量:", self._print_count)

                self._print_count = 0
                self._total = 0

                if self._is_login:
                    self._wait_queue.put_nowait(None)

        self._is_last = is_last

        return True

    def _check_rsp_ret(
            self, pRspInfo: tdapi.CThostFtdcRspInfoField, rsp=None, is_last: bool = True
    ) -> list:
        """检查响应

        True: 成功 False: 失败
        """
        retlist = []
        if self._is_last:
            if pRspInfo and pRspInfo.ErrorID != 0:
                self.print(
                    f"响应失败, ErrorID={pRspInfo.ErrorID}, ErrorMsg={pRspInfo.ErrorMsg}"
                )
                return retlist.append(f"{pRspInfo.ErrorID}={pRspInfo.ErrorMsg}")
            self.print("响应成功")
            retlist.append((f"{'RetCode'}={'000'}"))
            retlist.append((f"{'RetMsg'}={'响应成功'}"))
            if rsp:
                for name, value in inspect.getmembers(rsp):
                    if name[0].isupper():
                        retlist.append(f"{name}={value}")
                self.print("响应内容:", ",".join(retlist))
            else:
                self.print("响应为空")
                retlist.append(f"{'RetContent'}={'响应为空'}")
                return retlist

            if not is_last:
                self._print_count += 1
                self._total += 1
            else:
                if self._is_login:
                    self._wait_queue.put_nowait(None)

        else:
            if self._print_count < self._print_max:
                if rsp:
                    retlist.append((f"{'RetCode'}={'000'}"))
                    retlist.append((f"{'RetMsg'}={'响应成功'}"))
                    for name, value in inspect.getmembers(rsp):
                        if name[0].isupper():
                            retlist.append(f"{name}={value}")
                    self.print("     ", ",".join(retlist))

                self._print_count += 1
            self._total += 1

            if is_last:
                self.print("总计数量:", self._total, "打印数量:", self._print_count)

                self._print_count = 0
                self._total = 0

                if self._is_login:
                    self._wait_queue.put_nowait(None)

        self._is_last = is_last

        return retlist



    @staticmethod
    def print_rsp_rtn(prefix, rsp_rtn) -> list:
        if rsp_rtn:
            params = []
            for name, value in inspect.getmembers(rsp_rtn):
                if name[0].isupper():
                    params.append(f"{name}={value}")
            print(">", prefix, ",".join(params))
        return params

    @staticmethod
    def print(*args, **kwargs):
        print("    ", *args, **kwargs)


    def OnFrontConnected(self):
        """交易前置连接成功"""
        print("交易前置连接成功")

        self.authenticate()

    def OnFrontDisconnected(self, nReason: int):
        """交易前置连接断开"""
        print("交易前置连接断开: nReason=", nReason)

        # todo 可以在这里定义交易连接断开时的逻辑

    def authenticate(self):
        """认证 demo"""
        print("> 认证")
        _req = tdapi.CThostFtdcReqAuthenticateField()
        _req.BrokerID = self._broker_id
        _req.UserID = self._user
        _req.AppID = self._appid
        _req.AuthCode = self._authcode
        self._check_req(_req, self._api.ReqAuthenticate(_req, 0))

    def OnRspAuthenticate(
            self,
            pRspAuthenticateField: tdapi.CThostFtdcRspAuthenticateField,
            pRspInfo: tdapi.CThostFtdcRspInfoField,
            nRequestID: int,
            bIsLast: bool,
    ):
        """客户端认证响应"""
        # if not self._check_rsp(pRspInfo, pRspAuthenticateField):
        #    return
        retlist = self._check_rsp_ret(pRspInfo, pRspAuthenticateField)
        if (retlist[0]).split('=')[1] != '000':
            # print("登录失败")
            return
        else:
            self._is_authenticate = True
            # 登录
            self.login()

    def login(self):
        """登录 demo"""
        print("> 登录")

        _req = tdapi.CThostFtdcReqUserLoginField()
        _req.BrokerID = self._broker_id
        _req.UserID = self._user
        _req.Password = self._password
        if sys.platform == "darwin":
            self._check_req(_req, self._api.ReqUserLogin(_req, 0, 0, ""))
        else:
            self._check_req(_req, self._api.ReqUserLogin(_req, 0))
        # exit()

    def _get_login_ret_sql(self, ret_list: list) -> dict:
        login_ret_dict = {}
        retdict = self.ret_format(ret_list)
        #datadate = datetime.datetime.today().strftime("%Y%m%d")
        sql = "insert into QUANT_FUTURE_TRADE_LOGIN(APPID,AUTHCODE,BROKERID,USERID,USERCODE,TRADINGDAY,CZCETIME,DCETIME,FFEXTIME,GFEXTIME,INETIME," \
              "SHFETIME,LOGINTIME,SESSIONID,SYSTEMNAME,TRANCODE,DATADATE) values (" \
              "'" + self._appid + "','" + self._authcode + "','" + self._broker_id + "','" + self._user + "','" + self._usercode + "','" + retdict.get(
            'TradingDay') + "','" + retdict.get('CZCETime') + "','" + retdict.get('DCETime') + "','" \
              + retdict.get('FFEXTime') + "','" + retdict.get('GFEXTime') + "','" + retdict.get(
            'INETime') + "','" + retdict.get('SHFETime') + "','" + retdict.get('LoginTime') + "','" + retdict.get(
            'SessionID') + "','" + retdict.get('SystemName') + "','" \
              + self._trantype + "','" + self._datadate + "'" + ")"
        login_ret_dict['SQL'] = sql
        login_ret_dict['SESSIONID'] = retdict.get('SessionID')
        return login_ret_dict


    def _get_confirm_ret_sql(self, ret_list: list) -> dict:
        login_ret_dict = {}
        retdict = self.ret_format(ret_list)
        #datadate = datetime.datetime.today().strftime("%Y%m%d")
        sql = "insert into QUANT_FUTURE_CONFIRM(APPID,AUTHCODE,BROKERID,USERID,TRADINGDAY,CZCETIME,DCETIME,FFEXTIME,GFEXTIME,INETIME," \
              "SHFETIME,LOGINTIME,SESSIONID,SYSTEMNAME,CONFIRMSTATUS,CONFIRMDATE,CONFIRMTIME,DATADATE) values (" \
              "'" + self._appid + "','" + self._authcode + "','" + self._broker_id + "','" + self._user + "','" + retdict.get(
            'TradingDay') + "','" + retdict.get('CZCETime') + "','" + retdict.get('DCETime') + "','" \
              + retdict.get('FFEXTime') + "','" + retdict.get('GFEXTime') + "','" + retdict.get(
            'INETime') + "','" + retdict.get('SHFETime') + "','" + retdict.get('LoginTime') + "','" + retdict.get(
            'SessionID') + "','" + retdict.get('SystemName') + "','" \
              + "" + "','" + "" + "','" + "" + "','" + self._datadate + "'" + ")"
        login_ret_dict['SQL'] = sql
        login_ret_dict['SESSIONID'] = retdict.get('SessionID')
        return login_ret_dict

    def OnRspUserLogin(
            self,
            pRspUserLogin: tdapi.CThostFtdcRspUserLoginField,
            pRspInfo: tdapi.CThostFtdcRspInfoField,
            nRequestID: int,
            bIsLast: bool,
    ):
        """登录响应"""
        '''
        print("订阅行情请求：", self._instruments)
        self._api.SubscribeMarketData(
            [i.encode("utf-8") for i in self._instruments], len(self._instruments)
        )
        '''
        retlist = self._check_rsp_ret(pRspInfo, pRspUserLogin)
        ##记录每次登录信息，获取sessionid,用于追踪整个交易链##
        login_ret_dict = self._get_login_ret_sql(ret_list=retlist)
        login_ret_sql = login_ret_dict['SQL']
        self._db_insert(login_ret_sql)
        self._login_session_id = login_ret_dict['SESSIONID']
        if (retlist[0]).split('=')[1] == '000':
            print("登录成功")
            ##根据交易类型写不同的表#######
        else:
            return
        self._is_login = True




    def qry_instrument(
            self, exchange_id: str = "", product_id: str = "", instrument_id: str = ""
    ):
        """请求查询合约"""
        print("> 请求查询合约")
        _req = tdapi.CThostFtdcQryInstrumentField()
        # 填空可以查询到所有合约
        # 也可分别根据交易所、品种、合约 三个字段查询指定的合约
        _req.ExchangeID = exchange_id
        _req.ProductID = product_id
        _req.InstrumentID = instrument_id
        self._db_insert(sqlstr="truncate table QUANT_FUTURE_INSTRUMENT")
        self._check_req(_req, self._api.ReqQryInstrument(_req, 0))

    def _get_instrument_sql(self, instrument_ret_dict: dict) -> dict:
        instrument_dict = {}
        retdict = instrument_ret_dict
        #datadate = datetime.datetime.today().strftime("%Y%m%d")
        #datatime = datetime.datetime.now().strftime("%H:%M:%S")
        sql = "insert into QUANT_FUTURE_INSTRUMENT (EXCHANGEID,INSTRUMENTNAME,PRODUCTCLASS,DELIVERYYEAR,DELIVERYMONTH,MAXMARKETORDERVOLUME" \
              ",MINMARKETORDERVOLUME,MAXLIMITORDERVOLUME,MINLIMITORDERVOLUME,VOLUMEMULTIPLE,PRICETICK,CREATEDATE,OPENDATE,EXPIREDATE,STARTDELIVDATE" \
              ",ENDDELIVDATE,INSTLIFEPHASE,ISTRADING,POSITIONTYPE,POSITIONDATETYPE,LONGMARGINRATIO,SHORTMARGINRATIO,MAXMARGINSIDEALGORITHM" \
              ",STRIKEPRICE,OPTIONSTYPE,UNDERLYINGMULTIPLE,COMBINATIONTYPE,INSTRUMENTID,EXCHANGEINSTID,PRODUCTID,UNDERLYINGINSTRID)values(" \
              "'" + str(retdict.get('ExchangeID')) + "','" + str(retdict.get('InstrumentName')) + "','" + str(retdict.get('ProductClass')) + \
              "','" + str(retdict.get('DeliveryYear')) + "'," + "lpad('" + str(retdict.get('DeliveryMonth')) + "',2," + "'0')" + "," + str(retdict.get('MaxMarketOrderVolume')) +\
              "," +str(retdict.get('MinMarketOrderVolume')) + "," + str(retdict.get('MaxLimitOrderVolume')) + "," + str(retdict.get('MinLimitOrderVolume')) +\
              "," +retdict.get('VolumeMultiple') + "," + str(retdict.get('PriceTick')) + ",'" + str(retdict.get('CreateDate')) + \
              "','" + retdict.get('OpenDate') + "','" + retdict.get('ExpireDate') + "','" + str(retdict.get('StartDelivDate')) + \
              "','" + str(retdict.get('EndDelivDate')) + "','" + str(retdict.get('InstLifePhase')) + "','" + str(retdict.get('IsTrading')) + \
              "','" + str(retdict.get('PositionType')) + "','" + str(retdict.get('PositionDateType')) + "'" + \
              "," + str(retdict.get('LongMarginRatio'))[:7] + "," + str(retdict.get('ShortMarginRatio'))[:7] + ",'" + str(retdict.get('MaxMarginSideAlgorithm')) + \
              "'," + str(retdict.get('StrikePrice'))[:7]+ ",'" + str(retdict.get('OptionsType')) + "'," + str(retdict.get('UnderlyingMultiple'))[:2] + \
              ",'" + str(retdict.get('CombinationType')) + "','" + str(retdict.get('InstrumentID')) + "','" + retdict.get('ExchangeInstID') + \
              "','" + str(retdict.get('ProductID')) + "','" + str(retdict.get('UnderlyingInstrID')) +"'" + ")"
        print('instrument_sql is:' + sql)
        instrument_dict['SQL'] = sql
        #instrument_dict['SESSIONID'] = retdict.get('SessionID')
        return instrument_dict




    def OnRspOrderInsert(
            self,
            pInputOrder: tdapi.CThostFtdcInputOrderField,
            pRspInfo: tdapi.CThostFtdcRspInfoField,
            nRequestID: int,
            bIsLast: bool,
    ):
        """报单录入请求响应"""
        # self._check_rsp(pRspInfo, pInputOrder, bIsLast)
        retlist = self._check_rsp_ret(pRspInfo, pInputOrder, bIsLast)



    def OnRspOrderAction(
            self,
            pInputOrderAction: tdapi.CThostFtdcInputOrderActionField,
            pRspInfo: tdapi.CThostFtdcRspInfoField,
            nRequestID: int,
            bIsLast: bool,
    ):
        """报单操作请求响应"""
        # self._check_rsp(pRspInfo, pInputOrderAction, bIsLast)
        retlist = self._check_rsp_ret(pRspInfo, pInputOrderAction, bIsLast)
        exit()

    def _get_order_ret_sql(self, order_dict: dict) -> dict:
        order_ret_dict = {}
        retdict = order_dict
        # datadate = datetime.datetime.today().strftime("%Y%m%d")
        sql = "insert into QUANT_FUTURE_ORDER_RET(USERCODE,ACCOUNTID,ACTIVETIME,ACTIVETRADERID,ACTIVEUSERID,BRANCHID,BROKERID,BROKERORDERSEQ,BUSINESSUNIT," \
              "CANCELTIME,CLEARINGPARTID,CLIENTID,COMBHEDGEFLAG,COMBOFFSETFLAG,CONTINGENTCONDITION,CURRENCYID,DIRECTION,EXCHANGEID,EXCHANGEINSTID,FORCECLOSEREASON," \
              "FRONTID,IPADDRESS,INSERTDATE,INSERTTIME,INSTALLID,INSTRUMENTID,INVESTUNITID,INVESTORID,ISAUTOSUSPEND,ISSWAPORDER,LIMITPRICE," \
              "MACADDRESS,MINVOLUME,NOTIFYSEQUENCE,ORDERLOCALID,ORDERPRICETYPE,ORDERREF,ORDERSOURCE,ORDERSTATUS,ORDERSUBMITSTATUS,ORDERSYSID,ORDERTYPE," \
              "PARTICIPANTID,RELATIVEORDERSYSID,REQUESTID,SEQUENCENO,SESSIONID,SETTLEMENTID,STATUSMSG,STOPPRICE,SUSPENDTIME,TIMECONDITION,TRADERID," \
              "TRADINGDAY,UPDATETIME,USERFORCECLOSE,USERID,USERPRODUCTINFO,VOLUMECONDITION,VOLUMETOTAL,VOLUMETOTALORIGINAL,VOLUMETRADED,ZCETOTALTRADEDVOLUME," \
              "DATADATE) values (" \
              "'" + self._usercode + "','" + str(retdict.get('AccountID')) + "','" + str(
            retdict.get('ActiveTime')) + "','" + str(retdict.get('ActiveTraderID')) + \
              "','" + str(retdict.get('ActiveUserID')) + "','" + str(retdict.get('BranchID')) + "','" + str(
            retdict.get('BrokerID')) + "','" + str(retdict.get('BrokerOrderSeq')) + \
              "','" + str(retdict.get('BusinessUnit')) + "','" + str(retdict.get('CancelTime')) + "','" + retdict.get(
            'ClearingPartID') + "','" + str(retdict.get('ClientID')) + \
              "','" + str(retdict.get('CombHedgeFlag')) + "','" + retdict.get('CombOffsetFlag') + "','" + retdict.get(
            'ContingentCondition') + "','" + str(retdict.get('CurrencyID')) + "','" + str(retdict.get('Direction')) + \
              "','" + retdict.get('ExchangeID') + "','" + retdict.get('ExchangeInstID') + "','" + str(
            retdict.get('ForceCloseReason')) + "','" + str(retdict.get('FrontID')) + \
              "','" + str(retdict.get('IPAddress')) + "','" + retdict.get('InsertDate') + "','" + retdict.get(
            'InsertTime') + "','" + str(retdict.get('InstallID')) + \
              "','" + str(retdict.get('InstrumentID')) + "','" + str(retdict.get('InvestUnitID')) + "','" + str(
            retdict.get('InvestorID')) + "','" + str(retdict.get('IsAutoSuspend')) + \
              "','" + str(retdict.get('IsSwapOrder')) + "'," + str(retdict.get('LimitPrice')) + ",'" + str(
            retdict.get('MacAddress')) + "'," + str(retdict.get('MinVolume')) + \
              ",'" + str(retdict.get('NotifySequence')) + "','" + retdict.get('OrderLocalID') + "','" + str(
            retdict.get('OrderPriceType')) + "','" + retdict.get('OrderRef') + \
              "','" + str(retdict.get('OrderSource')) + "','" + str(retdict.get('OrderStatus')) + "','" + str(
            retdict.get('OrderSubmitStatus')) + "','" + retdict.get('OrderSysID') + \
              "','" + str(retdict.get('OrderType')) + "','" + str(retdict.get('ParticipantID')) + "','" + str(
            retdict.get('RelativeOrderSysID')) + "','" + str(retdict.get('RequestID')) + \
              "','" + str(retdict.get('SequenceNo')) + "','" + str(retdict.get('SessionID')) + "','" + str(
            retdict.get('SettlementID')) + "','" + str(retdict.get('StatusMsg')) + \
              "'," + str(retdict.get('StopPrice')) + ",'" + str(retdict.get('SuspendTime')) + "','" + str(
            retdict.get('TimeCondition')) + "','" + str(retdict.get('TraderID')) + \
              "','" + str(retdict.get('TradingDay')) + "','" + str(retdict.get('UpdateTime')) + "','" + str(
            retdict.get('UserForceClose')) + "','" + str(retdict.get('UserID')) + \
              "','" + str(retdict.get('UserProductInfo')) + "','" + str(retdict.get('VolumeCondition')) + "'," + str(
            retdict.get('VolumeTotal')) + "," + str(retdict.get('VolumeTotalOriginal')) + \
              "," + str(retdict.get('VolumeTraded')) + "," + str(
            retdict.get('ZCETotalTradedVolume')) + ",'" + self._datadate + "'" + ")"
        print('tempsql is:' + sql)
        order_ret_dict['SQL'] = sql
        order_ret_dict['SESSIONID'] = retdict.get('SessionID')
        return order_ret_dict

    def _update_order_req_sql(self, order_dict: dict) -> dict:
        order_update_req_dict = {}
        update_sql = "update QUANT_FUTURE_ORDER_REQ "
        order_sessionid = str(order_dict.get('SessionID'))
        order_usercode = self._usercode
        order_investorid = str(order_dict.get('InvestorID'))
        order_frontid = str(order_dict.get('FrontID'))
        order_orderref = str(order_dict.get('OrderRef'))
        order_ordersysid = str(order_dict.get('OrderSysID'))
        order_orderstatus = str(order_dict.get('OrderStatus'))
        order_statusmsg = str(order_dict.get('StatusMsg'))
        if (order_orderstatus == 'a') and (order_ordersysid != ''):
            update_sql += "set frontid='" + order_frontid + "',ordersysid='" + order_ordersysid + "',tradestatus='0',orderref='" + \
                          order_orderref + "',orderdate='" + self._datadate + "',ordertime='" + self._datatime + "' where usercode='" + order_usercode + \
                          "' and investorid='" + order_investorid + "' and sessionid='" + order_sessionid + "'"
        if (order_orderstatus == '0'):
            update_sql += "set tradestatus='1',uptdate='" + self._datadate + "',upttime='" + self._datatime + "' where usercode='" + order_usercode + \
                          "' and investorid='" + order_investorid + "' and ordersysid='" + order_ordersysid + "'"
        if (order_orderstatus == '3') and (order_ordersysid != ''):
            update_sql += "set frontid='" + order_frontid + "',ordersysid='" + order_ordersysid + "',tradestatus='0',orderref='" + order_orderref + \
                          "', orderdate='" + self._datadate + "',ordertime='" + self._datatime + "' where usercode='" + order_usercode + \
                          "' and investorid='" + order_investorid + "' and sessionid='" + order_sessionid + "'"
        if (order_orderstatus == '5') and (order_ordersysid != ''):
            update_sql += "set tradestatus='2',uptdate='" + self._datadate + "',upttime='" + self._datatime + "' where usercode='" + order_usercode + \
                          "' and investorid='" + order_investorid + "' and ordersysid='" + order_ordersysid + "'"
        if (order_orderstatus == '5') and (order_ordersysid == None):
            update_sql += "set tradestatus='2',uptdate='" + self._datadate + "',upttime='" + self._datatime + "' where usercode='" + order_usercode + \
                          "' and investorid='" + order_investorid + "' and sessionid='" + order_sessionid + "'"

        order_update_req_dict['SQL'] = update_sql
        order_update_req_dict['SESSIONID'] = self._login_session_id
        print('order_req_upsql is:' + update_sql)
        return order_update_req_dict

    def OnRtnOrder(self, pOrder: tdapi.CThostFtdcOrderField):
        """报单通知，当执行ReqOrderInsert后并且报出后，收到返回则调用此接口，私有流回报。"""
        retlist = self.print_rsp_rtn("报单通知", pOrder)
        order_ret_dic = self.ret_format(ret_list=retlist)
        order_sql_dic = self._get_order_ret_sql(order_dict=order_ret_dic)
        sql = order_sql_dic['SQL']
        self._db_insert(sql)
        ##更新报单请求表order_req
        if (str(order_ret_dic.get('OrderSysID')) != ''):
            order_up_req_dic = self._update_order_req_sql(order_dict=order_ret_dic)
            upsql = order_up_req_dic['SQL']
            self._db_update(sqlstr=upsql)
            self._ordersysid=order_ret_dic.get('OrderSysID')
        # print("order sql is:"+sql)
        # print("dic is:" + order_ret_dic['OrderLocalID'])
        # time.sleep(5)
        # exit()
        # self.release()

    def _get_order_deal_sql(self, order_dict: dict) -> dict:
        order_deal_dict = {}
        retdict = order_dict
        # datadate = datetime.datetime.today().strftime("%Y%m%d")
        sql = "insert into QUANT_FUTURE_TRADE_DEAL(USERCODE,BROKERID,BROKERORDERSEQ,BUSINESSUNIT,CLEARINGPARTID,CLIENTID,DIRECTION,EXCHANGEID,EXCHANGEINSTID," \
              "HEDGEFLAG,INSTRUMENTID,INVESTUNITID,INVESTORID,OFFSETFLAG,ORDERLOCALID,ORDERREF,ORDERSYSID,PARTICIPANTID,PRICE,PRICESOURCE," \
              "SEQUENCENO,SETTLEMENTID,TRADEDATE,TRADEID,TRADESOURCE,TRADETIME,TRADETYPE,TRADERID,TRADINGDAY,TRADINGROLE,USERID," \
              "VOLUME,STATUS,DATADATE) values (" \
              "'" + self._usercode + "','" + str(retdict.get('BrokerID')) + "','" + str(
            retdict.get('BrokerOrderSeq')) + "','" + str(retdict.get('BusinessUnit')) + \
              "','" + str(retdict.get('ClearingPartID')) + "','" + str(retdict.get('ClientID')) + "','" + str(
            retdict.get('Direction')) + "','" + str(retdict.get('ExchangeID')) + \
              "','" + str(retdict.get('ExchangeInstID')) + "','" + str(retdict.get('HedgeFlag')) + "','" + retdict.get(
            'InstrumentID') + "','" + str(retdict.get('InvestUnitID')) + \
              "','" + str(retdict.get('InvestorID')) + "','" + str(retdict.get('OffsetFlag')) + "','" + retdict.get(
            'OrderLocalID') + "','" + str(retdict.get('OrderRef')) + "','" + str(retdict.get('OrderSysID')) + \
              "','" + retdict.get('ParticipantID') + "'," + retdict.get('Price') + ",'" + str(
            retdict.get('PriceSource')) + "','" + str(retdict.get('SequenceNo')) + \
              "','" + str(retdict.get('SettlementID')) + "','" + retdict.get('TradeDate') + "','" + str(
            retdict.get('TradeID')) + "','" + str(retdict.get('TradeSource')) + \
              "','" + str(retdict.get('TradeTime')) + "','" + str(retdict.get('TradeType')) + "','" + str(
            retdict.get('TraderID')) + "','" + str(retdict.get('TradingDay')) + \
              "','" + str(retdict.get('TradingRole')) + "'," + str(retdict.get('UserID')) + "," + retdict.get(
            'Volume') + ",'1','" + self._datadate + "'" + ")"
        print('tempsql is:' + sql)
        order_deal_dict['SQL'] = sql
        order_deal_dict['SESSIONID'] = retdict.get('SessionID')
        return order_deal_dict

    def OnRtnTrade(self, pTrade: tdapi.CThostFtdcTradeField):
        """成交通知，报单发出后有成交则通过此接口返回。私有流"""
        retlist = self.print_rsp_rtn("成交通知", pTrade)
        order_deal_dic = self.ret_format(ret_list=retlist)
        order_sql_dic = self._get_order_deal_sql(order_dict=order_deal_dic)
        sql = order_sql_dic['SQL']
        self._db_insert(sql)
        #exit()
        # self.release()

    def OnErrRtnOrderInsert(
            self,
            pInputOrder: tdapi.CThostFtdcInputOrderField,
            pRspInfo: tdapi.CThostFtdcRspInfoField,
    ):
        """"""
        # self._check_rsp(pRspInfo, pInputOrder)
        retlist = self._check_rsp_ret(pRspInfo, pInputOrder)


    def wait(self):
        # 阻塞 等待
        self._wait_queue.get()
        #input("-------------------------------- 按任意键退出 trader api demo ")

        self.release()


if __name__ == "__main__":
    spi = CTdSpiImpl(
        config.conn_user,
        config.conn_pass,
        config.conn_db,
    )
    # 等待登录成功
    while True:
        time.sleep(1)
        if spi.is_login:
            break


    spi.wait()