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
        trade_type:str,
        root_path: str,
    ):
        print("-------------------------------- 启动 trader api demo ")
        super().__init__()
        self._front = front
        self._user = user
        self._usercode = usercode
        self._password = passwd
        self._authcode = authcode
        self._appid = appid
        self._broker_id = broker_id
        self._trantype=trade_type
        self._root_path = root_path
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
        self._confirmtype = {'001', '006', '007', '101', '102', '105', '106','107','108'}
        self._positiontype={'001', '006', '007', '101', '102', '105', '106','107','108'}
        self._wait_queue = queue.Queue(6)
        self._conn = cx_Oracle.connect(conn_user, conn_pass, conn_db)
        self._conn_cursor = self._conn.cursor()
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

    def GetInstrumentInfo(self,exchange_id:str,instrument_id:str):
        sql="select * from QUANT_FUTURE_INSTRUMENT where exchangeid='"+exchange_id+\
            "' and instrumentid='"+instrument_id+"'"
        retdict=self._db_select_rows_list(sqlstr=sql)[0]
        infodict={}
        infodict["pricetick"]=retdict.get("PRICETICK")
        infodict["volumemultiple"]=retdict.get("VOLUMEMULTIPLE")
        return infodict

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
        #confirmtype={'001','006','007','101','102','105','106'}
        if (retlist[0]).split('=')[1] == '000':
            ##根据交易类型写不同的表#######
            if (self._trantype in self._confirmtype):
                trandate = self.getcurrdate()
                sqlstr = "select count(*) from QUANT_FUTURE_CONFIRM where tradingday='" + trandate + "' and userid='"+self._user+"'"
                confirm_cnt = self._db_select_cnt(sqlstr=sqlstr)
                if (int(confirm_cnt) > 0):
                    print("交易日:[" + trandate + "]确认单已确认")
                else:
                    confirm_ret_dict = self._get_confirm_ret_sql(ret_list=retlist)
                    confirm_ret_sql = confirm_ret_dict['SQL']
                    self._db_insert(confirm_ret_sql)
                    self.settlement_info_confirm()
                # self._login_session_id=confirm_ret_dict['SESSIONID']
        else:
            return
        self._is_login = True

    def settlement_info_confirm(self):
        """投资者结算结果确认"""
        print("> 投资者结算结果确认")

        _req = tdapi.CThostFtdcSettlementInfoConfirmField()
        _req.BrokerID = self._broker_id
        _req.InvestorID = self._user
        self._check_req(_req, self._api.ReqSettlementInfoConfirm(_req, 0))
        time.sleep(1)
        #return self._login_session_id
    def _get_confirm_update_sql(self, ret_list: list, sessionid: str, userid: str) -> str:
        confirm_ret_dict = self.ret_format(ret_list)
        sql = "update QUANT_FUTURE_CONFIRM set confirmstatus='" + confirm_ret_dict.get('RetCode') + \
              "',confirmdate='" + confirm_ret_dict.get('ConfirmDate') + "',confirmtime='" + confirm_ret_dict.get(
            'ConfirmTime') + \
              "' where sessionid='" + sessionid + "' and userid='" + userid + "'"
        return sql

    def OnRspSettlementInfoConfirm(
            self,
            pSettlementInfoConfirm: tdapi.CThostFtdcSettlementInfoConfirmField,
            pRspInfo: tdapi.CThostFtdcRspInfoField,
            nRequestID: int,
            bIsLast: bool,
    ):
        """投资者结算结果确认响应"""
        retlist = self._check_rsp_ret(pRspInfo, pSettlementInfoConfirm)
        if (retlist[0]).split('=')[1] == '000':
            sql = self._get_confirm_update_sql(ret_list=retlist, sessionid=self._login_session_id, userid=self._user)
            self._db_update(sql)

            print("-----更新投资结果确认完成------")
        else:
            return

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

    def OnRspQryInstrument(
            self,
            pInstrument: tdapi.CThostFtdcInstrumentField,
            pRspInfo: tdapi.CThostFtdcRspInfoField,
            nRequestID: int,
            bIsLast: bool,
    ):
        """请求查询合约响应"""

        # if not self._check_rsp(pRspInfo, pInstrument, bIsLast):
        #    return
        retlist = self._check_rsp_ret(pRspInfo, pInstrument, bIsLast)
        retdict = self.ret_format(ret_list=retlist)
        instrument_dict = self._get_instrument_sql(instrument_ret_dict=retdict)
        if (retlist[0]).split('=')[1] == '000':
            instrument_sql = instrument_dict['SQL']
            self._db_insert(sqlstr=instrument_sql)
            return
        else:
            return

    def qry_instrument_commission_rate(self, instrument_id: str = ""):
        """请求查询合约手续费率"""
        print("> 请求查询合约手续费率")
        _req = tdapi.CThostFtdcQryInstrumentCommissionRateField()
        _req.BrokerID = self._broker_id
        _req.InvestorID = self._user
        # 若不指定合约ID, 则返回当前持仓对应合约的手续费率
        _req.InstrumentID = instrument_id
        self._check_req(_req, self._api.ReqQryInstrumentCommissionRate(_req, 0))

    def OnRspQryInstrumentCommissionRate(
            self,
            pInstrumentCommissionRate: tdapi.CThostFtdcInstrumentCommissionRateField,
            pRspInfo: tdapi.CThostFtdcRspInfoField,
            nRequestID: int,
            bIsLast: bool,
    ):
        """请求查询合约手续费率响应"""
        # if not self._check_rsp(pRspInfo, pInstrumentCommissionRate, bIsLast):
        #    return
        retlist = self._check_rsp_ret(pRspInfo, pInstrumentCommissionRate, bIsLast)
        if (retlist[0]).split('=')[1] != '000':
            return

    def qry_instrument_margin_rate(self, instrument_id: str = ""):
        """请求查询合约保证金率"""
        print("> 请求查询合约保证金率")
        _req = tdapi.CThostFtdcQryInstrumentMarginRateField()
        _req.BrokerID = self._broker_id
        _req.InvestorID = self._user
        _req.HedgeFlag = tdapi.THOST_FTDC_HF_Speculation
        # 若不指定合约ID, 则返回当前持仓对应合约的保证金率
        _req.InstrumentID = instrument_id
        self._check_req(_req, self._api.ReqQryInstrumentMarginRate(_req, 0))

    def OnRspQryInstrumentMarginRate(
            self,
            pInstrumentMarginRate: tdapi.CThostFtdcInstrumentMarginRateField,
            pRspInfo: tdapi.CThostFtdcRspInfoField,
            nRequestID: int,
            bIsLast: bool,
    ):
        """请求查询合约保证金率响应"""
        # if not self._check_rsp(pRspInfo, pInstrumentMarginRate, bIsLast):
        #    return
        retlist = self._check_rsp_ret(pRspInfo, pInstrumentMarginRate, bIsLast)
        if (retlist[0]).split('=')[1] != '000':
            return

    def qry_depth_market_data(self, exchange_id:str,instrument_id: str = ""):
        """请求查询行情，只能查询当前快照，不能查询历史行情"""
        print("> 请求查询行情")
        _req = tdapi.CThostFtdcQryDepthMarketDataField()
        # 若不指定合约ID, 则返回所有合约的行情
        _req.InstrumentID = instrument_id
        _req.ExchangeID=exchange_id
        self._check_req(_req, self._api.ReqQryDepthMarketData(_req, 0))
        time.sleep(2)

    def OnRspQryDepthMarketData(
            self,
            pDepthMarketData: tdapi.CThostFtdcDepthMarketDataField,
            pRspInfo: tdapi.CThostFtdcRspInfoField,
            nRequestID: int,
            bIsLast: bool,
    ):
        """请求查询行情响应"""
        # if not self._check_rsp(pRspInfo, pDepthMarketData, bIsLast):
        #    return
        retlist = self._check_rsp_ret(pRspInfo, pDepthMarketData, bIsLast)
        retdict=self.ret_format(ret_list=retlist)
        if (retlist[0]).split('=')[1] != '000':
            return
        else:
            self._lastprice=retdict.get('LastPrice')


    def _get_order_req_sql(self, order_dict: dict) -> dict:
        order_req_dict = {}
        retdict = order_dict
        #datadate = datetime.datetime.today().strftime("%Y%m%d")
        #datatime = datetime.datetime.now().strftime("%H:%M:%S")
        sql = "insert into QUANT_FUTURE_ORDER_REQ(USERCODE,ACCOUNTID,BROKERID,BUSINESSUNIT,CLIENTID,COMBHEDGEFLAG,COMBOFFSETFLAG,CONTINGENTCONDITION," \
              "CURRENCYID,DIRECTION,EXCHANGEID,FORCECLOSEREASON,IPADDRESS,INSTRUMENTID,INVESTORID,ISAUTOSUSPEND,ISSWAPORDER,LIMITPRICE,MACADDRESS," \
              "MINVOLUME,ORDERPRICETYPE,ORDERREF,REQUESTID,STOPPRICE,TIMECONDITION,USERFORCECLOSE,USERID,VOLUMECONDITION,VOLUMETOTALORIGINAL,SESSIONID," \
              "TRADESTATUS,ORDERDATE,ORDERTIME,DATADATE) values (" \
              "'" + self._usercode + "','" + str(retdict.get('AccountID')) + "','" + self._broker_id + "','" + str(retdict.get('BusinessUnit')) + \
              "','" + str(retdict.get('ClientID')) + "','" + str(retdict.get('CombHedgeFlag')) + "','" + str(retdict.get('CombOffsetFlag')) +\
              "','" +str(retdict.get('ContingentCondition')) + "','" + str(retdict.get('CurrencyID')) + "','" + str(retdict.get('Direction')) +\
              "','" +retdict.get('ExchangeID') + "','" + str(retdict.get('ForceCloseReason')) + "','" + str(retdict.get('IPAddress')) + \
              "','" + retdict.get('InstrumentID') + "','" + retdict.get('InvestorID') + "','" + str(retdict.get('IsAutoSuspend')) + \
              "','" + str(retdict.get('IsSwapOrder')) + "'," + str(retdict.get('LimitPrice')) + ",'" + str(retdict.get('MacAddress')) + \
              "'," + str(retdict.get('MinVolume')) + ",'" + str(retdict.get('OrderPriceType')) + "'" + \
              ",'" + str(retdict.get('OrderRef')) + "','" + str(retdict.get('RequestID')) + "'," + str(retdict.get('StopPrice')) + \
              ",'" + str(retdict.get('TimeCondition')) + "'" + ",'" + str(retdict.get('UserForceClose')) + "','" + str(retdict.get('UserID')) + \
              "','" + str(retdict.get('VolumeCondition')) + "'," + str(retdict.get('VolumeTotalOriginal')) + ",'" + retdict.get('SessionID') + \
              "','0','" + self._datadate + "','" + self._datatime + "','" + self._datadate + "'" + ")"
        print('order_req_sql is:' + sql)
        order_req_dict['SQL'] = sql
        order_req_dict['SESSIONID'] = retdict.get('SessionID')
        return order_req_dict

    def _get_update_position_detail_after_order_req_sql(self, position_dict: dict)->dict:
        update_position_detail_dict={}
        retdict=position_dict
        tmpsql="select count(*) from QUANT_FUTURE_POSITION_DETAIL where USERCODE='"+self._usercode+"' and INVESTORID='"+retdict.get('InvestorID')+\
               "' and INSTRUMENTID='"+retdict.get('InstrumentID')+"' and POSIDIRECTION='"+str(retdict.get('PosiDirection'))+"'"
        select_cnt=self._db_select_cnt(sqlstr=tmpsql)
        if(int(select_cnt)>0):
            upsql="update QUANT_FUTURE_POSITION_DETAIL set ABANDONFROZEN="+str(retdict.get('AbandonFrozen'))+",CASHIN="+str(retdict.get('CashIn'))+",CLOSEAMOUNT="+str(retdict.get('CloseAmount'))+\
                  ",CLOSEPROFIT="+str(retdict.get('CloseProfit'))+",CLOSEPROFITBYDATE="+str(retdict.get('CloseProfitByDate'))+",CLOSEPROFITBYTRADE="+str(retdict.get('CloseProfitByTrade'))+\
                  ",CLOSEVOLUME="+str(retdict.get('CloseVolume'))+",COMBLONGFROZEN="+str(retdict.get('CombLongFrozen'))+",COMBSHORTFROZEN="+str(retdict.get('CombShortFrozen'))+ \
                  ",COMMISSION=" + str(retdict.get('Commission')) + ",EXCHANGEMARGIN=" + str(retdict.get('ExchangeMargin')) + ",FROZENCASH=" + str(retdict.get('LongFrozen'))+\
                  ",FROZENCOMMISSION=" + str(retdict.get('FrozenCommission')) + ",FROZENMARGIN=" + str(retdict.get('FrozenMargin')) + ",LONGFROZEN=" + str(retdict.get('LongFrozen'))+ \
                  ",LONGFROZENAMOUNT=" + str(retdict.get('LongFrozenAmount')) + ",MARGINRATEBYMONEY=" + str(retdict.get('MarginRateByMoney')) + ",MARGINRATEBYVOLUME=" + str(retdict.get('MarginRateByVolume')) + \
                  ",OPENAMOUNT=" + str(retdict.get('OpenAmount')) + ",OPENCOST=" + str(retdict.get('OpenCost')) + ",OPENVOLUME=" + str(retdict.get('OpenVolume')) + \
                  ",POSITION=" + str(retdict.get('Position')) + ",POSITIONCOST=" + str(retdict.get('PositionCost')) + ",POSITIONCOSTOFFSET=" + str(retdict.get('PositionCostOffset')) + \
                  ",POSITIONDATE='" + str(retdict.get('PositionDate')) + "',POSITIONPROFIT=" + str(retdict.get('PositionProfit')) + ",PREMARGIN=" + str(retdict.get('PreMargin')) + \
                  ",PRESETTLEMENTPRICE=" + str(retdict.get('PreSettlementPrice')) + ",SETTLEMENTID='" + str(retdict.get('SettlementID')) + \
                  "',SETTLEMENTPRICE=" + str(retdict.get('SettlementPrice')) + ",SHORTFROZEN=" + str(retdict.get('ShortFrozen')) + ",SHORTFROZENAMOUNT=" + str(retdict.get('ShortFrozenAmount')) + \
                  ",STRIKEFROZEN=" + str(retdict.get('StrikeFrozen')) + ",STRIKEFROZENAMOUNT=" + str(retdict.get('StrikeFrozenAmount')) + ",TASPOSITION=" + str(retdict.get('TasPosition')) + \
                  ",TASPOSITIONCOST=" + str(retdict.get('TasPositionCost')) + ",TODAYPOSITION=" + str(retdict.get('TodayPosition')) + ",TRADINGDAY='" + str(retdict.get('TradingDay')) + \
                  "',USEMARGIN=" + str(retdict.get('UseMargin')) + ",YDPOSITION=" + str(retdict.get('YdPosition')) + ",YDSTRIKEFROZEN=" + str(retdict.get('YdStrikeFrozen')) + \
                  ",UPTTIME='" + self._datatime + "',uptdate='" + self._datadate + "' where usercode='"+self._usercode+"' and investorid='"+retdict.get('InvestorID')+\
                  "' and INSTRUMENTID='"+retdict.get('InstrumentID')+"' and EXCHANGEID='"+retdict.get('ExchangeID')+"' and POSIDIRECTION='"+retdict.get('PosiDirection')+"'"
            print("upsql_after_position sql is:"+upsql)
            update_position_detail_dict['SQL']=upsql
            update_position_detail_dict['FLAG']=1
            return update_position_detail_dict
        else:
            selectsql="select exchangeid,instrumentname,instrumentid,volumemultiple from QUANT_FUTURE_INSTRUMENT where instrumentid='"+retdict.get('InstrumentID')+"' and exchangeid='"+retdict.get('ExchangeID')+"'"
            selectdict=self._db_select_rows(sqlstr=selectsql)
            selectrowdict=selectdict['rows'][0]
            col_name=selectdict['col_name']
            #print(selectrowdict[col_name.index('INSTRUMENTNAME')])
            temp_instrumentname =selectrowdict[col_name.index('INSTRUMENTNAME')]
            temp_volumemultiple=int(selectrowdict[col_name.index('VOLUMEMULTIPLE')])
            temp_position=int(retdict.get('Position'))
            temp_opencost=float(retdict.get('OpenCost'))
            temp_positioncost=float(retdict.get('PositionCost'))
            temp_usemargin=float(retdict.get('UseMargin'))
            if (temp_position == 0 or temp_volumemultiple == 0):
                temp_position = 1
                temp_volumemultiple = 1
            aver_price=temp_opencost/(temp_position*temp_volumemultiple)
            if (temp_usemargin == 0 or temp_positioncost == 0):
                temp_usemargin = 1
                temp_positioncost = 1
            temp_positionrate=temp_usemargin/temp_positioncost
            insertsql="insert into QUANT_FUTURE_POSITION_DETAIL(USERCODE,ABANDONFROZEN,BROKERID,CASHIN,CLOSEAMOUNT,CLOSEPROFIT,CLOSEPROFITBYDATE,CLOSEPROFITBYTRADE," \
              "CLOSEVOLUME,COMBLONGFROZEN,COMBPOSITION,COMBSHORTFROZEN,COMMISSION,EXCHANGEID,EXCHANGEMARGIN,FROZENCASH,FROZENCOMMISSION,FROZENMARGIN,HEDGEFLAG," \
              "INSTRUMENTID,INVESTUNITID,INVESTORID,LONGFROZEN,LONGFROZENAMOUNT,MARGINRATEBYMONEY,MARGINRATEBYVOLUME,OPENAMOUNT,OPENCOST,OPENVOLUME,POSIDIRECTION," \
              "POSITION,POSITIONCOST,POSITIONCOSTOFFSET,POSITIONDATE,POSITIONPROFIT,PREMARGIN,PRESETTLEMENTPRICE,SETTLEMENTID,SETTLEMENTPRICE,SHORTFROZEN,SHORTFROZENAMOUNT," \
              "STRIKEFROZEN,STRIKEFROZENAMOUNT,TASPOSITION,TASPOSITIONCOST,TODAYPOSITION,TRADINGDAY,USEMARGIN,YDPOSITION,YDSTRIKEFROZEN,INSTRUMENTNAME,AVEPRICE," \
              "VOLUMEMULTIPLE,POSTIONRATE,UPTTIME,UPTDATE,DATADATE) values (" \
              "'" + self._usercode + "'," + str(retdict.get('AbandonFrozen')) + ",'" + self._broker_id + "'," + str(retdict.get('CashIn')) + \
              "," + str(retdict.get('CloseAmount')) + "," + str(retdict.get('CloseProfit')) + "," + str(retdict.get('CloseProfitByDate'))+","+str(retdict.get('CloseProfitByTrade'))+","+str(retdict.get('CloseVolume')) +\
              "," + str(retdict.get('CombLongFrozen')) + "," + str(retdict.get('CombPosition')) + "," + str(retdict.get('CombShortFrozen'))+","+str(retdict.get('Commission'))+",'"+str(retdict.get('ExchangeID')) +\
              "'," + str(retdict.get('ExchangeMargin')) + "," + str(retdict.get('FrozenCash')) + "," + str(retdict.get('FrozenCommission'))+","+str(retdict.get('FrozenMargin'))+",'"+str(retdict.get('HedgeFlag')) +\
              "','" + str(retdict.get('InstrumentID')) + "','" + str(retdict.get('InvestUnitID')) + "','" + str(retdict.get('InvestorID'))+"',"+str(retdict.get('LongFrozen'))+","+str(retdict.get('LongFrozenAmount')) +\
              "," + str(retdict.get('MarginRateByMoney')) + "," + str(retdict.get('MarginRateByVolume')) + "," + str(retdict.get('OpenAmount'))+","+str(temp_opencost)+","+str(retdict.get('OpenVolume')) +\
              ",'" + str(retdict.get('PosiDirection')) + "'," + str(temp_position) + "," + str(temp_positioncost)+","+str(retdict.get('PositionCostOffset'))+",'"+str(retdict.get('PositionDate')) +\
              "'," + str(retdict.get('PositionProfit')) + "," + str(retdict.get('PreMargin')) + "," + str(retdict.get('PreSettlementPrice'))+",'"+str(retdict.get('SettlementID'))+"',"+str(retdict.get('SettlementPrice')) +\
              "," + str(retdict.get('ShortFrozen')) + "," + str(retdict.get('ShortFrozenAmount')) + "," + str(retdict.get('StrikeFrozen'))+","+str(retdict.get('StrikeFrozenAmount'))+","+str(retdict.get('TasPosition')) +\
              "," + str(retdict.get('TasPositionCost')) + "," + str(retdict.get('TodayPosition')) + ",'" + str(retdict.get('TradingDay'))+"',"+str(temp_usemargin)+","+str(retdict.get('YdPosition')) + \
              "," + str(retdict.get('YdStrikeFrozen')) + ",'" + temp_instrumentname + "'," + str(aver_price) + "," + str(temp_volumemultiple) + "," + str(temp_positionrate) + \
              ",'" + self._datatime + "','" + self._datadate + "','" + self._datadate + "'" + ")"
            update_position_detail_dict['SQL']=insertsql
            update_position_detail_dict['FLAG']=0
            return update_position_detail_dict

    def market_order_insert(
        self, exchange_id: str, instrument_id: str, buysellflag:str='0',trantype:str='0',volume: int = 1,price: float=0.00,
    ):
        """报单录入请求(市价单)

        - 录入错误时对应响应OnRspOrderInsert、OnErrRtnOrderInsert，
        - 正确时对应回报OnRtnOrder、OnRtnTrade。
        """
        print("> 报单录入请求(市价单)")

        # 市价单, 注意选择一个相对活跃的合约
        # simnow 目前貌似不支持市价单，所以会被自动撤销！！！
        _req = tdapi.CThostFtdcInputOrderField()
        _req.BrokerID = self._broker_id
        _req.InvestorID = self._user
        _req.ExchangeID = exchange_id
        _req.InstrumentID = instrument_id
        _req.LimitPrice = price
        #_req.OrderPriceType = tdapi.THOST_FTDC_OPT_AnyPrice  # 价格类型市价单
        _req.OrderPriceType=tdapi.THOST_FTDC_OPT_LimitPrice
        if(buysellflag=='0'):#0代表买，1代表卖
            _req.Direction=tdapi.THOST_FTDC_D_Buy
        else:
            _req.Direction=tdapi.THOST_FTDC_D_Sell
        #_req.Direction = tdapi.THOST_FTDC_D_Buy  # 买
        if(trantype=='0'):#0代表开仓，1代表平仓
            _req.CombOffsetFlag=tdapi.THOST_FTDC_OF_Open
        else:
            _req.CombOffsetFlag=tdapi.THOST_FTDC_OF_Close
        #_req.CombOffsetFlag = tdapi.THOST_FTDC_OF_Open  # 开仓
        _req.CombHedgeFlag = tdapi.THOST_FTDC_HF_Speculation
        _req.VolumeTotalOriginal = volume
        _req.IsAutoSuspend = 0
        _req.IsSwapOrder = 0
        _req.TimeCondition = tdapi.THOST_FTDC_TC_GFD
        _req.VolumeCondition = tdapi.THOST_FTDC_VC_AV
        _req.ContingentCondition = tdapi.THOST_FTDC_CC_Immediately
        _req.ForceCloseReason = tdapi.THOST_FTDC_FCC_NotForceClose
        order_dict = {}
        order_dict['AccountID'] = ''
        order_dict['BrokerID'] = _req.BrokerID
        order_dict['BusinessUnit'] = ''
        order_dict['ClientID'] = ''
        order_dict['CombOffsetFlag'] = _req.CombOffsetFlag
        order_dict['CombHedgeFlag'] = _req.CombHedgeFlag
        order_dict['ContingentCondition'] = _req.ContingentCondition
        order_dict['CurrencyID'] = ''
        order_dict['Direction'] = _req.Direction
        order_dict['ExchangeID'] = _req.ExchangeID
        order_dict['ForceCloseReason'] = _req.ForceCloseReason
        order_dict['IPAddress'] = ''
        order_dict['InstrumentID'] = _req.InstrumentID
        order_dict['InvestUnitID'] = ''
        order_dict['InvestorID'] = _req.InvestorID
        order_dict['IsAutoSuspend'] = _req.IsAutoSuspend
        order_dict['IsSwapOrder'] = _req.IsSwapOrder
        order_dict['LimitPrice'] = _req.LimitPrice
        order_dict['MacAddress'] = ''
        order_dict['MinVolume'] = 0
        order_dict['OrderPriceType'] = _req.OrderPriceType
        order_dict['OrderRef'] = ''
        order_dict['RequestID'] = 0
        order_dict['StopPrice'] = 0
        order_dict['TimeCondition'] = _req.TimeCondition
        order_dict['UserForceClose'] = 0
        order_dict['UserID'] = ''
        order_dict['VolumeCondition'] = _req.VolumeCondition
        order_dict['VolumeTotalOriginal'] = _req.VolumeTotalOriginal
        order_dict['SessionID'] = self._login_session_id
        print(order_dict)
        order_req_dict = self._get_order_req_sql(order_dict=order_dict)
        sql = order_req_dict['SQL']
        self._db_insert(sqlstr=sql)
        self._check_req(_req, self._api.ReqOrderInsert(_req, 0))
        time.sleep(3)

    def limit_order_insert(
        self,
        exchange_id: str,
        instrument_id: str,
        buysellflag:str='0',
        trantype:str='0',
        volume: int = 1,
        price: float=0.00
    ):
        """报单录入请求(限价单)

        - 录入错误时对应响应OnRspOrderInsert、OnErrRtnOrderInsert，
        - 正确时对应回报OnRtnOrder、OnRtnTrade。
        """
        print("> 报单录入请求(限价单)")

        # 限价单 注意选择一个相对活跃的合约及合适的价格
        _req = tdapi.CThostFtdcInputOrderField()
        _req.BrokerID = self._broker_id
        _req.InvestorID = self._user
        _req.ExchangeID = exchange_id
        _req.InstrumentID = instrument_id  # 合约ID
        _req.LimitPrice = price  # 价格
        _req.OrderPriceType = tdapi.THOST_FTDC_OPT_LimitPrice  # 价格类型限价单
        if (buysellflag == '0'):  # 0代表买，1代表卖
            _req.Direction = tdapi.THOST_FTDC_D_Buy
        else:
            _req.Direction = tdapi.THOST_FTDC_D_Sell
            # _req.Direction = tdapi.THOST_FTDC_D_Buy  # 买
        if (trantype == '0'):  # 0代表开仓，1代表平仓
            _req.CombOffsetFlag = tdapi.THOST_FTDC_OF_Open
        else:
            _req.CombOffsetFlag = tdapi.THOST_FTDC_OF_Close
        _req.CombHedgeFlag = tdapi.THOST_FTDC_HF_Speculation
        _req.VolumeTotalOriginal = volume
        _req.IsAutoSuspend = 0
        _req.IsSwapOrder = 0
        _req.TimeCondition = tdapi.THOST_FTDC_TC_GFD
        _req.VolumeCondition = tdapi.THOST_FTDC_VC_AV
        _req.ContingentCondition = tdapi.THOST_FTDC_CC_Immediately
        _req.ForceCloseReason = tdapi.THOST_FTDC_FCC_NotForceClose
        order_dict = {}
        order_dict['AccountID'] = ''
        order_dict['BrokerID'] = _req.BrokerID
        order_dict['BusinessUnit'] = ''
        order_dict['ClientID'] = ''
        order_dict['CombOffsetFlag'] = _req.CombOffsetFlag
        order_dict['CombHedgeFlag'] = _req.CombHedgeFlag
        order_dict['ContingentCondition'] = _req.ContingentCondition
        order_dict['CurrencyID'] = ''
        order_dict['Direction'] = _req.Direction
        order_dict['ExchangeID'] = _req.ExchangeID
        order_dict['ForceCloseReason'] = _req.ForceCloseReason
        order_dict['IPAddress'] = ''
        order_dict['InstrumentID'] = _req.InstrumentID
        order_dict['InvestUnitID'] = ''
        order_dict['InvestorID'] = _req.InvestorID
        order_dict['IsAutoSuspend'] = _req.IsAutoSuspend
        order_dict['IsSwapOrder'] = _req.IsSwapOrder
        order_dict['LimitPrice'] = _req.LimitPrice
        order_dict['MacAddress'] = ''
        order_dict['MinVolume'] = 0
        order_dict['OrderPriceType'] = _req.OrderPriceType
        order_dict['OrderRef'] = ''
        order_dict['RequestID'] = 0
        order_dict['StopPrice'] = 0
        order_dict['TimeCondition'] = _req.TimeCondition
        order_dict['UserForceClose'] = 0
        order_dict['UserID'] = ''
        order_dict['VolumeCondition'] = _req.VolumeCondition
        order_dict['VolumeTotalOriginal'] = _req.VolumeTotalOriginal
        order_dict['SessionID'] = self._login_session_id
        print(order_dict)
        order_req_dict = self._get_order_req_sql(order_dict=order_dict)
        sql = order_req_dict['SQL']
        self._db_insert(sqlstr=sql)
        self._check_req(_req, self._api.ReqOrderInsert(_req, 0))

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

    def _get_order_cancel_req_sql(self, order_dict: dict) -> dict:
        order_cancel_req_dict = {}
        retdict = order_dict
        # datadate = datetime.datetime.today().strftime("%Y%m%d")
        # datatime = datetime.datetime.now().strftime("%H:%M:%S")
        sql = "insert into QUANT_FUTURE_ORDER_CANCEL_REQ(USERCODE,BROKERID,EXCHANGEID,ACTIONFLAG,FRONTID,IPADDRESS,INSTRUMENTID," \
              "INVESTUNITID,INVESTORID,LIMITPRICE,MACADDRESS,ORDERACTIONREF,ORDERREF,ORDERSYSID,REQUESTID,SESSIONID,USERID,VOLUMECHANGE," \
              "DATATIME,DATADATE) values (" \
              "'" + self._usercode + "','" + self._broker_id + "','" + retdict.get('ExchangeID') + "','" + str(
            retdict.get('ActionFlag')) + \
              "','" + str(retdict.get('FrontID')) + "','" + str(retdict.get('IPAddress')) + "','" + retdict.get(
            'InstrumentID') + \
              "','" + str(retdict.get('InvestUnitID')) + "','" + retdict.get('InvestorID') + "'," + str(
            retdict.get('LimitPrice')) + \
              ",'" + str(retdict.get('MacAddress')) + "','" + str(retdict.get('OrderActionRef')) + "','" + str(
            retdict.get('OrderRef')) + "','" + str(retdict.get('OrderSysID')) + \
              "','" + str(retdict.get('RequestID')) + "','" + retdict.get('SessionID') + "','" + retdict.get('UserID') + \
              "'," + str(retdict.get('VolumeChange')) + ",'" + self._datatime + "','" + self._datadate + "'" + ")"
        print('order_cancel_req sql is:' + sql)
        order_cancel_req_dict['SQL'] = sql
        order_cancel_req_dict['SESSIONID'] = retdict.get('SessionID')
        return order_cancel_req_dict

    def order_cancel1(
            self, exchange_id: str, instrument_id: str, order_sys_id: str
    ):
        """报单撤销请求 方式一

        - 错误响应: OnRspOrderAction，OnErrRtnOrderAction
        - 正确响应：OnRtnOrder
        """
        print("> 报单撤销请求 方式一")

        # 撤单请求，首先需要有一笔未成交的订单，可以使用限价单，按照未成交订单信息填写撤单请求
        _req = tdapi.CThostFtdcInputOrderActionField()
        _req.BrokerID = self._broker_id
        _req.InvestorID = self._user
        _req.UserID = self._user
        _req.ExchangeID = exchange_id
        _req.InstrumentID = instrument_id
        _req.ActionFlag = tdapi.THOST_FTDC_AF_Delete

        _req.OrderSysID = order_sys_id  # OrderSysId 中空格也要带着
        cancel_order_dict = {}
        cancel_order_dict['ActionFlag'] = _req.ActionFlag
        cancel_order_dict['BrokerID'] = _req.BrokerID
        cancel_order_dict['ExchangeID'] = _req.ExchangeID
        cancel_order_dict['FrontID'] = '0'
        cancel_order_dict['IPAddress'] = ''
        cancel_order_dict['InstrumentID'] = _req.InstrumentID
        cancel_order_dict['InvestUnitID'] = ''
        cancel_order_dict['InvestorID'] = _req.InvestorID
        cancel_order_dict['LimitPrice'] = 0.0
        cancel_order_dict['MacAddress'] = ''
        cancel_order_dict['OrderActionRef'] = 0
        cancel_order_dict['OrderRef'] = ''
        cancel_order_dict['OrderSysID'] = order_sys_id
        cancel_order_dict['RequestID'] = 0
        cancel_order_dict['SessionID'] = self._login_session_id
        cancel_order_dict['UserID'] = _req.UserID
        cancel_order_dict['VolumeChange'] = 0

        order_cancel_req_dict = self._get_order_cancel_req_sql(order_dict=cancel_order_dict)
        sql = order_cancel_req_dict['SQL']
        self._db_insert(sqlstr=sql)
        # 若成功，会通过 报单回报 返回新的订单状态, 若失败则会响应失败
        self._check_req(_req, self._api.ReqOrderAction(_req, 0))
        # exit()

    def order_cancel2(
            self,
            exchange_id: str,
            instrument_id: str,
            front_id: int,
            session_id: int,
            order_ref: str,
    ):
        """报单撤销请求 方式二

        - 错误响应: OnRspOrderAction，OnErrRtnOrderAction
        - 正确响应：OnRtnOrder
        """
        print("> 报单撤销请求 方式二")

        # 撤单请求，首先需要有一笔未成交的订单，可以使用限价单，按照未成交订单信息填写撤单请求
        _req = tdapi.CThostFtdcInputOrderActionField()
        _req.BrokerID = self._broker_id
        _req.InvestorID = self._user
        _req.UserID = self._user
        _req.ExchangeID = exchange_id
        _req.InstrumentID = instrument_id
        _req.ActionFlag = tdapi.THOST_FTDC_AF_Delete

        _req.OrderRef = order_ref
        _req.FrontID = front_id
        _req.SessionID = session_id

        # 若成功，会通过 报单回报 返回新的订单状态, 若失败则会响应失败
        self._check_req(_req, self._api.ReqOrderAction(_req, 0))

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
        try:
            retlist = self.print_rsp_rtn("成交通知", pTrade)
            order_deal_dic = self.ret_format(ret_list=retlist)
            order_sql_dic = self._get_order_deal_sql(order_dict=order_deal_dic)
            sql = order_sql_dic['SQL']
            self._db_insert(sql)
        except Exception as e:
            # 处理异常，可以是记录日志或者重试等策略
            print(f"Error in task: {e}")


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

    def qry_trading_code(self, exchange_id: str):
        """请求查询交易编码"""
        print("> 请求查询交易编码")
        req = tdapi.CThostFtdcQryTradingCodeField()
        req.BrokerID = self._broker_id
        req.InvestorID = self._user
        req.ExchangeID = exchange_id
        self._check_req(req, self._api.ReqQryTradingCode(req, 0))

    def OnRspQryTradingCode(
            self,
            pTradingCode: tdapi.CThostFtdcTradingCodeField,
            pRspInfo: tdapi.CThostFtdcRspInfoField,
            nRequestID: int,
            bIsLast: bool,
    ):
        """请求查询交易编码响应"""
        # self._check_rsp(pRspInfo, pTradingCode, bIsLast)
        retlist = self._check_rsp_ret(pRspInfo, pTradingCode, bIsLast)

    def qry_exchange(self, exchange_id: str):
        """查询交易所"""
        print("> 查询交易所")
        req = tdapi.CThostFtdcQryExchangeField()
        req.ExchangeID = exchange_id
        self._check_req(req, self._api.ReqQryExchange(req, 0))

    def OnRspQryExchange(
            self,
            pExchange: tdapi.CThostFtdcExchangeField,
            pRspInfo: tdapi.CThostFtdcRspInfoField,
            nRequestID: int,
            bIsLast: bool,
    ):
        """查询交易所应答"""
        # self._check_rsp(pRspInfo, pExchange, bIsLast)
        retlist = self._check_rsp_ret(pRspInfo, pExchange, bIsLast)

    def user_password_update(self, new_password: str, old_password: str):
        """用户口令变更"""
        print("> 用户口令变更请求")

        req = tdapi.CThostFtdcUserPasswordUpdateField()
        req.BrokerID = self._broker_id
        req.UserID = self._user
        req.OldPassword = old_password
        req.NewPassword = new_password

        self._check_req(req, self._api.ReqUserPasswordUpdate(req, 0))

    def OnRspUserPasswordUpdate(
            self,
            pUserPasswordUpdate: tdapi.CThostFtdcUserPasswordUpdateField,
            pRspInfo: tdapi.CThostFtdcRspInfoField,
            nRequestID: int,
            bIsLast: bool,
    ):
        """用户口令变更响应"""
        # self._check_rsp(pRspInfo, pUserPasswordUpdate, bIsLast)
        retlist = self._check_rsp_ret(pRspInfo, pUserPasswordUpdate, bIsLast)

    def qry_order_comm_rate(self, instrument_id: str):
        """查询申报费率"""
        print("> 请求查询申报费率")
        req = tdapi.CThostFtdcQryInstrumentOrderCommRateField()
        req.BrokerID = self._broker_id
        req.InvestorID = self._user
        req.InstrumentID = instrument_id
        self._check_req(req, self._api.ReqQryInstrumentOrderCommRate(req, 0))

    def OnRspQryInstrumentOrderCommRate(
            self,
            pInstrumentOrderCommRate: tdapi.CThostFtdcInstrumentOrderCommRateField,
            pRspInfo: tdapi.CThostFtdcRspInfoField,
            nRequestID: int,
            bIsLast: bool,
    ):
        """查询申报费率应答"""
        self._check_rsp(pRspInfo, pInstrumentOrderCommRate, bIsLast)

    def qry_investor_position(self, instrument_id: str = ""):
        """查询投资者持仓"""
        print("> 请求查询投资者持仓")
        req = tdapi.CThostFtdcQryInvestorPositionField()
        req.BrokerID = self._broker_id
        req.InvestorID = self._user
        req.InstrumentID = instrument_id  # 可指定合约
        self._check_req(req, self._api.ReqQryInvestorPosition(req, 0))
        time.sleep(3)
        return self._login_session_id


    def OnRspQryInvestorPosition(
            self,
            pInvestorPosition: tdapi.CThostFtdcInvestorPositionField,
            pRspInfo: tdapi.CThostFtdcRspInfoField,
            nRequestID: int,
            bIsLast: bool,
    ):
        """查询投资者持仓响应"""
        # self._check_rsp(pRspInfo, pInvestorPosition, bIsLast)
        retlist = self._check_rsp_ret(pRspInfo, pInvestorPosition, bIsLast)
        retdict = self.ret_format(retlist)
        position_sql_dict = self._get_update_position_detail_after_order_req_sql(position_dict=retdict)
        if (position_sql_dict['FLAG'] == 0):
            self._db_insert(position_sql_dict['SQL'])
        else:
            self._db_update(position_sql_dict['SQL'])
        if(self._trantype in self._positiontype):
            self.qry_investor_trading_account()
        # print(retlist)


    def qry_investor_position_detail(self, instrument_id: str = ""):
        """查询投资者持仓"""
        print("> 请求查询投资者持仓明细")
        req = tdapi.CThostFtdcQryInvestorPositionDetailField()
        req.BrokerID = self._broker_id
        req.InvestorID = self._user
        req.InstrumentID = instrument_id  # 可指定合约
        self._check_req(req, self._api.ReqQryInvestorPositionDetail(req, 0))

    def OnRspQryInvestorPositionDetail(
            self,
            pInvestorPositionDetail: tdapi.CThostFtdcInvestorPositionDetailField,
            pRspInfo: tdapi.CThostFtdcRspInfoField,
            nRequestID: int,
            bIsLast: bool,
    ):
        """查询投资者持仓明细响应"""
        # self._check_rsp(pRspInfo, pInvestorPositionDetail, bIsLast)
        retlist = self._check_rsp_ret(pRspInfo, pInvestorPositionDetail, bIsLast)



    def qry_investor_trading_account(self):
        """查询投资者持仓账户"""
        print("> 请求查询投资者持仓账户")
        req = tdapi.CThostFtdcQryTradingAccountField()
        req.BrokerID = self._broker_id
        req.InvestorID = self._user
        req.CurrencyID = "CNY"  # 可指定币种
        self._check_req(req, self._api.ReqQryTradingAccount(req,0))
        if(self._trantype not in self._positiontype):
            time.sleep(2)


    def _get_update_position_after_order_req_sql(self, position_dict: dict)->dict:
        update_position_dict={}
        retdict=position_dict
        tmpsql="select count(*) from QUANT_FUTURE_POSITION where USERCODE='"+self._usercode+"' and ACCOUNTID='"+retdict.get('AccountID')+"'"
        select_cnt=self._db_select_cnt(sqlstr=tmpsql)
        if(int(select_cnt)>0):
            upsql="update QUANT_FUTURE_POSITION set AVAILABLE="+str(retdict.get('Available'))+",BALANCE="+str(retdict.get('Balance'))+",BIZTYPE='"+str(retdict.get('CloseAmount'))+\
                  "',CASHIN="+str(retdict.get('CashIn'))+",CLOSEPROFIT="+str(retdict.get('CloseProfit'))+",COMMISSION="+str(retdict.get('Commission'))+\
                  ",CREDIT="+str(retdict.get('Credit'))+",CURRMARGIN="+str(retdict.get('CurrMargin'))+",DELIVERYMARGIN="+str(retdict.get('DeliveryMargin'))+ \
                  ",DEPOSIT=" + str(retdict.get('Deposit')) + ",EXCHANGEDELIVERYMARGIN=" + str(retdict.get('ExchangeDeliveryMargin')) + ",EXCHANGEMARGIN=" + str(retdict.get('ExchangeMargin'))+\
                  ",FROZENCASH=" + str(retdict.get('FrozenCash')) + ",FROZENCOMMISSION=" + str(retdict.get('FrozenCommission')) + ",FROZENMARGIN=" + str(retdict.get('FrozenMargin'))+ \
                  ",FROZENSWAP=" + str(retdict.get('FrozenSwap')) + ",FUNDMORTGAGEAVAILABLE=" + str(retdict.get('FundMortgageAvailable')) + ",FUNDMORTGAGEIN=" + str(retdict.get('FundMortgageIn')) + \
                  ",FUNDMORTGAGEOUT=" + str(retdict.get('FundMortgageOut')) + ",INTEREST=" + str(retdict.get('Interest')) + ",INTERESTBASE=" + str(retdict.get('InterestBase')) + \
                  ",MORTGAGE=" + str(retdict.get('Mortgage')) + ",MORTGAGEABLEFUND=" + str(retdict.get('MortgageableFund')) + ",POSITIONPROFIT=" + str(retdict.get('PositionProfit')) + \
                  ",PREBALANCE=" + str(retdict.get('PreBalance')) + ",PRECREDIT=" + str(retdict.get('PreCredit')) + ",PREDEPOSIT=" + str(retdict.get('PreDeposit')) + \
                  ",PREFUNDMORTGAGEIN=" + str(retdict.get('PreFundMortgageIn')) + ",PREFUNDMORTGAGEOUT=" + str(retdict.get('PreFundMortgageOut')) + ",PREMARGIN=" + str(retdict.get('PreMargin')) + \
                  ",PREMORTGAGE=" + str(retdict.get('PreMortgage')) + ",REMAINSWAP=" + str(retdict.get('RemainSwap')) + ",RESERVE=" + str(retdict.get('Reserve')) + \
                  ",RESERVEBALANCE=" + str(retdict.get('ReserveBalance')) + ",SETTLEMENTID='" + str(retdict.get('SettlementID')) + "',SPECPRODUCTCLOSEPROFIT=" + str(retdict.get('SpecProductCloseProfit')) + \
                  ",SPECPRODUCTCOMMISSION=" + str(retdict.get('SpecProductCommission')) + ",SPECPRODUCTEXCHANGEMARGIN=" + str(retdict.get('SpecProductExchangeMargin')) + ",SPECPRODUCTFROZENCOMMISSION=" + str(retdict.get('SpecProductFrozenCommission')) + \
                  ",SPECPRODUCTMARGIN=" + str(retdict.get('SpecProductMargin')) + ",SPECPRODUCTPOSITIONPROFIT=" + str(retdict.get('SpecProductPositionProfit')) + ",SPECPRODUCTPOSITIONPROFITBYALG=" + str(retdict.get('SpecProductPositionProfitByAlg')) + \
                  ",TRADINGDAY='" + str(retdict.get('TradingDay')) + "',WITHDRAW=" + str(retdict.get('Withdraw')) + ",WITHDRAWQUOTA=" + str(retdict.get('WithdrawQuota')) + \
                  ",UPTTIME='" + self._datatime + "',uptdate='" + self._datadate + "' where usercode='"+self._usercode+"' and ACCOUNTID='"+retdict.get('AccountID')+"'"
            update_position_dict['SQL']=upsql
            update_position_dict['FLAG']=1
            return update_position_dict
        else:
            insertsql="insert into QUANT_FUTURE_POSITION(USERCODE,ACCOUNTID,AVAILABLE,BALANCE,BIZTYPE,BROKERID,CASHIN,CLOSEPROFIT," \
              "COMMISSION,CREDIT,CURRMARGIN,CURRENCYID,DELIVERYMARGIN,DEPOSIT,EXCHANGEDELIVERYMARGIN,EXCHANGEMARGIN,FROZENCASH,FROZENCOMMISSION,FROZENMARGIN," \
              "FROZENSWAP,FUNDMORTGAGEAVAILABLE,FUNDMORTGAGEIN,FUNDMORTGAGEOUT,INTEREST,INTERESTBASE,MORTGAGE,MORTGAGEABLEFUND,POSITIONPROFIT,PREBALANCE,PRECREDIT," \
              "PREDEPOSIT,PREFUNDMORTGAGEIN,PREFUNDMORTGAGEOUT,PREMARGIN,PREMORTGAGE,REMAINSWAP,RESERVE,RESERVEBALANCE,SETTLEMENTID,SPECPRODUCTCLOSEPROFIT,SPECPRODUCTCOMMISSION," \
              "SPECPRODUCTEXCHANGEMARGIN,SPECPRODUCTFROZENCOMMISSION,SPECPRODUCTFROZENMARGIN,SPECPRODUCTMARGIN,SPECPRODUCTPOSITIONPROFIT,SPECPRODUCTPOSITIONPROFITBYALG,TRADINGDAY,WITHDRAW,WITHDRAWQUOTA," \
              "UPTTIME,UPTDATE,DATADATE) values (" \
              "'" + self._usercode+ "'," + str(retdict.get('AccountID')) + ","  + str(retdict.get('Available')) + \
              "," + str(retdict.get('Balance')) + ",'" + str(retdict.get('BizType')) + "','" + str(retdict.get('BrokerID'))+"',"+str(retdict.get('CashIn'))+","+str(retdict.get('CloseProfit')) +\
              "," + str(retdict.get('Commission')) + "," + str(retdict.get('Credit')) + "," + str(retdict.get('CurrMargin'))+",'"+str(retdict.get('CurrencyID'))+"',"+str(retdict.get('DeliveryMargin')) +\
              "," + str(retdict.get('Deposit')) + "," + str(retdict.get('ExchangeDeliveryMargin')) + "," + str(retdict.get('ExchangeMargin'))+","+str(retdict.get('FrozenCash'))+","+str(retdict.get('FrozenCommission')) +\
              "," + str(retdict.get('FrozenMargin')) + "," + str(retdict.get('FrozenSwap')) + "," + str(retdict.get('FundMortgageAvailable'))+","+str(retdict.get('FundMortgageIn'))+","+str(retdict.get('FundMortgageOut')) +\
              "," + str(retdict.get('Interest')) + "," + str(retdict.get('InterestBase')) + "," + str(retdict.get('Mortgage'))+","+str(retdict.get('MortgageableFund'))+","+str(retdict.get('PositionProfit')) +\
              "," + str(retdict.get('PreBalance')) + "," + str(retdict.get('PreCredit')) + "," + str(retdict.get('PreDeposit'))+","+str(retdict.get('PreFundMortgageIn'))+","+str(retdict.get('PreFundMortgageOut')) +\
              "," + str(retdict.get('PreMargin')) + "," + str(retdict.get('PreMortgage')) + "," + str(retdict.get('RemainSwap'))+","+str(retdict.get('Reserve'))+","+str(retdict.get('ReserveBalance')) +\
              ",'" + str(retdict.get('SettlementID')) + "'," + str(retdict.get('SpecProductCloseProfit')) + "," + str(retdict.get('SpecProductCommission'))+","+str(retdict.get('SpecProductExchangeMargin'))+","+str(retdict.get('SpecProductFrozenCommission')) +\
              "," + str(retdict.get('SpecProductFrozenMargin')) + "," + str(retdict.get('SpecProductMargin')) + "," + str(retdict.get('SpecProductPositionProfit'))+","+str(retdict.get('SpecProductPositionProfitByAlg'))+",'"+str(retdict.get('TradingDay')) + \
              "'," + str(retdict.get('Withdraw')) + "," +  str(retdict.get('WithdrawQuota')) +",'" + self._datatime + "','" + self._datadate + "','" + self._datadate + "'" + ")"
            update_position_dict['SQL']=insertsql
            update_position_dict['FLAG']=0
            return update_position_dict

    def OnRspQryTradingAccount(
            self,
            pTradingAccount:tdapi.CThostFtdcTradingAccountField,
            pRspInfo: tdapi.CThostFtdcRspInfoField,
            nRequestID: int,
            bIsLast: bool,
    ):
        retlist=self._check_rsp_ret(pRspInfo,pTradingAccount,bIsLast)
        retdict = self.ret_format(retlist)
        position_sql_dict = self._get_update_position_after_order_req_sql(position_dict=retdict)
        if (position_sql_dict['FLAG'] == 0):
            self._db_insert(position_sql_dict['SQL'])
        else:
            self._db_update(position_sql_dict['SQL'])
        return self._login_session_id

    def OpenForLongOnly(self,paradict:dict):
        instrumentinfodic=self.GetInstrumentInfo(exchange_id=paradict.get("exchangeid"), instrument_id=paradict.get("instrumentid"))
        pricetick=instrumentinfodic.get("pricetick")
        self.qry_depth_market_data(exchange_id=paradict.get("exchangeid"), instrument_id=paradict.get("instrumentid"))
        self.market_order_insert(exchange_id=paradict.get("exchangeid"), instrument_id=paradict.get("instrumentid"),
                                 buysellflag="0", trantype="0",
                                 volume=int(paradict.get("volume")), price=float(self._lastprice)+float(pricetick))
        # time.sleep(1)
        retdict = {}
        retdict['SESSIONID'] = self._login_session_id
        retdict['ORDERSYSID'] = self._ordersysid
        self.qry_investor_position()
        return retdict

    def OpenForShortOnly(self,paradict:dict):
        instrumentinfodic=self.GetInstrumentInfo(exchange_id=paradict.get("exchangeid"), instrument_id=paradict.get("instrumentid"))
        pricetick=instrumentinfodic.get("pricetick")
        self.qry_depth_market_data(exchange_id=paradict.get("exchangeid"), instrument_id=paradict.get("instrumentid"))
        self.market_order_insert(exchange_id=paradict.get("exchangeid"), instrument_id=paradict.get("instrumentid"),
                                 buysellflag="1", trantype="0",
                                 volume=int(paradict.get("volume")), price=float(self._lastprice)-float(pricetick))
        # time.sleep(1)
        retdict = {}
        retdict['SESSIONID'] = self._login_session_id
        retdict['ORDERSYSID'] = self._ordersysid
        self.qry_investor_position()
        return retdict

    def LongToShort(self,paradict:dict):
        instrumentinfodic = self.GetInstrumentInfo(exchange_id=paradict.get("exchangeid"),
                                                   instrument_id=paradict.get("instrumentid"))
        pricetick = instrumentinfodic.get("pricetick")
        self.qry_depth_market_data(exchange_id=paradict.get("exchangeid"), instrument_id=paradict.get("instrumentid"))
        ##平多单
        self.market_order_insert(exchange_id=paradict.get("exchangeid"), instrument_id=paradict.get("instrumentid"),
                                 buysellflag="1", trantype="1",
                                 volume=int(paradict.get("volume")), price=float(self._lastprice) - float(pricetick))
        ##买空单
        self.market_order_insert(exchange_id=paradict.get("exchangeid"), instrument_id=paradict.get("instrumentid"),
                                 buysellflag="1", trantype="0",
                                 volume=int(paradict.get("volume")), price=float(self._lastprice) - float(pricetick))

        # time.sleep(1)
        retdict = {}
        retdict['SESSIONID'] = self._login_session_id
        retdict['ORDERSYSID'] = self._ordersysid
        self.qry_investor_position()
        return retdict

    def ShortToLong(self, paradict: dict):
        instrumentinfodic = self.GetInstrumentInfo(exchange_id=paradict.get("exchangeid"),
                                                   instrument_id=paradict.get("instrumentid"))
        pricetick = instrumentinfodic.get("pricetick")
        self.qry_depth_market_data(exchange_id=paradict.get("exchangeid"), instrument_id=paradict.get("instrumentid"))
        ##平多单
        self.market_order_insert(exchange_id=paradict.get("exchangeid"), instrument_id=paradict.get("instrumentid"),
                                 buysellflag="0", trantype="1",
                                 volume=int(paradict.get("volume")), price=float(self._lastprice) + float(pricetick))
        ##买空单
        self.market_order_insert(exchange_id=paradict.get("exchangeid"), instrument_id=paradict.get("instrumentid"),
                                 buysellflag="0", trantype="0",
                                 volume=int(paradict.get("volume")), price=float(self._lastprice) + float(pricetick))
        # time.sleep(1)
        retdict = {}
        retdict['SESSIONID'] = self._login_session_id
        retdict['ORDERSYSID'] = self._ordersysid
        self.qry_investor_position()
        return retdict

    def CloseForLongOnly(self,paradict:dict):
        instrumentinfodic = self.GetInstrumentInfo(exchange_id=paradict.get("exchangeid"),
                                                   instrument_id=paradict.get("instrumentid"))
        pricetick = instrumentinfodic.get("pricetick")
        self.qry_depth_market_data(exchange_id=paradict.get("exchangeid"), instrument_id=paradict.get("instrumentid"))
        self.market_order_insert(exchange_id=paradict.get("exchangeid"), instrument_id=paradict.get("instrumentid"),
                                 buysellflag="1", trantype="1",
                                 volume=int(paradict.get("volume")), price=float(self._lastprice) - float(pricetick))
        # time.sleep(1)
        retdict = {}
        retdict['SESSIONID'] = self._login_session_id
        retdict['ORDERSYSID'] = self._ordersysid
        self.qry_investor_position()
        return retdict

    def CloseForShortOnly(self,paradict:dict):
        instrumentinfodic = self.GetInstrumentInfo(exchange_id=paradict.get("exchangeid"),
                                                   instrument_id=paradict.get("instrumentid"))
        pricetick = instrumentinfodic.get("pricetick")
        self.qry_depth_market_data(exchange_id=paradict.get("exchangeid"), instrument_id=paradict.get("instrumentid"))
        self.market_order_insert(exchange_id=paradict.get("exchangeid"), instrument_id=paradict.get("instrumentid"),
                                 buysellflag="0", trantype="1",
                                 volume=int(paradict.get("volume")), price=float(self._lastprice) + float(pricetick))
        # time.sleep(1)
        retdict = {}
        retdict['SESSIONID'] = self._login_session_id
        retdict['ORDERSYSID'] = self._ordersysid
        self.qry_investor_position()
        return retdict

    def wait(self):
        # 阻塞 等待
        self._wait_queue.get()
        #input("-------------------------------- 按任意键退出 trader api demo ")

        self.release()
    #执行指令并获取返回值
    def deal_proc_ret(self,trancode,paradict:dict):
        if(trancode=='001'):
            self.settlement_info_confirm()
            #time.sleep(1)
            return self._login_session_id
        elif(trancode=='002'):
            ret=self.qry_investor_position()
            #time.sleep(1)
            return ret
        elif(trancode=='003'):
            self.qry_instrument()
            time.sleep(120)
        elif(trancode=='004'):
            self.qry_instrument_commission_rate(instrument_id=paradict.get("instrumentid"))
        elif(trancode=='005'):
            self.qry_instrument_margin_rate(instrument_id=paradict.get("instrumentid"))
        elif(trancode=='006'):
            self.market_order_insert(exchange_id=paradict.get("exchangeid"),instrument_id=paradict.get("instrumentid"),
                                     buysellflag=paradict.get("buysellflag"),trantype=paradict.get("trantype"),
                                     volume=int(paradict.get("volume")),price=float(paradict.get("price")))
            #time.sleep(1)
            retdict={}
            retdict['SESSIONID']=self._login_session_id
            retdict['ORDERSYSID']=self._ordersysid
            return retdict
        elif(trancode=='007'):
            self.limit_order_insert(exchange_id=paradict.get("exchangeid"),instrument_id=paradict.get("instrumentid"),
                                     buysellflag=paradict.get("buysellflag"),trantype=paradict.get("trantype"),
                                     volume=int(paradict.get("volume")),price=float(paradict.get("price")))
        elif(trancode=='008'):
            self.order_cancel1(exchange_id=paradict.get("exchangeid"),instrument_id=paradict.get("instrumentid"),
                               order_sys_id=paradict.get("ordersysid"))
            time.sleep(1)
        elif(trancode=='009'):
            self.order_cancel2(exchange_id=paradict.get("exchangeid"),instrument_id=paradict.get("instrumentid"),
                               front_id=paradict.get("frontid"),session_id=paradict.get("sessionid"),
                               order_ref=paradict.get("orderref"))
        elif(trancode=='010'):
            self.qry_trading_code(exchange_id=paradict.get("exchangeid"))
        elif(trancode=='011'):
            self.user_password_update(new_password=paradict.get("newpassword"),old_password=paradict.get("oldpassword"))
        elif(trancode=='012'):
            self.qry_order_comm_rate(instrument_id=paradict.get("instrumentid"))
        elif(trancode=='013'):
            self.qry_investor_position(instrument_id=paradict.get("instrumentid"))
        elif(trancode=='014'):
            self.qry_investor_position_detail(instrument_id=paradict.get("instrumentid"))
        elif(trancode=='015'):###查询持仓资金
            ret=self.qry_investor_trading_account()
            return ret

        elif(trancode=='016'):
            self.qry_depth_market_data(exchange_id=paradict.get("exchangeid"),instrument_id=paradict.get("instrumentid"))
            #time.sleep(1)
            return self._lastprice
        elif(trancode=='101'):
            self.OpenForLongOnly(paradict=paradict)
        elif(trancode=='102'):
            self.OpenForShortOnly(paradict=paradict)
        elif(trancode=='105'):
            self.LongToShort(paradict=paradict)
        elif(trancode=='106'):
            self.ShortToLong(paradict=paradict)
        elif(trancode=='107'):
            self.CloseForLongOnly(paradict=paradict)
        elif(trancode=='108'):
            self.CloseForShortOnly(paradict=paradict)



def InitProc(frontinfo:str,user:str,usercode:str,password:str,authcode:str,
             appid:str,brokerid:str,connuser:str,connpass:str,
             conndb:str,tradetype:str,rootpath:str):

    FrontInfo=frontinfo
    User=user
    UserCode=usercode
    Password=password
    Authcode=authcode
    Appid=appid
    BrokerId=brokerid
    ConnUser=connuser
    ConnPass=connpass
    ConnDb=conndb
    TradeType=tradetype
    RootPath=rootpath
    spi = CTdSpiImpl(
        front=FrontInfo,
        user=User,
        usercode=UserCode,
        passwd=Password,
        authcode=Authcode,
        appid=Appid,
        broker_id=BrokerId,
        conn_user=ConnUser,
        conn_pass=ConnPass,
        conn_db=ConnDb,
        trade_type=TradeType,
        root_path=RootPath,
    )
    return spi
def test():
    print("dddddddddd")
if __name__ == "__main__":
    FrontInfo = sys.argv[1]
    User = sys.argv[2]
    UserCode=sys.argv[3]
    Password = sys.argv[4]
    Authcode = sys.argv[5]
    Appid = sys.argv[6]
    BrokerId = sys.argv[7]
    ConnUser = sys.argv[8]
    ConnPass = sys.argv[9]
    ConnDb = sys.argv[10]
    TradeType = sys.argv[11]
    RootPath = sys.argv[12]
    RetType = sys.argv[13]
    ParaDictStr = sys.argv[14]
    ParaDict={}
    if(ParaDictStr!=""):
        tempparalist=ParaDictStr.split(',')
        ParaDict["exchangeid"]=tempparalist[0]
        ParaDict["instrumentid"]=tempparalist[1]
        ParaDict["volume"]=int(tempparalist[2])
    spi = CTdSpiImpl(
        front=FrontInfo,
        user=User,
        usercode=UserCode,
        passwd=Password,
        authcode=Authcode,
        appid=Appid,
        broker_id=BrokerId,
        conn_user=ConnUser,
        conn_pass=ConnPass,
        conn_db=ConnDb,
        trade_type=TradeType,
        root_path=RootPath,
    )
    # 等待登录成功
    while True:
        time.sleep(1)
        if spi.is_login:
            break

    # 代码中的请求参数编写时测试通过, 不保证以后一定成功。
    # 需要测试哪个请求, 取消下面对应的注释, 并按需修改参请求参数即可。
    if RetType=="Y":
        ret=spi.deal_proc_ret(TradeType,ParaDict)
        #return ret
        #time.sleep(1)
    else:
        spi.deal_proc(TradeType,ParaDict)

    spi.wait()