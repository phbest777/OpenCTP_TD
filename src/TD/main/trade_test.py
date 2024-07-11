"""
    交易API demo
"""
import inspect
import queue
import sys
import time
import datetime
import cx_Oracle

sys.path.append('D:\PythonProject\OpenCTP_TD')
sys.path.append('D:\ProgramData\Anaconda3\envs\CTPAPIDEV')
from openctp_ctp import tdapi

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
    ):
        print("-------------------------------- 启动 trader api demo ")
        super().__init__()
        self._trantype = '007'
        self._front = front
        self._user = user
        self._usercode = usercode
        self._password = passwd
        self._authcode = authcode
        self._appid = appid
        self._broker_id = broker_id

        self._is_authenticate = False
        self._is_login = False

        self._is_last = True
        self._print_max = 2
        self._print_count = 0
        self._total = 0
        self._login_session_id = ''
        self._wait_queue = queue.Queue(2)

        self._api: tdapi.CThostFtdcTraderApi = (
            tdapi.CThostFtdcTraderApi.CreateFtdcTraderApi("D:\\PythonProject\\OpenCTP_TD\\src\\TD\\data\\" + self._user)
        )
        self._conn = cx_Oracle.connect(conn_user, conn_pass, conn_db)
        self._conn_cursor = self._conn.cursor()

        print("初始化数据库成功-------")
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

    @property
    def is_login(self):
        return self._is_login

    def release(self):
        # 释放实例
        self._api.Release()

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
        now = datetime.datetime.now()
        year = now.year
        month = now.month
        day = now.day
        temptime = datetime.datetime(year, month, day, 15, 00)  ##当天下午三点之后的交易算作第二天
        currenttime = datetime.datetime.today()
        if now > temptime:
            currenttime = datetime.datetime.today() + datetime.timedelta(days=1)
        return currenttime.strftime("%Y%m%d")

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
                return retlist.append(f"{999}={'响应为空'}")

            if not is_last:
                self._print_count += 1
                self._total += 1
            else:
                if self._is_login:
                    self._wait_queue.put_nowait(None)

        else:
            if self._print_count < self._print_max:
                if rsp:
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

    def ret_format(self, ret_list: list) -> dict:
        ret_dict = {key.strip(): value for key, sep, value in (item.partition('=') for item in ret_list)}
        return ret_dict

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
        datadate = datetime.datetime.today().strftime("%Y%m%d")
        sql = "insert into QUANT_FUTURE_TRADE_LOGIN(APPID,AUTHCODE,BROKERID,USERID,USERCODE,TRADINGDAY,CZCETIME,DCETIME,FFEXTIME,GFEXTIME,INETIME," \
              "SHFETIME,LOGINTIME,SESSIONID,SYSTEMNAME,TRANCODE,DATADATE) values (" \
              "'" + self._appid + "','" + self._authcode + "','" + self._broker_id + "','" + self._user + "','" + self._usercode + "','" + retdict.get(
            'TradingDay') + "','" + retdict.get('CZCETime') + "','" + retdict.get('DCETime') + "','" \
              + retdict.get('FFEXTime') + "','" + retdict.get('GFEXTime') + "','" + retdict.get(
            'INETime') + "','" + retdict.get('SHFETime') + "','" + retdict.get('LoginTime') + "','" + retdict.get(
            'SessionID') + "','" + retdict.get('SystemName') + "','" \
              + self._trantype + "','" + datadate + "'" + ")"
        login_ret_dict['SQL'] = sql
        login_ret_dict['SESSIONID'] = retdict.get('SessionID')
        return login_ret_dict

    def _get_confirm_ret_sql(self, ret_list: list) -> dict:
        login_ret_dict = {}
        retdict = self.ret_format(ret_list)
        datadate = datetime.datetime.today().strftime("%Y%m%d")
        sql = "insert into QUANT_FUTURE_CONFIRM(APPID,AUTHCODE,BROKERID,USERID,TRADINGDAY,CZCETIME,DCETIME,FFEXTIME,GFEXTIME,INETIME," \
              "SHFETIME,LOGINTIME,SESSIONID,SYSTEMNAME,CONFIRMSTATUS,CONFIRMDATE,CONFIRMTIME,DATADATE) values (" \
              "'" + self._appid + "','" + self._authcode + "','" + self._broker_id + "','" + self._user + "','" + retdict.get(
            'TradingDay') + "','" + retdict.get('CZCETime') + "','" + retdict.get('DCETime') + "','" \
              + retdict.get('FFEXTime') + "','" + retdict.get('GFEXTime') + "','" + retdict.get(
            'INETime') + "','" + retdict.get('SHFETime') + "','" + retdict.get('LoginTime') + "','" + retdict.get(
            'SessionID') + "','" + retdict.get('SystemName') + "','" \
              + "" + "','" + "" + "','" + "" + "','" + datadate + "'" + ")"
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
        retlist = self._check_rsp_ret(pRspInfo, pRspUserLogin)
        ##记录每次登录信息，获取sessionid,用于追踪整个交易链##
        login_ret_dict = self._get_login_ret_sql(ret_list=retlist)
        login_ret_sql = login_ret_dict['SQL']
        self._db_insert(login_ret_sql)
        self._login_session_id = login_ret_dict['SESSIONID']
        if (retlist[0]).split('=')[1] == '000':
            ##根据交易类型写不同的表#######
            if (self._trantype == '001'):
                trandate = self.getcurrdate()
                sqlstr = "select count(*) from QUANT_FUTURE_CONFIRM where tradingday='" + trandate + "'"
                confirm_cnt = self._db_select_cnt(sqlstr=sqlstr)
                if (int(confirm_cnt) > 0):
                    print("交易日:[" + trandate + "]确认单已确认")
                    exit()
                else:
                    confirm_ret_dict = self._get_confirm_ret_sql(ret_list=retlist)
                    confirm_ret_sql = confirm_ret_dict['SQL']
                    self._db_insert(confirm_ret_sql)
                # self._login_session_id=confirm_ret_dict['SESSIONID']
        else:
            return
        self._is_login = True

    # def _get_confirm_ret(self,retdict):

    def settlement_info_confirm(self):
        """投资者结算结果确认"""
        print("> 投资者结算结果确认")

        _req = tdapi.CThostFtdcSettlementInfoConfirmField()
        _req.BrokerID = self._broker_id
        _req.InvestorID = self._user
        self._check_req(_req, self._api.ReqSettlementInfoConfirm(_req, 0))

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
        self._check_req(_req, self._api.ReqQryInstrument(_req, 0))

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
        if (retlist[0]).split('=')[1] != '000':
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

    def qry_depth_market_data(self, instrument_id: str = ""):
        """请求查询行情，只能查询当前快照，不能查询历史行情"""
        print("> 请求查询行情")
        _req = tdapi.CThostFtdcQryDepthMarketDataField()
        # 若不指定合约ID, 则返回所有合约的行情
        _req.InstrumentID = instrument_id
        self._check_req(_req, self._api.ReqQryDepthMarketData(_req, 0))

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
        if (retlist[0]).split('=')[1] != '000':
            return

    def _get_order_req_sql(self, order_dict: dict) -> dict:
        order_req_dict = {}
        retdict = order_dict
        datadate = datetime.datetime.today().strftime("%Y%m%d")
        datatime = datetime.datetime.now().strftime("%H:%M:%S")
        sql = "insert into QUANT_FUTURE_ORDER_REQ(USERCODE,ACCOUNTID,BROKERID,BUSINESSUNIT,CLIENTID,COMBHEDGEFLAG,COMBOFFSETFLAG,CONTINGENTCONDITION," \
              "CURRENCYID,DIRECTION,EXCHANGEID,FORCECLOSEREASON,IPADDRESS,INSTRUMENTID,INVESTORID,ISAUTOSUSPEND,ISSWAPORDER,LIMITPRICE,MACADDRESS," \
              "MINVOLUME,ORDERPRICETYPE,ORDERREF,REQUESTID,STOPPRICE,TIMECONDITION,USERFORCECLOSE,USERID,VOLUMECONDITION,VOLUMETOTALORIGINAL,SESSIONID," \
              "ORDERDATE,ORDERTIME,DATADATE) values (" \
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
              "','" + datadate + "','" + datatime + "','" + datadate + "'" + ")"
        print('order_req_sql is:' + sql)
        order_req_dict['SQL'] = sql
        order_req_dict['SESSIONID'] = retdict.get('SessionID')
        return order_req_dict

    def market_order_insert(
            self, exchange_id: str, instrument_id: str, volume: int = 1
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
        _req.LimitPrice = 1955
        # _req.OrderPriceType = tdapi.THOST_FTDC_OPT_AnyPrice  # 价格类型市价单
        _req.OrderPriceType = tdapi.THOST_FTDC_OPT_LimitPrice
        _req.Direction = tdapi.THOST_FTDC_D_Buy  # 买
        _req.CombOffsetFlag = tdapi.THOST_FTDC_OF_Open  # 开仓
        # _req.Direction=tdapi.THOST_FTDC_D_Sell    #卖
        # _req.CombOffsetFlag= tdapi.THOST_FTDC_OF_Close #平仓
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
        # return
        # exit()

    def limit_order_insert(
            self,
            exchange_id: str,
            instrument_id: str,
            price: float,
            volume: int = 1,
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
        _req.Direction = tdapi.THOST_FTDC_D_Buy  # 买
        _req.CombOffsetFlag = tdapi.THOST_FTDC_OF_Open  # 开仓
        _req.CombHedgeFlag = tdapi.THOST_FTDC_HF_Speculation
        _req.VolumeTotalOriginal = volume
        _req.IsAutoSuspend = 0
        _req.IsSwapOrder = 0
        _req.TimeCondition = tdapi.THOST_FTDC_TC_GFD
        _req.VolumeCondition = tdapi.THOST_FTDC_VC_AV
        _req.ContingentCondition = tdapi.THOST_FTDC_CC_Immediately
        _req.ForceCloseReason = tdapi.THOST_FTDC_FCC_NotForceClose
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
        datadate = datetime.datetime.today().strftime("%Y%m%d")
        datatime = datetime.datetime.now().strftime("%H:%M:%S")
        sql = "insert into QUANT_FUTURE_ORDER_CANCEL_REQ(USERCODE,BROKERID,EXCHANGEID,ACTIONFLAG,FRONTID,IPADDRESS,INSTRUMENTID," \
              "INVESTUNITID,INVESTORID,LIMITPRICE,MACADDRESS,ORDERACTIONREF,ORDERREF,ORDERSYSID,REQUESTID,SESSIONID,USERID,VOLUMECHANGE," \
              "DATATIME,DATADATE) values (" \
              "'" + self._usercode + "','"  + self._broker_id + "','" + retdict.get('ExchangeID') + "','" + str(retdict.get('ActionFlag')) +\
              "','" + str(retdict.get('FrontID')) + "','" + str(retdict.get('IPAddress')) + "','" + retdict.get('InstrumentID') +\
              "','" +str(retdict.get('InvestUnitID')) + "','" + retdict.get('InvestorID') + "'," + str(retdict.get('LimitPrice')) +\
              ",'" +str(retdict.get('MacAddress')) + "','" + str(retdict.get('OrderActionRef')) + "','"+ str(retdict.get('OrderRef')) + "','" + str(retdict.get('OrderSysID')) + \
              "','" + str(retdict.get('RequestID'))+ "','" + retdict.get('SessionID') + "','" + retdict.get('UserID') + \
              "'," + str(retdict.get('VolumeChange')) + ",'"+ datatime + "','" + datadate + "'" + ")"
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
        cancel_order_dict={}
        cancel_order_dict['ActionFlag']=_req.ActionFlag
        cancel_order_dict['BrokerID']=_req.BrokerID
        cancel_order_dict['ExchangeID']=_req.ExchangeID
        cancel_order_dict['FrontID']='0'
        cancel_order_dict['IPAddress']=''
        cancel_order_dict['InstrumentID']=_req.InstrumentID
        cancel_order_dict['InvestUnitID']=''
        cancel_order_dict['InvestorID']=_req.InvestorID
        cancel_order_dict['LimitPrice']=0.0
        cancel_order_dict['MacAddress']=''
        cancel_order_dict['OrderActionRef']=0
        cancel_order_dict['OrderRef']=''
        cancel_order_dict['OrderSysID']=order_sys_id
        cancel_order_dict['RequestID']=0
        cancel_order_dict['SessionID']=self._login_session_id
        cancel_order_dict['UserID']=_req.UserID
        cancel_order_dict['VolumeChange']=0

        order_cancel_req_dict = self._get_order_cancel_req_sql(order_dict=cancel_order_dict)
        sql = order_cancel_req_dict['SQL']
        self._db_insert(sqlstr=sql)
        # 若成功，会通过 报单回报 返回新的订单状态, 若失败则会响应失败
        self._check_req(_req, self._api.ReqOrderAction(_req, 0))
        #exit()
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
        datadate = datetime.datetime.today().strftime("%Y%m%d")
        sql = "insert into QUANT_FUTURE_ORDER_RET(USERCODE,ACCOUNTID,ACTIVETIME,ACTIVETRADERID,ACTIVEUSERID,BRANCHID,BROKERID,BROKERORDERSEQ,BUSINESSUNIT," \
              "CANCELTIME,CLEARINGPARTID,CLIENTID,COMBHEDGEFLAG,COMBOFFSETFLAG,CONTINGENTCONDITION,CURRENCYID,DIRECTION,EXCHANGEID,EXCHANGEINSTID,FORCECLOSEREASON," \
              "FRONTID,IPADDRESS,INSERTDATE,INSERTTIME,INSTALLID,INSTRUMENTID,INVESTUNITID,INVESTORID,ISAUTOSUSPEND,ISSWAPORDER,LIMITPRICE," \
              "MACADDRESS,MINVOLUME,NOTIFYSEQUENCE,ORDERLOCALID,ORDERPRICETYPE,ORDERREF,ORDERSOURCE,ORDERSTATUS,ORDERSUBMITSTATUS,ORDERSYSID,ORDERTYPE," \
              "PARTICIPANTID,RELATIVEORDERSYSID,REQUESTID,SEQUENCENO,SESSIONID,SETTLEMENTID,STATUSMSG,STOPPRICE,SUSPENDTIME,TIMECONDITION,TRADERID," \
              "TRADINGDAY,UPDATETIME,USERFORCECLOSE,USERID,USERPRODUCTINFO,VOLUMECONDITION,VOLUMETOTAL,VOLUMETOTALORIGINAL,VOLUMETRADED,ZCETOTALTRADEDVOLUME," \
              "DATADATE) values (" \
              "'" + self._usercode + "','" + str(retdict.get('AccountID')) + "','" + str(retdict.get('ActiveTime')) + "','" + str(retdict.get('ActiveTraderID')) + \
              "','" + str(retdict.get('ActiveUserID')) + "','" + str(retdict.get('BranchID')) + "','" + str(retdict.get('BrokerID')) + "','" + str(retdict.get('BrokerOrderSeq')) + \
              "','" + str(retdict.get('BusinessUnit')) + "','" + str(retdict.get('CancelTime')) + "','" + retdict.get('ClearingPartID') + "','" + str(retdict.get('ClientID')) +\
              "','" + str(retdict.get('CombHedgeFlag')) + "','" + retdict.get('CombOffsetFlag') + "','" + retdict.get('ContingentCondition') + "','" + str(retdict.get('CurrencyID')) + "','" + str(retdict.get('Direction')) + \
              "','" + retdict.get('ExchangeID') + "','" + retdict.get('ExchangeInstID') + "','" + str(retdict.get('ForceCloseReason')) + "','" + str(retdict.get('FrontID')) +\
              "','" + str(retdict.get('IPAddress')) + "','" + retdict.get('InsertDate') + "','" + retdict.get('InsertTime') + "','" + str(retdict.get('InstallID')) +\
              "','" + str(retdict.get('InstrumentID')) + "','" + str(retdict.get('InvestUnitID')) + "','" + str(retdict.get('InvestorID')) + "','" + str(retdict.get('IsAutoSuspend')) + \
              "','" + str(retdict.get('IsSwapOrder')) + "'," + str(retdict.get('LimitPrice')) + ",'" + str(retdict.get('MacAddress')) + "'," + str(retdict.get('MinVolume')) + \
              ",'" + str(retdict.get('NotifySequence')) + "','" + retdict.get('OrderLocalID') + "','" + str(retdict.get('OrderPriceType')) + "','" + retdict.get('OrderRef') + \
              "','" + str(retdict.get('OrderSource')) + "','" + str(retdict.get('OrderStatus')) + "','" + str(retdict.get('OrderSubmitStatus')) + "','" + retdict.get('OrderSysID') + \
              "','" + str(retdict.get('OrderType')) + "','" + str(retdict.get('ParticipantID')) + "','" + str(retdict.get('RelativeOrderSysID')) + "','" + str(retdict.get('RequestID')) + \
              "','" + str(retdict.get('SequenceNo')) + "','" + str(retdict.get('SessionID')) + "','" + str(retdict.get('SettlementID')) + "','" + str(retdict.get('StatusMsg')) + \
              "'," + str(retdict.get('StopPrice')) + ",'" + str(retdict.get('SuspendTime')) + "','" + str(retdict.get('TimeCondition')) + "','" + str(retdict.get('TraderID')) + \
              "','" + str(retdict.get('TradingDay')) + "','" + str(retdict.get('UpdateTime')) + "','" + str(retdict.get('UserForceClose')) + "','" + str(retdict.get('UserID')) + \
              "','" + str(retdict.get('UserProductInfo')) + "','" + str(retdict.get('VolumeCondition')) + "'," + str(retdict.get('VolumeTotal')) + "," + str(retdict.get('VolumeTotalOriginal')) + \
              "," + str(retdict.get('VolumeTraded')) + "," + str(retdict.get('ZCETotalTradedVolume')) + ",'" + datadate + "'" + ")"
        print('tempsql is:' + sql)
        order_ret_dict['SQL'] = sql
        order_ret_dict['SESSIONID'] = retdict.get('SessionID')
        return order_ret_dict

    def OnRtnOrder(self, pOrder: tdapi.CThostFtdcOrderField):
        """报单通知，当执行ReqOrderInsert后并且报出后，收到返回则调用此接口，私有流回报。"""
        retlist = self.print_rsp_rtn("报单通知", pOrder)
        order_ret_dic = self.ret_format(ret_list=retlist)
        order_sql_dic=self._get_order_ret_sql(order_dict=order_ret_dic)
        sql=order_sql_dic['SQL']
        self._db_insert(sql)
        #print("order sql is:"+sql)
        #print("dic is:" + order_ret_dic['OrderLocalID'])
        # time.sleep(5)
        #exit()
        # self.release()

    def _get_order_deal_sql(self, order_dict: dict) -> dict:
        order_deal_dict = {}
        retdict = order_dict
        datadate = datetime.datetime.today().strftime("%Y%m%d")
        sql = "insert into QUANT_FUTURE_TRADE_DEAL(USERCODE,BROKERID,BROKERORDERSEQ,BUSINESSUNIT,CLEARINGPARTID,CLIENTID,DIRECTION,EXCHANGEID,EXCHANGEINSTID," \
              "HEDGEFLAG,INSTRUMENTID,INVESTUNITID,INVESTORID,OFFSETFLAG,ORDERLOCALID,ORDERREF,ORDERSYSID,PARTICIPANTID,PRICE,PRICESOURCE," \
              "SEQUENCENO,SETTLEMENTID,TRADEDATE,TRADEID,TRADESOURCE,TRADETIME,TRADETYPE,TRADERID,TRADINGDAY,TRADINGROLE,USERID," \
              "VOLUME,STATUS,DATADATE) values (" \
              "'" + self._usercode + "','" + str(retdict.get('BrokerID')) + "','" + str(retdict.get('BrokerOrderSeq')) + "','" + str(retdict.get('BusinessUnit')) + \
              "','" + str(retdict.get('ClearingPartID')) + "','" + str(retdict.get('ClientID')) + "','" + str(retdict.get('Direction')) + "','" + str(retdict.get('ExchangeID')) + \
              "','" + str(retdict.get('ExchangeInstID')) + "','" + str(retdict.get('HedgeFlag')) + "','" + retdict.get('InstrumentID') + "','" + str(retdict.get('InvestUnitID')) +\
              "','" + str(retdict.get('InvestorID')) + "','" + str(retdict.get('OffsetFlag')) + "','" + retdict.get('OrderLocalID') + "','" + str(retdict.get('OrderRef')) + "','" + str(retdict.get('OrderSysID')) + \
              "','" + retdict.get('ParticipantID') + "'," + retdict.get('Price') + ",'" + str(retdict.get('PriceSource')) + "','" + str(retdict.get('SequenceNo')) +\
              "','" + str(retdict.get('SettlementID')) + "','" + retdict.get('TradeDate') + "','" + str(retdict.get('TradeID')) + "','" + str(retdict.get('TradeSource')) +\
              "','" + str(retdict.get('TradeTime')) + "','" + str(retdict.get('TradeType')) + "','" + str(retdict.get('TraderID')) + "','" + str(retdict.get('TradingDay')) + \
              "','" + str(retdict.get('TradingRole')) + "'," + str(retdict.get('UserID')) + "," + retdict.get('Volume') + ",'1','"  + datadate + "'" + ")"
        print('tempsql is:' + sql)
        order_deal_dict['SQL'] = sql
        order_deal_dict['SESSIONID'] = retdict.get('SessionID')
        return order_deal_dict

    def OnRtnTrade(self, pTrade: tdapi.CThostFtdcTradeField):
        """成交通知，报单发出后有成交则通过此接口返回。私有流"""
        retlist=self.print_rsp_rtn("成交通知", pTrade)
        order_deal_dic=self.ret_format(ret_list=retlist)
        order_sql_dic=self._get_order_deal_sql(order_dict=order_deal_dic)
        sql = order_sql_dic['SQL']
        self._db_insert(sql)
        exit()
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

    def wait(self):
        # 阻塞 等待
        # while not self._wait_queue.empty():
        #    time.sleep(2)
        self._wait_queue.get()
        # input()
        # try:
        #    self._wait_queue.get()
        # except queue.Empty:
        self.release()
        # exit()
        # exit()
        # input("-------------------------------- 按任意键退出 trader api demo ")


if __name__ == "__main__":
    spi = CTdSpiImpl(
        config.fronts["电信1"]["td"],
        config.user,
        'phbest',
        config.password,
        config.authcode,
        config.appid,
        config.broker_id,
        config.conn_user,
        config.conn_pass,
        config.conn_db,
    )
    # 等待登录成功
    while True:
        time.sleep(1)
        if spi.is_login:
            break

    # 代码中的请求参数编写时测试通过, 不保证以后一定成功。
    # 需要测试哪个请求, 取消下面对应的注释, 并按需修改参请求参数即可。

    #spi.settlement_info_confirm()
    # spi.qry_instrument()
    # spi.qry_instrument(exchange_id="CZCE")
    # spi.qry_instrument(product_id="AP")
    # spi.qry_instrument(instrument_id="AP404")
    # spi.qry_instrument_commission_rate("br2409")
    # spi.qry_instrument_commission_rate("ZC309")
    # spi.qry_instrument_margin_rate()
    # spi.qry_instrument_margin_rate(instrument_id="ZC309")
    # spi.qry_depth_market_data()
    # spi.qry_depth_market_data(instrument_id="ZC309")
    #spi.market_order_insert("CZCE", "SA409", 5)
    # spi.limit_order_insert("CZCE", "CF411", 15000)
    #spi.order_cancel1("CZCE", "SA409", "      354936")
    # spi.order_cancel2("CZCE", "CF411", 1, -1111111, "3")
    # spi.qry_trading_code("CZCE")
    # spi.qry_exchange("DCE")
    # spi.user_password_update("sWJedore20@#0808", "sWJedore20@#0807")
    # spi.qry_order_comm_rate("ss2407")
    #spi.qry_investor_position("SA409")
    spi.qry_investor_position_detail("SA409")

    spi.wait()
