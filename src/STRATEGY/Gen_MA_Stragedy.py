import copy
import time
import asyncio
import websockets
import cx_Oracle
class OneMinuteTick:
    OneMinuteDic = {
        "InstrumentID": "",
        "UpdateTime": "99:99:99",
        "UpdateMinute": "99:99",
        "LastPrice": 0.00,
        "HighPrice": 0.00,
        "LowPrice": 9999999.00,
        "OpenPrice": 0.00,
        "OneMinOpenPrice": 0.00,
        "PreSettlementPrice": 0.00,
        "PreClosePrice": 0.00,
        "OneMinPreClosePrice": 1.00,
        "TickVolume": 0,
        "Volume": 0,
        "TickTurnover": 0.00,
        "Turnover": 0.00,
        "OpenInterest": 0,
        "PreOpenInterest": 0,
        "MinusInterest": 0,
        "MA5":0.00,
        "MA10":0.00,
        "MA20":0.00,
        "TradingDay": "99999999",
    }
    OneMinuteData = {
        "InstrumentID": "",
        "UpdateTime": "99:99:99",
        "UpdateMinute": "99:99",
        "LastPrice": 0.00,
        "HighPrice": 0.00,
        "LowPrice": 9999999.00,
        "OpenPrice": 0.00,
        "PreSettlementPrice": 0.00,
        "PreClosePrice": 0.00,
        "TickVolume": 0,
        "Volume": 0,
        "TickTurnover": 0.00,
        "Turnover": 0.00,
        "OpenInterest": 0,
        "PreOpenInterest": 0,
        "MinusInterest": 0,
        "TradingDay": "99999999",
    }
    return_str={
        "code":000,
        "returnstr":""
    }
    bar_dict = {}
    bar_dict_data={}

    def __init__(self, instruments,conn,cursor):
        self._conn = conn
        self._conn_cursor=cursor
        self._ave_MA5=0.00
        self._ave_MA10=0.00
        self._ave_MA20=0.00
        '''
        self.OneMinuteDic["InstrumentID"] = pDepthMarketData.InstrumentID
        self.OneMinuteDic["UpdateTime"]=pDepthMarketData.UpdateTime
        self.OneMinuteDic["LastPrice"]=pDepthMarketData.LastPrice

        print("hello:"+pDepthMarketData.InstrumentID+",price is:"+pDepthMarketData.LastPrice+",high price is:"+)
        '''
        for instrument_id in instruments:
            # 初始化Bar字段
            self.OneMinuteDic["InstrumentID"] = instrument_id
            self.bar_dict[instrument_id] = self.OneMinuteDic.copy()
            # print("bar_dict is"+str(self.bar_dict[instrument_id]["LastPrice"]))
    def _db_select_rows_list(self,sqlstr:str)->list:
        self._conn_cursor.execute(sqlstr)
        columns = [col[0] for col in self._conn_cursor.description]
        rows = self._conn_cursor.fetchall()
        result_list = [dict(zip(columns, row)) for row in rows]
        #print("select sql is:"+sqlstr)
        #self._conn_cursor.close()
        return result_list
    def _db_update(self, sqlstr: str):
        self._conn_cursor.execute(sqlstr)
        self._conn.commit()
        print("[" + sqlstr + "]" + "更新数据库成功")

    def GetOneMinute(self,pDepthMarketData):
        last_update_time = self.bar_dict[pDepthMarketData.InstrumentID]["UpdateTime"]
        is_new_1minute = (pDepthMarketData.UpdateTime[:-2] != last_update_time[:-2]) and pDepthMarketData.UpdateTime != '21:00:00'
        self.bar_dict[pDepthMarketData.InstrumentID]["UpdateMinute"] = pDepthMarketData.UpdateTime[:-3]
        self.bar_dict[pDepthMarketData.InstrumentID]["UpdateTime"] = pDepthMarketData.UpdateTime
        self.bar_dict[pDepthMarketData.InstrumentID]["TradingDay"] = pDepthMarketData.TradingDay
        #print("is_new_1minute is:"+str(is_new_1minute))
        if is_new_1minute:
            ave_sql = "select * from QUANT_FUTURE_AVE_HIS where instrumentid='" + pDepthMarketData.InstrumentID + "'"
            # print('ave_sql is:'+ave_sql)
            ave_list = self._db_select_rows_list(sqlstr=ave_sql)
            self._ave_MA5 = float(ave_list[0].get('ONEMINMA5'))
            self._ave_MA10 = float(ave_list[0].get('ONEMINMA10'))
            self._ave_MA20 = float(ave_list[0].get('ONEMINMA20'))
            print("ini ma5 is:"+str(self._ave_MA5))
            print("ini ma10 is:" + str(self._ave_MA10))
            print("ini ma20 is:" + str(self._ave_MA20))
            if last_update_time == "99:99:99":
                self.bar_dict[pDepthMarketData.InstrumentID]["Volume"] = 0
                self.bar_dict[pDepthMarketData.InstrumentID]["Turnover"] = 0.00
                self.bar_dict[pDepthMarketData.InstrumentID]["MA5"] = self._ave_MA5
                self.bar_dict[pDepthMarketData.InstrumentID]["MA10"] = self._ave_MA10
                self.bar_dict[pDepthMarketData.InstrumentID]["MA20"] = self._ave_MA20
                print("first ma5 is:" + str(self._ave_MA5))
                print("first ma10 is:" + str(self._ave_MA10))
                print("first ma20 is:" + str(self._ave_MA20))
                '''
                if pDepthMarketData.UpdateTime[:-3]=="09:00":
                    self.bar_dict[pDepthMarketData.InstrumentID]["HighPrice"] = pDepthMarketData.LastPrice
                    self.bar_dict[pDepthMarketData.InstrumentID]["LowPrice"] = pDepthMarketData.LastPrice
                    self.bar_dict[pDepthMarketData.InstrumentID]["TickVolume"] = 0
                    self.bar_dict[pDepthMarketData.InstrumentID]["TickTurnover"] = 0.00
                    self.bar_dict[pDepthMarketData.InstrumentID]["OpenInterest"] = pDepthMarketData.OpenInterest
                    self.bar_dict[pDepthMarketData.InstrumentID]["PreOpenInterest"] = pDepthMarketData.PreOpenInterest
                    self.bar_dict[pDepthMarketData.InstrumentID]["PreSettlementPrice"] = pDepthMarketData.PreSettlementPrice
                    self.bar_dict[pDepthMarketData.InstrumentID]["PreClosePrice"] = pDepthMarketData.PreClosePrice
                    self.bar_dict[pDepthMarketData.InstrumentID]["OneMinPreClosePrice"]=1.0
                    temp_Minute_bar_dic=self.bar_dict[pDepthMarketData.InstrumentID].copy()
                    return(self.GetOneMinuteStr(temp_Minute_bar_dic))
                '''

            else:


                #ave_MA60=float(ave_list[0].get('ONEMINMA60'))

                oneMinuteDic_Temp = self.bar_dict[pDepthMarketData.InstrumentID].copy()
                self.bar_dict[pDepthMarketData.InstrumentID]["HighPrice"]=pDepthMarketData.LastPrice
                self.bar_dict[pDepthMarketData.InstrumentID]["LowPrice"]=pDepthMarketData.LastPrice
                self.bar_dict[pDepthMarketData.InstrumentID]["Volume"] = pDepthMarketData.Volume - self.bar_dict[pDepthMarketData.InstrumentID]["TickVolume"]
                self.bar_dict[pDepthMarketData.InstrumentID]["Turnover"] = pDepthMarketData.Turnover - self.bar_dict[pDepthMarketData.InstrumentID]["TickTurnover"]
                self.bar_dict[pDepthMarketData.InstrumentID]["OneMinOpenPrice"]=pDepthMarketData.LastPrice
                self.bar_dict[pDepthMarketData.InstrumentID]["OneMinPreClosePrice"]=self.bar_dict[pDepthMarketData.InstrumentID]["LastPrice"]
                self.bar_dict[pDepthMarketData.InstrumentID]["TickVolume"] = pDepthMarketData.Volume
                self.bar_dict[pDepthMarketData.InstrumentID]["TickTurnover"] = pDepthMarketData.Turnover
                self.bar_dict[pDepthMarketData.InstrumentID]["PreClosePrice"] = pDepthMarketData.PreClosePrice
                #self.bar_dict[pDepthMarketData.InstrumentID]["MA5"] = ave_MA5
                #self.bar_dict[pDepthMarketData.InstrumentID]["MA10"] = ave_MA10
                #self.bar_dict[pDepthMarketData.InstrumentID]["MA20"] = ave_MA20
                return(self.GetOneMinuteStr(oneMinuteDic_Temp))

        else:
            self.bar_dict[pDepthMarketData.InstrumentID]["LastPrice"] = pDepthMarketData.LastPrice
            self.bar_dict[pDepthMarketData.InstrumentID]["Volume"] += pDepthMarketData.Volume - self.bar_dict[pDepthMarketData.InstrumentID]["TickVolume"]
            self.bar_dict[pDepthMarketData.InstrumentID]["TickVolume"] = pDepthMarketData.Volume
            self.bar_dict[pDepthMarketData.InstrumentID]["Turnover"] += pDepthMarketData.Turnover - self.bar_dict[pDepthMarketData.InstrumentID]["TickTurnover"]
            self.bar_dict[pDepthMarketData.InstrumentID]["TickTurnover"] = pDepthMarketData.Turnover
            self.bar_dict[pDepthMarketData.InstrumentID]["OpenInterest"] = pDepthMarketData.OpenInterest
            self.bar_dict[pDepthMarketData.InstrumentID]["PreOpenInterest"] = pDepthMarketData.PreOpenInterest
            self.bar_dict[pDepthMarketData.InstrumentID]["MinusInterest"] = pDepthMarketData.OpenInterest - pDepthMarketData.PreOpenInterest
            self.bar_dict[pDepthMarketData.InstrumentID]["PreSettlementPrice"] = pDepthMarketData.PreSettlementPrice
            self.bar_dict[pDepthMarketData.InstrumentID]["PreClosePrice"] = pDepthMarketData.PreClosePrice
            if self.bar_dict[pDepthMarketData.InstrumentID]["HighPrice"] <= pDepthMarketData.LastPrice:
                self.bar_dict[pDepthMarketData.InstrumentID]["HighPrice"] = pDepthMarketData.LastPrice
            if self.bar_dict[pDepthMarketData.InstrumentID]["LowPrice"] >= pDepthMarketData.LastPrice:
                self.bar_dict[pDepthMarketData.InstrumentID]["LowPrice"] = pDepthMarketData.LastPrice
            self.bar_dict[pDepthMarketData.InstrumentID]["OpenPrice"] = pDepthMarketData.OpenPrice
        return "ddd"
        '''
        sql = "insert into QUANT_FUTURE_MD_ONEMIN (TRADINGDAY,INSTRUMENTID,LASTPRICE,HIGHESTPRICE,LOWESTPRICE,PRESETTLEMENTPRICE" \
              ",PRECLOSEPRICE,PREOPENINTEREST,OPENPRICE,VOLUME,TURNOVER,OPENINTEREST" \
              ",UPDATETIME,UPDATEMINUTE,UPRATIO,INTERESTMINUS,INTERESTRATIO)values(" \
              "'" + pDepthMarketData.TradingDay + "','" + self.bar_dict[pDepthMarketData.InstrumentID]["InstrumentID"] + \
              "'," + str(self.bar_dict[pDepthMarketData.InstrumentID]["LastPrice"]) + \
              "," + str(self.bar_dict[pDepthMarketData.InstrumentID]["HighPrice"]) + \
              "," + str(self.bar_dict[pDepthMarketData.InstrumentID]["LowPrice"]) + \
              "," + str(self.bar_dict[pDepthMarketData.InstrumentID]["PreSettlementPrice"]) + \
              "," + str(self.bar_dict[pDepthMarketData.InstrumentID]["PreClosePrice"]) + \
              "," + str(self.bar_dict[pDepthMarketData.InstrumentID]["PreOpenInterest"]) + \
              "," + str(self.bar_dict[pDepthMarketData.InstrumentID]["OpenPrice"]) + \
              "," + str(self.bar_dict[pDepthMarketData.InstrumentID]["Volume"]) + \
              "," + str(self.bar_dict[pDepthMarketData.InstrumentID]["Turnover"]) + \
              "," + str(self.bar_dict[pDepthMarketData.InstrumentID]["OpenInterest"]) + \
              ",'" + self.bar_dict[pDepthMarketData.InstrumentID]["UpdateTime"] + \
              "','" + self.bar_dict[pDepthMarketData.InstrumentID]["UpdateMinute"] + \
              "'," + str((pDepthMarketData.LastPrice - pDepthMarketData.PreSettlementPrice) / pDepthMarketData.PreSettlementPrice) + \
              "," + str(self.bar_dict[pDepthMarketData.InstrumentID]["MinusInterest"]) + \
              "," + str((pDepthMarketData.OpenInterest - pDepthMarketData.PreOpenInterest) / pDepthMarketData.PreOpenInterest) + ")"

        # print("sqlstr is:"+sql)
        #self.return_str["code"] = "001"
        #self.return_str["returnstr"]=sql
        #return self.return_str
        #sql="is_new_1minute is:"+str(is_new_1minute)
        return sql
        '''
    def GetOneMinuteStr(self,oneminuteDic):
        latest_ma5=(self._ave_MA5*4+oneminuteDic["LastPrice"])/5
        latest_ma10=(self._ave_MA10*9+oneminuteDic["LastPrice"])/10
        latest_ma20=(self._ave_MA20*19+oneminuteDic["LastPrice"])/20
        self._ave_MA5=latest_ma5
        self._ave_MA10=latest_ma10
        self._ave_MA20=latest_ma20
        print("update ma5 is:" + str(self._ave_MA5))
        print("update ma10 is:" + str(self._ave_MA10))
        print("update ma20 is:" + str(self._ave_MA20))
        sql="insert into QUANT_FUTURE_MD_ONEMIN (TRADINGDAY,INSTRUMENTID,LASTPRICE,HIGHESTPRICE,LOWESTPRICE,PRESETTLEMENTPRICE" \
              ",PRECLOSEPRICE,PREOPENINTEREST,OPENPRICE,VOLUME,TURNOVER,OPENINTEREST" \
              ",UPDATETIME,UPDATEMINUTE,UPRATIO,INTERESTMINUS,INTERESTRATIO,ONEMINOPENPRICE,AVERPRICE,ONEMINUPRATIO,ONEMINUPPRICE,ONETIMESTAMP,MA5,MA10,MA20 )values(" \
              "'" + oneminuteDic["TradingDay"] + "','" + oneminuteDic["InstrumentID"] + \
              "'," + str(oneminuteDic["LastPrice"]) + \
              "," + str(oneminuteDic["HighPrice"]) + \
              "," + str(oneminuteDic["LowPrice"]) + \
              "," + str(oneminuteDic["PreSettlementPrice"]) + \
              "," + str(oneminuteDic["PreClosePrice"]) + \
              "," + str(oneminuteDic["PreOpenInterest"]) + \
              "," + str(oneminuteDic["OpenPrice"]) + \
              "," + str(oneminuteDic["Volume"]) + \
              "," + str(oneminuteDic["Turnover"]) + \
              "," + str(oneminuteDic["OpenInterest"]) + \
              ",'" + oneminuteDic["UpdateTime"] + \
              "','" + oneminuteDic["UpdateMinute"] + \
              "'," + str((oneminuteDic["LastPrice"] - oneminuteDic["PreSettlementPrice"]) / oneminuteDic["PreSettlementPrice"]) + \
              "," + str(oneminuteDic["MinusInterest"]) + \
              "," + str((oneminuteDic["OpenInterest"] - oneminuteDic["PreOpenInterest"]) / oneminuteDic["PreOpenInterest"]) +\
              ","+str(oneminuteDic["OneMinOpenPrice"])+\
              ","+str(oneminuteDic["TickTurnover"] / oneminuteDic["TickVolume"])+\
              ","+str((oneminuteDic["LastPrice"] - oneminuteDic["OneMinPreClosePrice"]) / oneminuteDic["OneMinPreClosePrice"])+\
              ","+str(oneminuteDic["LastPrice"] - oneminuteDic["OneMinPreClosePrice"])+\
              ",'"+str(time.time()*1000)[:13]+"',"+str(latest_ma5)+\
              ","+str(latest_ma10)+\
              ","+str(latest_ma20)+")"
        up_ave_sql="update QUANT_FUTURE_AVE_HIS set ONEMINMA5="+str(latest_ma5)+",ONEMINMA10="+str(latest_ma10)+\
                   ",ONEMINMA20="+str(latest_ma20)+" where instrumentid='"+oneminuteDic["InstrumentID"]+"'"
        #print(up_ave_sql)
        self._db_update(sqlstr=up_ave_sql)
        #self.return_str["code"]="002"
        #self.return_str["returnstr"]=sql
        return sql
