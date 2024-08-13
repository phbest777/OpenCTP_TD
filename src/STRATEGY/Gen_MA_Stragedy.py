import copy
import time
import asyncio
import websockets

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
    bar_ret_dict={}

    def __init__(self, instruments):
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



    def GetOneMinute(self,pDepthMarketData):
        last_update_time = self.bar_dict[pDepthMarketData.InstrumentID]["UpdateTime"]
        is_new_1minute = (pDepthMarketData.UpdateTime[:-2] != last_update_time[:-2]) and pDepthMarketData.UpdateTime != '21:00:00'
        self.bar_dict[pDepthMarketData.InstrumentID]["UpdateMinute"] = pDepthMarketData.UpdateTime[:-3]
        self.bar_dict[pDepthMarketData.InstrumentID]["UpdateTime"] = pDepthMarketData.UpdateTime
        self.bar_dict[pDepthMarketData.InstrumentID]["TradingDay"] = pDepthMarketData.TradingDay
        #print("is_new_1minute is:"+str(is_new_1minute))
        if is_new_1minute:
            if last_update_time == "99:99:99":
                self.bar_dict[pDepthMarketData.InstrumentID]["Volume"] = 0
                self.bar_dict[pDepthMarketData.InstrumentID]["Turnover"] = 0.00
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
                bar_dict_data=self.bar_dict[pDepthMarketData.InstrumentID].copy()
                self.bar_dict[pDepthMarketData.InstrumentID]["HighPrice"]=pDepthMarketData.LastPrice
                self.bar_dict[pDepthMarketData.InstrumentID]["LowPrice"]=pDepthMarketData.LastPrice
                self.bar_dict[pDepthMarketData.InstrumentID]["Volume"] = pDepthMarketData.Volume - self.bar_dict[pDepthMarketData.InstrumentID]["TickVolume"]
                self.bar_dict[pDepthMarketData.InstrumentID]["Turnover"] = pDepthMarketData.Turnover - self.bar_dict[pDepthMarketData.InstrumentID]["TickTurnover"]
                self.bar_dict[pDepthMarketData.InstrumentID]["OneMinOpenPrice"]=pDepthMarketData.LastPrice
                self.bar_dict[pDepthMarketData.InstrumentID]["OneMinPreClosePrice"]=self.bar_dict[pDepthMarketData.InstrumentID]["LastPrice"]
                self.bar_dict[pDepthMarketData.InstrumentID]["TickVolume"] = pDepthMarketData.Volume
                self.bar_dict[pDepthMarketData.InstrumentID]["TickTurnover"] = pDepthMarketData.Turnover
                self.bar_dict[pDepthMarketData.InstrumentID]["PreClosePrice"] = pDepthMarketData.PreClosePrice
                return(self.GetOneMinuteStr(bar_dict_data))

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
        sql="insert into QUANT_FUTURE_MD_ONEMIN (TRADINGDAY,INSTRUMENTID,LASTPRICE,HIGHESTPRICE,LOWESTPRICE,PRESETTLEMENTPRICE" \
              ",PRECLOSEPRICE,PREOPENINTEREST,OPENPRICE,VOLUME,TURNOVER,OPENINTEREST" \
              ",UPDATETIME,UPDATEMINUTE,UPRATIO,INTERESTMINUS,INTERESTRATIO,ONEMINOPENPRICE,AVERPRICE,ONEMINUPRATIO,ONEMINUPPRICE,ONETIMESTAMP )values(" \
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
              ",'"+str(time.time()*1000)[:13]+"')"
        #self.return_str["code"]="002"
        #self.return_str["returnstr"]=sql
        return sql
    def GenTradeSignal(self,):
        self.bar_dict_data[""]
        print('eeeee')
