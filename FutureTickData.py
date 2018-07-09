import os
import re
import csv
import json
import shutil
from datetime import *
import time
import copy
import timeit
import pandas as pd
import numpy as np
import zipfile
import rarfile
from dataUlt import *
from HdfUtility import *

class HisFutureTick(object):
    #----------------------------------------------------------------------
    def __init__(self, data_path, data_temp, bar_path ):
        """Constructor"""
        self.data_path = data_path
        self.data_temp = data_temp
        self.bar_path = bar_path
        self.hdf = HdfUtility()
    def packedTick2Bar(self,file_packedtick_ex= ['night','.txt'], file_unpacked_ex = ['Survey.txt'], path_temp='temp',
                       path_output_bar='bar', freq=['5T','15T','30T','H']):
        '''packed tick data 2 Bar data'''
        start_time = timeit.default_timer()
        rarpaths = []
        #   get the rar file
        for root, dirs, files in os.walk(self.data_path):
            for file in files:
                filepath = os.path.join(root, file)
                if os.path.splitext(filepath)[-1] == '.rar':
                    rarpaths.append(filepath)
        #   iterate every rar file
        for rarpath in rarpaths:
            print(rarpath)
            try:
                f = rarfile.RarFile(rarpath)
                f.extractall(path=self.data_temp,pwd='www.jinshuyuan.net')
            except:
                print('存在损坏文件.\n')
            else:
                pass
            #   get the tick data file
            files_tick = self.listFiles(path =self.data_temp)
            file_SN_df = self.getSeriesNum(tickfiles=files_tick)
            #   get the bar data and save it in the path_output_bar, file name is as TickerSim_freq.csv
            for idx, row in file_SN_df.iterrows():
                symbol = row[EXT_Info_TickerSim]
                exchange = row[EXT_Info_Exchange]
                print(exchange,symbol,row[EXT_Info_TradeDate])
                #1 min bar
                bar1m = self.tick2Bar1m(filename_tick = row[EXT_Info_File], tradetime=['AM', 'PM'])
                bar1m.insert(0,EXT_Bar_Asset,bar1m.Ticker+'.'+exchange)
                bar1m.drop('Ticker',axis=1,inplace=True)
                bar1m_fm=bar1m.reset_index().set_index([EXT_Bar_Date,EXT_Bar_Asset])
                #self.hdf.hdfWrite(self.bar_path,exchange,symbol,bar1m_fm,EXT_Rawdata,None,EXT_Period_1m)
                #other freq bars
                #-------------------------------------
                # new part
                tradetime = ['AM', 'PM']
                #get info
                tickerSim = symbol
                tradeDate = row[EXT_Info_TradeDate]
                timeRange = self.getTradeTimeRange(tickerSim, type_l=tradetime)
                #-------------------------------------
                for fr in freq:
                    #getResampleBar新增个参数
                    bars_fr = self.getResampleBar(bardata1m=bar1m,tradetime = timeRange, tradeDate =tradeDate, freq=fr)
                    bars_fr.insert(0,EXT_Bar_Asset,bar1m[EXT_Bar_Asset])
                    bars_fr = bars_fr.reset_index()
                    bars_fr.rename(columns={'index':EXT_Bar_Date},inplace = True)
                    bars_fr_fm=bars_fr.set_index([EXT_Bar_Date,EXT_Bar_Asset])
                    self.hdf.hdfWrite(self.bar_path,exchange,symbol,bars_fr_fm,EXT_Rawdata,None,EXT_Freq_Period[fr])
            shutil.rmtree(self.data_temp)
        elapsed = timeit.default_timer() - start_time
        print("--- %s seconds ---" % elapsed)
        return

    #----------------------------------------------------------------------
    #TODO: finish the evening trading tick data in future
    #def tick2Bar1m(self, filename_tick, exchange=EXT_EXCHANGE_CFE, tickerSim=EXT_CFE_IF, dateStr='20160104', tradetime = ['AM', 'PM'] ):
    def tick2Bar1m(self, filename_tick, tradetime = ['AM', 'PM'] ):
        """Tick data to Bar 1Min data"""
        #get info
        info = self.getTickDataInfo(unpackedFilenameStr = filename_tick)
        exchange = info[EXT_Info_Exchange]
        ticker = info[EXT_Info_Ticker]
        tickerSim = info[EXT_Info_TickerSim]
        tradeDate = info[EXT_Info_TradeDate]
        ## read the raw tick data from file
        #code = exchange
        #if exchange == None:
            #code = tickerSim
        #header_dict = EXT_TICK2Bar_Dict[info[EXT_Info_Exchange]]
        csvreader = csv.reader(open(filename_tick))
        tick_rawdata = pd.DataFrame([row for row in csvreader])
        tick_rawdata.columns = tick_rawdata.ix[0,:].tolist()
        tick_rawdata = tick_rawdata.drop([0])
        # 换手率 ＝ 成交量 / 持仓量
        tick_rawdata['换手率'] = tick_rawdata['成交量'].astype(int) / tick_rawdata['持仓'].astype(int)
        header_dict = EXT_NewData_Header
        #TODO: add the evening trading in future
        timeRange = self.getTradeTimeRange(tickerSim, type_l=tradetime)

        if tick_rawdata.size <= 0:
            #empyt file
            data_empty = copy.deepcopy(header_dict)
            for k in data_empty.keys():
                data_empty[k] = np.NaN
            data_empty[EXT_Bar_Date]=pd.to_datetime(tradeDate+' '+timeRange[-1][-1])
            bar1min_fmt = pd.DataFrame(data_empty, index=[0])
            bar1min_fmt.set_index(EXT_Bar_Date, inplace=True)
        else:
            ## get cleared tick data
            tick_data = pd.DataFrame(index=tick_rawdata.index)
            for x in header_dict.keys():
                if x == EXT_Bar_Ticker:
                    tick_data[x] =  tick_rawdata[header_dict[x]]
                elif x == EXT_Bar_Date:
                    tick_data[x] =  pd.to_datetime(tick_rawdata[header_dict[x]])
                else:   
                    tick_data[x] =  tick_rawdata[header_dict[x]].astype(float)
            #add datetime index
            #tick_data.index = tick_data[EXT_Bar_Date]
            tick_data.set_index(EXT_Bar_Date, inplace=True)

            #clear the data
            tick_data.loc[tick_data[EXT_Bar_Volume]<0, EXT_Bar_Volume] = 0
            tick_data.loc[tick_data[EXT_Bar_Turnover]<0, EXT_Bar_Turnover] = 0
            tick_data.loc[tick_data[EXT_Bar_OpenInterest]<0, EXT_Bar_OpenInterest] = 0
            # save tick data
            tick_data.insert(0,EXT_Bar_Asset,'.'.join([ticker,exchange]))
            tick_data.drop(columns='Ticker',inplace=True)
            tick_data_fm = tick_data.reset_index()
            tick_data_fm.set_index([EXT_Bar_Date,EXT_Bar_Asset],inplace=True)
            self.hdf.hdfWrite(self.bar_path,exchange,tickerSim,tick_data_fm,EXT_Rawdata,None,EXT_Period_tick)
            ## get the 1min Bar data
            bar1min_raw = tick_data.resample(rule='T').agg(EXT_Bar_Rule)


            time1m = self.getTradeTime(dateStr=tradeDate, tradetimeRange=timeRange, freq='T')
            bar1min_fmt=bar1min_raw.ix[time1m]
            bar1min_fmt.index.name= EXT_Bar_Date

        #fill up the NaNs vaule with pre-value or 0
        bar1min_fmt[EXT_Bar_Volume]           = bar1min_fmt[EXT_Bar_Volume].fillna(value=0)
        bar1min_fmt[EXT_Bar_Turnover]         = bar1min_fmt[EXT_Bar_Turnover].fillna(value=0)
        bar1min_fmt[EXT_Bar_Close]            = bar1min_fmt[EXT_Bar_Close].fillna(method='ffill')
        bar1min_fmt[EXT_Bar_OpenInterest]     = bar1min_fmt[EXT_Bar_OpenInterest].fillna(method='ffill')
        bar1min_fmt[EXT_Bar_Open]             = bar1min_fmt[EXT_Bar_Open].fillna(value=bar1min_fmt[EXT_Bar_Close])
        bar1min_fmt[EXT_Bar_High]             = bar1min_fmt[EXT_Bar_High].fillna(value=bar1min_fmt[EXT_Bar_Close])
        bar1min_fmt[EXT_Bar_Low]              = bar1min_fmt[EXT_Bar_Low].fillna(value=bar1min_fmt[EXT_Bar_Close])
        bar1min_fmt[EXT_Bar_Ticker]           = ticker
        return bar1min_fmt

    #TODO: now only support CFE.
    def futureTickStitch(self, allsymbol = EXT_CFE_ALL):
        for symbol in allsymbol:
            dom_rule = self.hdf.hdfRead(self.bar_path,EXT_EXCHANGE_CFE,symbol,kind1='Stitch',kind2='00',kind3=None,startdate=EXT_Start,enddate=EXT_End)
            sub_rule = self.hdf.hdfRead(self.bar_path,EXT_EXCHANGE_CFE,symbol,kind1='Stitch',kind2='01',kind3=None,startdate=EXT_Start,enddate=EXT_End)
            dom_rule.reset_index(inplace=True)
            sub_rule.reset_index(inplace=True)
            for freq in ['60m','30m','15m','5m','1m']:
                print(symbol, freq)
                hfdata = self.hdf.hdfRead(self.bar_path,EXT_EXCHANGE_CFE,symbol,kind1='Rawdata',kind2=None,kind3=freq,startdate=EXT_Start,enddate=EXT_End)
                hfdata.reset_index(inplace=True)
                
                dom_rule['tick'] = dom_rule[EXT_Bar_Date].astype(str)
                sub_rule['tick'] = sub_rule[EXT_Bar_Date].astype(str)
                hfdata['tick'] = hfdata[EXT_Bar_Date].astype(str).str.slice(0,10)
                temp = hfdata.drop_duplicates(subset=[EXT_Bar_Date])[[EXT_Bar_Date,'tick']]
                dom_data = temp.merge(dom_rule, on=['tick'],how='left').drop(columns=['tick',EXT_Bar_Date])
                sub_data = temp.merge(sub_rule, on=['tick'],how='left').drop(columns=['tick',EXT_Bar_Date])
                dom_data = dom_data.merge(hfdata, on=[EXT_Bar_Date,EXT_Bar_Asset],how='left').drop(columns=['tick'])
                sub_data = sub_data.merge(hfdata, on=[EXT_Bar_Date,EXT_Bar_Asset],how='left').drop(columns=['tick'])
                dom_data.rename(columns={EXT_Bar_Date:EXT_Bar_Date},inplace=True)
                sub_data.rename(columns={EXT_Bar_Date:EXT_Bar_Date},inplace=True)
                dom_data = dom_data.set_index([EXT_Bar_Date,EXT_Bar_Asset])
                sub_data = sub_data.set_index([EXT_Bar_Date,EXT_Bar_Asset])
                
                self.hdf.hdfWrite(self.bar_path,EXT_EXCHANGE_CFE,symbol,dom_data,EXT_Stitch,EXT_Series_00,freq)
                self.hdf.hdfWrite(self.bar_path,EXT_EXCHANGE_CFE,symbol,sub_data,EXT_Stitch,EXT_Series_01,freq)

    #----------------------------------------------------------------------
    def getResampleBar(self, bardata1m, tradetime,tradeDate, freq='5T'):
        '''1min bar to 'freq' bar'''
        time_freqm = self.getTradeTime(dateStr=tradeDate, tradetimeRange = tradetime, freq=freq)
        if freq=='H':
            bar_data = bardata1m.copy()
            if bar_data.index.size < 6:
                bar_data = bar_data.resample(rule=freq, label ='right', closed ='right').agg(EXT_Bar_Rule)
            else:
                bar_data['label'] = np.NaN
                bar_data['label'].ix[time_freqm] = [i for i in range(len(time_freqm))]
                bar_data['label'] = bar_data['label'].fillna(method = 'bfill')
                bar_data = bar_data.groupby('label').agg(EXT_Bar_Rule)
                bar_data.index =  time_freqm
            bar_data_fmt = bar_data.copy()
        else:
            bar_data = bardata1m.resample(rule=freq, label ='right', closed ='right').agg(EXT_Bar_Rule)
            bar_data_fmt = bar_data.ix[time_freqm]
        bar_data_fmt = bar_data_fmt.dropna(axis=0, how = 'all')
        return bar_data_fmt

    #----------------------------------------------------------------------
    def getTradeTime(self, dateStr,tradetimeRange, freq='T'):
        for i in range(len(tradetimeRange)):
            daterange_tmp = pd.date_range(start = dateStr+' '+tradetimeRange[i][0], end = dateStr+' '+tradetimeRange[i][1], freq = freq, closed=None)
            if i==0:
                daterange = daterange_tmp
            else:
                daterange = daterange.append(daterange_tmp)
        return daterange

    #----------------------------------------------------------------------
    def getTradeTimeRange(self, tickerSim, type_l=['AM', 'PM', 'EV']):
        '''  get the trading data of tickerSim  '''
        ticker1 = EXT_DCE_ALL+EXT_SHFE_ALL+EXT_CZCE_ALL
        ticker2 = [EXT_CFE_TF, EXT_CFE_T]
        ticker3 = [EXT_CFE_IF, EXT_CFE_IC,EXT_CFE_IH]

        if tickerSim in(ticker1) and 'AM' in type_l:
            type_l[type_l.index('AM')]='AM1'
        elif tickerSim in(ticker2) and 'AM' in type_l:
            type_l[type_l.index('AM')]='AM2'
        elif tickerSim in(ticker3) and 'AM' in type_l:
            type_l[type_l.index('AM')]='AM3'

        tt = []
        for i in type_l:
            tt.append(EXT_TradingTime_Dict[i])
        return tt
    #----------------------------------------------------------------------
    def getTickDataInfo(self, unpackedFilenameStr):
        '''get the tick data info: ticker, tickerSim, tradingdate, exchange_name'''
        #get ticker
        str1 = unpackedFilenameStr.split('\\')[-1][:-4]
        ticker = re.findall(r'[a-zA-Z]+[0-9]+',str1)
        if ticker == []:
            return
        else:
            ticker = ticker[0].upper()
        dateStr = re.findall(r'[0-9]+',str1)[-1]
# =============================================================================
#         match = re.search(pattern='\.', string=str2)
#         ticker=str2[0:match.start()]
#         #get month,day
#         str1, month_day = os.path.split(str1)
#         #get tickerSim name
#         str1, tickerSim = os.path.split(str1)
#         #get year,month
#         str1, year_month = os.path.split(str1)
#         dateStr = year_month+month_day[2:4]
# =============================================================================
        tickerSim = re.findall(r'[A-Z]+',ticker)[0]
        if tickerSim in EXT_DCE_ALL:
            exchange ='DCE'
        elif tickerSim in EXT_SHFE_ALL:
            exchange ='SHFE'
        elif tickerSim in EXT_CFE_ALL:
            exchange ='CFE'
        elif tickerSim in EXT_CZCE_ALL:
            exchange ='CZCE'

        #info={EXT_Info_Exchange:exchange, EXT_Info_TickerSim:tickerSim, EXT_Info_Ticker:ticker, EXT_Info_TradeDate: dateStr}
        info={EXT_Info_File:unpackedFilenameStr, EXT_Info_Exchange:exchange, EXT_Info_TickerSim:tickerSim, EXT_Info_Ticker:ticker, EXT_Info_TradeDate: dateStr}
        return info

    #----------------------------------------------------------------------
    def getSeriesNum(self, tickfiles):
        '''
        get the future series numbers on the file names,
        so DONOT miss any files in the tickdata path,
        else the Num maybe not right
        '''
        #tickers = list()
        #file_num_dict = dict()
        #for f in tickfiles:
            #path,fileName = os.path.split(f)
            #ticker = fileName[0:re.search(pattern='\.', string=fileName).start()]
            #tickers.append(ticker)
            #file_num_dict[f]= ticker
        #tickers.sort()
        #for k in file_num_dict.keys():
            #file_num_dict[k]=tickers.index(value=file_num_dict[k])

        info_l = []
        for f in tickfiles:
            info_l.append(self.getTickDataInfo(unpackedFilenameStr=f))
            
        info_l = [i for i in info_l if i != None]
        file_num_df = pd.DataFrame.from_dict(info_l, orient='columns')
        file_num_df['gbc'] = file_num_df[EXT_Info_TickerSim] + file_num_df[EXT_Info_TradeDate]
        file_num_df[EXT_Info_SeriesNum] = file_num_df.groupby('gbc')[EXT_Info_Ticker].rank(ascending=True)
        return file_num_df
    #----------------------------------------------------------------------
    def mkdir(self, path, isTrunk=False):
        '''make the temp path'''
        path_full = os.path.join(self.data_path, path)
        if isTrunk:
            self.rmdir(path=path)
            os.makedirs(path_full)
        elif not os.path.exists(path):
            os.makedirs(path_full)
        return path_full

    def rmdir(self, path):
        path = os.path.join(self.data_path, path)
        if os.path.exists(path=path):
            shutil.rmtree(path)
    #----------------------------------------------------------------------
    def listFiles(self, path, pattern = re.compile(r'[a-zA-Z]+[0-9]{4}_[0-9]{8}')):
        '''get all the tick data files in temp path recursively.'''
        csvpaths = []
        for root, dirs, files in os.walk(path):
            for file in files:
                filepath = os.path.join(root, file)
                if os.path.splitext(filepath)[-1] == '.csv' and \
                    len(pattern.findall(filepath)) > 0:
                    csvpaths.append(filepath)
        return csvpaths


