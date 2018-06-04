import os
import re
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
from pyunpack import Archive
from dataUlt import *
from HdfUtility import *

f = rarfile.RarFile('F:\\Fut_Tick_PanKou_Daily\\FutAC_Tick_PanKou_Daily_2017\\FutAC_Tick_PanKou_Daily_201701.rar')
f.extractall(pwd='www.jinshuyuan.net')
data_path = 'F:\\Fut_Tick_PanKou_Daily'

class HisFutureTick(object):
    #----------------------------------------------------------------------
    def __init__(self, data_path, bar_path ):
        """Constructor"""
        self.data_path = data_path
        self.bar_path = bar_path

    def packedTick2Bar(self,file_packedtick_ex= ['night','.txt'], file_unpacked_ex = ['Survey.txt'], path_temp='temp',
                       path_output_bar='bar', freq=['5T','15T','30T','H']):
        '''packed tick data 2 Bar data'''
        start_time = timeit.default_timer()
        #get full path
        
        path_packedtick = os.path.join(self.data_path)
        for i in os.listdir(self.data_path):
            if os.path.splitext(i)[1] == '.txt':
                with open(os.path.join(self.data_path,i)) as f:
                    password = [re.findall(r'[a-zA-Z0-9]+.[a-zA-Z0-9]+.[a-zA-Z0-9]+',i) for i in f.readlines() if re.findall(r'[a-zA-Z0-9]+.[a-zA-Z0-9]+.[a-zA-Z0-9]+',i) != []]
        for i in os.listdir(self.data_path):
            if os.path.splitext(i)[1] != '.txt':
                f_packed = self.listFiles(path = os.path.join(self.data_path,i),patter_ex=file_packedtick_ex)
                for f in f_packed:
                    self.unpack(f, path_temp)
                #   get the tick data file
                    files_tick = self.listFiles(path =path_temp_full, patter_ex=file_unpacked_ex)
                    file_SN_df = self.getSeriesNum(tickfiles=files_tick)
                #   get the bar data and save it in the path_output_bar, file name is as TickerSim_freq.csv
                    for idx, row in file_SN_df.iterrows():
                        symbol = row[EXT_Info_TickerSim]
                        #1 min bar
                        bar1m = self.tick2Bar1m(filename_tick = row[EXT_Info_File], tradetime=['AM', 'PM'])
                        bar1m.insert(0,EXT_Out_Asset,bar1m.Ticker+'.'+self.exchange)
                        bar1m.drop('Ticker',axis=1,inplace=True)
                        bar1m_fm=bar1m.reset_index().set_index([EXT_Bar_DateTime,EXT_Out_Asset])
                        hdf.hdfWrite(self.bar_path,self.exchange,symbol,bar1m_fm,EXT_Rawdata,None,EXT_Period_1m)
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
                            bars_fr.insert(0,EXT_Out_Asset,bar1m[EXT_Out_Asset])
                            bars_fr = bars_fr.reset_index()
                            bars_fr.rename(columns={'index':EXT_Bar_DateTime},inplace = True)
                            bars_fr_fm=bars_fr.set_index([EXT_Bar_DateTime,EXT_Out_Asset])
                            hdf.hdfWrite(self.bar_path,self.exchange,symbol,bars_fr_fm,EXT_Rawdata,None,EXT_Freq_Period[fr])

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
        tick_rawdata = pd.read_table(filename_tick,sep =',')
        colnames = ",".join(tick_rawdata.columns)
        colnames = colnames.replace(' ', '')
        if EXT_Bar_LastTurnover not in colnames:
            tick_rawdata.insert(0,EXT_Bar_LastTurnover,np.zeros(tick_rawdata.shape[0]))
            colnames = colnames+','+EXT_Bar_LastTurnover
            tick_rawdata.insert(0,EXT_Bar_Turnover,np.zeros(tick_rawdata.shape[0]))
            colnames = colnames+','+EXT_Bar_Turnover
        header_dict = EXT_TickFileHeaderMap_Dict[colnames] #这里原来是EXT_TickFileHeaderMap_Dict[colnames]
        #TODO: add the evening trading in future
        timeRange = self.getTradeTimeRange(tickerSim, type_l=tradetime)

        if tick_rawdata.size <= 0:
            #empyt file
            data_empty = copy.deepcopy(header_dict)
            for k in data_empty.keys():
                data_empty[k] = np.NaN
            data_empty[EXT_Bar_DateTime]=pd.to_datetime(tradeDate+' '+timeRange[-1][-1])
            del data_empty[EXT_Bar_Time]  #新增
            bar1min_fmt = pd.DataFrame(data_empty, index=[0])
            bar1min_fmt.set_index(EXT_Bar_DateTime, inplace=True)
        else:
            ## get cleared tick data
            tick_data = pd.DataFrame(index=tick_rawdata.index)
            for x in header_dict.keys():
                tick_data[x] =  tick_rawdata[header_dict[x]]

            #create the trade datetime, the logic may be different on exchange or ticker
            if exchange == EXT_EXCHANGE_CFE :
                tick_data[EXT_Bar_DateTime] = pd.to_datetime(tradeDate+' '+ tick_data[EXT_Bar_Time])
            elif exchange == EXT_EXCHANGE_DCE :
                tick_data[EXT_Bar_DateTime] = pd.to_datetime(tradeDate+' '+tick_data[EXT_Bar_Time])
            elif exchange == EXT_EXCHANGE_CZCE :
                tick_data[EXT_Bar_DateTime] = pd.to_datetime(tradeDate+' '+tick_data[EXT_Bar_Time])
            else:
                raise NameError(exchange)
            tick_data = tick_data.drop_duplicates(EXT_Bar_DateTime,keep = 'first')
            #add datetime index
            #tick_data.index = tick_data[EXT_Bar_DateTime]
            tick_data.set_index(EXT_Bar_DateTime, inplace=True)

            #clear the data
            tick_data.loc[tick_data[EXT_Bar_Volume]<0, EXT_Bar_Volume] = 0
            tick_data.loc[tick_data[EXT_Bar_Turnover]<0, EXT_Bar_Turnover] = 0
            tick_data.loc[tick_data[EXT_Bar_OpenInterest]<0, EXT_Bar_OpenInterest] = 0

            ## get the 1min Bar data
            bar1min_raw = tick_data.resample(rule='T', label ='right', closed ='right').agg(EXT_Bar_Rule)


            time1m = self.getTradeTime(dateStr=tradeDate, tradetimeRange=timeRange, freq='T')
            bar1min_fmt=bar1min_raw.ix[time1m]
            bar1min_fmt.index.name= EXT_Bar_DateTime

            #bar1min_fmt = self.getEmptyBar1mOfDay(dateStr, tradingtimeArray=td)
            ##the timeseries maybe not continuous in minutes
            #bar1min_fmt.update(bar1min_raw)

        #fill up the NaNs vaule with pre-value or 0
        bar1min_fmt[EXT_Bar_Volume]           = bar1min_fmt[EXT_Bar_Volume].fillna(value=0)
        bar1min_fmt[EXT_Bar_Turnover]         = bar1min_fmt[EXT_Bar_Turnover].fillna(value=0)
        bar1min_fmt[EXT_Bar_Close]            = bar1min_fmt[EXT_Bar_Close].fillna(method='ffill')
        bar1min_fmt[EXT_Bar_OpenInterest]     = bar1min_fmt[EXT_Bar_OpenInterest].fillna(method='ffill')
        bar1min_fmt[EXT_Bar_Open]             = bar1min_fmt[EXT_Bar_Open].fillna(value=bar1min_fmt[EXT_Bar_Close])
        bar1min_fmt[EXT_Bar_High]             = bar1min_fmt[EXT_Bar_High].fillna(value=bar1min_fmt[EXT_Bar_Close])
        bar1min_fmt[EXT_Bar_Low]              = bar1min_fmt[EXT_Bar_Low].fillna(value=bar1min_fmt[EXT_Bar_Close])
        #add ticker column
        if tickerSim in EXT_CZCE_ALL:
            bar1min_fmt[EXT_Bar_Ticker]       = tickerSim+'1'+ticker[-3:]
        else:
            bar1min_fmt[EXT_Bar_Ticker]       = ticker
        return bar1min_fmt
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
    def getEmptyBar1mOfDay(self, dateStr,tradetimeRange):
        '''get the empty 1 minute bar on trade date time range'''
        date1m = self.getTradeTime1m(dateStr=dateStr, tradetimeRange=tradetimeRange)
        bar_dict = {EXT_Bar_Open:np.nan, EXT_Bar_Close:np.nan, EXT_Bar_High:np.nan,EXT_Bar_Low:np.nan, EXT_Bar_Volume:0, EXT_Bar_Turnover:0, EXT_Bar_OpenInterest:np.nan}
        bars = pd.DataFrame(data=bar_dict, index=date1m)
        return bars
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
        str1, str2 = os.path.split(unpackedFilenameStr)
        match = re.search(pattern='\.', string=str2)
        ticker=str2[0:match.start()]
        #get month,day
        str1, month_day = os.path.split(str1)
        #get tickerSim name
        str1, tickerSim = os.path.split(str1)
        #get year,month
        str1, year_month = os.path.split(str1)
        dateStr = year_month+month_day[2:4]
        ##get exchange
        #str1, exchange = os.path.split(str1)
        #if exchange == 'DCE':
            #exchange = EXT_EXCHANGE_DCE
        #elif exchange == 'SHFE':
            #exchange = EXT_EXCHANGE_SHFE
        #elif exchange == 'CZCE':
            #exchange = EXT_EXCHANGE_CZCE
        #elif exchange == 'CFFEX':
            #exchange = EXT_EXCHANGE_CFE

        #info={EXT_Info_Exchange:exchange, EXT_Info_TickerSim:tickerSim, EXT_Info_Ticker:ticker, EXT_Info_TradeDate: dateStr}
        info={EXT_Info_File:unpackedFilenameStr, EXT_Info_Exchange:self.exchange, EXT_Info_TickerSim:tickerSim, EXT_Info_Ticker:ticker, EXT_Info_TradeDate: dateStr}
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
    def listFiles(self, path, patter_ex=['Survey.txt','night']):
        '''get all the tick data files in temp path recursively.'''
        files_list = []
        for root, dirs, files in os.walk(path, topdown=False):
            for name in files:
                if not any(p in name for p in patter_ex):
                    files_list.append(os.path.join(root, name))
        return files_list
    #----------------------------------------------------------------------
    def unpack(self, filename, path_temp='temp'):
        '''un pack the zip/rar file to the temp_dir '''
        path_output = os.path.join(self.data_path, path_temp)
        #if '.rar' in filename.lower():
            #self.unrar(rar_file=filename, dir_name=path_output)
        #elif '.zip' in filename.lower():
            #self.unzip(zip_name=filename, unzip_dir=path.output)
        self.mkdir(path=path_temp, isTrunk=True)
        Archive(filename).extractall(path_output)
##Below functions not tested, unrar may need install other depended softwares firstly.
    def unzip(self, zip_name, unzip_dir):
        unzip_dir = unzip_dir.decode('utf-8')
        zip_name = zip_name.decode('utf-8')
        if not os.path.exists(unzip_dir):
            os.mkdir(unzip_dir)
        zfobj = zipfile.ZipFile(zip_name)
        for file_name in zfobj.namelist():
            file_name = file_name.replace('\\', '/')
            if file_name.endswith('/'):
                # try:
                #     file_name = file_name.decode('utf-8')
                # except UnicodeDecodeError:
                #     file_name = file_name.decode('gbk')
                os.mkdir(os.path.join(unzip_dir, file_name))
            else:
                # try:
                ext_filename = os.path.join(unzip_dir, file_name)
                # except UnicodeDecodeError:
                #     ext_filename = os.path.join(unzip_dir, file_name.decode('gbk'))
                ext_filedir = os.path.dirname(ext_filename)
                if not os.path.exists(ext_filedir):
                    os.mkdir(ext_filedir)
                data = zfobj.read(file_name)
                with open(ext_filename, 'w') as f:
                    f.write(data)
        zfobj.close()

    def gzip(self, zip_name, file_dir):
        zip_name = zip_name.decode('utf-8')
        file_dir = file_dir.decode('utf-8')
        filelist = []
        if os.path.isfile(file_dir):
            filelist.append(file_dir)
        else:
            for root, dirs, files in os.walk(file_dir):
                for file in files:
                    filelist.append(os.path.join(root, file))

            zf = zipfile.ZipFile(zip_name, 'w', zipfile.zlib.DEFLATED)
            for tar in filelist:
                arcname = tar[len(file_dir):]
                zf.write(tar, arcname)
            zf.close()

    def unrar(self, rar_file, dir_name):      # rarfile需要unrar支持, linux下pip install unrar, windows下在winrar文件夹找到unrar,加到path里
        rarobj = rarfile.RarFile(rar_file)
        rarobj.extractall(dir_name)
if __name__ == '__main__':
    a = HisFutureTick(data_path,EXT_Hdf_Path)
    a.packedTick2Bar()
