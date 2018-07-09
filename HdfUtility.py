import numpy as np
import pandas as pd
from pandas.io.pytables import HDFStore
import re
from dataUlt import *
import h5py
pd.set_option('io.hdf.default_format','table')
'''
HDF
    /Rawdata
        /CFE
            /IC
                /1m
                /5m
                /30m
                /1h
                /1d
    /Stitch
        /CFE
            /IF
               /Rule
                  /00
                  /01
               /Period
                  /1m
                  /5m
                  /15m
                  /60m
                  /1d
    /Indicator
        /CFE
            /IF
                /Indicator_name
'''
class HdfUtility:

    def hdfRead(self,path,excode,symbol,kind1,kind2,kind3,startdate=EXT_Start,enddate=EXT_End,is_stitch=True):
        # kind1为 'Rawdata',Stitch','Indicator'
        # kind2为 '00' '01'
        # kind3为 '1d' '60m' '30m' '15m' '5m' '1m'
        # 读各个频率的Rawdata: kind1='Rawdata',kind2=None,kind3='1d'
        # 读StitchRule:       kind1='Stitch', kind2='00',kind3=None
        # 读STitchData:       kind1='Stitch', kind2='00',kind3='1d'
        # 读Indicator：       kind1='Indicator',kind2='Indicator_name',kind3=None
        store = HDFStore(path,mode = 'r')
        if kind1 == EXT_Rawdata:
            key = '/'.join([kind1,excode,symbol,kind3])
        elif kind1 == EXT_Stitch:
            key = '/'.join([kind1,excode,symbol,EXT_Rule,kind2]) if kind3 == None else '/'.join([kind1,excode,symbol,EXT_Period,kind3,kind2])
        elif kind1 == EXT_Indicator:
            key = '/'.join([kind1,excode,symbol,kind2])
        else:
            print("kind not supported")
            return
        data = store[key].ix[((store[key].index.get_level_values(0)>=pd.to_datetime(startdate))&(store[key].index.get_level_values(0)<=pd.to_datetime(enddate))),:]
        if kind1 == EXT_Stitch and is_stitch == True and kind3 != None:
            data[EXT_Bar_Open] = data[EXT_AdjFactor] * data[EXT_Bar_Open]
            data[EXT_Bar_High] = data[EXT_AdjFactor] * data[EXT_Bar_High]
            data[EXT_Bar_Low] = data[EXT_AdjFactor] * data[EXT_Bar_Low]
            data[EXT_Bar_Close] = data[EXT_AdjFactor] * data[EXT_Bar_Close]
        store.close()
        if kind1 == EXT_Indicator:
            f = h5py.File(path,'r')
            params = f[key].attrs['Params']
            f.close()
            return data,params
        return data

    def hdfWrite(self,path,excode,symbol,indata,kind1,kind2,kind3):
        # kind1为 'Rawdata'、'Stitch'、'Indicator'
        # kind2为 '00' '01'
        # kind3为 '1d' '60m' '30m' '15m' '5m' '1m'
        # 写各个频率的Rawdata: kind1='Rawdata',kind2=None,kind3='1d'
        # 写StitchRule:       kind1='Stitch', kind2='00',kind3=None
        # 写StitchData:       kind1='Stitch', kind2='00',kind3='1d'
        # 写Indicator：       kind1='Indicator',kind2='Indicator_name',kind3='params'
        store = HDFStore(path,mode='a')
        if kind1 == EXT_Rawdata:
            key = '/'.join([kind1,excode,symbol,kind3])
        elif kind1 == EXT_Stitch:
            key = '/'.join([kind1,excode,symbol,EXT_Rule,kind2]) if kind3 == None else '/'.join([kind1,excode,symbol,EXT_Period,kind3,kind2])
        elif kind1 == EXT_Indicator:
            key = '/'.join([kind1,excode,symbol,kind2])
        else:
            print("kind not supported")
            return

        if kind1 ==EXT_Indicator:
            f = h5py.File(path,'a')
            try:
                store[key]
            except KeyError: # 路径不存在时创建
                store[key] = indata
                for param_names, value in kind3.items():
                    f[key].attrs['param_names'] = value
            else:
                temp = 0
                try: 
                    f[key].attrs[[i for i in kind3.keys()][0]]#不存在该参数
                except KeyError:
                    store[key] = indata
                    for param_names, value in kind3.items():
                        f[key].attrs['param_names'] = value
                else:
                    for param_names, value in kind3.items():
                        temp = (f[key].attrs[param_names] != value)+temp
                    if temp == 0: #Params匹配时合并
                        adddata = indata[~indata.index.isin(store[key].index)]
                        store.append(key,adddata)
                    else: # Params不匹配时覆盖
                        store[key] = indata
                        for param_names, value in kind3.items():
                            f[key].attrs['param_names'] = value
            f.close()
            store.close()
        else:
            try:
                store[key]
            except KeyError:
                store[key] = indata
            else:
                adddata = indata[~indata.index.isin(store[key].index)]
                if kind2 in [EXT_Series_00,EXT_Series_01]:
                    adddata[EXT_AdjFactor] = adddata[EXT_AdjFactor]*store[key][EXT_AdjFactor].iloc[-1]/adddata[EXT_AdjFactor].iloc[0]
                store.append(key,adddata)
            store.close()
    
    def hdfDel(self,grouppath):
        # ex.grouppath = '/Rawdata/CFE/IF'
        with h5py.File(path,'a') as f:
            del f[grouppath]
        print(grouppath+' removed ')

        

