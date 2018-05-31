# encoding: UTF-8
EXT_Data_Path = 'E:\\work\\data_hft'
EXT_Hdf_Path = 'C:\\Users\\user\\GitHub\\Project1\\out.hdf5'
EXT_Wind_User = 'fe'
EXT_Wind_Password = 'fe'
EXT_Wind_Link = '192.168.100.22:1521/winddb'
EXT_Start = '20120101'
EXT_End = '20171231'

EXT_Info_File = 'filename'
EXT_Info_Exchange = 'exchange'
EXT_Info_Ticker = 'ticker'
EXT_Info_TickerSim = 'tickerSim'
EXT_Info_TradeDate = 'tradeDate'
EXT_Info_SeriesNum = 'seriesNum'

EXT_Rawdata = 'Rawdata'
EXT_Stitch = 'Stitch'
EXT_Indicator = 'Indicator'
EXT_Period = 'Period'
EXT_Rule = 'Rule'
EXT_Series_00 = '00'
EXT_Series_01 = '01'
EXT_Period_1m = '1m'
EXT_Period_5m = '5m'
EXT_Period_15m = '15m'
EXT_Period_60m = '60m'
EXT_Period_1d = '1d'

# code of exchange
EXT_EXCHANGE_SHFE = 'SHFE' #shang hai Suo
EXT_EXCHANGE_DCE  = 'DCE'  #Da Lian Suo
EXT_EXCHANGE_CZCE = 'CZCE' #ZhengZhou Suo
EXT_EXCHANGE_CFE  = 'CFE'  #Zhong Jin Suo

# code for Commodity/finance Future
EXT_CFE_IF = 'IF'
EXT_CFE_IH = 'IH'
EXT_CFE_IC = 'IC'
EXT_CFE_T  = 'T'
EXT_CFE_TF = 'TF'
EXT_CFE_STOCK = ['IF','IC','IH']
EXT_CFE_BOND = ['TF','T']
EXT_CFE_ALL = EXT_CFE_STOCK + EXT_CFE_BOND

EXT_SHFE_CU = 'CU'
EXT_SHFE_AL = 'AL'
EXT_SHFE_ZN = 'ZN'
EXT_SHFE_RU = 'RU'
EXT_SHFE_FU = 'FU'
EXT_SHFE_AU = 'AU'
EXT_SHFE_AG = 'AG'
EXT_SHFE_RB = 'RB'
EXT_SHFE_WR = 'WR'
EXT_SHFE_PB = 'PB'
EXT_SHFE_BU = 'BU'
EXT_SHFE_HC = 'HC'
EXT_SHFE_NI = 'NI'
EXT_SHFE_SN = 'SN'
EXT_SHFE_ALL = ['CU','AL','ZN','RU','AU','AG','RB','PB','BU','HC','NI','SN'] #'FU'：没有数据'WR'：有数据，但交易量太少画不出图
# code for Commodity/finance Future in DCE
EXT_DCE_A = 'A'
EXT_DCE_B = 'B'
EXT_DCE_M = 'M'
EXT_DCE_C = 'C'
EXT_DCE_Y = 'Y'
EXT_DCE_P = 'P'
EXT_DCE_L = 'L'
EXT_DCE_V = 'V'
EXT_DCE_J = 'J'
EXT_DCE_I = 'I'
EXT_DCE_JM = 'JM'
EXT_DCE_JD = 'JD'
EXT_DCE_FB = 'FB'
EXT_DCE_BB = 'BB'
EXT_DCE_PP = 'PP'
EXT_DCE_CS = 'CS'
EXT_DCE_ALL = ['A','M','C','Y','P','L','V','J','I','JM','JD','FB','BB','PP','CS']#'B',
# code for Commodity/finance Future in CZCE
EXT_CZCE_PM = 'PM'
EXT_CZCE_WH = 'WH'
EXT_CZCE_CF = 'CF'
EXT_CZCE_SR = 'SR'
EXT_CZCE_OI = 'OI'
EXT_CZCE_TA = 'TA'
EXT_CZCE_RI = 'RI'
EXT_CZCE_LR = 'LR'
EXT_CZCE_MA = 'MA'
EXT_CZCE_FG = 'FG'
EXT_CZCE_RS = 'RS'
EXT_CZCE_RM = 'RM'
EXT_CZCE_TC = 'TC'
EXT_CZCE_ZC = 'ZC'
EXT_CZCE_JR = 'JR'
EXT_CZCE_SF = 'SF'
EXT_CZCE_SM = 'SM'
EXT_CZCE_ER = 'ER'
EXT_CZCE_ME = 'ME'
EXT_CZCE_RO = 'RO'
EXT_CZCE_WS = 'WS'
# =============================================================================
# EXT_CZCE_ALL = [EXT_CZCE_PM,EXT_CZCE_WH,EXT_CZCE_CF,EXT_CZCE_SR,EXT_CZCE_OI,
#                 EXT_CZCE_TA,EXT_CZCE_RI,EXT_CZCE_LR,EXT_CZCE_MA,EXT_CZCE_FG,
#                 EXT_CZCE_RS,EXT_CZCE_RM, EXT_CZCE_TC,EXT_CZCE_ZC,EXT_CZCE_JR,
#                 EXT_CZCE_SF,EXT_CZCE_SM,EXT_CZCE_ER,EXT_CZCE_ME,EXT_CZCE_RO,EXT_CZCE_WS]
# =============================================================================
EXT_CZCE_ALL = [EXT_CZCE_CF,EXT_CZCE_SR,EXT_CZCE_TA,EXT_CZCE_LR,EXT_CZCE_FG,
                EXT_CZCE_RM, ]#EXT_CZCE_SF、EXT_CZCE_JR,EXT_CZCE_SM, 交易量少，画不出图,但是有数据
# Wind filename
EXT_CFE_STOCK_FILE = 'filesync.CIndexFuturesEODPrices'
EXT_CFE_BOND_FILE = 'filesync.CBondFuturesEODPrices'
EXT_SHFE_DATA_FILE = 'filesync.CCommodityFuturesEODPrices'
EXT_DCE_DATA_FILE = 'filesync.CCommodityFuturesEODPrices'
EXT_CZCE_DATA_FILE = 'filesync.CCommodityFuturesEODPrices'
EXT_Delistdate_File = 'filesync.CFuturesDescription'

#Splitor
EXT_SPLITOR_PORINT = '.'
EXT_SPLITOR_COMMA = ','
EXT_SPLITOR_UNDERSCORE = '_'
EXT_SPLITOR_SEMICOLON = ';'
EXT_SPLITOR_COLON = ':'

#begin end time of futures
EXT_AM_Begin1 = '09:00:00'
EXT_AM_Begin2 = '09:15:00'#T/TF include the aggregate auction trading
EXT_AM_Begin3 = '09:30:00'#IF/IC/IC include the aggregate auction trading
EXT_AM_End    = '11:30:00'
EXT_PM_Begin  = '13:00:00'
EXT_PM_End    = '15:00:00'
EXT_EV_Begin  = '21:00:00'
EXT_EV_End    = '23:30:00'
EXT_TradingTime_Dict = {'AM1':[EXT_AM_Begin1, EXT_AM_End],
                       'AM2':[EXT_AM_Begin2, EXT_AM_End],
                       'AM3':[EXT_AM_Begin3, EXT_AM_End],
                       'PM': [EXT_PM_Begin, EXT_PM_End],
                       'EV': [EXT_EV_Begin, EXT_EV_End]}

#Bar
EXT_Bar_TickerSim = 'TickerSim'
EXT_Bar_Ticker = 'Ticker'
EXT_Bar_Date = 'Date'
EXT_Bar_Time = 'Time'
EXT_Bar_DateTime = 'DateTime'
EXT_Bar_Open = 'Open'
EXT_Bar_Close = 'Close'
EXT_Bar_High = 'High'
EXT_Bar_Low = 'Low'
EXT_Bar_Volume = 'Volume'
EXT_Bar_OpenInterest = 'OpenInterest'
EXT_Bar_Turnover = 'Turnover'
EXT_Bar_PreSettle = 'PreSettle'
EXT_Bar_Settle = 'Settle'
EXT_Bar_UpLimit = 'UpLimit'
EXT_DownLimit = 'DownLimit'
EXT_AdjFactor = 'AdjFactor'
EXT_Bar_LastTurnover = 'LastTurnover'
EXT_Bar_Indicator = 'LastTurnover'

EXT_In_Header = 'trade_dt,s_info_windcode,s_dq_presettle,s_dq_open,s_dq_high,s_dq_low,s_dq_close,s_dq_settle,s_dq_volume,s_dq_oi'
EXT_In_Header2 = 's_info_windcode,s_info_delistdate'
EXT_In_Date = 'trade_dt'
EXT_In_Asset = 's_info_windcode'
EXT_In_Delistdate = 's_info_delistdate'

EXT_Out_Header = 'Date,Asset,PreSettle,Open,High,Low,Close,Settle,Volume,OpenInterest'
EXT_Out_Header2 = 'Asset,Delistdate'
EXT_Out_Date = 'Date'
EXT_Out_Asset = 'Asset'
EXT_Out_AdjFactor = 'AdjFactor'
EXT_Out_Close = 'Close'
EXT_Out_OpenInterest = 'OpenInterest'
EXT_Out_Delistdate = 'Delistdate'

#citicsf tick file hear, may be change for one TickerSim, so I workaround it by a tickfileheadmap dict.
EXT_Header_CSF1 = 'Time,LastPrice,LVolume,BidPrice,BidVolume,AskPrice,AskVolume,OpenInterest,TradeVolume,LastTurnover,Turnover'
EXT_Header_CSF2 = 'InstrumentID,TradingDay,UpdateTime,LastPrice,BidPrice1,BidVolume1,AskPrice1,AskVolume1,Volume,Turnover,OpenInterest,UpperLimitPrice,LowerLimitPrice,OpenPrice,PreSettlementPrice,PreClosePrice,PreOpenInterest'
EXT_Header_CSF3 = 'Time,LastPrice,LVolume,BidPrice,BidVolume,AskPrice,AskVolume,OpenInterest,TradeVolume'
#bar rule, how to get the bar data by tick data
#Rule1 for CZCE because of missing data —— Turnover data
EXT_Bar_Rule = { EXT_Bar_Open:'first', EXT_Bar_Close: 'last', EXT_Bar_High:'max',
                      EXT_Bar_Low:'min',EXT_Bar_Volume: 'sum',EXT_Bar_Turnover:'sum', EXT_Bar_OpenInterest:'last'}
EXT_Bar_Rule1 = { EXT_Bar_Open:'first', EXT_Bar_Close: 'last', EXT_Bar_High:'max',
                      EXT_Bar_Low:'min',EXT_Bar_Volume: 'sum', EXT_Bar_OpenInterest:'last'}
#format the header, convert the tick file column to bar header
EXT_Bar_Header = [EXT_Bar_Ticker, EXT_Bar_Open, EXT_Bar_Close, EXT_Bar_High, EXT_Bar_Low, EXT_Bar_Volume, EXT_Bar_Turnover, EXT_Bar_OpenInterest] #EXT_Bar_DateTime is already exsits as dataframe index.
EXT_CFE_Header = {EXT_Bar_Time:'Time', EXT_Bar_Open:'LastPrice', EXT_Bar_Close:'LastPrice', EXT_Bar_High:'LastPrice',
                  EXT_Bar_Low:'LastPrice', EXT_Bar_Volume:'LVolume', EXT_Bar_Turnover:'LastTurnover', EXT_Bar_OpenInterest:'OpenInterest'}
EXT_CFE_Header2 = {EXT_Bar_Time:'UpdateTime', EXT_Bar_Open:'LastPrice', EXT_Bar_Close:'LastPrice', EXT_Bar_High:'LastPrice',
                  EXT_Bar_Low:'LastPrice', EXT_Bar_Volume:'Volume', EXT_Bar_Turnover:'Turnover', EXT_Bar_OpenInterest:'OpenInterest'}
EXT_CZCE_Header = {EXT_Bar_Time:'Time', EXT_Bar_Open:'LastPrice', EXT_Bar_Close:'LastPrice', EXT_Bar_High:'LastPrice',
                  EXT_Bar_Low:'LastPrice', EXT_Bar_Volume:'LVolume', EXT_Bar_OpenInterest:'OpenInterest'}
#header mapping to formatted header
EXT_TickFileHeaderMap_Dict = {EXT_Header_CSF1:EXT_CFE_Header, EXT_Header_CSF2:EXT_CFE_Header2,EXT_Header_CSF3:EXT_CZCE_Header}

#deprecated, use the EXT_TickFileHeaderMap_Dict.
EXT_TICK2Bar_Dict = {EXT_EXCHANGE_CFE:EXT_CFE_Header}

## future series Num
EXT_Series_1 = '00'
EXT_Series_2 = '01'
EXT_Series_3 = '02'
EXT_Series_4 = '03'
EXT_Series_5 = '04'
EXT_Series_6 = '05'
EXT_Series_7 = '06'
EXT_Series_8 = '07'
EXT_Series_9 = '08'
EXT_Series_10 = '09'
EXT_Series_11 = '10'
EXT_Series_12 = '11'
EXT_Series_dict = {1:EXT_Series_1, 2:EXT_Series_2, 3:EXT_Series_3, 4:EXT_Series_4,
                   5:EXT_Series_5, 6:EXT_Series_6, 7:EXT_Series_7, 8:EXT_Series_8,
                   9:EXT_Series_9, 10:EXT_Series_10, 11:EXT_Series_11, 12:EXT_Series_12}

EXT_FILE_CSV = '.csv'
EXT_Freq_Period = {'5T':'5m','15T':'15m','30T':'30m','H':'60m'}

# Header to delete when processing data
EXT_Del_Header = 'AdjFactor,PreSettle,Open,High,Low,Close,Settle,Volume,OpenInterest,ret'