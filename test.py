from dataUlt import *
from HdfUtility import *
from FutureTickData import *
from HisDayData import *

# get future daily data from Wind database
a = HisDayData()
a.getData(is_save_stitch=True)

# put future high freq data to hdf
data_path = 'F:\\Fut_Tick_PanKou_Daily\\FutSF_Tick_PanKou_Daily_2018'
data_temp = 'F:\\temp'
bar_path = EXT_Hdf_Path
a = HisFutureTick(data_path,data_temp,bar_path)
a.packedTick2Bar()  # unpack rawdata and save it to hdf
a.futureTickStitch() # use stitchrule from hdf to stitch high freq data and save it to hdf

# method to read from hdf
hdf=HdfUtility()
# read rawdata:          kind1='Rawdata',kind2=None,kind3='1d'
x = hdf.hdfRead(EXT_Hdf_Path,'CFE','IC','Rawdata',None,'1m',startdate=EXT_Start,enddate=EXT_End)
# read stitchrule:       kind1='Stitch', kind2='00',kind3=None
x = hdf.hdfRead(EXT_Hdf_Path,'CFE','IC','Stitch','00',None,startdate=EXT_Start,enddate=EXT_End)
# read stitchdata:       kind1='Stitch', kind2='00',kind3='1d'
x = hdf.hdfRead(EXT_Hdf_Path,'CFE','IC','Stitch','00','1d',startdate=EXT_Start,enddate=EXT_End)