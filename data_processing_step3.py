#import everything YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY
import numpy as np
import pandas as pd
import os
#YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY
BOMpath=os.getcwd()+"\\DATA\BOM data\\Formated_to_csv"
GTFSpath=os.getcwd()+"\\DATA\\GTFS data\\from_milad\\buses"
GISpath=os.getcwd()+"\\work_gis"
BOpath = os.getcwd()+"\\DATA\\Bus Occupancy\\csv_form"
list_date_bo=["20160808","20160809","20160810","20160811","20160812","20160813","20160814","20161122","20161123","20161124","20161125","20161126","20161127","20161226","20161227","20161228","20161229","20161230","20161231","20170101"]
name = 'Pdataset_10_V04'
df = pd.read_csv(os.getcwd()+"\\Output\\"+name+'.csv')
print(df.head())
print(df.shape)

# function a :  cut at 0.3 mm then ln(x)
df['R_fa']=0
df.loc[df['R']>=0.3,'R_fa'] = np.log(df['R'])
df['R_fa']=df['R_fa'].replace([np.nan, -np.inf], 0)

# function b:  cut at 0.03 mm then ln(x)
df['R_fb']=0
df.loc[df['R']>=0.03,'R_fb'] = np.log(df['R'])
df['R_fb']=df['R_fb'].replace([np.nan, -np.inf], 0)

# function b3:  cut at 0.003 mm then ln(x)
df['R_fb3']=0
df.loc[df['R']>=0.003,'R_fb3'] = np.log(df['R'])
df['R_fb3']=df['R_fb3'].replace([np.nan, -np.inf], 0)

# function c :  cut at 0.03 mm but the upper part is reserved
df['R_fc']=df['R']
df.loc[df['R']< 0.03,'R_fc'] = 0

# function d :  cut at 0.03 mm and sqrt(x)
df['R_fd']=df['R']
df.loc[df['R']< 0.03,'R_fd'] = 0
df.loc[df['R']>=0.03,'R_fd'] = np.sqrt(df['R'])

# lag
df['lag2_TR85'] = df['R85'].shift(2)
df.loc[df["TRANSIT_STOP_SEQUENCE"] ==1,['lag2_TR85']] = 0
df['lag3_TR85'] = df['R85'].shift(3)
df.loc[df["TRANSIT_STOP_SEQUENCE"] ==1,['lag3_TR85']] = 0
df['lag4_TR85'] = df['R85'].shift(4)
df.loc[df["TRANSIT_STOP_SEQUENCE"] ==1,['lag4_TR85']] = 0

df['lag1_lnR'] = df['ln(R)'].shift(1)
df.loc[df["TRANSIT_STOP_SEQUENCE"] ==1,['lag1_lnR']] = 0
df['lag2_lnR'] = df['ln(R)'].shift(2)
df.loc[df["TRANSIT_STOP_SEQUENCE"] ==1,['lag2_lnR']] = 0
df['lag3_lnR'] = df['ln(R)'].shift(3)
df.loc[df["TRANSIT_STOP_SEQUENCE"] ==1,['lag3_lnR']] = 0
df['lag4_lnR'] = df['ln(R)'].shift(4)
df.loc[df["TRANSIT_STOP_SEQUENCE"] ==1,['lag4_lnR']] = 0

#filter limit min
limit = 25
##df=df[(df['DELAY'] < limit)&(df['DELAY'] > -limit)]
##blacklist = []
##for index,row in df.iterrows():
##    if (row['DELAY'] > limit) or (row['DELAY'] < -limit):
##        blacklist.append(row['TRIP_CODE'])
##df = df[~df['TRIP_CODE'].isin(blacklist)]

df.to_csv(os.getcwd()+"\\Output\\"+name+".csv",index=False)