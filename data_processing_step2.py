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
name = 'dataset_10'
df = pd.read_csv(os.getcwd()+"\\Output\\"+name+'.csv')

# Rainfall cutout at 85th percentile
line85 = df['R'].quantile(0.85)
line90 = df['R'].quantile(0.90)
df['R85']=0
df.loc[df['R']< line85,'R85'] = 0
df.loc[df['R']>=line85,'R85'] = 1
df['R90']=0
df.loc[df['R']< line90,'R90'] = 0
df.loc[df['R']>=line90,'R90'] = 1

#Operation Temp
df['T-20']=df['T']-20
df['T_diff_2']=df['T-20']*df['T-20']

df['lag1_T-25']= df['T-25'].shift()
df['lag1_T-20']= df['T-20'].shift()
df['lag1_T'] = df['T'].shift()
df['lag1_R'] = df['R'].shift()
df['lag1_H'] = df['H'].shift()
df['lag1_W'] = df['W'].shift()
df['lag1_P'] =  df['P'].shift()
df['lag1_R85'] = df['R85'].shift()
df['lag1_R90'] = df['R90'].shift()
df['lag1_DELAY'] = df['DELAY'].shift()
df.loc[df["TRANSIT_STOP_SEQUENCE"] ==1,['lag1_T-25','lag1_T-20','lag1_T','lag1_R','lag1_H','lag1_W','lag1_P','lag1_R85','lag1_R90','lag1_DELAY']] = 0

#2 lag
df['lag2_T-25'] = df['lag1_T-25'].shift()
df['lag2_T-20'] = df['lag1_T-20'].shift()
df['lag2_T'] = df['lag1_T'].shift()
df['lag2_R'] = df['lag1_R'].shift()
df['lag2_H'] = df['lag1_H'].shift()
df['lag2_W'] = df['lag1_W'].shift()
df['lag2_P'] = df['lag1_P'].shift()
df['lag2_R85'] = df['lag1_R85'].shift()
df['lag2_R90'] = df['lag1_R90'].shift()
df['lag2_DELAY'] = df['lag1_DELAY'].shift()
df.loc[df["TRANSIT_STOP_SEQUENCE"] ==1,['lag2_T-25','lag2_T-20','lag2_T','lag2_R','lag2_H','lag2_W','lag2_P','lag2_R85','lag2_R90','lag2_DELAY']] = 0

#3 lag
df['lag3_T-25'] = df['lag2_T-25'].shift()
df['lag3_T-20'] = df['lag2_T-20'].shift()
df['lag3_T'] = df['lag2_T'].shift()
df['lag3_R'] = df['lag2_R'].shift()
df['lag3_H'] = df['lag2_H'].shift()
df['lag3_W'] = df['lag2_W'].shift()
df['lag3_P'] = df['lag2_P'].shift()
df['lag3_R85'] = df['lag2_R85'].shift()
df['lag3_R90'] = df['lag2_R90'].shift()
df['lag3_DELAY'] = df['lag2_DELAY'].shift()
df.loc[df["TRANSIT_STOP_SEQUENCE"] ==1,['lag3_T-25','lag3_T-20','lag3_T','lag3_R','lag3_H','lag3_W','lag3_P','lag3_R85','lag3_R90','lag3_DELAY']] = 0

#4 lag
df['lag4_T-25'] = df['lag3_T-25'].shift()
df['lag4_T-20'] = df['lag3_T-20'].shift()
df['lag4_T'] = df['lag3_T'].shift()
df['lag4_R'] = df['lag3_R'].shift()
df['lag4_H'] = df['lag3_H'].shift()
df['lag4_W'] = df['lag3_W'].shift()
df['lag4_P'] = df['lag3_P'].shift()
df['lag4_R85'] = df['lag3_R85'].shift()
df['lag4_R90'] = df['lag3_R90'].shift()
df['lag4_DELAY'] = df['lag3_DELAY'].shift()
df.loc[df["TRANSIT_STOP_SEQUENCE"] ==1,['lag4_T-25','lag4_T-20','lag4_T','lag4_R','lag4_H','lag4_W','lag4_P','lag4_R85','lag4_R90','lag4_DELAY']] = 0

#5 lag
df['lag5_T-25'] = df['lag4_T-25'].shift()
df['lag5_T-20'] = df['lag4_T-20'].shift()
df['lag5_T'] = df['lag4_T'].shift()
df['lag5_R'] = df['lag4_R'].shift()
df['lag5_H'] = df['lag4_H'].shift()
df['lag5_W'] = df['lag4_W'].shift()
df['lag5_P'] = df['lag4_P'].shift()
df['lag5_R85'] = df['lag4_R85'].shift()
df['lag5_R90'] = df['lag4_R90'].shift()
df['lag5_DELAY'] = df['lag4_DELAY'].shift()
df.loc[df["TRANSIT_STOP_SEQUENCE"] ==1,['lag5_T-25','lag5_T-20','lag5_T','lag5_R','lag5_H','lag5_W','lag5_P','lag5_R85','lag5_R90','lag5_DELAY']] = 0

#6 lag
df['lag6_T-25'] = df['lag5_T-25'].shift()
df['lag6_T-20'] = df['lag5_T-20'].shift()
df['lag6_T']    = df['lag5_T'].shift()
df['lag6_R']    = df['lag5_R'].shift()
df['lag6_H']    = df['lag5_H'].shift()
df['lag6_W']    = df['lag5_W'].shift()
df['lag6_P']    = df['lag5_P'].shift()
df['lag6_R85']  = df['lag5_R85'].shift()
df['lag6_R90']  = df['lag5_R90'].shift()
df['lag6_DELAY']= df['lag5_DELAY'].shift()
df.loc[df["TRANSIT_STOP_SEQUENCE"] ==1,['lag6_T-25','lag6_T-20','lag6_T','lag6_R','lag6_H','lag6_W','lag6_P','lag6_R85','lag6_R90','lag6_DELAY']] = 0

#7 lag
df['lag7_T-25'] = df['lag6_T-25'].shift()
df['lag7_T-20'] = df['lag6_T-20'].shift()
df['lag7_T']    = df['lag6_T'].shift()
df['lag7_R']    = df['lag6_R'].shift()
df['lag7_H']    = df['lag6_H'].shift()
df['lag7_W']    = df['lag6_W'].shift()
df['lag7_P']    = df['lag6_P'].shift()
df['lag7_R85']  = df['lag6_R85'].shift()
df['lag7_R90']  = df['lag6_R90'].shift()
df['lag7_DELAY']= df['lag6_DELAY'].shift()
df.loc[df["TRANSIT_STOP_SEQUENCE"] ==1,['lag7_T-25','lag7_T-20','lag7_T','lag7_R','lag7_H','lag7_W','lag7_P','lag7_R85','lag7_R90','lag7_DELAY']] = 0

#8 lag
df['lag8_T-25'] = df['lag7_T-25'].shift()
df['lag8_T-20'] = df['lag7_T-20'].shift()
df['lag8_T']    = df['lag7_T'].shift()
df['lag8_R']    = df['lag7_R'].shift()
df['lag8_H']    = df['lag7_H'].shift()
df['lag8_W']    = df['lag7_W'].shift()
df['lag8_P']    = df['lag7_P'].shift()
df['lag8_R85']  = df['lag7_R85'].shift()
df['lag8_R90']  = df['lag7_R90'].shift()
df['lag8_DELAY']= df['lag7_DELAY'].shift()
df.loc[df["TRANSIT_STOP_SEQUENCE"] ==1,['lag8_T-25','lag8_T-20','lag8_T','lag8_R','lag8_H','lag8_W','lag8_P','lag8_R85','lag8_R90','lag8_DELAY']] = 0

#9 lag
df['lag9_T-25'] = df['lag8_T-25'].shift()
df['lag9_T-20'] = df['lag8_T-20'].shift()
df['lag9_T']    = df['lag8_T'].shift()
df['lag9_R']    = df['lag8_R'].shift()
df['lag9_H']    = df['lag8_H'].shift()
df['lag9_W']    = df['lag8_W'].shift()
df['lag9_P']    = df['lag8_P'].shift()
df['lag9_R85']  = df['lag8_R85'].shift()
df['lag9_R90']  = df['lag8_R90'].shift()
df['lag9_DELAY']= df['lag8_DELAY'].shift()
df.loc[df["TRANSIT_STOP_SEQUENCE"] ==1,['lag9_T-25','lag9_T-20','lag9_T','lag9_R','lag9_H','lag9_W','lag9_P','lag9_R85','lag9_R90','lag9_DELAY']] = 0

#10 lag
df['lag10_T-25'] = df['lag9_T-25'].shift()
df['lag10_T-20'] = df['lag9_T-20'].shift()
df['lag10_T']    = df['lag9_T'].shift()
df['lag10_R']    = df['lag9_R'].shift()
df['lag10_H']    = df['lag9_H'].shift()
df['lag10_W']    = df['lag9_W'].shift()
df['lag10_P']    = df['lag9_P'].shift()
df['lag10_R85']  = df['lag9_R85'].shift()
df['lag10_R90']  = df['lag9_R90'].shift()
df['lag10_DELAY']= df['lag9_DELAY'].shift()
df.loc[df["TRANSIT_STOP_SEQUENCE"] ==1,['lag10_T-25','lag10_T-20','lag10_T','lag10_R','lag10_H','lag10_W','lag10_P','lag10_R85','lag10_R90','lag10_DELAY']] = 0

#intersection
#THIS IS FOR dataset_6 ONLYYYYYYY

#
##df_x =  pd.read_csv(os.getcwd()+"\\DATA\\Physical aspects\\road_intersection.csv")
##df['INTERSECTION_S'] = np.nan
##df['INTERSECTION_U'] = np.nan
##df['INTERSECTION'] = np.nan
##for index in range(len(df)):
##    a = df.loc[index,'ROUTE_VARIANT']
##    b = df.loc[index,'TRANSIT_STOP_SEQUENCE']
##    df.loc[index,'INTERSECTION_S']=df_x[(df_x['ROUTE_VARIANT'] == a) & (df_x['TRANSIT_STOP_SEQUENCE'] == b)]['INTERSECTION_S'].item()
##
##    df.loc[index,'INTERSECTION_U']=df_x[(df_x['ROUTE_VARIANT'] == a) & (df_x['TRANSIT_STOP_SEQUENCE'] == b)]['INTERSECTION_U'].item()
##
##    df.loc[index,'INTERSECTION']=df_x[(df_x['ROUTE_VARIANT'] == a) & (df_x['TRANSIT_STOP_SEQUENCE'] == b)]['INTERSECTION'].item()
##
##
##del df_x
##df[['INTERSECTION_S','INTERSECTION_U','INTERSECTION']].head()

# and the rain should cut at 0.3 mm
df['R0.3mm']=0
df.loc[df['R']< 0.3,'R0.3mm'] = 0
df.loc[df['R']>=0.3,'R0.3mm'] = 1

df['R0.03mm']=0
df.loc[df['R']< 0.03,'R0.03mm'] = 0
df.loc[df['R']>=0.03,'R0.03mm'] = 1

df['R0.003mm']=0
df.loc[df['R']< 0.003,'R0.003mm'] = 0
df.loc[df['R']>=0.003,'R0.003mm'] = 1

# make abs |DELAY|
df['|DELAY|'] = df['DELAY'].abs()
df['|lag1_DELAY|'] = df['lag1_DELAY'].abs()
df['|lag2_DELAY|'] = df['lag2_DELAY'].abs()
df['|lag3_DELAY|'] = df['lag3_DELAY'].abs()
df['|lag4_DELAY|'] = df['lag4_DELAY'].abs()
df['|lag5_DELAY|'] = df['lag5_DELAY'].abs()

df['ln(R)']=np.log(df['R'])
df['ln(R)']=df['ln(R)'].replace([np.nan, -np.inf], 0)

# function :  cut at 0.03 mm then ln(x)
df['ln(R)cut']=np.log(df['R'])
df.loc[df['R']< 0.03,'ln(R)cut'] = 0
df.loc[df['R']>=0.03,'ln(R)cut'] = np.log(df['R'])
df['ln(R)cut']=df['ln(R)cut'].replace([np.nan, -np.inf], 0)
print('************************')
print(df['R'].describe())
print('************************')
print(df['ln(R)cut'].describe())

# HOLIDAYS
df_x =  pd.read_csv(os.getcwd()+"\\DATA\\Day_type\\daytype_text.txt",sep='\t')
df['HOLIDAY'] = np.nan
df['WEEKDAY'] = np.nan
df['SATURDAY'] = np.nan
df['SUNDAY'] = np.nan
df['SCHOOL'] = np.nan
for index in range(len(df)):
    a = df.loc[index,'CALENDAR_DATE']
    df.loc[index,'HOLIDAY'] =df_x[(df_x['CALENDAR_DATE'] == a)]['HOLIDAY'].item()
    df.loc[index,'WEEKDAY'] =df_x[(df_x['CALENDAR_DATE'] == a)]['WEEKDAY'].item()
    df.loc[index,'SATURDAY']  =df_x[(df_x['CALENDAR_DATE'] == a)]['SATURDAY'].item()
    df.loc[index,'SUNDAY']   =df_x[(df_x['CALENDAR_DATE'] == a)]['SUNDAY'].item()
    df.loc[index,'SCHOOL']   =df_x[(df_x['CALENDAR_DATE'] == a)]['SCHOOL'].item()
del df_x



df.to_csv(os.getcwd()+"\\Output\\"+"P"+name+"_V04"+".csv",index=False)