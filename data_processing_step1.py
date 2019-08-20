# -*- coding: utf-8 -*-
"""
Created on Tue May 21 22:17:27 2019

@author: z5176863
"""
#import everything YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY
import numpy as np
import pandas as pd
from datetime import datetime
import os
from os import listdir
from os.path import isfile, join
#YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY
BOMpath=os.getcwd()+"\\DATA\BOM data\\Formated_to_csv"
GTFSpath=os.getcwd()+"\\DATA\\GTFS data\\from_milad\\buses"
GISpath=os.getcwd()+"\\work_gis"
BOpath = os.getcwd()+"\\DATA\\Bus Occupancy\\csv_form"
list_date_bo=["20160808","20160809","20160810","20160811","20160812","20160813","20160814","20161122","20161123","20161124","20161125","20161126","20161127","20161226","20161227","20161228","20161229","20161230","20161231","20170101"]
def distance_twopoints(x1,y1,x2,y2):
    x1=float(x1)
    y1=float(y1)
    x2=float(x2)
    y2=float(y2)
    D = ((x1-x2)**2+(y1-y2)**2)**(1/2)
    return(D)

def time_difference(time_start, time_end):
    '''Calculate the difference between two times on the same date.

    Args:
        time_start: the time to use as a starting point
        time_end: the time to use as an end point

    Returns:
        the difference between time_start and time_end. For example:

        >>> time_difference('15:00', '16:00')
        60

        >>> time_difference('11:00', '13:10')
        130
    '''

    start = datetime.strptime(time_start, "%H:%M")
    end = datetime.strptime(time_end, "%H:%M")
    difference = end - start
    minutes = difference.total_seconds() / 60
    return int(minutes)

def f5(seq, idfun=None):
   #https://www.peterbe.com/plog/uniqifiers-benchmark
   # order preserving
   if idfun is None:
       def idfun(x): return x
   seen = {}
   result = []
   for item in seq:
       marker = idfun(item)
       # in old Python versions:
       # if seen.has_key(marker)
       # but in new ones:
       if marker in seen: continue
       seen[marker] = 1
       result.append(item)
   return result

def weather_extract(BOMSTN,date,peak):
    """go get the bom data at that st on that date """
    df = pd.read_csv(BOMpath+"\\"+"new_csv_of_"+str(BOMSTN)+".csv")
    df = df[df['YYYY']==int(date[:4])]
    df = df[(df['MM']==int(date[5:6]))|(df['MM']==int(date[4:6]))]
    df = df[df['DD']==int(date[6:8])]
    #df.to_csv("weather_extract_1.csv")
    if peak=='AM':
        hour=8 #assume AM peak is 8-9AM
    if peak=='PM':
        hour=5 #assume AM peak is 5-6PM
    df = df[(df['HH']==hour)|(df['HH']==hour-1)]
    df.to_csv("weather_extract_2.csv")
    df.reset_index()
    #Temperature
    T=df.iloc[1]['T']
    #rainfall
    R=df.iloc[1]['R']-df.iloc[0]['R']
    #humidity
    H=df.iloc[1]['H']
    #windspeed
    W=df.iloc[1]['W']
    #pressure
    P=df.iloc[1]['P']
    waether_con=[T,R,H,W,P]
    return(waether_con)

def weather_con_buffer(date,stop_id,peak):
    """Obtains weather conditon from nearest BOM station given stops and date"""
    #locate the BOM station
    df = pd.read_csv(GISpath+"\\"+"INTERSECTION Stops within 20km.csv", sep=",")
    df = df[df['stop_id']==stop_id]
    #if there is one 1 intersection
    if len(df)==1 :
        BOMSTN=df.iloc[0]['BOMSTN']
    #but if mu;tiple overlap,narrow down to the nearest one
    temp_list=[0]*len(df)
    for n in range(0,len(df)):
        x1=df.iloc[n]['stop_lon']
        y1=df.iloc[n]['stop_lat']
        x2=df.iloc[n]['LONG']
        y2=df.iloc[n]['LAT']
        temp_list[n]=distance_twopoints(x1,y1,x2,y2)
    min_val = min(temp_list)
    index_min = temp_list.index(min_val)
    BOMSTN=df.iloc[index_min]['BOMSTN']
    weather_con=weather_extract(BOMSTN,date,peak)
    return(weather_con)

def weather_con_invert(stop_id,date,peak):
    """ get weather condition at a stop by invert distance averrage"""
    #read the distance
    #F:\UNSW matters\Research Thesis\work_area\work_gis
    dfc = pd.read_csv(GISpath+"\\"+"Distance Matrix V6.csv", sep=",",header=0)
    dfc['ID']= dfc['ID'].astype(str)
    #filter to only a stop that we specify
    print(stop_id)
    dfc = dfc[dfc['ID']==stop_id]
    dfc.to_csv("weather_con_invert_1.csv")
    dfc = dfc.set_index('ID').T
    dfc['T']=np.nan #temp
    dfc['R']=np.nan #rain
    dfc['H']=np.nan #humid
    dfc['W']=np.nan #wind
    dfc['P']=np.nan #pressure
    dfc['d-1']=np.nan
    #now that the cal table is ready let's begin populating number
    dfc['d-1']=1/dfc.iloc[:, 0]
    dfc.to_csv("weather_con_invert_2.csv")
    for index,row in dfc.iterrows():
        #for n in range(0,len(df)):
        weather_con = weather_extract(index,date,peak)
        #write them in the cal table
        dfc.at[index,'T']=weather_con[0]
        dfc.at[index,'R']=weather_con[1]
        dfc.at[index,'H']=weather_con[2]
        dfc.at[index,'W']=weather_con[3]
        dfc.at[index,'P']=weather_con[4]
    denominator = dfc['d-1'].sum(axis = 0, skipna = True)
    dfc['Tx'] = dfc['T']*dfc['d-1']
    dfc['Rx'] = dfc['R']*dfc['d-1']
    dfc['Hx'] = dfc['H']*dfc['d-1']
    dfc['Wx'] = dfc['W']*dfc['d-1']
    dfc['Px'] = dfc['P']*dfc['d-1']
    numerator_T = dfc['Tx'].sum(axis = 0, skipna = True)
    numerator_R = dfc['Rx'].sum(axis = 0, skipna = True)
    numerator_H = dfc['Hx'].sum(axis = 0, skipna = True)
    numerator_W = dfc['Wx'].sum(axis = 0, skipna = True)
    numerator_P = dfc['Px'].sum(axis = 0, skipna = True)
    T=numerator_T/denominator
    R=numerator_R/denominator
    H=numerator_H/denominator
    W=numerator_W/denominator
    P=numerator_P/denominator
    waether_con=[T,R,H,W,P]
    del dfc
    return(waether_con)

def table_stoptime_v1(date,bus,bound):
    """create a table of stop time from GTFS"""
    #read routes.txt of a day to get route_id
    file_list = [f for f in listdir(GTFSpath) if isfile(join(GTFSpath, f))]
    #https://www.geeksforgeeks.org/python-finding-strings-with-given-substring-in-list/
    subs = str(date)
    res = list(filter(lambda x: subs in x, file_list))
    folder_name = res[0][:32]
    df = pd.read_csv(GTFSpath+"\\"+folder_name+"\\"+"routes.txt", sep=",")
    df = df[df['route_short_name'] == bus]
    df = df[df['route_desc'] == 'Sydney Buses Network']
    #print(df.shape)
    df.reset_index()
    #print(df)
    #route_id = df.iloc[df.index[0],'route_id']
    route_id = df.iloc[0]['route_id']
    #now that we know route time to go find it in trips
    df = pd.read_csv(GTFSpath+"\\"+folder_name+"\\"+"trips.txt", sep=",")
    df = df[df['route_id']==route_id]
    if bound == 'Inbound':
        direction_id = 1
    if bound == 'Outbound':
        direction_id = 0
    df = df[df['direction_id']==direction_id]
    #make a list of trip ID
    trip_list = df['trip_id'].tolist()
    #go to stop_time
    df = pd.read_csv(GTFSpath+"\\"+folder_name+"\\"+"stop_times.txt", sep=",")
    for item in trip_list:
        df2 = df[df['trip_id']==item]
        df2 = df2[['trip_id','arrival_time',"departure_time","stop_id",'stop_sequence']]
        df2.to_csv(os.getcwd()+"\\Output\\by gtfs\\"+bus+"-"+str(item)+".csv",index=False)
    return()

def table_stoptime_v2(date,buslist,bound,peak):
    """use bus occupancy data instead"""
    DD = date[6:8]
    YY = date[2:4]
    m = {
        "01":'JAN',
        "02":'FEB',
        "03":'MAR',
        "04":'APR',
        "05":'MAY',
        "06":'JUN',
        "07":'JUL',
        "08":'AUG',
        "09":'SEP',
        "10":'OCT',
        "11":'NOV',
        "12":'DEC'
        }
    MM = m[date[4:6]]
    DDMMYY = DD+"-"+MM+"-"+YY
    df_big = pd.read_csv(BOpath+"\\Bus_Occupancy_"+DDMMYY+".csv", sep=",")
    df_big = df_big[df_big['DIRECTION']==bound]
    if peak == "AM":
        df_big = df_big[df_big['TIMETABLE_HOUR_BAND']=="08:00 to 09:00"]
    if peak == "PM":
        df_big = df_big[df_big['TIMETABLE_HOUR_BAND']=="17:00 to 18:00"]
    for bus in buslist:
        df = df_big
        print(bus)
        df = df[df['ROUTE']==bus]
        df["DELAY"] = 0
        df["TIMETABLE_TIME"] = df["TIMETABLE_TIME"].astype(str)
        df["ACTUAL_TIME"] = df["ACTUAL_TIME"].astype(str)
        df.reset_index()
        for index,row in df.iterrows():
            #print(row['ACTUAL_TIME'])
            row['DELAY'] = time_difference(row["TIMETABLE_TIME"],row["ACTUAL_TIME"])
            #print("the difference at"+str(index)+" is "+str(row['DELAY'])+" min")
            df.at[index,'DELAY']=row['DELAY']
        #put weather data in
        df['T']=np.nan #temp
        df['R']=np.nan #rain
        df['H']=np.nan #humid
        df['W']=np.nan #wind
        df['P']=np.nan #pressure
        for index,row in df.iterrows():
            stop_id = str(row['TRANSIT_STOP'])
            weather_info = weather_con_invert(stop_id,date,peak)
            df.at[index,'T']=weather_info[0]
            df.at[index,'R']=weather_info[1]
            df.at[index,'H']=weather_info[2]
            df.at[index,'W']=weather_info[3]
            df.at[index,'P']=weather_info[4]
        #Assume that , the‘standard operating temperature’ has been defined as 15 C,which is the expected temperature of ‘cool morning weather’in Melbourne.
        df['T-25']=df['T']-25
        df.to_csv(os.getcwd()+"\\Output\\by bo\\"+date+"-"+bus+"-"+peak+bound+".csv",index=False)
    return()

def seperate_each_bo(my_peak,my_bound,route_set):
    for day in list_date_bo:
        table_stoptime_v2(day,route_set,my_bound,my_peak)
    print("!-Done-!")
    return

def raw_file_list_in(mypath):
    onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    return (onlyfiles)

def how_many_route(mypath):
    """give me a list of all bus routes"""
    filelist=raw_file_list_in(mypath)
    for n in range(0,len(filelist)):
        df = pd.read_csv(mypath+filelist[n],sep=",")
        df['ROUTE'] = df['ROUTE'].astype(str)
        if n==0:
            buslist1 = df['ROUTE'].unique().tolist()
        else:
            buslist2 = df['ROUTE'].unique().tolist()
            buslist1.extend(buslist2)
    buslist1=f5(buslist1)
    buslist1.sort(key = str.lower)
    return(buslist1)

def how_many_route_common(mypath):
    """give me a list of all unique bus routes that are common in all dates"""
    filelist=raw_file_list_in(mypath)
    for n in range(0,len(filelist)):
        df = pd.read_csv(mypath+filelist[n],sep=",")
        if n==0:
            buslist1 = df['ROUTE'].unique().tolist()
        else:
            buslist2 = df['ROUTE'].unique().tolist()
            buslist1 = list(set(buslist1) & set(buslist2))
    buslist1.sort()
    return(buslist1)

def concat_them(route_selected,name):
    """concact all AM/PM IN/OUT Date base one the given route"""
    MyEmptydf = pd.DataFrame()
    for route in route_selected:
        for date in list_date_bo:
            for peak in ['AM','PM']:
                for bound in ['Inbound','Outbound']:
                    df1 = pd.read_csv(os.getcwd()+"\\Output\\by bo\\"+date+"-"+route+"-"+peak+bound+".csv")
                    MyEmptydf = pd.concat([MyEmptydf, df1])
    #export
    #"For_Training.csv"
    MyEmptydf.to_csv(os.getcwd()+"\\Output\\"+name,index=False)
    return

mypath = os.getcwd()+"\\DATA\\Bus Occupancy\\csv_form\\"
alist = how_many_route(mypath)
clist = how_many_route_common(mypath)
route_selected = ['247','301','324','325','373','374','377','380','389','393','394','396','399','428','431','461','520','610X','L90','M52']
route_selected.sort(key = str.lower)

#####
print("All FUNCTIONS READY!!")
#####

#list_date_bo.reverse()
#seperate_each_bo("AM","Inbound",route_selected)
#seperate_each_bo("AM","Outbound",route_selected)
#seperate_each_bo("PM","Inbound",route_selected)
#seperate_each_bo("PM","Outbound",route_selected)
#concat_them(['288','303','343','381','392','397','438','440','607X','M20'],"dataset_10.csv")

#### Make a sound ####
import winsound
duration = 1000 #millisecond
freq = 440 #Hz
winsound.Beep(freq,duration)
####################