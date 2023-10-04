#import các library để dùng
from influxdb import InfluxDBClient, DataFrameClient
import pyodbc as pyodbc
import pandas_datareader as web
from sqlalchemy import create_engine
import pandas as pd
import time
import datetime
from datetime import datetime, timedelta

#Connect to influxdb
print('***********************Chuyển dữ liệu ALERT UTILITY từ influxdb sang SQL Server****************************')
print("Loading...")
client = InfluxDBClient(host='S01', port=8086, username='etm', password='etm#123', verify_ssl=True)

client.switch_database('winccoa')

start_datetime = datetime.now() - timedelta(days=1, hours=8)
start_datetime = pd.to_datetime(start_datetime).isoformat() + "Z"

# end_datetime = datetime.now() - timedelta(hours=6)
end_datetime = pd.to_datetime('today').isoformat() + "Z"
#------------------PHẦN ALERT------------------------ WHERE ("name" = \'System1:S7_1.value.PVLAST\')
results = client.query('SELECT  "_abbr_0", "_prior", "_text_0", "_ack_time", "_ack_user", "_ackable", "_direction", "alarmvalue_bool", "alarmvalue_float", "alarmvalue_int", "_comment", "_class", "_panel", "name" FROM "ALERT"."ALERT" WHERE ("name" =~ /System1:UTI_*/ AND "name" =~ /System1:Compressor*/ AND "name" =~ /System1:MSB0*/ AND "time" >= \''+ start_datetime +'\' AND "time" <= \'' + end_datetime + '\')')
#ép từ <class:influxdb resultSet về list>
eventData = list(results.get_points(measurement='ALERT'))
#print(eventData)
listTagname = []
listTime = []
listFloat = []
listInt = []
listBool = []
listMsg = []
listShortName = []
listPriority = []
listAckTime = []
listAckUser = []
listAckAble = []
listDirection = []
listComment = []
listAlertClass = []
listPanel = []
listHour = []
listFilter = []

#Đưa data vô list
for i in eventData:
    listTagname.append(i['name'])
    
    #Sửa định dạng và cộng thêm 7h
    da = i['time'].split('T')
    ti = da[1].split('Z')
    d = da[0] + ' ' + ti[0]
    a_date = pd.to_datetime(d) + pd.DateOffset(hours=7)
    a_date = a_date.strftime('%Y-%m-%d %H:%M:%S.%f')

    iHour = pd.to_datetime(d) + pd.DateOffset(hours=7)
    iHour = iHour.strftime('%H')
    listHour.append(iHour)

    iFilter = pd.to_datetime(d) + pd.DateOffset(hours=7)
    iFilter = iFilter.strftime('%Y-%m-%d %H')
    listFilter.append(iFilter)
    
    listTime.append(a_date)
    listFloat.append(i['alarmvalue_float'])
    listInt.append(i['alarmvalue_int'])
    listBool.append(i['alarmvalue_bool'])
    listMsg.append(i['_text_0'])
    listAckAble.append(i['_ackable'])
    listAckUser.append(i['_ack_user'])
    
    date = pd.to_datetime(i['_ack_time'], format='%Y-%m-%d %H:%M:%S.%f')  
    ack_date = pd.to_datetime(date) + pd.DateOffset(hours=7)
    ack_date = ack_date.strftime('%Y-%m-%d %H:%M:%S.%f')
    
    listAckTime.append(ack_date)
    listDirection.append(i['_direction'])
    listComment.append(i['_comment'])
    listPanel.append(i['_panel'])
    listPriority.append(i['_prior'])
    listShortName.append(i['_abbr_0'])
    listAlertClass.append(i['_class'])

#Đưa list vô dataframe
df = pd.DataFrame({
    'tag_name': listTagname,
    'short_name' : listShortName,
    'priority' : listPriority,
    'alert_time' : listTime, 
    'float_value':listFloat, 
    'int_value':listInt, 
    'bool_value':listBool, 
    'event_msg' : listMsg,
    'direction' : listDirection,
    'ack_time' : listAckTime,
    'ack_able' : listAckAble,
    'ack_user' : listAckUser,
    'alert_class' : listAlertClass,
    'from_panel' : listPanel,
    'alert_comment' : listComment,
    'iHour' : listHour,
    'iFilter' : listFilter
})
#--------------------***************---------------------
#Connect to SQL Server
engine = create_engine('mssql+pyodbc://S02/ArchiveDB?driver=SQL+Server+Native+Client+11.0')
connection = engine.connect()

result = connection.execute("SELECT DB_NAME(), SYSTEM_USER;")
db_info = result.first()
df.to_sql('ALERT_UTILITY', con = engine, if_exists = 'append', chunksize=1000, index=False)
print("Hoàn tất")
print(f"Connected to database '{db_info[0]}' as user '{db_info[1]}'")
#time.sleep(84600)