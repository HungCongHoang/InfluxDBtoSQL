#import các library để dùng
from influxdb import InfluxDBClient, DataFrameClient
import pyodbc as pyodbc
import pandas_datareader as web
from sqlalchemy import create_engine
import pandas as pd
import time
from datetime import datetime, timedelta

print('***********************Chuyển dữ liệu EVENT PRODUCTION từ influxdb sang SQL Server****************************')
print("Loading...")
#Connect to influxdb
client = InfluxDBClient(host='S01', port=8086, username='etm', password='etm#123', verify_ssl=True)

client.switch_database('winccoa')

start_datetime = datetime.now() - timedelta(days=1, hours=8)
start_datetime = pd.to_datetime(start_datetime).isoformat() + "Z"

# end_datetime = datetime.now() - timedelta(hours=6)
end_datetime = pd.to_datetime('today').isoformat() + "Z"

results = client.query('SELECT "original_value_bool","original_value_float","name", "_user" FROM "EVENT"."EVENT" WHERE ("name" =~ /System1:PRO_*/ AND "time" >= \''+ start_datetime +'\' AND "time" <= \'' + end_datetime + '\')')
#ép từ <class:influxdb resultSet về list>

eventData = list(results.get_points(measurement='EVENT'))
listTime = []
listFloatValue = []
listTagname = []
listUser = []
listBoolValue = []
#Đưa data vô list
for i in eventData:
    listTagname.append(i['name'])
    
    #Sửa định dạng và cộng thêm 7h
    da = i['time'].split('T')
    ti = da[1].split('Z')
    d = da[0] + ' ' + ti[0]
    req_date = pd.to_datetime(d) + pd.DateOffset(hours=7)
    req_date = req_date.strftime('%Y-%m-%d %H:%M:%S.%f')
    
    listTime.append(req_date)
    listFloatValue.append(i['original_value_float'])
    listBoolValue.append(i['original_value_bool'])
    listUser.append(i['_user'])

#Đưa list vô dataframe
df = pd.DataFrame({
    'tag_name': listTagname,
    'event_time' : listTime,
    'float_value':listFloatValue, 
    'bool_value':listBoolValue, 
    'event_user' : listUser
})

#--------------------***************---------------------
#Connect to SQL Server
engine = create_engine('mssql+pyodbc://S02/ArchiveDB?driver=SQL+Server+Native+Client+11.0')
connection = engine.connect()

result = connection.execute("SELECT DB_NAME(), SYSTEM_USER;")
db_info = result.first()

#đưa data vô sql server ở đây là table eventOA  replace
df.to_sql('EVENT_PRODUCTION', con = engine, if_exists = 'replace', chunksize=1000, index=False)

print('Hoàn tất')
print(f"Connected to database '{db_info[0]}' as user '{db_info[1]}'")
#time.sleep(300)