#import các library để dùng
from influxdb import InfluxDBClient, DataFrameClient
import pyodbc as pyodbc
import pandas_datareader as web
from sqlalchemy import create_engine
import pandas as pd
import time
from datetime import datetime, timedelta

print('**********Chuyển dữ liệu EVENT UTILITY từ influxdb sang SQL Server*************')
print("Loading...")
#Connect to influxdb
client = InfluxDBClient(host='S01', port=8086, username='etm', password='etm#123', verify_ssl=True)

#pip install tên cái package mà mình muốn cài
#pip install influxdb -> để python connect với influxDB
#pip install pyodbc -> để python connect với SQL SERVER
#pip install pandas_datareader -> -> để đọc data từ influxDB
#pip install sqlalchemy -> dùng để đưa data từ 1 table giả vào table thật trong SQL SERVER
#pip install pandas -> dùng để tạo ra 1 cái table giả có data
#pip install pyinstaller ->dùng để chuyển từ file .py sang .exe
#pyinstaller filename.py -> dùng để chuyển file .py sang file .exe

#print(client.get_list_database())
#Dùng database winccoa trong influxdb
client.switch_database('winccoa')

start_datetime = datetime.now() - timedelta(days=1, hours=8)#1/12/2022 8h = 30/11/2022 23h
start_datetime = pd.to_datetime(start_datetime).isoformat() + "Z"

# end_datetime = datetime.now() - timedelta(hours=6)
end_datetime = pd.to_datetime('today').isoformat() + "Z"
# print(start_datetime,end_datetime)
results = client.query('SELECT "original_value_bool","original_value_float","name", "_user" FROM "EVENT"."EVENT" WHERE ("name" =~ /System1:UTI_*/ AND "name" =~ /.value.PVLAST/ AND "time" >= \''+ start_datetime +'\' AND "time" <= \'' + end_datetime + '\')')

#ép từ <class:influxdb resultSet về list>
eventData = list(results.get_points(measurement='EVENT')) #[{"value":56, "user":0}, {}] tập hợp
#danh sách {["value":56, "user":0], ["value":666, "user":0]}
# print(eventData)
listTime = [] #mảng
listFloatValue = []
listTagname = []
listUser = []
listBoolValue = []
#Đưa data vô list
for i in eventData:
    listTagname.append(i['name']) #[pro1.value.PVLAST, pro2]
    #Sửa định dạng và cộng thêm 7h
    da = i['time'].split('T')
    #2022-12-02T03:07:42.994000Z
    #da[0] = 2022-12-02
    #da[1] = 03:07:42.994000Z
    ti = da[1].split('Z')# = 03:07:42.994000Z
    #ti[0] = 03:07:42.994000
    d = da[0] + ' ' + ti[0] #2022-12-02 03:07:42.994000
    
    req_date = pd.to_datetime(d) + pd.DateOffset(hours=7) #2022-12-02 10:07:42.994000
    req_date = req_date.strftime('%Y-%m-%d %H:%M:%S.%f') #2022-12-02 10:07:42.994000
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
engine = create_engine('mssql+pyodbc://sa:Pass@work1@S02/ArchiveDB?driver=SQL+Server+Native+Client+11.0')
engine = create_engine('mssql+pyodbc://S02/ArchiveDB?driver=SQL+Server+Native+Client+11.0')

#create_engine(url="mysql+pymysql://{0}:{1}@{2}:{3}/{4}".format(user, password, host, port, database))
connection = engine.connect()

result = connection.execute("SELECT DB_NAME(), SYSTEM_USER;")

db_info = result.first()

#đưa data vô sql server ở đây là table eventOA  replace
df.to_sql('EVENT_UTILITY', con = engine, if_exists = 'replace', chunksize=1000, index=False)#remove


print('Hoàn tất')
print(f"Connected to database '{db_info[0]}' as user '{db_info[1]}'")
# time.sleep(300)