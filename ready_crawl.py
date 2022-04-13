
import pandas as pd
import pyodbc
from pyodbc import *
import time
from sqlalchemy import create_engine
start = time.time()

################################ GET DATA FROM DATABASE 1 #################################
conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=192.168.200.36;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')
cursor = conn.cursor()
cursor.execute('SELECT * FROM DB1_VOD')
records = cursor.fetchall()
db1_VOD = []
columnNames = [column[0] for column in cursor.description]
for record in records:
    db1_VOD.append( dict( zip( columnNames , record ) ) )
db1_VOD = pd.DataFrame(db1_VOD)
db1 = db1_VOD.copy()
################################ GET DATA FROM DATABASE 2 #################################
conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')
cursor = conn.cursor()
cursor.execute('SELECT * FROM DB2_VOD')
records = cursor.fetchall()
db2_VOD = []
columnNames = [column[0] for column in cursor.description]
for record in records:
#    print(record)
    db2_VOD.append( dict( zip( columnNames , record ) ) )
db2_VOD = pd.DataFrame(db2_VOD)
db2 = db2_VOD.copy()
#db2.to_excel('db2.xlsx', index = False)
##################### LENZ #####################
lenz_input=pd.read_csv(r'E:\python codes\VOD\vod_data\1400\lenz_vod\LenzEsfand1400.csv')
vod_lenz_raw = lenz_input.copy()
vod_lenz_raw = vod_lenz_raw.drop(len(vod_lenz_raw)-1)
vod_lenz = vod_lenz_raw.groupby(['content name']).sum().reset_index()

del vod_lenz['average access duration (hour)']
vod_lenz = vod_lenz.rename(columns={"content name":"Title"})
vod_lenz = vod_lenz.rename(columns={"access users":"ActiveUsers"})
vod_lenz = vod_lenz.rename(columns={"access times":"Visit"})
vod_lenz = vod_lenz.rename(columns={"access duration (hour)":"DurationMin"})
vod_lenz['DurationMin'] = round(vod_lenz['DurationMin']*60,0)

vod_lenz['TitleCleaned1'] = vod_lenz['Title']

from RemoveData import *
remove_data_in = vod_lenz.copy()
remove_data_out = RemoveData(remove_data_in)
vod_lenz = remove_data_out.copy()

vod_lenz['TitleCleaned1'] = vod_lenz['TitleCleaned1'].str.replace('دوبله', '')
vod_lenz['TitleCleaned1'] = vod_lenz['TitleCleaned1'].str.replace('(', '')
vod_lenz['TitleCleaned1'] = vod_lenz['TitleCleaned1'].str.replace(')', '')
vod_lenz['TitleCleaned1'] = vod_lenz['TitleCleaned1'].str.replace('سریال', '')

vod_lenz = vod_lenz.reset_index()
del vod_lenz['index']

vod_lenz['TitleCleaned1'] = vod_lenz['TitleCleaned1'].astype(str)
for i in range(0,len(vod_lenz)):     # len(vod_lenz)
     print("S: ", i)
     x_name_content=vod_lenz.loc[i, 'TitleCleaned1']
     head, sep, tail = x_name_content.partition('فصل')
     vod_lenz.loc[i, 'TitleCleaned1'] = head
     vod_lenz.loc[i, 'Season'] = tail

for i in range(0,len(vod_lenz)):     # len(vod_lenz)
     print("E: ", i)
     x_name_content=vod_lenz.loc[i, 'Season']
     head, sep, tail = x_name_content.partition('قسمت')
     vod_lenz.loc[i, 'Season'] = head
     vod_lenz.loc[i, 'Epizode'] = tail

vod_lenz1 = vod_lenz[vod_lenz.TitleCleaned1.str.contains("قسمت")]
vod_lenz2 = vod_lenz[~vod_lenz.TitleCleaned1.str.contains("قسمت")]
vod_lenz1 = vod_lenz1.reset_index()
del vod_lenz1['index']
for i in range(0,len(vod_lenz1)):     # len(vod_lenz)
    print(i)
    x_name_content=vod_lenz1.loc[i, 'TitleCleaned1']
    head, sep, tail = x_name_content.partition('قسمت')
    vod_lenz1.loc[i, 'TitleCleaned1'] = head
    vod_lenz1.loc[i, 'Epizode'] = tail

vod_lenz1['Season'] = '01'
del vod_lenz
vod_lenz = vod_lenz2.append([vod_lenz1])
del vod_lenz1
del vod_lenz2
#####
vod_lenz1 = vod_lenz[vod_lenz.Epizode.str.contains("-")]
vod_lenz2 = vod_lenz[~vod_lenz.Epizode.str.contains("-")]
vod_lenz1 = vod_lenz1.reset_index()
del vod_lenz1['index']
for i in range(0,len(vod_lenz1)):     # len(vod_lenz)
     x_name_content=vod_lenz1.loc[i, 'Epizode']
     head, sep, tail = x_name_content.partition('-')
     vod_lenz1.loc[i, 'Epizode'] = head

del vod_lenz
vod_lenz = vod_lenz2.append([vod_lenz1])
del vod_lenz1
del vod_lenz2
#####
vod_lenz = vod_lenz.reset_index()
del vod_lenz['index']
for i in range(0,len(vod_lenz)):     # len(vod_lenz)
     x_name_content=vod_lenz.loc[i, 'Epizode']
     head, sep, tail = x_name_content.partition(':')
     vod_lenz.loc[i, 'Epizode'] = head
#####
vod_lenz = vod_lenz.reset_index()
del vod_lenz['index']
for i in range(0,len(vod_lenz)):     # len(vod_lenz)
     x_name_content=vod_lenz.loc[i, 'Epizode']
     head, sep, tail = x_name_content.partition('&')
     vod_lenz.loc[i, 'Epizode'] = head
#####
vod_lenz = vod_lenz.reset_index()
del vod_lenz['index']
for i in range(0,len(vod_lenz)):     # len(vod_lenz)
     x_name_content=vod_lenz.loc[i, 'Epizode']
     head, sep, tail = x_name_content.partition('بخش')
     vod_lenz.loc[i, 'Epizode'] = head
#####
vod_lenz = vod_lenz.reset_index()
del vod_lenz['index']
for i in range(0,len(vod_lenz)):     # len(vod_lenz)
     x_name_content=vod_lenz.loc[i, 'Epizode']
     head, sep, tail = x_name_content.partition('_')
     vod_lenz.loc[i, 'Epizode'] = head
#####
vod_lenz = vod_lenz.reset_index()
del vod_lenz['index']
for i in range(0,len(vod_lenz)):     # len(vod_lenz)
     x_name_content=vod_lenz.loc[i, 'Epizode']
     head, sep, tail = x_name_content.partition(',')
     vod_lenz.loc[i, 'Epizode'] = head
#####
vod_lenz = vod_lenz.reset_index()
del vod_lenz['index']
for i in range(0,len(vod_lenz)):     # len(vod_lenz)
     x_name_content=vod_lenz.loc[i, 'Epizode']
     head, sep, tail = x_name_content.partition('(')
     vod_lenz.loc[i, 'Epizode'] = head
#####
   
#vod_lenz1 = vod_lenz[vod_lenz.Epizode.str.contains("و")]
#vod_lenz2 = vod_lenz[~vod_lenz.Epizode.str.contains("و")]
#vod_lenz1 = vod_lenz1.reset_index()
#del vod_lenz1['index']
#for i in range(0,len(vod_lenz1)):     # len(vod_lenz)
#     x_name_content=vod_lenz1.loc[i, 'Epizode']
#     head, sep, tail = x_name_content.partition('و')
#     vod_lenz1.loc[i, 'Epizode'] = head
#
#del vod_lenz
#vod_lenz = vod_lenz2.append([vod_lenz1])
#del vod_lenz1
#del vod_lenz2
#####
vod_lenz_repeat_1 = vod_lenz.copy()
vod_lenz = vod_lenz_repeat_1.copy()

#vod_lenz['Epizode'] = vod_lenz['Epizode'].str.replace('اول', '01')
#vod_lenz['Epizode'] = vod_lenz['Epizode'].str.replace('دوم', '02')
#vod_lenz['Epizode'] = vod_lenz['Epizode'].str.replace('سوم', '03')
#vod_lenz['Epizode'] = vod_lenz['Epizode'].str.replace('چهارم', '04')
#vod_lenz['Epizode'] = vod_lenz['Epizode'].str.replace('پنجم', '05')
#vod_lenz['Epizode'] = vod_lenz['Epizode'].str.replace('ششم', '06')
#vod_lenz['Epizode'] = vod_lenz['Epizode'].str.replace('هفتم', '07')
#vod_lenz['Epizode'] = vod_lenz['Epizode'].str.replace('هشتم', '08')
#vod_lenz['Epizode'] = vod_lenz['Epizode'].str.replace('نهم', '09')
#vod_lenz['Epizode'] = vod_lenz['Epizode'].str.replace('دهم', '10')
#vod_lenz['Epizode'] = vod_lenz['Epizode'].str.replace('یازدهم', '11')
#vod_lenz['Epizode'] = vod_lenz['Epizode'].str.replace('دوازدهم', '12')
#vod_lenz['Epizode'] = vod_lenz['Epizode'].str.replace('سیزدهم', '13')
#vod_lenz['Epizode'] = vod_lenz['Epizode'].str.replace('چهاردهم', '14')
#vod_lenz['Epizode'] = vod_lenz['Epizode'].str.replace('پانزدهم', '15')
#vod_lenz['Epizode'] = vod_lenz['Epizode'].str.replace('شانزدهم', '16')
#vod_lenz['Epizode'] = vod_lenz['Epizode'].str.replace('هفدهم', '17')
#vod_lenz['Epizode'] = vod_lenz['Epizode'].str.replace('هجدهم', '18')
#vod_lenz['Epizode'] = vod_lenz['Epizode'].str.replace('نوزدهم', '19')
#vod_lenz['Epizode'] = vod_lenz['Epizode'].str.replace('بیستم', '20')

vod_lenz = vod_lenz.reset_index()
del vod_lenz['index']
for i in range(0,len(vod_lenz)):     # len(vod_lenz)
     x_name_content=vod_lenz.loc[i, 'Season']
     head, sep, tail = x_name_content.partition(':')
     vod_lenz.loc[i, 'Season'] = head

#vod_lenz['Season'] = vod_lenz['Season'].str.replace('اول', '01')
#vod_lenz['Season'] = vod_lenz['Season'].str.replace('دوم', '02')
#vod_lenz['Season'] = vod_lenz['Season'].str.replace('سوم', '03')
#vod_lenz['Season'] = vod_lenz['Season'].str.replace('چهارم', '04')
#vod_lenz['Season'] = vod_lenz['Season'].str.replace('پنجم', '05')
#vod_lenz['Season'] = vod_lenz['Season'].str.replace('ششم', '06')
#vod_lenz['Season'] = vod_lenz['Season'].str.replace('هفتم', '07')
#vod_lenz['Season'] = vod_lenz['Season'].str.replace('هشتم', '08')
#vod_lenz['Season'] = vod_lenz['Season'].str.replace('نهم', '09')
#vod_lenz['Season'] = vod_lenz['Season'].str.replace('دهم', '10')
#vod_lenz['Season'] = vod_lenz['Season'].str.replace('یازدهم', '11')
#vod_lenz['Season'] = vod_lenz['Season'].str.replace('دوازدهم', '12')
#vod_lenz['Season'] = vod_lenz['Season'].str.replace('سیزدهم', '13')
#vod_lenz['Season'] = vod_lenz['Season'].str.replace('چهاردهم', '14')
#vod_lenz['Season'] = vod_lenz['Season'].str.replace('پانزدهم', '15')
#vod_lenz['Season'] = vod_lenz['Season'].str.replace('شانزدهم', '16')
#vod_lenz['Season'] = vod_lenz['Season'].str.replace('هفدهم', '17')
#vod_lenz['Season'] = vod_lenz['Season'].str.replace('هجدهم', '18')
#vod_lenz['Season'] = vod_lenz['Season'].str.replace('نوزدهم', '19')
#vod_lenz['Season'] = vod_lenz['Season'].str.replace('بیستم', '20')

#vod_lenz['TitleCleaned1'] = vod_lenz['TitleCleaned1'].str.replace('0', '۰')
#vod_lenz['TitleCleaned1'] = vod_lenz['TitleCleaned1'].str.replace('1', '۱')
#vod_lenz['TitleCleaned1'] = vod_lenz['TitleCleaned1'].str.replace('2', '۲')
#vod_lenz['TitleCleaned1'] = vod_lenz['TitleCleaned1'].str.replace('3', '۳')
#vod_lenz['TitleCleaned1'] = vod_lenz['TitleCleaned1'].str.replace('4', '۴')
#vod_lenz['TitleCleaned1'] = vod_lenz['TitleCleaned1'].str.replace('5', '۵')
#vod_lenz['TitleCleaned1'] = vod_lenz['TitleCleaned1'].str.replace('6', '۶')
#vod_lenz['TitleCleaned1'] = vod_lenz['TitleCleaned1'].str.replace('7', '۷')
#vod_lenz['TitleCleaned1'] = vod_lenz['TitleCleaned1'].str.replace('8', '۸')
#vod_lenz['TitleCleaned1'] = vod_lenz['TitleCleaned1'].str.replace('9', '۹')
#vod_lenz['TitleCleaned1'] = vod_lenz['TitleCleaned1'].str.replace('٠', '۰')
#vod_lenz['TitleCleaned1'] = vod_lenz['TitleCleaned1'].str.replace('١', '۱')
#vod_lenz['TitleCleaned1'] = vod_lenz['TitleCleaned1'].str.replace('٢', '۲')
#vod_lenz['TitleCleaned1'] = vod_lenz['TitleCleaned1'].str.replace('٣', '۳')
#vod_lenz['TitleCleaned1'] = vod_lenz['TitleCleaned1'].str.replace('٤', '۴')
#vod_lenz['TitleCleaned1'] = vod_lenz['TitleCleaned1'].str.replace('٥', '۵')
#vod_lenz['TitleCleaned1'] = vod_lenz['TitleCleaned1'].str.replace('٦', '۶')
#vod_lenz['TitleCleaned1'] = vod_lenz['TitleCleaned1'].str.replace('٧', '۷')
#vod_lenz['TitleCleaned1'] = vod_lenz['TitleCleaned1'].str.replace('٨', '۸')
#vod_lenz['TitleCleaned1'] = vod_lenz['TitleCleaned1'].str.replace('٩', '۹')
#vod_lenz['TitleCleaned1'] = vod_lenz['TitleCleaned1'].str.replace('ي', 'ی')
#vod_lenz['TitleCleaned1'] = vod_lenz['TitleCleaned1'].str.replace('ؤ','و')
#vod_lenz['TitleCleaned1'] = vod_lenz['TitleCleaned1'].str.replace('ك','ک')

#total_vod_1399_Ghadimi_v2 = pd.read_excel(r'E:\python codes\VOD\kh_ghadimi\total_vod_1399_Ghadimi_v2.xlsx')
#total_vod_1399_Ghadimi_v2=total_vod_1399_Ghadimi_v2.rename(columns={"شماره فصل":"season"})
#total_vod_1399_Ghadimi_v2 = total_vod_1399_Ghadimi_v2.fillna("NO")
#total_vod_1399_Ghadimi_v2['season'] = total_vod_1399_Ghadimi_v2['season'].astype(str)
#total_vod_1399_Ghadimi_v2 = total_vod_1399_Ghadimi_v2[~total_vod_1399_Ghadimi_v2.season.str.contains("NO")]
#total_vod_1399_Ghadimi_v2['season'] = total_vod_1399_Ghadimi_v2['season'].astype(str).replace('\.0', '', regex=True)

vod_lenz['TitleCleaned1'] = vod_lenz['TitleCleaned1'].str.strip()
vod_lenz['Season'] = vod_lenz['Season'].str.strip() 
vod_lenz['Epizode'] = vod_lenz['Epizode'].str.strip() 
vod_lenz['Season'] = vod_lenz['Season'].apply(lambda x: x.zfill(2))
vod_lenz ['Season'].replace('', '00', inplace=True)
vod_lenz['Epizode'] = vod_lenz['Epizode'].apply(lambda x: x.zfill(3))
vod_lenz ['Epizode'].replace('', '000', inplace=True)
vod_lenz = vod_lenz.reset_index()
del vod_lenz['index']

vod_lenz['Season'] = vod_lenz['Season'].str.replace('0ا', '01')

vod_lenz['Season'] = vod_lenz['Season'].str.replace('موج سواری 2', '00')
vod_lenz['Season'] = vod_lenz['Season'].str.replace('قتل ها', '00')
vod_lenz['Season'] = vod_lenz['Season'].str.replace('شکار', '00')
vod_lenz['Season'] = vod_lenz['Season'].str.replace('2019-2020', '00')

vod_lenz['Epizode'] = vod_lenz['Epizode'].str.replace('4 غریبه', '004')
vod_lenz['Epizode'] = vod_lenz['Epizode'].str.replace('3 زخم ها', '003')
vod_lenz['Epizode'] = vod_lenz['Epizode'].str.replace('2 کله آهنی', '002')
vod_lenz['Epizode'] = vod_lenz['Epizode'].str.replace('2 بهای قدرت 4', '002')
vod_lenz['Epizode'] = vod_lenz['Epizode'].str.replace('2 بهای قدرت 3', '002')
vod_lenz['Epizode'] = vod_lenz['Epizode'].str.replace('2 بهای قدرت 2', '002')
vod_lenz['Epizode'] = vod_lenz['Epizode'].str.replace('2 بهای قدرت 1', '002')
vod_lenz['Epizode'] = vod_lenz['Epizode'].str.replace('17 قسمت پایانی', '017')
vod_lenz['Epizode'] = vod_lenz['Epizode'].str.replace('1 خیزش لاک پشت های نینجا', '001')
vod_lenz['Epizode'] = vod_lenz['Epizode'].str.replace('1 آغاز ش', '001')
#print(vod_lenz.loc[10158, 'Title'])
####### edit of contents #######
vod_lenz_1 = vod_lenz [vod_lenz.Title.str.contains('فصل 1 قسمت 2 : ایکس ری')]
vod_lenz_2 = vod_lenz [~vod_lenz.Title.str.contains('فصل 1 قسمت 2 : ایکس ری')]
vod_lenz_1['TitleCleaned1'] = 'ایکس ری'
vod_lenz_1['Season'] = '01'
vod_lenz_1['Epizode'] = '002'
vod_lenz = vod_lenz_1.append([vod_lenz_2])
###
vod_lenz_1 = vod_lenz [vod_lenz.Title.str.contains('فصل 1 قسمت 3 : اعتماد')]
vod_lenz_2 = vod_lenz [~vod_lenz.Title.str.contains('فصل 1 قسمت 3 : اعتماد')]
vod_lenz_1['TitleCleaned1'] = 'اعتماد'
vod_lenz_1['Season'] = '01'
vod_lenz_1['Epizode'] = '003'
vod_lenz = vod_lenz_1.append([vod_lenz_2])
###
vod_lenz_1 = vod_lenz [vod_lenz.Title.str.contains('فصل 1 قسمت 4 : تازه کار')]
vod_lenz_2 = vod_lenz [~vod_lenz.Title.str.contains('فصل 1 قسمت 4 : تازه کار')]
vod_lenz_1['TitleCleaned1'] = 'تازه کار'
vod_lenz_1['Season'] = '01'
vod_lenz_1['Epizode'] = '004'
vod_lenz = vod_lenz_1.append([vod_lenz_2])
###
vod_lenz_1 = vod_lenz [vod_lenz.Title.str.contains('فصل شکار')]
vod_lenz_2 = vod_lenz [~vod_lenz.Title.str.contains('فصل شکار')]
vod_lenz_1['TitleCleaned1'] = vod_lenz_1['Title']
vod_lenz_1['Season'] = ''
vod_lenz = vod_lenz_1.append([vod_lenz_2])
###
vod_lenz_1 = vod_lenz [vod_lenz.Title.str.contains('فصل 1 قسمت 2 : ایکس ری')]
vod_lenz_2 = vod_lenz [~vod_lenz.Title.str.contains('فصل 1 قسمت 2 : ایکس ری')]
vod_lenz_1['TitleCleaned1'] = 'ایکس ری'
vod_lenz_1['Season'] = '01'
vod_lenz_1['Epizode'] = '002'
vod_lenz = vod_lenz_1.append([vod_lenz_2])
###
vod_lenz_1 = vod_lenz [vod_lenz.Title.str.contains('فصل قتل ها')]
vod_lenz_2 = vod_lenz [~vod_lenz.Title.str.contains('فصل قتل ها')]
vod_lenz_1['TitleCleaned1'] = vod_lenz_1['Title']
vod_lenz_1['Season'] = ''
vod_lenz = vod_lenz_1.append([vod_lenz_2])
###
vod_lenz_1 = vod_lenz [vod_lenz.Title.str.contains('فصل موج سواری 2')]
vod_lenz_2 = vod_lenz [~vod_lenz.Title.str.contains('فصل موج سواری 2')]
vod_lenz_1['TitleCleaned1'] = vod_lenz_1['Title']
vod_lenz_1['Season'] = ''
vod_lenz = vod_lenz_1.append([vod_lenz_2])
###
vod_lenz_1 = vod_lenz [vod_lenz.Title.str.contains('فصل 1 قسمت 1 : خاموش شدن')]
vod_lenz_2 = vod_lenz [~vod_lenz.Title.str.contains('فصل 1 قسمت 1 : خاموش شدن')]
vod_lenz_1['TitleCleaned1'] = 'خاموش شدن'
vod_lenz_1['Season'] = '01'
vod_lenz_1['Epizode'] = '001'
vod_lenz = vod_lenz_1.append([vod_lenz_2])
###
vod_lenz_1 = vod_lenz [vod_lenz.Title.str.contains('فصل 1 قسمت 5 : راولو')]
vod_lenz_2 = vod_lenz [~vod_lenz.Title.str.contains('فصل 1 قسمت 5 : راولو')]
vod_lenz_1['TitleCleaned1'] = 'راولو'
vod_lenz_1['Season'] = '01'
vod_lenz_1['Epizode'] = '005'
vod_lenz = vod_lenz_1.append([vod_lenz_2])
###
vod_lenz_1 = vod_lenz [vod_lenz.Title.str.contains('فصل 1 قسمت 6 : سوزاندن انسان')]
vod_lenz_2 = vod_lenz [~vod_lenz.Title.str.contains('فصل 1 قسمت 6 : سوزاندن انسان')]
vod_lenz_1['TitleCleaned1'] = 'سوزاندن انسان'
vod_lenz_1['Season'] = '01'
vod_lenz_1['Epizode'] = '006'
vod_lenz = vod_lenz_1.append([vod_lenz_2])
###
vod_lenz_1 = vod_lenz [vod_lenz.Title.str.contains('قسمت نوزدهم -شاعر آئینی مهدی زنگنه')]
vod_lenz_2 = vod_lenz [~vod_lenz.Title.str.contains('قسمت نوزدهم -شاعر آئینی مهدی زنگنه')]
vod_lenz_1['TitleCleaned1'] = 'شاعر آئینی مهدی زنگنه'
vod_lenz_1['Season'] = ''
vod_lenz_1['Epizode'] = '019'
vod_lenz = vod_lenz_1.append([vod_lenz_2])
###
vod_lenz_1 = vod_lenz [vod_lenz.Title.str.contains('قسمت 1-سیزده نه در')]
vod_lenz_2 = vod_lenz [~vod_lenz.Title.str.contains('قسمت 1-سیزده نه در')]
vod_lenz_1['TitleCleaned1'] = 'سیزده نه در'
vod_lenz_1['Season'] = ''
vod_lenz_1['Epizode'] = '001'
vod_lenz = vod_lenz_1.append([vod_lenz_2])
###
vod_lenz_1 = vod_lenz [vod_lenz.Title.str.contains('قسمت 2-سیزده نه در')]
vod_lenz_2 = vod_lenz [~vod_lenz.Title.str.contains('قسمت 2-سیزده نه در')]
vod_lenz_1['TitleCleaned1'] = 'سیزده نه در'
vod_lenz_1['Season'] = ''
vod_lenz_1['Epizode'] = '002'
vod_lenz = vod_lenz_1.append([vod_lenz_2])
###
vod_lenz_1 = vod_lenz [vod_lenz.Title.str.contains('قسمت سی ام-شاعر آئینی محمد عظیمی')]
vod_lenz_2 = vod_lenz [~vod_lenz.Title.str.contains('قسمت سی ام-شاعر آئینی محمد عظیمی')]
vod_lenz_1['TitleCleaned1'] = 'شاعر آئینی محمد عظیمی'
vod_lenz_1['Season'] = ''
vod_lenz_1['Epizode'] = '030'
vod_lenz = vod_lenz_1.append([vod_lenz_2])
###
vod_lenz_1 = vod_lenz [vod_lenz.Title.str.contains('نوروز رنگی فصل 1 پشت صحنه')]
vod_lenz_2 = vod_lenz [~vod_lenz.Title.str.contains('نوروز رنگی فصل 1 پشت صحنه')]
vod_lenz_1['TitleCleaned1'] = 'نوروز رنگی'
vod_lenz_1['Season'] = '01'
vod_lenz_1['Epizode'] = '000'
vod_lenz = vod_lenz_1.append([vod_lenz_2])

################################
vod_lenz['Season'] = vod_lenz['Season'].str.replace('-', '')

from ConvertData import *
convert_data_in = vod_lenz.copy()
convert_data_out = ConvertData(convert_data_in)
vod_lenz = convert_data_out.copy()

vod_lenz1 = vod_lenz[vod_lenz.Epizode.str.contains("و")]
vod_lenz2 = vod_lenz[~vod_lenz.Epizode.str.contains("و")]
vod_lenz1 = vod_lenz1.reset_index()
del vod_lenz1['index']
for i in range(0,len(vod_lenz1)):     # len(vod_lenz)
     x_name_content=vod_lenz1.loc[i, 'Epizode']
     head, sep, tail = x_name_content.partition('و')
     vod_lenz1.loc[i, 'Epizode'] = head

del vod_lenz
vod_lenz = vod_lenz2.append([vod_lenz1])
del vod_lenz1
del vod_lenz2

vod_lenz['Season'] = vod_lenz['Season'].str.strip()
vod_lenz['Epizode'] = vod_lenz['Epizode'].str.strip()
vod_lenz['Season'] = vod_lenz['Season'].apply(lambda x: x.zfill(2))
vod_lenz['Epizode'] = vod_lenz['Epizode'].apply(lambda x: x.zfill(3))

vod_lenz['TitleCleaned1'] = vod_lenz['TitleCleaned1'].str.replace('زیر خاکی','زیرخاکی')
vod_lenz = vod_lenz.reset_index()
del vod_lenz['index']
for i in range(0, len(vod_lenz)):    # len(vod_lenz)
    print(i)
    if vod_lenz.loc[i, 'Season'] == '00' and vod_lenz.loc[i, 'Epizode'] == '000':
        vod_lenz.loc[i, 'FilmSerial'] = 'فیلم'
        vod_lenz.loc[i, 'code_FilmSerial'] = '01'
    else:
        vod_lenz.loc[i, 'FilmSerial'] = 'سریال'
        vod_lenz.loc[i, 'code_FilmSerial'] = '02'

#total_vod_1399_Ghadimi_v2 = total_vod_1399_Ghadimi_v2.reset_index()
#del total_vod_1399_Ghadimi_v2['index']
#for i in range(0, len(vod_lenz)):
#    print(i)
#    for j in range(0, len(total_vod_1399_Ghadimi_v2)):
#        if vod_lenz.loc[i, 'TitleCleaned1'] == total_vod_1399_Ghadimi_v2.loc[j, 'title']:
#            vod_lenz.loc[i, 'TitleCleaned1'] = total_vod_1399_Ghadimi_v2.loc[j, 'title_first']
#            vod_lenz.loc[i, 'Season'] = total_vod_1399_Ghadimi_v2.loc[j, 'season']
#            break


vod_lenz_repeat_2 = vod_lenz.copy()
vod_lenz = vod_lenz_repeat_2.copy()

vod_lenz['TitleCleaned1'] = vod_lenz['TitleCleaned1'].str.split().str.join(" ")

db1_new = db1.copy()
db1_new.drop_duplicates(subset =['ID'], keep = 'last', inplace = True)
del db1_new['Title']
del db1_new['Visit']
del db1_new['ActiveUsers']
del db1_new['DurationMin']
del db1_new['Operators']
del db1_new['Month']
del db1_new['Year']
del db1_new['DateTime']
del db1_new['FilmSerial']

db1_new.drop_duplicates(subset =['ID'], keep = 'last', inplace = True)
vod_lenz_db1_merge= pd.merge(vod_lenz, db1_new, on = ['TitleCleaned1', 'Season', 'Epizode'])
vod_lenz_db1_merge.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode'], keep = 'last', inplace = True)

vod_lenz_step1 = vod_lenz_db1_merge.copy()

vod_lenz.insert(9, 'ID', '')
vod_lenz.insert(10, 'IDS', '')
vod_lenz.insert(11, 'TitleCleaned2', '')

vod_lenz_db1_merge2 = vod_lenz.append([vod_lenz_step1])
vod_lenz_db1_merge2.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode'], keep = False, inplace = True)

del db1_new['Epizode']
del vod_lenz_db1_merge2['FilmSerial']
del vod_lenz_db1_merge2['ID']
del vod_lenz_db1_merge2['IDS']
del vod_lenz_db1_merge2['TitleCleaned2']
vod_lenz_db1_merge3 = pd.merge(vod_lenz_db1_merge2, db1_new, on = ['TitleCleaned1', 'Season'])
vod_lenz_db1_merge3.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode'], keep = 'last', inplace = True)
vod_lenz_step2 = vod_lenz_db1_merge3.copy()
vod_lenz_step2['ID'] = vod_lenz_step2['ID'].str[0:11]
vod_lenz_step2['ID'] = vod_lenz_step2['ID'] + vod_lenz_step2['Epizode']

vod_lenz_db1_merge2.insert(8, 'ID', '')
vod_lenz_db1_merge2.insert(9, 'IDS', '')
vod_lenz_db1_merge2.insert(10, 'TitleCleaned2', '')

vod_lenz_db1_merge4 = vod_lenz_db1_merge2.append([vod_lenz_step2])
vod_lenz_db1_merge4.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode'], keep = False, inplace = True)

vod_lenz_step2 = vod_lenz_step2.reset_index()
del vod_lenz_step2['index']
for i in range(0, len(vod_lenz_step2)):
    print(i)
    if vod_lenz_step2.loc[i, 'code_FilmSerial'] == '01':
        vod_lenz_step2.loc[i, 'FilmSerial'] = 'فیلم'
    elif vod_lenz_step2.loc[i, 'code_FilmSerial'] == '02':
        vod_lenz_step2.loc[i, 'FilmSerial'] = 'سریال'

vod_lenz_db1_merge4 = vod_lenz_db1_merge4.reset_index()
del vod_lenz_db1_merge4['index']
vod_lenz_step3 = vod_lenz_db1_merge4.copy()
vod_lenz_step3 = vod_lenz_step3.reset_index()
del vod_lenz_step3['index']
for i in range(0, len(vod_lenz_step3)):
    print(i)
    if vod_lenz_step3.loc[i, 'code_FilmSerial'] == '01':
        vod_lenz_step3.loc[i, 'FilmSerial'] = 'فیلم'
    elif vod_lenz_step3.loc[i, 'code_FilmSerial'] == '02':
        vod_lenz_step3.loc[i, 'FilmSerial'] = 'سریال'

vod_lenz_crawl = pd.DataFrame()
vod_lenz_crawl['Title'] = vod_lenz_step3['Title']
vod_lenz_crawl['Season'] = vod_lenz_step3['Season']
vod_lenz_crawl['TitleCleaned1'] = vod_lenz_step3['TitleCleaned1']
vod_lenz_crawl.drop_duplicates(subset =['TitleCleaned1', 'Season'], keep = 'last', inplace = True)
del vod_lenz_crawl['Season']
del vod_lenz_crawl['TitleCleaned1']
vod_lenz_crawl = vod_lenz_crawl.reset_index()
del vod_lenz_crawl['index']
for i in range(0, len(vod_lenz_crawl)):
    vod_lenz_crawl.loc[i, 'ID'] = i
    i = i + 1

#vod_lenz_crawl = vod_lenz_crawl.drop(1)
#vod_lenz_crawl = vod_lenz_crawl.drop(2)
#vod_lenz_crawl = vod_lenz_crawl.drop(3)

########
# GO DataBase 1
########

########
# GO crawl
engine = create_engine('postgresql://postgres:12344321@10.32.141.17/lenz',pool_size=20, max_overflow=100,)
con=engine.connect()
vod_lenz_crawl.to_sql('lenz_input',con,if_exists='replace', index=False)
con.close()

########

##################### TVA #####################
vod_tva_input=pd.read_excel(r'E:\python codes\VOD\vod_data\1400\tva_vod\TvaEsfand1400.xlsx', sheet_name='Videos')
vod_tva_raw = vod_tva_input.copy()

vod_tva = vod_tva_raw.groupby(['Video']).sum().reset_index()
del vod_tva['Traffic (bytes)']
vod_tva = vod_tva.rename(columns={"Sessions":"Visit"})
vod_tva = vod_tva.rename(columns={"Users":"ActiveUsers"})
vod_tva = vod_tva.rename(columns={"Video":"Title"})
vod_tva['DurationMin'] = round(vod_tva['Visit']*vod_tva['Avg. Duration (sec)']/60, 2)
del vod_tva['Avg. Duration (sec)']
vod_tva['Title'] = vod_tva['Title'].astype(str)

vod_tva['Title'] = vod_tva['Title'].str.strip()

from RemoveData import *
remove_data_in = vod_tva.copy()
remove_data_out = RemoveData(remove_data_in)
vod_tva = remove_data_out.copy()

#vod_tva.to_excel('vod_tva.xlsx', index=False)
#vod_tva = pd.read_excel(r'E:\python codes\VOD\vod_tva.xlsx')
vod_tva = vod_tva.reset_index()
del vod_tva['index']

vod_tva['Title'] = vod_tva['Title'].astype(str)
for i in range(0,len(vod_tva)):     # len(vod_tva)
     print("S: ", i)
     x_name_content=vod_tva.loc[i, 'Title']
     head, sep, tail = x_name_content.partition('(')
     vod_tva.loc[i, 'TitleCleaned1'] = head
     vod_tva.loc[i, 'Season'] = tail

for i in range(0,len(vod_tva)):     # len(vod_tva)
     print("E: ", i)
     x_name_content=vod_tva.loc[i, 'Season']
     head, sep, tail = x_name_content.partition('E')
     vod_tva.loc[i, 'Epizode'] = tail
     vod_tva.loc[i, 'Season'] = head

vod_tva['Season'] = vod_tva['Season'].str.replace('S', '')
vod_tva['Season'] = vod_tva['Season'].str.replace(')', '')
vod_tva['Season'] = vod_tva['Season'].str.replace('(', '')
vod_tva['Epizode'] = vod_tva['Epizode'].str.replace(')', '')
vod_tva['Epizode'] = vod_tva['Epizode'].str.replace('(', '')
vod_tva['Season'] = vod_tva['Season'].str.replace('(مینی سریال)', '')
vod_tva['Season'] = vod_tva['Season'].str.replace('(بروسلی)', '')
vod_tva['Season'] = vod_tva['Season'].str.replace('(دوبله)', '')
vod_tva['Season'] = vod_tva['Season'].str.replace('(انگلیس)', '')

vod_tva['TitleCleaned1'] = vod_tva['TitleCleaned1'].str.strip()
vod_tva['Season'] = vod_tva['Season'].str.strip()
vod_tva['Epizode'] = vod_tva['Epizode'].str.strip()

for i in range(0,len(vod_tva)):     # len(vod_tva)
     print(i)
     if len(vod_tva.loc[i, 'Season']) > 2:
         vod_tva.loc[i, 'Season'] = ''
####### edit of contents #######
vod_tva_1 = vod_tva [vod_tva.Title.str.contains('اسبهای والدز')]
vod_tva_2 = vod_tva [~vod_tva.Title.str.contains('اسبهای والدز')]
vod_tva_1['TitleCleaned1'] = vod_tva_1['Title']
vod_tva_1['Season'] = ''
vod_tva = vod_tva_1.append([vod_tva_2])
################################
from ConvertData import *
convert_data_in = vod_tva.copy()
convert_data_out = ConvertData(convert_data_in)
vod_tva = convert_data_out.copy()

vod_tva['Season'] = vod_tva['Season'].apply(lambda x: x.zfill(2))
vod_tva['Epizode'] = vod_tva['Epizode'].apply(lambda x: x.zfill(3))

#vod_tva['TitleCleaned1'] = vod_tva['TitleCleaned1'].str.replace('0', '۰')
#vod_tva['TitleCleaned1'] = vod_tva['TitleCleaned1'].str.replace('1', '۱')
#vod_tva['TitleCleaned1'] = vod_tva['TitleCleaned1'].str.replace('2', '۲')
#vod_tva['TitleCleaned1'] = vod_tva['TitleCleaned1'].str.replace('3', '۳')
#vod_tva['TitleCleaned1'] = vod_tva['TitleCleaned1'].str.replace('4', '۴')
#vod_tva['TitleCleaned1'] = vod_tva['TitleCleaned1'].str.replace('5', '۵')
#vod_tva['TitleCleaned1'] = vod_tva['TitleCleaned1'].str.replace('6', '۶')
#vod_tva['TitleCleaned1'] = vod_tva['TitleCleaned1'].str.replace('7', '۷')
#vod_tva['TitleCleaned1'] = vod_tva['TitleCleaned1'].str.replace('8', '۸')
#vod_tva['TitleCleaned1'] = vod_tva['TitleCleaned1'].str.replace('9', '۹')
#vod_tva['TitleCleaned1'] = vod_tva['TitleCleaned1'].str.replace('٠', '۰')
#vod_tva['TitleCleaned1'] = vod_tva['TitleCleaned1'].str.replace('١', '۱')
#vod_tva['TitleCleaned1'] = vod_tva['TitleCleaned1'].str.replace('٢', '۲')
#vod_tva['TitleCleaned1'] = vod_tva['TitleCleaned1'].str.replace('٣', '۳')
#vod_tva['TitleCleaned1'] = vod_tva['TitleCleaned1'].str.replace('٤', '۴')
#vod_tva['TitleCleaned1'] = vod_tva['TitleCleaned1'].str.replace('٥', '۵')
#vod_tva['TitleCleaned1'] = vod_tva['TitleCleaned1'].str.replace('٦', '۶')
#vod_tva['TitleCleaned1'] = vod_tva['TitleCleaned1'].str.replace('٧', '۷')
#vod_tva['TitleCleaned1'] = vod_tva['TitleCleaned1'].str.replace('٨', '۸')
#vod_tva['TitleCleaned1'] = vod_tva['TitleCleaned1'].str.replace('٩', '۹')
#vod_tva['TitleCleaned1'] = vod_tva['TitleCleaned1'].str.replace('ي', 'ی')
#vod_tva['TitleCleaned1'] = vod_tva['TitleCleaned1'].str.replace('ؤ','و')
#vod_tva['TitleCleaned1'] = vod_tva['TitleCleaned1'].str.replace('ك','ک')

del db1['ActiveUsers']
del db1['DateTime']
del db1['DurationMin']
del db1['Month']
del db1['Operators']
del db1['Title']
del db1['Visit']
del db1['Year']
db1.drop_duplicates(subset =['ID'], keep = 'last', inplace = True)
vod_tva_db1_merge= pd.merge(vod_tva, db1, on = ['TitleCleaned1', 'Season', 'Epizode'])
vod_tva_db1_merge.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode'], keep = 'last', inplace = True)

vod_tva_step1 = vod_tva_db1_merge.copy()

vod_tva.insert(7, 'FilmSerial', '')
vod_tva.insert(8, 'ID', '')
vod_tva.insert(9, 'IDS', '')
vod_tva.insert(10, 'TitleCleaned2', '')

vod_tva_db1_merge2 = vod_tva.append([vod_tva_step1])
vod_tva_db1_merge2.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode'], keep = False, inplace = True)

del db1['Epizode']
del vod_tva_db1_merge2['FilmSerial']
del vod_tva_db1_merge2['ID']
del vod_tva_db1_merge2['IDS']
del vod_tva_db1_merge2['TitleCleaned2']
vod_tva_db1_merge3 = pd.merge(vod_tva_db1_merge2, db1, on = ['TitleCleaned1', 'Season'])
vod_tva_db1_merge3.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode'], keep = 'last', inplace = True)
vod_tva_step2 = vod_tva_db1_merge3.copy()
vod_tva_step2['ID'] = vod_tva_step2['ID'].str[0:11]
vod_tva_step2['ID'] = vod_tva_step2['ID'] + vod_tva_step2['Epizode']

vod_tva_db1_merge2.insert(7, 'FilmSerial', '')
vod_tva_db1_merge2.insert(8, 'ID', '')
vod_tva_db1_merge2.insert(9, 'IDS', '')
vod_tva_db1_merge2.insert(10, 'TitleCleaned2', '')

vod_tva_db1_merge4 = vod_tva_db1_merge2.append([vod_tva_step2])
vod_tva_db1_merge4.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode'], keep = False, inplace = True)

vod_tva_step3 = vod_tva_db1_merge4.copy()

########
# GO DataBase 1
########
vod_tva_for_crawl = vod_tva_step3.copy()
vod_tva_for_crawl.drop_duplicates(subset =['TitleCleaned1', 'Season'], keep = False, inplace = True)

vod_tva_for_crawl.to_excel('vod_tva_for_crawl.xlsx', index=False)
#vod_tva_for_crawl=pd.read_excel(r'E:\python codes\VOD\vod_tva_for_crawl.xlsx')
vod_tva_for_crawl.drop_duplicates(subset =['TitleCleaned1', 'Season'], keep = 'first', inplace = True)
vod_tva_for_crawl = vod_tva_for_crawl.reset_index()
del vod_tva_for_crawl['index']
for i in range(0, len(vod_tva_for_crawl)):
    vod_tva_for_crawl.loc[i, 'code'] = i
    i = i + 1

vod_tva_for_crawl2 = vod_tva_for_crawl.iloc[:, [0,11]]
vod_tva_for_crawl2.insert(2, 'TitleForCrawl', '')
for i in range(0,len(vod_tva_for_crawl2)):     # len(vod_tva_for_crawl2)
     print("S: ", i)
     x_name_content=vod_tva_for_crawl2.loc[i, 'Title']
     head, sep, tail = x_name_content.partition('(')
     vod_tva_for_crawl2.loc[i, 'TitleForCrawl'] = head

vod_tva_for_crawl2['TitleForCrawl'] = vod_tva_for_crawl2['TitleForCrawl'].str.strip()
vod_tva_for_crawl2.to_excel('vod_tva_for_crawl2.xlsx',index=False)
vod_tva_for_crawl.to_excel('vod_tva_for_crawl.xlsx',index=False)
########
# GO Crawl
########

########
# GO DataBase 2
########


DB1_VOD.to_excel('DB1_VOD.xlsx', index = False)
print("--- %s min ---" % round((time.time() - start)/60, 2))



lenz_lenz = db1.query('Operators == "تیوا"')

lenz_lenz1 = lenz_lenz.query('Season != "00"')
lenz_lenz1.drop_duplicates(subset =['IDS'], keep = 'first', inplace = True)


from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from bs2json import bs2json
import re
import requests
from urllib.request import urlopen as uReq
import time
import sys
url_filimo='https://www.filimo.com/'
list_title = pd.DataFrame()
link_all = requests.get(url_filimo)
soup_title = BeautifulSoup(link_all.text, 'html.parser')
i = 0
for link_item in soup_title.findAll('div', {'class': "item"}):
    for link_link in link_item.findAll('a'):
        list_title.loc[i, 'LinkAddress'] = link_link.get('href')
        i = i + 1
        print("i: ", i)

list_title2 = list_title.copy()
list_title = list_title2.copy()

list_title = list_title [list_title.LinkAddress.str.contains('/m/')]
list_title.drop_duplicates(subset =['LinkAddress'], keep = 'last', inplace = True)
list_title = list_title.reset_index()
del list_title['index']

















