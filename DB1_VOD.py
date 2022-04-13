
import pandas as pd
import pyodbc
from pyodbc import *
import time
start = time.time()

######################## TRANSFER TO DATABASE ########################
drivers = pyodbc.drivers()
#for driver in drivers:
#      print(driver)

conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

for index, row in DB1_VOD.iterrows():
    cursor.execute("INSERT INTO VOD.dbo.DB1_VOD (ID,IDS,title,title_cleaned,season,epizode,visit,active_users,duration_min,operators,month,year,FilmSerial,DateTime) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)", row.ID, row.IDS, row.title,row.title_cleaned,row.season,row.epizode,row.visit,row.active_users,row.duration_min,row.operators,row.month,row.year,row.FilmSerial,row.DateTime)
    conn.commit()

######################## GET DATA FROM DB1_VOD ########################
drivers = pyodbc.drivers()
conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')
cursor = conn.cursor()
cursor.execute('SELECT * FROM DB1_VOD')
records = cursor.fetchall()
DataFromDB = []
columnNames = [column[0] for column in cursor.description]
for record in records:
    DataFromDB.append( dict( zip( columnNames , record ) ) )
df_DataFromDB = pd.DataFrame(DataFromDB)
db1 = df_DataFromDB.copy()
###################################### AIO #########################################
vod_aio_raw=pd.read_excel(r'E:\python codes\VOD\vod_data\1400\aio_vod\AioEsfand1400.xlsx')

vod_aio = vod_aio_raw.groupby(['title_fa']).sum().reset_index()

#del vod_aio['id']
del vod_aio['publish_date']
del vod_aio['imdb_rank']
vod_aio=vod_aio.rename(columns={"viewer":"Visit"})
vod_aio=vod_aio.rename(columns={"title_fa":"Title"})

vod_aio['Title'] = vod_aio['Title'].astype(str)
vod_aio_1 = vod_aio[~vod_aio.Title.str.contains("فصل")]
vod_aio_2 = vod_aio[vod_aio.Title.str.contains("فصل")]

vod_aio_2 = vod_aio_2.reset_index()
del vod_aio_2['index']

for i in range(0,len(vod_aio_2)):     # len(vod_aio2)
     print("S: ", i)
     x_name_content=vod_aio_2.loc[i, 'Title']
     head, sep, tail = x_name_content.partition('فصل')
     vod_aio_2.loc[i, 'TitleCleaned1'] = head
     vod_aio_2.loc[i, 'Season'] = tail

for i in range(0,len(vod_aio_2)):     # len(vod_aio2)
     print("S: ", i)
     x_name_content=vod_aio_2.loc[i, 'Season']
     head, sep, tail = x_name_content.partition('قسمت')
     vod_aio_2.loc[i, 'Season'] = head
     vod_aio_2.loc[i, 'Epizode'] = tail

vod_aio_2['Season'] = vod_aio_2['Season'].str.strip()
vod_aio_2['Epizode'] = vod_aio_2['Epizode'].str.strip()

vod_aio_2['Season'] = vod_aio_2['Season'].apply(lambda x: x.zfill(2))
vod_aio_2['Epizode'] = vod_aio_2['Epizode'].apply(lambda x: x.zfill(3))
vod_aio_2 = vod_aio_2.reset_index()
del vod_aio_2['index']

####### edit of contents #######
vod_aio_22 = vod_aio_2[~vod_aio_2.Title.str.contains("فصل کشتن")]
vod_aio_2_1 = vod_aio_2[vod_aio_2.Title.str.contains("فصل کشتن")]

vod_aio_22 = vod_aio_22[~vod_aio_2.Title.str.contains("فصل موج‌سواری")]
vod_aio_2_2 = vod_aio_2[vod_aio_2.Title.str.contains("فصل موج‌سواری")]

vod_aio_22 = vod_aio_22[~vod_aio_2.Title.str.contains("فصل موج سواری ۲: عطش موجی")]
vod_aio_2_3 = vod_aio_2[vod_aio_2.Title.str.contains("فصل موج سواری ۲: عطش موجی")]

vod_aio_22 = vod_aio_22[~vod_aio_2.Title.str.contains("فصل شکار۴: گرخیده")]
vod_aio_2_4 = vod_aio_2[vod_aio_2.Title.str.contains("فصل شکار۴: گرخیده")]

vod_aio_2_combine = vod_aio_2_1.append([vod_aio_2_2,vod_aio_2_3,vod_aio_2_4])

vod_aio_2_combine['TitleCleaned1'] = vod_aio_2_combine['Title']
vod_aio_2_combine['Season'] = '00'

vod_aio_2 = vod_aio_22.append([vod_aio_2_combine])
############################

#print(vod_aio_2.loc[1402, 'Title'])

vod_aio_1.insert(2, 'TitleCleaned1', '')
vod_aio_1.insert(3, 'Season', '')
vod_aio_1.insert(4, 'Epizode', '')
vod_aio = vod_aio_1.append([vod_aio_2])
vod_aio = vod_aio.reset_index()
del vod_aio['index']

#vod_aio['TitleCleaned1'] = vod_aio['TitleCleaned1'].str.replace('0', '۰')
#vod_aio['TitleCleaned1'] = vod_aio['TitleCleaned1'].str.replace('1', '۱')
#vod_aio['TitleCleaned1'] = vod_aio['TitleCleaned1'].str.replace('2', '۲')
#vod_aio['TitleCleaned1'] = vod_aio['TitleCleaned1'].str.replace('3', '۳')
#vod_aio['TitleCleaned1'] = vod_aio['TitleCleaned1'].str.replace('4', '۴')
#vod_aio['TitleCleaned1'] = vod_aio['TitleCleaned1'].str.replace('5', '۵')
#vod_aio['TitleCleaned1'] = vod_aio['TitleCleaned1'].str.replace('6', '۶')
#vod_aio['TitleCleaned1'] = vod_aio['TitleCleaned1'].str.replace('7', '۷')
#vod_aio['TitleCleaned1'] = vod_aio['TitleCleaned1'].str.replace('8', '۸')
#vod_aio['TitleCleaned1'] = vod_aio['TitleCleaned1'].str.replace('9', '۹')
#vod_aio['TitleCleaned1'] = vod_aio['TitleCleaned1'].str.replace('٠', '۰')
#vod_aio['TitleCleaned1'] = vod_aio['TitleCleaned1'].str.replace('١', '۱')
#vod_aio['TitleCleaned1'] = vod_aio['TitleCleaned1'].str.replace('٢', '۲')
#vod_aio['TitleCleaned1'] = vod_aio['TitleCleaned1'].str.replace('٣', '۳')
#vod_aio['TitleCleaned1'] = vod_aio['TitleCleaned1'].str.replace('٤', '۴')
#vod_aio['TitleCleaned1'] = vod_aio['TitleCleaned1'].str.replace('٥', '۵')
#vod_aio['TitleCleaned1'] = vod_aio['TitleCleaned1'].str.replace('٦', '۶')
#vod_aio['TitleCleaned1'] = vod_aio['TitleCleaned1'].str.replace('٧', '۷')
#vod_aio['TitleCleaned1'] = vod_aio['TitleCleaned1'].str.replace('٨', '۸')
#vod_aio['TitleCleaned1'] = vod_aio['TitleCleaned1'].str.replace('٩', '۹')
#vod_aio['TitleCleaned1'] = vod_aio['TitleCleaned1'].str.replace('ي', 'ی')
#vod_aio['TitleCleaned1'] = vod_aio['TitleCleaned1'].str.replace('ؤ','و')
#vod_aio['TitleCleaned1'] = vod_aio['TitleCleaned1'].str.replace('ك','ک')
#vod_aio['TitleCleaned1'] = vod_aio['TitleCleaned1'].str.strip() 

from ConvertData import *
convert_data_in = vod_aio.copy()
convert_data_out = ConvertData(convert_data_in)
vod_aio = convert_data_out.copy()

db1_data = db1.copy()
del db1_data['ActiveUsers']
del db1_data['DateTime']
del db1_data['DurationMin']
del db1_data['Month']
del db1_data['Operators']
del db1_data['Title']
del db1_data['Visit']
del db1_data['Year']
db1_data.drop_duplicates(subset =['ID'], keep = 'last', inplace = True)
vod_aio_db1_merge = pd.merge(vod_aio, db1_data, on = ['TitleCleaned1', 'Season', 'Epizode'])
vod_aio_db1_merge.drop_duplicates(subset =['ID'], keep = 'last', inplace = True)
vod_aio_db1_merge.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode'], keep = 'last', inplace = True)
vod_aio_step1 = vod_aio_db1_merge.copy()

vod_aio.insert(5, 'FilmSerial', '')
vod_aio.insert(6, 'ID', '')
vod_aio.insert(7, 'IDS', '')
vod_aio.insert(8, 'TitleCleaned2', '')
vod_aio_step22 = vod_aio.append([vod_aio_db1_merge])
vod_aio_step22.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode'], keep = False, inplace = True)

del vod_aio_step22['FilmSerial']
del vod_aio_step22['ID']
del vod_aio_step22['IDS']
del vod_aio_step22['TitleCleaned2']
del db1_data['Epizode']
vod_aio_db1_merge2 = pd.merge(vod_aio_step22, db1_data, on = ['TitleCleaned1', 'Season'])
vod_aio_db1_merge2.drop_duplicates(subset =['TitleCleaned1', 'Season'], keep = 'last', inplace = True)
vod_aio_db1_merge2 = vod_aio_db1_merge2.reset_index()
del vod_aio_db1_merge2['index']
vod_aio_step2 = vod_aio_db1_merge2.copy()
vod_aio_step2['ID'] = vod_aio_step2['ID'].str[0:11]
vod_aio_step2['ID'] = vod_aio_step2['ID'] + vod_aio_step2['Epizode']

vod_aio_step22.insert(5, 'FilmSerial', '')
vod_aio_step22.insert(6, 'ID', '')
vod_aio_step22.insert(7, 'IDS', '')
vod_aio_step22.insert(8, 'TitleCleaned2', '')

vod_aio_step3 = vod_aio_step22.append([vod_aio_step2])
vod_aio_step3.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode'], keep = False, inplace = True)
vod_aio_step3 = vod_aio_step3.reset_index()
del vod_aio_step3['index']

for i in range(0, len(vod_aio_step3)):
    print(i)
    if vod_aio_step3.loc[i, 'Epizode'] == "000":
        vod_aio_step3.loc[i, 'FilmSerial'] = "فیلم"
        vod_aio_step3.loc[i, 'code_FilmSerial'] = "01"
    if vod_aio_step3.loc[i, 'Epizode'] != "000":
        vod_aio_step3.loc[i, 'FilmSerial'] = "سریال"
        vod_aio_step3.loc[i, 'code_FilmSerial'] = "02"

vod_aio_step3.sort_values('TitleCleaned1', axis = 0, ascending = False, inplace = True, na_position ='last')
vod_aio_step3 = vod_aio_step3.reset_index()
del vod_aio_step3['index']

IDNumberFirst = db1['ID'].max()
IDNumberFirst = IDNumberFirst[0:7]
IDNumberFirst = int(IDNumberFirst)
IDNumberFirst = IDNumberFirst + 1
id_first = IDNumberFirst    # write last id_first + 1
vod_aio_step3.loc[0, 'id_first'] = id_first
for i in range(1, len(vod_aio_step3)):
    print(i)
    if vod_aio_step3.loc[i, 'TitleCleaned1'] == vod_aio_step3.loc[i-1, 'TitleCleaned1']:
        vod_aio_step3.loc[i, 'id_first'] = id_first
    else:
        id_first = id_first + 1
        vod_aio_step3.loc[i, 'id_first'] = id_first

vod_aio_step3['id_first'] = vod_aio_step3['id_first'].astype(int).astype(str) 
       
vod_aio_step3['ID'] = vod_aio_step3['id_first']+vod_aio_step3['code_FilmSerial']+vod_aio_step3['Season']+vod_aio_step3['Epizode']
vod_aio_step3['IDS'] = vod_aio_step3['id_first']+vod_aio_step3['Season']

del vod_aio_step3['id_first']
del vod_aio_step3['code_FilmSerial']

vod_aio = vod_aio_step1.append([vod_aio_step2, vod_aio_step3])

from RemoveData import *
remove_data_in = vod_aio.copy()
remove_data_out = RemoveData(remove_data_in)
vod_aio = remove_data_out.copy()

#total_vod_1399_Ghadimi_v2 = pd.read_excel(r'E:\python codes\VOD\kh_ghadimi\total_vod_1399_Ghadimi_v2.xlsx')
#total_vod_1399_Ghadimi_v2=total_vod_1399_Ghadimi_v2.rename(columns={"شماره فصل":"Season"})
#total_vod_1399_Ghadimi_v2 = total_vod_1399_Ghadimi_v2.fillna("NO")
#total_vod_1399_Ghadimi_v2['Season'] = total_vod_1399_Ghadimi_v2['Season'].astype(str)
#total_vod_1399_Ghadimi_v2 = total_vod_1399_Ghadimi_v2[~total_vod_1399_Ghadimi_v2.Season.str.contains("NO")]
#total_vod_1399_Ghadimi_v2['Season'] = total_vod_1399_Ghadimi_v2['Season'].astype(str).replace('\.0', '', regex=True)
#total_vod_1399_Ghadimi_v2['Season'] = total_vod_1399_Ghadimi_v2['Season'].apply(lambda x: x.zfill(2))
#
#total_vod_1399_Ghadimi_v2 = total_vod_1399_Ghadimi_v2.reset_index()
#del total_vod_1399_Ghadimi_v2['index']
#vod_aio = vod_aio.reset_index()
#del vod_aio['index']
#for i in range(0, len(vod_aio)):
#    print(i)
#    for j in range(0, len(total_vod_1399_Ghadimi_v2)):
#        if vod_aio.loc[i, 'TitleCleaned1'] == total_vod_1399_Ghadimi_v2.loc[j, 'title']:
#            vod_aio.loc[i, 'TitleCleaned1'] = total_vod_1399_Ghadimi_v2.loc[j, 'title_first']
#            vod_aio.loc[i, 'Season'] = total_vod_1399_Ghadimi_v2.loc[j, 'Season']
#            break

vod_aio.insert(9, 'Operators', 'آیو')
vod_aio.insert(10, 'Month', 'اسفند')             # month
vod_aio.insert(11, 'Year', 1400)               # year
vod_aio.insert(12, 'ActiveUsers', 0)
vod_aio.insert(13, 'DurationMin', 0)
vod_aio.insert(14, 'DateTime', '')  # month number

vod_aio['DateTime_year'] = '2022'  # change year
vod_aio['DateTime_month'] = '03'   # change month
vod_aio['DateTime_day'] = '01'
vod_aio['DateTime_time'] = '01'
vod_aio['DateTime'] = vod_aio['DateTime_year']+vod_aio['DateTime_month']+vod_aio['DateTime_day']+vod_aio['DateTime_time']
del vod_aio['DateTime_year']
del vod_aio['DateTime_month']
del vod_aio['DateTime_day']
del vod_aio['DateTime_time']

vod_aio.dtypes
vod_aio['Year'] = vod_aio['Year'].astype('int').astype('str')
vod_aio['ActiveUsers'] = vod_aio['ActiveUsers'].astype('int')
vod_aio['DurationMin'] = vod_aio['DurationMin'].astype('float')

vod_aio_final = vod_aio.copy()
###### StatisticsMonthOperators ######
vod_aio_StatisticsMonthOperators = pd.DataFrame()
vod_aio_StatisticsMonthOperators['Operators'] = vod_aio_final['Operators']
vod_aio_StatisticsMonthOperators['DateTime'] = vod_aio_final['DateTime']
vod_aio_StatisticsMonthOperators['Visit'] = vod_aio_final['Visit']
vod_aio_StatisticsMonthOperators['Duration'] = vod_aio_final['DurationMin']

vod_aio_StatisticsMonthOperators = vod_aio_StatisticsMonthOperators.groupby(['Operators', 'DateTime']).sum().reset_index()
drivers = pyodbc.drivers()

conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

for index, row in vod_aio_StatisticsMonthOperators.iterrows():
    print(index)
    cursor.execute("INSERT INTO VOD.dbo.StatisticsMonthOperators (Operators,DateTime,Visit,Duration) values(?,?,?,?)", row.Operators,row.DateTime,row.Visit,row.Duration)
    conn.commit()
###########################

#vod_aio_final.dtypes
#########
drivers = pyodbc.drivers()
conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()
for index, row in vod_aio_final.iterrows():
    print(index)
    cursor.execute("INSERT INTO VOD.dbo.DB1_VOD (ID,IDS,Title,TitleCleaned1,TitleCleaned2,Season,Epizode,Visit,ActiveUsers,DurationMin,Operators,Month,Year,FilmSerial,DateTime) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", row.ID, row.IDS, row.Title,row.TitleCleaned1,row.TitleCleaned2,row.Season,row.Epizode,row.Visit,row.ActiveUsers,row.DurationMin,row.Operators,row.Month,row.Year,row.FilmSerial,row.DateTime)
    conn.commit()

###################################### LENZ #########################################
vod_lenz_step3 = vod_lenz_step3.sort_values(['Season', 'TitleCleaned1'], ascending=[False, False])
vod_lenz_step3 = vod_lenz_step3.reset_index()
del vod_lenz_step3['index']
IDNumberFirst = db1['ID'].max()
IDNumberFirst = IDNumberFirst[0:7]
IDNumberFirst = int(IDNumberFirst)
IDNumberFirst = IDNumberFirst + 1
id_first = IDNumberFirst    # write last id_first + 1
vod_lenz_step3.loc[0, 'id_first'] = id_first
for i in range(1, len(vod_lenz_step3)):
    print(i)
    if vod_lenz_step3.loc[i, 'TitleCleaned1'] == vod_lenz_step3.loc[i-1, 'TitleCleaned1'] and \
       vod_lenz_step3.loc[i, 'Season'] == vod_lenz_step3.loc[i-1, 'Season']:
        vod_lenz_step3.loc[i, 'id_first'] = id_first
    else:
        id_first = id_first + 1
        vod_lenz_step3.loc[i, 'id_first'] = id_first

vod_lenz_step3['id_first'] = vod_lenz_step3['id_first'].astype(int).astype(str) 

#vod_lenz_step3['Epizode'] = vod_lenz_step3['Epizode'].str.extract('(\d+)', expand=False)
#vod_lenz_step3['Epizode'] = vod_lenz_step3['Epizode'].apply(lambda x: x.zfill(3))

vod_lenz_step3['ID'] = vod_lenz_step3['id_first']+vod_lenz_step3['code_FilmSerial']+vod_lenz_step3['Season']+vod_lenz_step3['Epizode']
vod_lenz_step3['IDS'] = vod_lenz_step3['id_first']+vod_lenz_step3['Season']

del vod_lenz_step3['id_first']

vod_lenz_final = vod_lenz_step1.append([vod_lenz_step2, vod_lenz_step3])
del vod_lenz_final['code_FilmSerial']

vod_lenz_final.insert(11, 'Month', 'اسفند')    # change month
vod_lenz_final.insert(12, 'Operators', 'لنز')
vod_lenz_final.insert(13, 'Year', '1400') 

vod_lenz_final['DateTime_year'] = '2022'  # change year
vod_lenz_final['DateTime_month'] = '03'   # change month
vod_lenz_final['DateTime_day'] = '01'
vod_lenz_final['DateTime_time'] = '01'
vod_lenz_final['DateTime'] = vod_lenz_final['DateTime_year']+vod_lenz_final['DateTime_month']+vod_lenz_final['DateTime_day']+vod_lenz_final['DateTime_time']
del vod_lenz_final['DateTime_year']
del vod_lenz_final['DateTime_month']
del vod_lenz_final['DateTime_day']
del vod_lenz_final['DateTime_time']
vod_lenz_final = vod_lenz_final.reset_index()
del vod_lenz_final['index']
vod_lenz_final = vod_lenz_final.fillna('')
#vod_lenz_final.dtypes
#vod_lenz_db1_merge.to_excel('vod_lenz_final.xlsx', index=False)
###### StatisticsMonthOperators ######
vod_lenz_StatisticsMonthOperators = pd.DataFrame()
vod_lenz_StatisticsMonthOperators['Operators'] = vod_lenz_final['Operators']
vod_lenz_StatisticsMonthOperators['DateTime'] = vod_lenz_final['DateTime']
vod_lenz_StatisticsMonthOperators['Visit'] = vod_lenz_final['Visit']
vod_lenz_StatisticsMonthOperators['Duration'] = vod_lenz_final['DurationMin']

vod_lenz_StatisticsMonthOperators = vod_lenz_StatisticsMonthOperators.groupby(['Operators', 'DateTime']).sum().reset_index()
drivers = pyodbc.drivers()

conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

for index, row in vod_lenz_StatisticsMonthOperators.iterrows():
    print(index)
    cursor.execute("INSERT INTO VOD.dbo.StatisticsMonthOperators (Operators,DateTime,Visit,Duration) values(?,?,?,?)", row.Operators,row.DateTime,row.Visit,row.Duration)
    conn.commit()
###########################

drivers = pyodbc.drivers()
conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()
for index, row in vod_lenz_final.iterrows():
    print(index)
    cursor.execute("INSERT INTO VOD.dbo.DB1_VOD (ID,IDS,Title,TitleCleaned1,TitleCleaned2,Season,Epizode,Visit,ActiveUsers,DurationMin,Operators,Month,Year,FilmSerial,DateTime) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", row.ID, row.IDS, row.Title,row.TitleCleaned1,row.TitleCleaned2,row.Season,row.Epizode,row.Visit,row.ActiveUsers,row.DurationMin,row.Operators,row.Month,row.Year,row.FilmSerial,row.DateTime)
    conn.commit()
###################################### TVA #########################################
#vod_tva_raw=pd.read_excel(r'E:\python codes\VOD\vod_data\1400\tva_vod\TvaShahrivar1400.xlsx', sheet_name='Videos')
#vod_tva = vod_tva_raw.groupby(['Video']).sum().reset_index()
#
#del vod_tva['Traffic (bytes)']
#vod_tva = vod_tva.rename(columns={"Sessions":"Visit"})
#vod_tva = vod_tva.rename(columns={"Users":"ActiveUsers"})
#vod_tva = vod_tva.rename(columns={"Video":"Title"})
#vod_tva['DurationMin'] = round(vod_tva['Visit']*vod_tva['Avg. Duration (sec)']/60, 2)
#del vod_tva['Avg. Duration (sec)']

vod_tva_remain = vod_tva_step3.copy()
vod_tva_remain.sort_values('Season', axis = 0, ascending = False, inplace = True, na_position ='last')
vod_tva_remain.sort_values('TitleCleaned1', axis = 0, ascending = False, inplace = True, na_position ='last')
vod_tva_remain = vod_tva_remain.reset_index()
del vod_tva_remain['index']

IDNumberFirst = db1['ID'].max()
IDNumberFirst = IDNumberFirst[0:7]
IDNumberFirst = int(IDNumberFirst)
IDNumberFirst = IDNumberFirst + 1
id_first = IDNumberFirst    # write last id_first + 1
vod_tva_remain.loc[0, 'id_first'] = id_first
for i in range(1, len(vod_tva_remain)):
    print(i)
    if vod_tva_remain.loc[i, 'TitleCleaned1'] == vod_tva_remain.loc[i-1, 'TitleCleaned1'] and \
       vod_tva_remain.loc[i, 'Season'] == vod_tva_remain.loc[i-1, 'Season']:
        vod_tva_remain.loc[i, 'id_first'] = id_first
    else:
        id_first = id_first + 1
        vod_tva_remain.loc[i, 'id_first'] = id_first

for i in range(0, len(vod_tva_remain)):    # len(vod_tva)
    if vod_tva_remain.loc[i, 'Epizode'] == '000':
        vod_tva_remain.loc[i, 'FilmSerial'] = 'فیلم'
        vod_tva_remain.loc[i, 'code_FilmSerial'] = '01'
    else:
        vod_tva_remain.loc[i, 'FilmSerial'] = 'سریال'
        vod_tva_remain.loc[i, 'code_FilmSerial'] = '02'

vod_tva_remain['id_first'] = vod_tva_remain['id_first'].astype(str) 
vod_tva_remain['id_first'] = vod_tva_remain['id_first'].astype(str).replace('\.0', '', regex=True)       
vod_tva_remain['ID'] = vod_tva_remain['id_first']+vod_tva_remain['code_FilmSerial']+vod_tva_remain['Season']+vod_tva_remain['Epizode']
vod_tva_remain['IDS'] = vod_tva_remain['id_first']+vod_tva_remain['Season']
del vod_tva_remain['code_FilmSerial']
del vod_tva_remain['id_first']

#total_vod_1399_Ghadimi_v2 = pd.read_excel(r'E:\python codes\VOD\kh_ghadimi\total_vod_1399_Ghadimi_v2.xlsx')
#total_vod_1399_Ghadimi_v2=total_vod_1399_Ghadimi_v2.rename(columns={"شماره فصل":"season"})
#total_vod_1399_Ghadimi_v2 = total_vod_1399_Ghadimi_v2.fillna("NO")
#total_vod_1399_Ghadimi_v2['season'] = total_vod_1399_Ghadimi_v2['season'].astype(str)
#total_vod_1399_Ghadimi_v2 = total_vod_1399_Ghadimi_v2[~total_vod_1399_Ghadimi_v2.season.str.contains("NO")]
#total_vod_1399_Ghadimi_v2['season'] = total_vod_1399_Ghadimi_v2['season'].astype(str).replace('\.0', '', regex=True)
#
#total_vod_1399_Ghadimi_v2 = total_vod_1399_Ghadimi_v2.reset_index()
#del total_vod_1399_Ghadimi_v2['index']
#for i in range(0, len(vod_tva_remain)):
#    print(i)
#    for j in range(0, len(total_vod_1399_Ghadimi_v2)):
#        if vod_tva_remain.loc[i, 'TitleCleaned1'] == total_vod_1399_Ghadimi_v2.loc[j, 'title']:
#            vod_tva_remain.loc[i, 'TitleCleaned1'] = total_vod_1399_Ghadimi_v2.loc[j, 'title_first']
#            vod_tva_remain.loc[i, 'Season'] = total_vod_1399_Ghadimi_v2.loc[j, 'season']
#            break


vod_tva_final = vod_tva_remain.append([vod_tva_step1, vod_tva_step2])
vod_tva_final.insert(11, 'Month', 'اسفند')    # change month
vod_tva_final.insert(12, 'Operators', 'تیوا')
vod_tva_final.insert(13, 'Year', '1400') 

vod_tva_final['DateTime_year'] = '2022'  # change year
vod_tva_final['DateTime_month'] = '03'   # change month
vod_tva_final['DateTime_day'] = '01'
vod_tva_final['DateTime_time'] = '01'
vod_tva_final['DateTime'] = vod_tva_final['DateTime_year']+vod_tva_final['DateTime_month']+vod_tva_final['DateTime_day']+vod_tva_final['DateTime_time']
del vod_tva_final['DateTime_year']
del vod_tva_final['DateTime_month']
del vod_tva_final['DateTime_day']
del vod_tva_final['DateTime_time']
vod_tva_final = vod_tva_final.reset_index()
del vod_tva_final['index']
vod_tva_final.dtypes
###### StatisticsMonthOperators ######
vod_tva_StatisticsMonthOperators = pd.DataFrame()
vod_tva_StatisticsMonthOperators['Operators'] = vod_tva_final['Operators']
vod_tva_StatisticsMonthOperators['DateTime'] = vod_tva_final['DateTime']
vod_tva_StatisticsMonthOperators['Visit'] = vod_tva_final['Visit']
vod_tva_StatisticsMonthOperators['Duration'] = vod_tva_final['DurationMin']

vod_tva_StatisticsMonthOperators = vod_tva_StatisticsMonthOperators.groupby(['Operators', 'DateTime']).sum().reset_index()
drivers = pyodbc.drivers()

conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

for index, row in vod_tva_StatisticsMonthOperators.iterrows():
    print(index)
    cursor.execute("INSERT INTO VOD.dbo.StatisticsMonthOperators (Operators,DateTime,Visit,Duration) values(?,?,?,?)", row.Operators,row.DateTime,row.Visit,row.Duration)
    conn.commit()
###########################
#vod_tva_final.to_excel('vod_tva_final.xlsx', index=False)
########
drivers = pyodbc.drivers()
conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()
for index, row in vod_tva_final.iterrows():
    print(index)
    cursor.execute("INSERT INTO VOD.dbo.DB1_VOD (ID,IDS,Title,TitleCleaned1,TitleCleaned2,Season,Epizode,Visit,ActiveUsers,DurationMin,Operators,Month,Year,FilmSerial,DateTime) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", row.ID, row.IDS, row.Title,row.TitleCleaned1,row.TitleCleaned2,row.Season,row.Epizode,row.Visit,row.ActiveUsers,row.DurationMin,row.Operators,row.Month,row.Year,row.FilmSerial,row.DateTime)
    conn.commit()

###################################### FILIMO #########################################
vod_filimo_main=pd.read_excel(r'E:\python codes\VOD\vod_data\1400\filimo_vod\FilimoDey1400.xlsx')
vod_filimo = vod_filimo_main.copy()
vod_filimo['Title'] = vod_filimo['Title'].astype(str)
vod_filimo = vod_filimo[~vod_filimo.Title.str.contains("nan")]

del vod_filimo['LinkAddress']
del vod_filimo['EnglishName']
del vod_filimo['Imdb']
del vod_filimo['Runtime']
del vod_filimo['Genres']
del vod_filimo['Country']
del vod_filimo['DubbedSubtitle']
del vod_filimo['Director']
del vod_filimo['Casts']
del vod_filimo['Producer']
del vod_filimo['Composer']
del vod_filimo['Singer']
del vod_filimo['Writer']
del vod_filimo['Cameraman']
del vod_filimo['Year']
del vod_filimo['AgeRange']

vod_filimo['Like'] = vod_filimo['Like'].astype(str)
vod_filimo['Like'] = vod_filimo['Like'].str.replace('nan', '0')
vod_filimo['Like'] = vod_filimo['Like'].fillna('0')
vod_filimo_vir = vod_filimo[vod_filimo.Like.str.contains(",")]
vod_filimo_novir = vod_filimo[~vod_filimo.Like.str.contains(",")]
vod_filimo_vir['Like'] = vod_filimo_vir['Like'].str.replace(',', '')
vod_filimo = vod_filimo_vir.append([vod_filimo_novir])

vod_filimo['Like'] = vod_filimo['Like'].str.strip()
vod_filimo['Like'] = vod_filimo['Like'].astype(int)

vod_filimo=vod_filimo.rename(columns={"Like":"Visit"})
del vod_filimo['Percent']

vod_filimo.drop_duplicates(subset =['Title', 'Season', 'Epizode'], keep = 'first', inplace = True)
vod_filimo = vod_filimo.reset_index()
del vod_filimo['index']

from RemoveData import *
remove_data_in = vod_filimo.copy()
remove_data_out = RemoveData(remove_data_in)
vod_filimo = remove_data_out.copy()

vod_filimo['Epizode'] = vod_filimo['Epizode'].str.strip()
vod_filimo['Epizode'] = vod_filimo['Epizode'].astype(str)
vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('پشت صحنه', '')
vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('قسمت پایانی', '')
vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('قسمت آخر فصل اول', '')
vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('قسمت آخر', '')
vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('بخش اول', '')
vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('بخش دوم', '')
vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('پایان فصل اول', '')
vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('پایان فصل دوم', '')
vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('پایان فصل سوم', '')
vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('پایان فصل چهارم', '')
vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('آخر', '')

#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('چهلم', '40')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('سی و نهم', '39')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('سی و هشتم', '38')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('سی و هفتم', '37')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('سی و ششم', '36')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('سی و پنجم', '35')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('سی و چهارم', '34')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('سی و سوم', '33')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('سی و دوم', '32')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('سی و یکم', '31')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('سی ام', '30')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('بیست و نهم', '29')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('بیست و هشتم', '28')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('بیست و هفتم', '27')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('بیست و ششم', '26')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('بیست و پنجم', '25')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('بیست و چهارم', '24')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('بیست و سوم', '23')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('بیست و دوم', '22')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('بیست و یکم', '21')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('بیستم', '20')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('نوزدهم', '19')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('هجدهم', '18')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('هفدهم', '17')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('شانزدهم', '16')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('پانزدهم', '15')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('چهاردهم', '14')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('سیزدهم', '13')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('دوازدهم', '12')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('یازدهم', '11')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('دهم', '10')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('نهم', '09')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('هشتم', '08')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('هفتم', '07')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('ششم', '06')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('پنجم', '05')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('چهارم', '04')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('سوم', '03')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('دوم', '02')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('اول', '01')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('سی و سه', '33')

vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('او وی ای', '')
vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('با زیرنویس مخصوص ناشنوایان', '')
vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('مخصوص نابینایان', '')
vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('مخصوص ناشنوایان', '')
vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('لالیگا', '')
vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('بوندسلیگا', '')
vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('سری آ', '')
vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('لیگ جزیره', '')
vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('ویژه نوروز', '')
vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('به روایت محمدرضا احمدی', '')
vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('تنت', '')
vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('جدید و برگزیده', '')
vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('اره ۹', '')
vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('ماه و ستاره', '')
vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('سرقت خودرو', '')
vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('ایدز', '')
vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('(', '')
vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace(')', '')
vod_filimo['Epizode'] = vod_filimo['Epizode'].str.strip()

vod_filimo_Nocolon = vod_filimo [~vod_filimo.Epizode.str.contains(':')]
vod_filimo_colon = vod_filimo [vod_filimo.Epizode.str.contains(':')]
vod_filimo_colon = vod_filimo_colon.reset_index()
del vod_filimo_colon['index']
for i in range(0,len(vod_filimo_colon)):
    print(i)
    x_name_content = vod_filimo_colon.loc[i, 'Epizode']
    head, sep, tail = x_name_content.partition(':')
    vod_filimo_colon.loc[i, 'Epizode'] = head

vod_filimo = vod_filimo_colon.append([vod_filimo_Nocolon])
vod_filimo = vod_filimo.reset_index()
del vod_filimo['index']

vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('قست', 'قسمت')

vod_filimo_ghesmat = vod_filimo [vod_filimo.Epizode.str.contains('قسمت')]
vod_filimo_Noghesmat = vod_filimo [~vod_filimo.Epizode.str.contains('قسمت')]

del vod_filimo_Noghesmat['Epizode']
vod_filimo_Noghesmat['Epizode'] = ''

vod_filimo_ghesmat = vod_filimo_ghesmat.reset_index()
del vod_filimo_ghesmat['index']

for i in range(0,len(vod_filimo_ghesmat)):
    print(i)
    try:
        x_name_content=vod_filimo_ghesmat.loc[i, 'Epizode']
        head, sep, tail = x_name_content.partition('قسمت')
        vod_filimo_ghesmat.loc[i, 'Epizode'] = tail
    except: pass
vod_filimo_ghesmat['Epizode'] = vod_filimo_ghesmat['Epizode'].str.strip()
vod_filimo = vod_filimo_ghesmat.append([vod_filimo_Noghesmat])
vod_filimo = vod_filimo.reset_index()
del vod_filimo['index']

vod_filimo_NOmosahebeh = vod_filimo [~vod_filimo.Epizode.str.contains('مصاحبه')]
vod_filimo_mosahebeh = vod_filimo [vod_filimo.Epizode.str.contains('مصاحبه')]
vod_filimo_mosahebeh = vod_filimo_mosahebeh.reset_index()
del vod_filimo_mosahebeh['index']

for i in range(0, len(vod_filimo_mosahebeh)):
    print(i)
    x_name_content=vod_filimo_mosahebeh.loc[i, 'Epizode']
    head, sep, tail = x_name_content.partition('مصاحبه')
    vod_filimo_mosahebeh.loc[i, 'Epizode'] = head

vod_filimo = vod_filimo_NOmosahebeh.append([vod_filimo_mosahebeh])
vod_filimo = vod_filimo.reset_index()
del vod_filimo['index']

vod_filimo111 = vod_filimo.copy()
vod_filimo = vod_filimo111.copy()

vod_filimo1 = vod_filimo [vod_filimo.Title.str.contains('بگو بخند')]
vod_filimo2 = vod_filimo [~vod_filimo.Title.str.contains('بگو بخند')]
vod_filimo1['Epizode'] = vod_filimo1.Epizode.str.extract('(.{,2})')
vod_filimo = vod_filimo1.append([vod_filimo2])
vod_filimo = vod_filimo.reset_index()
del vod_filimo['index']

vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('اهمیت وظیفه', '')
vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('های 1 تا 15', '')

vod_filimo_ghesmat = vod_filimo [vod_filimo.Epizode.str.contains('قسمت')]
vod_filimo_Noghesmat = vod_filimo [~vod_filimo.Epizode.str.contains('قسمت')]
vod_filimo_ghesmat['Epizode'] = vod_filimo_ghesmat.Epizode.str.extract('قسمت(.{,3})')
vod_filimo = vod_filimo_Noghesmat.append([vod_filimo_ghesmat])
vod_filimo = vod_filimo.reset_index()
del vod_filimo['index']
vod_filimo['Epizode'] = vod_filimo['Epizode'].str.strip()

vod_filimo['Visit'] = vod_filimo['Visit'].fillna(0)
vod_filimo['Visit'] = vod_filimo['Visit'].astype(int)
vod_filimo['Epizode'] = vod_filimo['Epizode'].astype(str)
vod_filimo1 = vod_filimo [~vod_filimo.Epizode.str.contains('و')]
vod_filimo1 = vod_filimo1.reset_index()
del vod_filimo1['index']
vod_filimo_Nospace = vod_filimo1 [~vod_filimo1.Epizode.str.contains(' ')]
vod_filimo_space = vod_filimo1 [vod_filimo1.Epizode.str.contains(' ')]

vod_filimo_space = vod_filimo_space.reset_index()
del vod_filimo_space['index']

for i in range(0,len(vod_filimo_space)):
    print(i)
    try:
        x_name_content=vod_filimo_space.loc[i, 'Epizode']
        head, sep, tail = x_name_content.partition(' ')
        vod_filimo_space.loc[i, 'Epizode'] = head
    except: pass
vod_filimo_space['Epizode'] = vod_filimo_space['Epizode'].str.strip()
vod_filimo1 = vod_filimo_space.append([vod_filimo_Nospace])
vod_filimo1 = vod_filimo1.reset_index()
del vod_filimo1['index']

vod_filimo2 = vod_filimo [vod_filimo.Epizode.str.contains('و')]
vod_filimo2['Epizode'] = vod_filimo2['Epizode'].str.replace('و', ' ')
vod_filimo2 = vod_filimo2.reset_index()
del vod_filimo2['index']
vod_filimo2_new = pd.DataFrame()
for i in range(0, len(vod_filimo2)):    # len(vod_filimo2)
    vod_filimo2_per = pd.DataFrame()
    Epizode = vod_filimo2.loc[i, 'Epizode']
    epizode_row = vod_filimo2.loc[i]
    epizode_row = pd.DataFrame({'ro': epizode_row})
    epizode_row = epizode_row.T
    epizode_row = epizode_row.reset_index()
    del epizode_row['index']
    epizode_split = Epizode.split()
    epizode_split = pd.DataFrame({'col': epizode_split})
    vod_filimo2_per = vod_filimo2_per.append([epizode_row]*len(epizode_split),ignore_index=True)
    vod_filimo2_per.fillna(method='ffill', inplace=True)
    for j in range(0, len(epizode_split)):
        vod_filimo2_per.loc[j, 'Epizode'] = epizode_split.loc[j, 'col']
        vod_filimo2_per.loc[j, 'Visit'] = round(epizode_row.loc[0, 'Visit']/len(epizode_split), 0)
    vod_filimo2_new = vod_filimo2_new.append([vod_filimo2_per])
    del Epizode
    del epizode_row
    del epizode_split
    del vod_filimo2_per

vod_filimo =  vod_filimo1.append([vod_filimo2_new])    

vod_filimo['number1'] = vod_filimo.Epizode.str.extract('(d)', expand=True)
vod_filimo['number2'] = vod_filimo.Epizode.str.extract('(dd)', expand=True)
vod_filimo['number3'] = vod_filimo.Epizode.str.extract('(ddd)', expand=True)
vod_filimo = vod_filimo.reset_index()
del vod_filimo['index']
#vod_filimo['epizode'] = vod_filimo['epizode'].astype(int, errors='ignore')
vod_filimo_types = vod_filimo.Epizode.apply(type)
vod_filimo_types = pd.DataFrame({'ro': vod_filimo_types})
vod_filimo['count_numbers'] = vod_filimo['Epizode'].str.count('d')
for i in range(0, len(vod_filimo)):
    print(i)
    if vod_filimo_types.loc[i, 'ro'] != int:
        if vod_filimo.loc[i, 'count_numbers'] == 1:
            vod_filimo.loc[i, 'Epizode'] = vod_filimo.loc[i, 'number1']
        elif vod_filimo.loc[i, 'count_numbers'] == 2:
            vod_filimo.loc[i, 'Epizode'] = vod_filimo.loc[i, 'number2']
        elif vod_filimo.loc[i, 'count_numbers'] == 3:
            vod_filimo.loc[i, 'Epizode'] = vod_filimo.loc[i, 'number3']

del vod_filimo['number3']    

vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace(r'D', '')

del vod_filimo['number1']
del vod_filimo['number2']
del vod_filimo['count_numbers']

#total_vod_1399_Ghadimi_v2 = pd.read_excel(r'E:\python codes\VOD\kh_ghadimi\total_vod_1399_Ghadimi_v2.xlsx')
#total_vod_1399_Ghadimi_v2=total_vod_1399_Ghadimi_v2.rename(columns={"شماره فصل":"season"})
#total_vod_1399_Ghadimi_v2 = total_vod_1399_Ghadimi_v2.fillna("NO")
#total_vod_1399_Ghadimi_v2['season'] = total_vod_1399_Ghadimi_v2['season'].astype(str)
#total_vod_1399_Ghadimi_v2 = total_vod_1399_Ghadimi_v2[~total_vod_1399_Ghadimi_v2.season.str.contains("NO")]
#total_vod_1399_Ghadimi_v2['season'] = total_vod_1399_Ghadimi_v2['season'].astype(str).replace('.0', '', regex=True)

vod_filimo['Season'] = vod_filimo['Season'].astype(str).replace('.0', '', regex=True)
vod_filimo['Season'] = vod_filimo['Season'].str.strip() 
vod_filimo['Epizode'] = vod_filimo['Epizode'].str.strip()
vod_filimo ['Season'].replace('', '00', inplace=True)
vod_filimo['Season'] = vod_filimo['Season'].astype(str)
vod_filimo['Epizode'] = vod_filimo['Epizode'].astype(str)
vod_filimo['Epizode'] = vod_filimo['Epizode'].fillna(0)
vod_filimo['Season'] = vod_filimo['Season'].fillna(0)
vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('nan', '0')
vod_filimo['Season'] = vod_filimo['Season'].str.replace('nan', '0')
vod_filimo['Season'] = vod_filimo['Season'].apply(lambda x: x.zfill(2))
vod_filimo['Epizode'] = vod_filimo['Epizode'].apply(lambda x: x.zfill(3))
vod_filimo ['Epizode'].replace('', '000', inplace=True)
for i in range(0, len(vod_filimo)):    # len(vod_filimo)
    print(i)
    if vod_filimo.loc[i, 'Season'] == '00':
        vod_filimo.loc[i, 'FilmSerial'] = 'فیلم'
        vod_filimo.loc[i, 'code_FilmSerial'] = '01'
    else:
        vod_filimo.loc[i, 'FilmSerial'] = 'سریال'
        vod_filimo.loc[i, 'code_FilmSerial'] = '02'

#### save ####
vod_filimo_repeat_1 = vod_filimo.copy()
vod_filimo = vod_filimo_repeat_1.copy()
##############
vod_filimo['TitleCleaned1'] = vod_filimo['Title']
vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('سریال', '')
vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('فیلم', '')
vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('انیمیشن', '')

vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('او وی ای', '')
vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('با زیرنویس مخصوص ناشنوایان', '')
vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('مخصوص نابینایان', '')
vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('مخصوص ناشنوایان', '')
vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('لالیگا', '')
vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('بوندسلیگا', '')
vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('سری آ', '')
vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('لیگ جزیره', '')
vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('ویژه نوروز', '')
vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('به روایت محمدرضا احمدی', '')
vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('تنت', '')
vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('جدید و برگزیده', '')
vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('اره ۹', '')
vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('ماه و ستاره', '')
vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('سرقت خودرو', '')
vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('ایدز', '')
vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('(', '')
vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace(')', '')
vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.strip()

#total_vod_1399_Ghadimi_v2 = total_vod_1399_Ghadimi_v2.reset_index()
#del total_vod_1399_Ghadimi_v2['index']
#for i in range(0, len(vod_filimo)):
#    print(i)
#    for j in range(0, len(total_vod_1399_Ghadimi_v2)):
#        if vod_filimo.loc[i, 'Title'] == total_vod_1399_Ghadimi_v2.loc[j, 'title']:
#            vod_filimo.loc[i, 'TitleCleaned1'] = total_vod_1399_Ghadimi_v2.loc[j, 'title_first']
#            vod_filimo.loc[i, 'Season'] = total_vod_1399_Ghadimi_v2.loc[j, 'season']
#            break

from ConvertData import *
convert_data_in = vod_filimo.copy()
convert_data_out = ConvertData(convert_data_in)
vod_filimo = convert_data_out.copy()

vod_filimo['Season'] = vod_filimo['Season'].apply(lambda x: x.zfill(2))
vod_filimo['Epizode'] = vod_filimo['Epizode'].apply(lambda x: x.zfill(3))

#vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('0', '۰')
#vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('1', '۱')
#vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('2', '۲')
#vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('3', '۳')
#vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('4', '۴')
#vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('5', '۵')
#vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('6', '۶')
#vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('7', '۷')
#vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('8', '۸')
#vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('9', '۹')
#vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('٠', '۰')
#vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('١', '۱')
#vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('٢', '۲')
#vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('٣', '۳')
#vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('٤', '۴')
#vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('٥', '۵')
#vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('٦', '۶')
#vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('٧', '۷')
#vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('٨', '۸')
#vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('٩', '۹')
#vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('ي', 'ی')
#vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('ؤ','و')
#vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.replace('ك','ک')
#vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.strip()
   
#vod_filimo['Season'] = vod_filimo['Season'].str.replace('۰', '0')
#vod_filimo['Season'] = vod_filimo['Season'].str.replace('۱', '1')
#vod_filimo['Season'] = vod_filimo['Season'].str.replace('۲', '2')
#vod_filimo['Season'] = vod_filimo['Season'].str.replace('۳', '3')
#vod_filimo['Season'] = vod_filimo['Season'].str.replace('۴', '4')
#vod_filimo['Season'] = vod_filimo['Season'].str.replace('۵', '5')
#vod_filimo['Season'] = vod_filimo['Season'].str.replace('۶', '6')
#vod_filimo['Season'] = vod_filimo['Season'].str.replace('۷', '7')
#vod_filimo['Season'] = vod_filimo['Season'].str.replace('۸', '8')
#vod_filimo['Season'] = vod_filimo['Season'].str.replace('۹', '9')
#
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('۰', '0')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('۱', '1')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('۲', '2')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('۳', '3')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('۴', '4')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('۵', '5')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('۶', '6')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('۷', '7')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('۸', '8')
#vod_filimo['Epizode'] = vod_filimo['Epizode'].str.replace('۹', '9')

vod_filimo.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode', 'FilmSerial', 'Title', 'code_FilmSerial', 'Visit'], keep = 'first', inplace = True)
vod_filimo = vod_filimo.sort_values(['TitleCleaned1', 'Season', 'Epizode', 'Visit'], ascending=[True, True, True, False])
vod_filimo.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode', 'FilmSerial', 'Title', 'code_FilmSerial'], keep = 'first', inplace = True)

vod_filimo = vod_filimo.reset_index()
del vod_filimo['index']

vod_filimo['DateTime_year'] = '2022'  # change year
vod_filimo['DateTime_month'] = '01'   # change month
vod_filimo['DateTime_day'] = '01'
vod_filimo['DateTime_time'] = '01'
vod_filimo['DateTime'] = vod_filimo['DateTime_year']+vod_filimo['DateTime_month']+vod_filimo['DateTime_day']+vod_filimo['DateTime_time']
del vod_filimo['DateTime_year']
del vod_filimo['DateTime_month']
del vod_filimo['DateTime_day']
del vod_filimo['DateTime_time']

vod_filimo['Title'] = vod_filimo['Title'].astype(str)
vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].astype(str)

vod_filimo.insert(8, 'ID', '')
vod_filimo.insert(9, 'IDS', '')
vod_filimo.insert(10, 'ActiveUsers', '')
vod_filimo.insert(11, 'DurationMin', '')
vod_filimo.insert(12, 'Operators', 'فیلیمو')   
vod_filimo.insert(13, 'Month', 'دی')      # change
vod_filimo.insert(14, 'Year', '1400')        # change
#### save ####
vod_filimo_repeat_2 = vod_filimo.copy()
vod_filimo = vod_filimo_repeat_2.copy()
##############
db1_vod_filimo_old = db1.query('Operators == "فیلیمو"')
db1_VOD_unfilimo_old = db1.query('Operators != "فیلیمو"')
#db1_vod_filimo_old = Tir.copy()
#db1_vod_filimo_old = Tir.append([Mordad])

db1_vod_filimo_old = db1_vod_filimo_old.reset_index()
del db1_vod_filimo_old['index']

db1_data = db1_vod_filimo_old.copy()
del db1_data['ActiveUsers']
del db1_data['DateTime']
del db1_data['DurationMin']
del db1_data['Month']
del db1_data['Operators']
del db1_data['Title']
db1_data=db1_data.rename(columns={"Visit":"Visit_old"})
del db1_data['Year']
del db1_data['TitleCleaned2']
del vod_filimo['ID']
del vod_filimo['IDS']
del vod_filimo['FilmSerial']
vod_filimo_groupby = vod_filimo.copy()
vod_filimo_groupby = vod_filimo_groupby.groupby(['TitleCleaned1', 'Season', 'Epizode']).sum().reset_index()
del vod_filimo['Visit']
vod_filimo = pd.merge(vod_filimo, vod_filimo_groupby, on = ['TitleCleaned1', 'Season', 'Epizode'])
####### compare of TitleCleaned1, Season and Epizode with old filimo = step 1 #######
db1_data = db1_data.groupby(['ID', 'IDS', 'TitleCleaned1', 'Season', 'Epizode']).sum().reset_index()
vod_filimo_db1_merge = pd.merge(vod_filimo, db1_data, on = ['TitleCleaned1', 'Season', 'Epizode'])
vod_filimo_db1_merge.drop_duplicates(subset =['ID'], keep = 'last', inplace = True)
vod_filimo_db1_merge.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode'], keep = 'last', inplace = True)
vod_filimo_db1_merge['Visit'] = vod_filimo_db1_merge['Visit'] - vod_filimo_db1_merge['Visit_old']
del vod_filimo_db1_merge['Visit_old']
vod_filimo_step1 = vod_filimo_db1_merge.copy()

vod_filimo.insert(12, 'FilmSerial', '')
vod_filimo.insert(13, 'ID', '')
vod_filimo.insert(14, 'IDS', '')
vod_filimo_step22 = vod_filimo.append([vod_filimo_db1_merge])
vod_filimo_step22.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode'], keep = False, inplace = True)
####### compare of TitleCleaned1 and Season with old filimo = step 2 #######
del vod_filimo_step22['FilmSerial']
del vod_filimo_step22['ID']
del vod_filimo_step22['IDS']
del db1_data['Epizode']
vod_filimo_db1_merge2 = pd.merge(vod_filimo_step22, db1_data, on = ['TitleCleaned1', 'Season'])
vod_filimo_db1_merge2.drop_duplicates(subset =['TitleCleaned1', 'Season'], keep = 'last', inplace = True)
vod_filimo_db1_merge2 = vod_filimo_db1_merge2.reset_index()
del vod_filimo_db1_merge2['index']
vod_filimo_step2 = vod_filimo_db1_merge2.copy()
vod_filimo_step2['ID'] = vod_filimo_step2['ID'].str[0:11]
vod_filimo_step2['ID'] = vod_filimo_step2['ID'] + vod_filimo_step2['Epizode']
vod_filimo_step2['FilmSerial'] = 'سریال'
vod_filimo_step2.insert(14, 'TitleCleaned2', '')
####### remain data from new filimo = step 3 #######
vod_filimo_step3 = vod_filimo_step22.append([vod_filimo_step2])
vod_filimo_step3.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode'], keep = False, inplace = True)
vod_filimo_step3 = vod_filimo_step3.reset_index()
del vod_filimo_step3['index']
del vod_filimo_step3['Visit_old']

##### step 2 #####
vod_filimo = vod_filimo_step3.copy()
db1_VOD_unfilimo_old = db1_VOD_unfilimo_old.reset_index()
del db1_VOD_unfilimo_old['index']

db1_Visit = db1_VOD_unfilimo_old.copy()
vod_filimo_Visit = pd.DataFrame()
vod_filimo_Visit['ID'] = db1_Visit['ID']
vod_filimo_Visit['Visit_old'] = db1_Visit['Visit']
vod_filimo_Visit = vod_filimo_Visit.groupby(['ID']).sum().reset_index()

db1_data = db1_VOD_unfilimo_old.copy()
del db1_data['ActiveUsers']
del db1_data['DateTime']
del db1_data['DurationMin']
del db1_data['Month']
del db1_data['Operators']
del db1_data['Title']
del db1_data['Visit']
del db1_data['Year']
del db1_data['TitleCleaned2']
del vod_filimo['ID']
del vod_filimo['IDS']
del vod_filimo['FilmSerial']
db1_data.drop_duplicates(subset =['ID'], keep = 'last', inplace = True)
####### compare of TitleCleaned1, Season and Epizode with old unfilimo = step 4 #######
vod_filimo_db1_merge = pd.merge(vod_filimo, db1_data, on = ['TitleCleaned1', 'Season', 'Epizode'])
vod_filimo_db1_merge.drop_duplicates(subset =['ID'], keep = 'last', inplace = True)
vod_filimo_db1_merge.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode'], keep = 'last', inplace = True)
vod_filimo_step4 = vod_filimo_db1_merge.copy()
####### remain data from new filimo = step 55 #######
vod_filimo.insert(13, 'FilmSerial', '')
vod_filimo.insert(14, 'ID', '')
vod_filimo.insert(15, 'IDS', '')
vod_filimo_step55 = vod_filimo.append([vod_filimo_db1_merge])
vod_filimo_step55.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode'], keep = False, inplace = True)
####### compare of TitleCleaned1 and Season with old unfilimo = step 5 #######
del vod_filimo_step55['FilmSerial']
del vod_filimo_step55['ID']
del vod_filimo_step55['IDS']
del vod_filimo_step55['TitleCleaned2']
del db1_data['Epizode']
vod_filimo_db1_merge2 = pd.merge(vod_filimo_step55, db1_data, on = ['TitleCleaned1', 'Season'])
vod_filimo_db1_merge2.drop_duplicates(subset =['TitleCleaned1', 'Season'], keep = 'last', inplace = True)
vod_filimo_db1_merge2 = vod_filimo_db1_merge2.reset_index()
del vod_filimo_db1_merge2['index']
vod_filimo_step5 = vod_filimo_db1_merge2.copy()
vod_filimo_step5['ID'] = vod_filimo_step5['ID'].str[0:11]
vod_filimo_step5['ID'] = vod_filimo_step5['ID'] + vod_filimo_step5['Epizode']

vod_filimo_step55.insert(12, 'FilmSerial', '')
vod_filimo_step55.insert(13, 'ID', '')
vod_filimo_step55.insert(14, 'IDS', '')
vod_filimo_step55.insert(15, 'TitleCleaned2', '')
vod_filimo_step5.insert(15, 'TitleCleaned2', '')
####### remain data from new filimo = step 6 #######
vod_filimo_step6 = vod_filimo_step55.copy()
#vod_filimo_step6 = vod_filimo_step55.append([vod_filimo_step2])
vod_filimo_step6.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode'], keep = False, inplace = True)
vod_filimo_step6 = vod_filimo_step6.reset_index()
del vod_filimo_step6['index']

##### data gather #####
#vod_filimo_step6 = vod_filimo.copy()
vod_filimo_step6 = vod_filimo_step6.sort_values(['Season', 'TitleCleaned1'], ascending=[False, False])
vod_filimo_step6 = vod_filimo_step6.reset_index()
del vod_filimo_step6['index']
IDNumberFirst = db1['ID'].max()
IDNumberFirst = IDNumberFirst[0:7]
IDNumberFirst = int(IDNumberFirst)
IDNumberFirst = IDNumberFirst + 1
id_first = IDNumberFirst    # write last id_first + 1
vod_filimo_step6.loc[0, 'id_first'] = id_first
for i in range(1, len(vod_filimo_step6)):
    print(i)
    if vod_filimo_step6.loc[i, 'TitleCleaned1'] == vod_filimo_step6.loc[i-1, 'TitleCleaned1'] and \
       vod_filimo_step6.loc[i, 'Season'] == vod_filimo_step6.loc[i-1, 'Season']:
        vod_filimo_step6.loc[i, 'id_first'] = id_first
    else:
        id_first = id_first + 1
        vod_filimo_step6.loc[i, 'id_first'] = id_first

vod_filimo_step6['id_first'] = vod_filimo_step6['id_first'].astype(int).astype(str) 
       
vod_filimo_step6['ID'] = vod_filimo_step6['id_first']+vod_filimo_step6['code_FilmSerial']+vod_filimo_step6['Season']+vod_filimo_step6['Epizode']
vod_filimo_step6['IDS'] = vod_filimo_step6['id_first']+vod_filimo_step6['Season']

del vod_filimo_step6['id_first']

vod_filimo_step6['FilmSerial'] = vod_filimo_step6['code_FilmSerial']
vod_filimo_step6['FilmSerial'] = vod_filimo_step6['FilmSerial'].str.replace('01', 'فیلم')
vod_filimo_step6['FilmSerial'] = vod_filimo_step6['FilmSerial'].str.replace('02', 'سریال')
    
#### edit ####
vod_filimo_step1=vod_filimo_step1.rename(columns={"code_FilmSerial":"FilmSerial"})
vod_filimo_step1['FilmSerial'] = vod_filimo_step1['FilmSerial'].str.replace('01', 'فیلم')
vod_filimo_step1['FilmSerial'] = vod_filimo_step1['FilmSerial'].str.replace('02', 'سریال')

vod_filimo_step1.insert(14, 'TitleCleaned2', '')
del vod_filimo_step2['Visit_old']
del vod_filimo_step2['code_FilmSerial']
del vod_filimo_step4['code_FilmSerial']
del vod_filimo_step5['code_FilmSerial']
#del vod_filimo_step6['Visit_old']
del vod_filimo_step6['code_FilmSerial']
##############

vod_filimo_final = vod_filimo_step1.append([vod_filimo_step2, vod_filimo_step4, vod_filimo_step5, vod_filimo_step6])
#del vod_filimo_final['code_FilmSerial']
vod_filimo_final.drop_duplicates(subset =['ID'], keep = 'last', inplace = True)

vod_filimo_final_merge_visit = pd.merge(vod_filimo_final, vod_filimo_Visit, on = ['ID'])
vod_filimo_final_merge_visit['Visit'] = vod_filimo_final_merge_visit['Visit'] - vod_filimo_final_merge_visit['Visit_old']

for i in range(0, len(vod_filimo_final_merge_visit)):
    print(i)
    if vod_filimo_final_merge_visit.loc[i, 'Visit'] < 0:
        vod_filimo_final_merge_visit.loc[i, 'Visit'] = 0

del vod_filimo_final_merge_visit['Visit_old']

vod_filimo_final_append_merge = vod_filimo_final.append([vod_filimo_final_merge_visit])
vod_filimo_final_append_merge.drop_duplicates(subset =['ID'], keep = False, inplace = True)
vod_filimo_final_new = vod_filimo_final_merge_visit.append([vod_filimo_final_append_merge])

vod_filimo_final_new['ActiveUsers'] = 0
vod_filimo_final_new['DurationMin'] = 0
vod_filimo_final_new = vod_filimo_final_new.reset_index()
del vod_filimo_final_new['index']
vod_filimo_final = vod_filimo_final_new.copy()
vod_filimo_final['DateTime'] = vod_filimo_final['DateTime'].astype(str)
vod_filimo_final['Visit'] = vod_filimo_final['Visit'].astype(int)
vod_filimo_final['DurationMin'] = vod_filimo_final['DurationMin'].astype(float)
vod_filimo_final.replace('nan', '', inplace=True)
vod_filimo_final = vod_filimo_final.fillna('')
for i in range(0, len(vod_filimo_final)):
    print(i)
    if vod_filimo_final.loc[i, 'Visit'] < 0:
        vod_filimo_final.loc[i, 'Visit'] = 0
vod_filimo_final.dtypes
vod_filimo_final.to_excel('vod_filimo_final.xlsx', index=False)
vod_filimo_final['Visit'].sum()
###### StatisticsMonthOperators ######
vod_filimo_StatisticsMonthOperators = pd.DataFrame()
vod_filimo_StatisticsMonthOperators['Operators'] = vod_filimo_final['Operators']
vod_filimo_StatisticsMonthOperators['DateTime'] = vod_filimo_final['DateTime']
vod_filimo_StatisticsMonthOperators['Visit'] = vod_filimo_final['Visit']
vod_filimo_StatisticsMonthOperators['Duration'] = vod_filimo_final['DurationMin']

vod_filimo_StatisticsMonthOperators = vod_filimo_StatisticsMonthOperators.groupby(['Operators', 'DateTime']).sum().reset_index()
drivers = pyodbc.drivers()

conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

for index, row in vod_filimo_StatisticsMonthOperators.iterrows():
    print(index)
    cursor.execute("INSERT INTO VOD.dbo.StatisticsMonthOperators (Operators,DateTime,Visit,Duration) values(?,?,?,?)", row.Operators,row.DateTime,row.Visit,row.Duration)
    conn.commit()
###########################
#vod_filimo_final = vod_filimo_step6.copy()
#vod_filimo_final=vod_filimo_final.rename(columns={"code_FilmSerial":"TitleCleaned2"})
############################################################################
drivers = pyodbc.drivers()
conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()
for index, row in vod_filimo_final.iterrows():
    print(index)
    cursor.execute("INSERT INTO VOD.dbo.DB1_VOD (ID,IDS,Title,TitleCleaned1,TitleCleaned2,Season,Epizode,Visit,ActiveUsers,DurationMin,Operators,Month,Year,FilmSerial,DateTime) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", row.ID, row.IDS, row.Title,row.TitleCleaned1,row.TitleCleaned2,row.Season,row.Epizode,row.Visit,row.ActiveUsers,row.DurationMin,row.Operators,row.Month,row.Year,row.FilmSerial,row.DateTime)
    conn.commit()

#vod_filimo_final.to_excel('vod_filimo_final.xlsx', index = False)
###################################### FILMGARDI #########################################
vod_filmgardi_main=pd.read_excel(r'E:\python codes\VOD\vod_data\1400\filmgardi_vod\FilmgardiShahrivar1400.xlsx')
vod_filmgardi = vod_filmgardi_main.copy()

del vod_filmgardi['link_address']
del vod_filmgardi['country']
del vod_filmgardi['season']
del vod_filmgardi['Runtime']
del vod_filmgardi['year']
del vod_filmgardi['language']
del vod_filmgardi['AgeRange']
del vod_filmgardi['Genre']
del vod_filmgardi['director']
del vod_filmgardi['producer']
del vod_filmgardi['composer']
del vod_filmgardi['editor']
del vod_filmgardi['writer']
del vod_filmgardi['Casts']
del vod_filmgardi['imdb']
del vod_filmgardi['dubbed']

vod_filmgardi['like'] = vod_filmgardi['like'].str.replace('رای ثبت شده', '')
vod_filmgardi['like'] = vod_filmgardi['like'].str.strip()
vod_filmgardi['like'] = vod_filmgardi['like'].fillna(0)
vod_filmgardi['visit'] = vod_filmgardi['like'].astype(int)
del vod_filmgardi['percent']
del vod_filmgardi['like']
vod_filmgardi['visit'] = vod_filmgardi['visit'].fillna(0)
vod_filmgardi['title'] = vod_filmgardi['title'].fillna('NO')
vod_filmgardi = vod_filmgardi [~vod_filmgardi.title.str.contains('NO')]

vod_filmgardi = vod_filmgardi.reset_index()
del vod_filmgardi['index']
new = vod_filmgardi["FilmSerial"].str.split('قسمت', n = 1, expand = True)
vod_filmgardi["season"]= new[0]
vod_filmgardi["epizode"]= new[1]
vod_filmgardi['season'] = vod_filmgardi['season'].str.replace('فصل', '')
vod_filmgardi['season'] = vod_filmgardi['season'].str.replace('.', '')
vod_filmgardi['season'] = vod_filmgardi['season'].str.strip()
vod_filmgardi['epizode'] = vod_filmgardi['epizode'].str.strip()
vod_filmgardi['season'] = vod_filmgardi['season'].fillna('00')
vod_filmgardi['epizode'] = vod_filmgardi['epizode'].fillna('000')
vod_filmgardi['season'] = vod_filmgardi['season'].apply(lambda x: x.zfill(2))
vod_filmgardi['epizode'] = vod_filmgardi['epizode'].apply(lambda x: x.zfill(3))

for i in range(0, len(vod_filmgardi)):    # len(vod_filimo)
    print(i)
    if vod_filmgardi.loc[i, 'season'] == '00' and vod_filmgardi.loc[i, 'epizode'] == '000':
        vod_filmgardi.loc[i, 'FilmSerial'] = 'فیلم'
        vod_filmgardi.loc[i, 'code_FilmSerial'] = '01'
    else:
        vod_filmgardi.loc[i, 'FilmSerial'] = 'سریال'
        vod_filmgardi.loc[i, 'code_FilmSerial'] = '02'

vod_filmgardi['title_cleaned'] = vod_filmgardi['title']

total_vod_1399_Ghadimi_v2 = pd.read_excel(r'E:\python codes\VOD\kh_ghadimi\total_vod_1399_Ghadimi_v2.xlsx')
total_vod_1399_Ghadimi_v2=total_vod_1399_Ghadimi_v2.rename(columns={"شماره فصل":"season"})
total_vod_1399_Ghadimi_v2 = total_vod_1399_Ghadimi_v2.fillna("NO")
total_vod_1399_Ghadimi_v2['season'] = total_vod_1399_Ghadimi_v2['season'].astype(str)
total_vod_1399_Ghadimi_v2 = total_vod_1399_Ghadimi_v2[~total_vod_1399_Ghadimi_v2.season.str.contains("NO")]
total_vod_1399_Ghadimi_v2['season'] = total_vod_1399_Ghadimi_v2['season'].astype(str).replace('\.0', '', regex=True)

total_vod_1399_Ghadimi_v2 = total_vod_1399_Ghadimi_v2.reset_index()
del total_vod_1399_Ghadimi_v2['index']
for i in range(0, len(vod_filmgardi)):
    print(i)
    for j in range(0, len(total_vod_1399_Ghadimi_v2)):
        if vod_filmgardi.loc[i, 'title_cleaned'] == total_vod_1399_Ghadimi_v2.loc[j, 'title']:
            vod_filmgardi.loc[i, 'title_cleaned'] = total_vod_1399_Ghadimi_v2.loc[j, 'title_first']
            vod_filmgardi.loc[i, 'season'] = total_vod_1399_Ghadimi_v2.loc[j, 'season']
            break


vod_filmgardi['title_cleaned'] = vod_filmgardi['title_cleaned'].str.replace('0', '۰')
vod_filmgardi['title_cleaned'] = vod_filmgardi['title_cleaned'].str.replace('1', '۱')
vod_filmgardi['title_cleaned'] = vod_filmgardi['title_cleaned'].str.replace('2', '۲')
vod_filmgardi['title_cleaned'] = vod_filmgardi['title_cleaned'].str.replace('3', '۳')
vod_filmgardi['title_cleaned'] = vod_filmgardi['title_cleaned'].str.replace('4', '۴')
vod_filmgardi['title_cleaned'] = vod_filmgardi['title_cleaned'].str.replace('5', '۵')
vod_filmgardi['title_cleaned'] = vod_filmgardi['title_cleaned'].str.replace('6', '۶')
vod_filmgardi['title_cleaned'] = vod_filmgardi['title_cleaned'].str.replace('7', '۷')
vod_filmgardi['title_cleaned'] = vod_filmgardi['title_cleaned'].str.replace('8', '۸')
vod_filmgardi['title_cleaned'] = vod_filmgardi['title_cleaned'].str.replace('9', '۹')
vod_filmgardi['title_cleaned'] = vod_filmgardi['title_cleaned'].str.replace('٠', '۰')
vod_filmgardi['title_cleaned'] = vod_filmgardi['title_cleaned'].str.replace('١', '۱')
vod_filmgardi['title_cleaned'] = vod_filmgardi['title_cleaned'].str.replace('٢', '۲')
vod_filmgardi['title_cleaned'] = vod_filmgardi['title_cleaned'].str.replace('٣', '۳')
vod_filmgardi['title_cleaned'] = vod_filmgardi['title_cleaned'].str.replace('٤', '۴')
vod_filmgardi['title_cleaned'] = vod_filmgardi['title_cleaned'].str.replace('٥', '۵')
vod_filmgardi['title_cleaned'] = vod_filmgardi['title_cleaned'].str.replace('٦', '۶')
vod_filmgardi['title_cleaned'] = vod_filmgardi['title_cleaned'].str.replace('٧', '۷')
vod_filmgardi['title_cleaned'] = vod_filmgardi['title_cleaned'].str.replace('٨', '۸')
vod_filmgardi['title_cleaned'] = vod_filmgardi['title_cleaned'].str.replace('٩', '۹')
vod_filmgardi['title_cleaned'] = vod_filmgardi['title_cleaned'].str.replace('ي', 'ی')
vod_filmgardi['title_cleaned'] = vod_filmgardi['title_cleaned'].str.replace('ؤ','و')
vod_filmgardi['title_cleaned'] = vod_filmgardi['title_cleaned'].str.replace('ك','ک')
vod_filmgardi['title_cleaned'] = vod_filmgardi['title_cleaned'].str.strip() 

vod_filmgardi['season'] = vod_filmgardi['season'].str.replace('۰', '0')
vod_filmgardi['season'] = vod_filmgardi['season'].str.replace('۱', '1')
vod_filmgardi['season'] = vod_filmgardi['season'].str.replace('۲', '2')
vod_filmgardi['season'] = vod_filmgardi['season'].str.replace('۳', '3')
vod_filmgardi['season'] = vod_filmgardi['season'].str.replace('۴', '4')
vod_filmgardi['season'] = vod_filmgardi['season'].str.replace('۵', '5')
vod_filmgardi['season'] = vod_filmgardi['season'].str.replace('۶', '6')
vod_filmgardi['season'] = vod_filmgardi['season'].str.replace('۷', '7')
vod_filmgardi['season'] = vod_filmgardi['season'].str.replace('۸', '8')
vod_filmgardi['season'] = vod_filmgardi['season'].str.replace('۹', '9')

vod_filmgardi['epizode'] = vod_filmgardi['epizode'].str.replace('۰', '0')
vod_filmgardi['epizode'] = vod_filmgardi['epizode'].str.replace('۱', '1')
vod_filmgardi['epizode'] = vod_filmgardi['epizode'].str.replace('۲', '2')
vod_filmgardi['epizode'] = vod_filmgardi['epizode'].str.replace('۳', '3')
vod_filmgardi['epizode'] = vod_filmgardi['epizode'].str.replace('۴', '4')
vod_filmgardi['epizode'] = vod_filmgardi['epizode'].str.replace('۵', '5')
vod_filmgardi['epizode'] = vod_filmgardi['epizode'].str.replace('۶', '6')
vod_filmgardi['epizode'] = vod_filmgardi['epizode'].str.replace('۷', '7')
vod_filmgardi['epizode'] = vod_filmgardi['epizode'].str.replace('۸', '8')
vod_filmgardi['epizode'] = vod_filmgardi['epizode'].str.replace('۹', '9')

vod_filmgardi['DateTime_year'] = '2021'  # change year
vod_filmgardi['DateTime_month'] = '09'   # change month
vod_filmgardi['DateTime_day'] = '01'
vod_filmgardi['DateTime_time'] = '01'
vod_filmgardi['DateTime'] = vod_filmgardi['DateTime_year']+vod_filmgardi['DateTime_month']+vod_filmgardi['DateTime_day']+vod_filmgardi['DateTime_time']
del vod_filmgardi['DateTime_year']
del vod_filmgardi['DateTime_month']
del vod_filmgardi['DateTime_day']
del vod_filmgardi['DateTime_time']

vod_filmgardi['title'] = vod_filmgardi['title'].astype(str)
vod_filmgardi['title_cleaned'] = vod_filmgardi['title_cleaned'].astype(str)

remove_data_in = vod_filmgardi.copy()
remove_data_out = RemoveData(remove_data_in)
vod_filmgardi = remove_data_out.copy()

vod_filmgardi = vod_filmgardi.groupby(['title_cleaned', 'season', 'epizode', 'DateTime', 'code_FilmSerial', 'FilmSerial', 'title']).sum().reset_index()

vod_filmgardi.insert(8, 'ID', '')
vod_filmgardi.insert(9, 'IDS', '')
vod_filmgardi.insert(10, 'active_users', '')
vod_filmgardi.insert(11, 'duration_min', '')
vod_filmgardi.insert(12, 'operators', 'فیلم گردی')   
vod_filmgardi.insert(13, 'month', 'شهریور')      # change
vod_filmgardi.insert(14, 'year', '1400')        # change

db1_dup = db1.copy()
db1_dup.drop_duplicates(subset =['ID'], keep = 'last', inplace = True)
del db1_dup['DateTime']
del db1_dup['active_users']
del db1_dup['duration_min']
del db1_dup['month']
del db1_dup['operators']
del db1_dup['title']
del db1_dup['visit']
del db1_dup['year']
db1_dup = db1_dup.reset_index()
del db1_dup['index']

vod_filmgardi_db1_dup_merge = pd.merge(vod_filmgardi, db1_dup, on =['title_cleaned', 'season', 'epizode', 'FilmSerial'])
del vod_filmgardi_db1_dup_merge['ID_x']
del vod_filmgardi_db1_dup_merge['IDS_x']
vod_filmgardi_db1_dup_merge=vod_filmgardi_db1_dup_merge.rename(columns={"ID_y":"ID"})
vod_filmgardi_db1_dup_merge=vod_filmgardi_db1_dup_merge.rename(columns={"IDS_y":"IDS"})
vod_filmgardi_db1_dup_merge.drop_duplicates(subset =['ID'], keep = 'last', inplace = True)
vod_filmgardi_db1_dup_merge.drop_duplicates(subset =['title_cleaned', 'season', 'epizode', 'FilmSerial'], keep = 'last', inplace = True)

vod_filmgardi_remain = vod_filmgardi.append([vod_filmgardi_db1_dup_merge])
vod_filmgardi_remain.drop_duplicates(subset =['title_cleaned', 'season', 'epizode', 'FilmSerial'], keep = False, inplace = True)
vod_filmgardi_remain['title_cleaned'] = vod_filmgardi_remain['title_cleaned'].str.strip() 
vod_filmgardi_remain.sort_values('title_cleaned', axis = 0, ascending = False, inplace = True, na_position ='last')
vod_filmgardi_remain = vod_filmgardi_remain.reset_index()
del vod_filmgardi_remain['index']

IDNumberFirst = db1['ID'].max()
IDNumberFirst = IDNumberFirst[0:7]
IDNumberFirst = int(IDNumberFirst)
IDNumberFirst = IDNumberFirst + 1
id_first = IDNumberFirst    # write last id_first + 1
vod_filmgardi_remain.loc[0, 'id_first'] = id_first
for i in range(1, len(vod_filmgardi_remain)):
    print(i)
    if vod_filmgardi_remain.loc[i, 'title_cleaned'] == vod_filmgardi_remain.loc[i-1, 'title_cleaned'] and \
       vod_filmgardi_remain.loc[i, 'FilmSerial'] == vod_filmgardi_remain.loc[i-1, 'FilmSerial']:
        vod_filmgardi_remain.loc[i, 'id_first'] = id_first
    else:
        id_first = id_first + 1
        vod_filmgardi_remain.loc[i, 'id_first'] = id_first

#a = vod_filmgardi_remain.copy()
#vod_filmgardi_remain = a.copy()
vod_filmgardi_remain['id_first'] = vod_filmgardi_remain['id_first'].astype(int).astype(str) 
vod_filmgardi_remain['ID'] = vod_filmgardi_remain['id_first'] + vod_filmgardi_remain['code_FilmSerial'] + vod_filmgardi_remain['season'] + vod_filmgardi_remain['epizode']
vod_filmgardi_remain['IDS'] = vod_filmgardi_remain['id_first']+vod_filmgardi_remain['season']

del vod_filmgardi_remain['id_first']
del vod_filmgardi_remain['code_FilmSerial']
del vod_filmgardi_db1_dup_merge['code_FilmSerial']

vod_filmgardi_final = vod_filmgardi_db1_dup_merge.append([vod_filmgardi_remain])
vod_filmgardi_final.drop_duplicates(subset =['ID'], keep = 'first', inplace = True)
vod_filmgardi_final = vod_filmgardi_final.reset_index()
del vod_filmgardi_final['index']

vod_filmgardi_final['visit'] = vod_filmgardi_final['visit'].astype(int).astype(str)
#################
vod_filmgardi_final['season'] = vod_filmgardi_final['season'].apply(lambda x: x.zfill(2))
vod_filmgardi_final.dtypes
###### StatisticsMonthOperators ######
vod_filmgardi_StatisticsMonthOperators = pd.DataFrame()
vod_filmgardi_StatisticsMonthOperators['Operators'] = vod_filmgardi_final['Operators']
vod_filmgardi_StatisticsMonthOperators['DateTime'] = vod_filmgardi_final['DateTime']
vod_filmgardi_StatisticsMonthOperators['Visit'] = vod_filmgardi_final['Visit']
vod_filmgardi_StatisticsMonthOperators['Duration'] = vod_filmgardi_final['DurationMin']

vod_filmgardi_StatisticsMonthOperators = vod_filmgardi_StatisticsMonthOperators.groupby(['Operators', 'DateTime']).sum().reset_index()
drivers = pyodbc.drivers()

conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

for index, row in vod_filmgardi_StatisticsMonthOperators.iterrows():
    print(index)
    cursor.execute("INSERT INTO VOD.dbo.StatisticsMonthOperators (Operators,DateTime,Visit,Duration) values(?,?,?,?)", row.Operators,row.DateTime,row.Visit,row.Duration)
    conn.commit()
###########################
#################

drivers = pyodbc.drivers()
conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()
for index, row in vod_filmgardi_final.iterrows():
    print(index)
    cursor.execute("INSERT INTO VOD.dbo.DB1_VOD (ID,IDS,title,title_cleaned,season,epizode,visit,active_users,duration_min,operators,month,year,FilmSerial,DateTime) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)", row.ID, row.IDS, row.title,row.title_cleaned,row.season,row.epizode,row.visit,row.active_users,row.duration_min,row.operators,row.month,row.year,row.FilmSerial,row.DateTime)
    conn.commit()
#################

vod_filmgardi.to_excel('vod_filmgardi.xlsx', index=False)


print("--- %s min ---" % round((time.time() - start)/60, 2))
#####################################################################################
a1 = db1_new.query('TitleCleaned1 == "پایتخت"')
a2 = db2_new.query('TitleCleaned1 == "پایتخت"')
db1_new = db1.copy()
db1_new.sort_values('Visit', axis = 0, ascending = False, inplace = True, na_position ='last')
db1_new.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode', 'FilmSerial', 'Month', 'Year', 'Operators', 'DateTime'], keep = 'first', inplace = True)
#db1_new = db1_new.groupby(['TitleCleaned1', 'Season', 'Epizode', 'FilmSerial', 'Month', 'Year', 'Operators']).sum().reset_index()

db1_new_filimo = db1_new.query('Operators == "فیلیمو"')
db1_new_no_filimo = db1_new.query('Operators != "فیلیمو"')
#a2 = db1_new_filimo[db1_new_filimo.TitleCleaned1.str.contains("ناشنوایان")]

db1_new_filimo['TitleCleaned1'] = db1_new_filimo['TitleCleaned1'].str.replace('او وی ای', '')
db1_new_filimo['TitleCleaned1'] = db1_new_filimo['TitleCleaned1'].str.replace('با زیرنویس مخصوص ناشنوایان', '')
db1_new_filimo['TitleCleaned1'] = db1_new_filimo['TitleCleaned1'].str.replace('مخصوص نابینایان', '')
db1_new_filimo['TitleCleaned1'] = db1_new_filimo['TitleCleaned1'].str.replace('مخصوص ناشنوایان', '')
db1_new_filimo['TitleCleaned1'] = db1_new_filimo['TitleCleaned1'].str.replace('لالیگا', '')
db1_new_filimo['TitleCleaned1'] = db1_new_filimo['TitleCleaned1'].str.replace('بوندسلیگا', '')
db1_new_filimo['TitleCleaned1'] = db1_new_filimo['TitleCleaned1'].str.replace('سری آ', '')
db1_new_filimo['TitleCleaned1'] = db1_new_filimo['TitleCleaned1'].str.replace('لیگ جزیره', '')
db1_new_filimo['TitleCleaned1'] = db1_new_filimo['TitleCleaned1'].str.replace('ویژه نوروز', '')
db1_new_filimo['TitleCleaned1'] = db1_new_filimo['TitleCleaned1'].str.replace('به روایت محمدرضا احمدی', '')
db1_new_filimo['TitleCleaned1'] = db1_new_filimo['TitleCleaned1'].str.replace('تنت', '')
db1_new_filimo['TitleCleaned1'] = db1_new_filimo['TitleCleaned1'].str.replace('جدید و برگزیده', '')
db1_new_filimo['TitleCleaned1'] = db1_new_filimo['TitleCleaned1'].str.replace('اره ۹', '')
db1_new_filimo['TitleCleaned1'] = db1_new_filimo['TitleCleaned1'].str.replace('ماه و ستاره', '')
db1_new_filimo['TitleCleaned1'] = db1_new_filimo['TitleCleaned1'].str.replace('سرقت خودرو', '')
db1_new_filimo['TitleCleaned1'] = db1_new_filimo['TitleCleaned1'].str.replace('ایدز', '')
db1_new_filimo['TitleCleaned1'] = db1_new_filimo['TitleCleaned1'].str.replace('(', '')
db1_new_filimo['TitleCleaned1'] = db1_new_filimo['TitleCleaned1'].str.replace(')', '')
db1_new_filimo['TitleCleaned1'] = db1_new_filimo['TitleCleaned1'].str.strip()

db1_new = db1_new_filimo.append([db1_new_no_filimo])
db1_new.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode', 'FilmSerial', 'Month', 'Year', 'Operators', 'DateTime'], keep = 'first', inplace = True)

db1_new = db1_new.sort_values(['TitleCleaned1', 'Season'], ascending=[False, False])

db1_new = db1_new.reset_index()
del db1_new['index']

id_first = 1000001
db1_new.loc[0, 'id_first'] = id_first
for i in range(1, len(db1_new)):
    print(i)
    if db1_new.loc[i, 'FilmSerial'] == db1_new.loc[i-1, 'FilmSerial'] and \
       db1_new.loc[i, 'Season'] == db1_new.loc[i-1, 'Season'] and \
       db1_new.loc[i, 'TitleCleaned1'] == db1_new.loc[i-1, 'TitleCleaned1']:
           db1_new.loc[i, 'id_first'] = id_first
    else:
        id_first = id_first + 1
        db1_new.loc[i, 'id_first'] = id_first
        
db1_new['id_first'] = db1_new['id_first'].astype(int).astype(str)



db1_new['code_FilmSerial'] = db1_new['FilmSerial']
db1_new['code_FilmSerial'] = db1_new['code_FilmSerial'].str.replace('فیلم', '01')
db1_new['code_FilmSerial'] = db1_new['code_FilmSerial'].str.replace('سریال', '02')

db1_new['ID'] = db1_new['id_first']+db1_new['code_FilmSerial']+db1_new['Season']+db1_new['Epizode']
db1_new['IDS'] = db1_new['id_first']+db1_new['Season']

del db1_new['id_first']
del db1_new['code_FilmSerial']

db1_new = db1_new.reset_index()
del db1_new['index']
db1_new.dtypes

drivers = pyodbc.drivers()
conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()
for index, row in db1_new.iterrows():
    print(index)
    cursor.execute("INSERT INTO VOD.dbo.DB1_VOD (ID,IDS,Title,TitleCleaned1,TitleCleaned2,Season,Epizode,Visit,ActiveUsers,DurationMin,Operators,Month,Year,FilmSerial,DateTime) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", row.ID, row.IDS, row.Title,row.TitleCleaned1,row.TitleCleaned2,row.Season,row.Epizode,row.Visit,row.ActiveUsers,row.DurationMin,row.Operators,row.Month,row.Year,row.FilmSerial,row.DateTime)
    conn.commit()
#######
db1_new_for_db2 = db1_new.copy()
del db1_new_for_db2['Epizode']
del db1_new_for_db2['Month']
del db1_new_for_db2['Year']
del db1_new_for_db2['Operators']
del db1_new_for_db2['ActiveUsers']
del db1_new_for_db2['DurationMin']
del db1_new_for_db2['Visit']
del db1_new_for_db2['DateTime']
del db1_new_for_db2['TitleCleaned2']

del db2['Title']
del db2['FilmSerial']
del db2['ID']
del db2['IDS']

db1_new_for_db2.drop_duplicates(subset =['IDS'], keep = 'first', inplace = True)
db2_new = pd.DataFrame()
db2_new = pd.merge(db2, db1_new_for_db2, on = ['TitleCleaned1', 'Season'])
db2_new.drop_duplicates(subset =['IDS'], keep = 'first', inplace = True)

db2_new = db2_new.reset_index()
del db2_new['index']

drivers = pyodbc.drivers()

conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

for index, row in db2_new.iterrows():
    print(index)
    cursor.execute("INSERT INTO VOD.dbo.DB2_VOD (ID,IDS,Title,TitleCleaned1,TitleCleaned2,Season,FilmSerial,Director,EnglishName,AgeRange,Casts,Genres,Imdb,Language,ReleaseDateGeorgian,ReleaseDateJalali,Runtime,Country,Producer,Writer,Composer,Editor,DubbedSubtitle,Singer,Cameraman) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", row.ID,row.IDS,row.Title,row.TitleCleaned1,row.TitleCleaned2,row.Season,row.FilmSerial,row.Director,row.EnglishName,row.AgeRange,row.Casts,row.Genres,row.Imdb,row.Language,row.ReleaseDateGeorgian,row.ReleaseDateJalali,row.Runtime,row.Country,row.Producer,row.Writer,row.Composer,row.Editor,row.DubbedSubtitle,row.Singer,row.Cameraman)
    conn.commit()

#####################################################
#####################################################
#####################################################
###### delete ######
total_vod_1399_Ghadimi_v2 = pd.read_excel(r'E:\python codes\VOD\kh_ghadimi\total_vod_1399_Ghadimi_v2.xlsx')
total_vod_1399_Ghadimi_v2=total_vod_1399_Ghadimi_v2.rename(columns={"شماره فصل":"season"})
total_vod_1399_Ghadimi_v2 = total_vod_1399_Ghadimi_v2.fillna("NO")
total_vod_1399_Ghadimi_v2['season'] = total_vod_1399_Ghadimi_v2['season'].astype(str)
total_vod_1399_Ghadimi_v2 = total_vod_1399_Ghadimi_v2[~total_vod_1399_Ghadimi_v2.season.str.contains("NO")]
total_vod_1399_Ghadimi_v2['season'] = total_vod_1399_Ghadimi_v2['season'].astype(str).replace('.0', '', regex=True)
total_vod_1399_Ghadimi_v2=total_vod_1399_Ghadimi_v2.rename(columns={"title":"TitleCleaned1"})
total_vod_1399_Ghadimi_v2['TitleCleaned1'] = total_vod_1399_Ghadimi_v2['TitleCleaned1'].str.replace('0', '۰')
total_vod_1399_Ghadimi_v2['TitleCleaned1'] = total_vod_1399_Ghadimi_v2['TitleCleaned1'].str.replace('1', '۱')
total_vod_1399_Ghadimi_v2['TitleCleaned1'] = total_vod_1399_Ghadimi_v2['TitleCleaned1'].str.replace('2', '۲')
total_vod_1399_Ghadimi_v2['TitleCleaned1'] = total_vod_1399_Ghadimi_v2['TitleCleaned1'].str.replace('3', '۳')
total_vod_1399_Ghadimi_v2['TitleCleaned1'] = total_vod_1399_Ghadimi_v2['TitleCleaned1'].str.replace('4', '۴')
total_vod_1399_Ghadimi_v2['TitleCleaned1'] = total_vod_1399_Ghadimi_v2['TitleCleaned1'].str.replace('5', '۵')
total_vod_1399_Ghadimi_v2['TitleCleaned1'] = total_vod_1399_Ghadimi_v2['TitleCleaned1'].str.replace('6', '۶')
total_vod_1399_Ghadimi_v2['TitleCleaned1'] = total_vod_1399_Ghadimi_v2['TitleCleaned1'].str.replace('7', '۷')
total_vod_1399_Ghadimi_v2['TitleCleaned1'] = total_vod_1399_Ghadimi_v2['TitleCleaned1'].str.replace('8', '۸')
total_vod_1399_Ghadimi_v2['TitleCleaned1'] = total_vod_1399_Ghadimi_v2['TitleCleaned1'].str.replace('9', '۹')
total_vod_1399_Ghadimi_v2['TitleCleaned1'] = total_vod_1399_Ghadimi_v2['TitleCleaned1'].str.replace('٠', '۰')
total_vod_1399_Ghadimi_v2['TitleCleaned1'] = total_vod_1399_Ghadimi_v2['TitleCleaned1'].str.replace('١', '۱')
total_vod_1399_Ghadimi_v2['TitleCleaned1'] = total_vod_1399_Ghadimi_v2['TitleCleaned1'].str.replace('٢', '۲')
total_vod_1399_Ghadimi_v2['TitleCleaned1'] = total_vod_1399_Ghadimi_v2['TitleCleaned1'].str.replace('٣', '۳')
total_vod_1399_Ghadimi_v2['TitleCleaned1'] = total_vod_1399_Ghadimi_v2['TitleCleaned1'].str.replace('٤', '۴')
total_vod_1399_Ghadimi_v2['TitleCleaned1'] = total_vod_1399_Ghadimi_v2['TitleCleaned1'].str.replace('٥', '۵')
total_vod_1399_Ghadimi_v2['TitleCleaned1'] = total_vod_1399_Ghadimi_v2['TitleCleaned1'].str.replace('٦', '۶')
total_vod_1399_Ghadimi_v2['TitleCleaned1'] = total_vod_1399_Ghadimi_v2['TitleCleaned1'].str.replace('٧', '۷')
total_vod_1399_Ghadimi_v2['TitleCleaned1'] = total_vod_1399_Ghadimi_v2['TitleCleaned1'].str.replace('٨', '۸')
total_vod_1399_Ghadimi_v2['TitleCleaned1'] = total_vod_1399_Ghadimi_v2['TitleCleaned1'].str.replace('٩', '۹')
total_vod_1399_Ghadimi_v2['TitleCleaned1'] = total_vod_1399_Ghadimi_v2['TitleCleaned1'].str.replace('ي', 'ی')
total_vod_1399_Ghadimi_v2['TitleCleaned1'] = total_vod_1399_Ghadimi_v2['TitleCleaned1'].str.replace('ؤ','و')
total_vod_1399_Ghadimi_v2['TitleCleaned1'] = total_vod_1399_Ghadimi_v2['TitleCleaned1'].str.replace('ك','ک')
total_vod_1399_Ghadimi_v2 = total_vod_1399_Ghadimi_v2.reset_index()
del total_vod_1399_Ghadimi_v2['index']
total_vod_1399_Ghadimi_v2['season'] = total_vod_1399_Ghadimi_v2['season'].astype(str)
total_vod_1399_Ghadimi_v2['season'] = total_vod_1399_Ghadimi_v2['season'].apply(lambda x: x.zfill(2))

for j in range(0, len(total_vod_1399_Ghadimi_v2)):  #len(total_vod_1399_Ghadimi_v2)
    print(j)
    for i in range(0, len(db2)):
        if total_vod_1399_Ghadimi_v2.loc[j, 'TitleCleaned1'] == db2.loc[i, 'TitleCleaned1']:
            db2.loc[i, 'Yes_No'] = 'Yes'
        elif total_vod_1399_Ghadimi_v2.loc[j, 'title_first'] == db2.loc[i, 'TitleCleaned1'] and \
             total_vod_1399_Ghadimi_v2.loc[j, 'season'] == db2.loc[i, 'Season']:
                db2.loc[i, 'Yes_No'] = 'Yes'


################################
db2[["TitleCleaned1", "NoData"]]= db2["TitleCleaned1"].str.split(":", n = 1, expand = True)
db2['NoData'] = db2['NoData'].astype(str)
db2_1 = db2 [db2.NoData.str.contains('None')]
db2_2 = db2 [~db2.NoData.str.contains('None')]
db2_2['TitleCleaned1'].replace('', 'NoData', inplace=True)
db2_2['TitleCleaned1'] = db2_2['TitleCleaned1'].fillna('NoData')
db2_21 = db2_2 [db2_2.TitleCleaned1.str.contains('NoData')]
db2_22 = db2_2 [~db2_2.TitleCleaned1.str.contains('NoData')]
db2_21['TitleCleaned1'] = db2_21['NoData']
db2_2 = db2_21.append([db2_22])
db2 = db2_1.append([db2_2])
del db2_1
del db2_2
del db2_21
del db2_22
del db2['NoData']
db2['TitleCleaned1'] = db2['TitleCleaned1'].str.strip()
################################
db2[["TitleCleaned1", "NoData"]]= db2["TitleCleaned1"].str.split("-", n = 1, expand = True)
db2['NoData'] = db2['NoData'].astype(str)
db2_1 = db2 [db2.NoData.str.contains('None')]
db2_2 = db2 [~db2.NoData.str.contains('None')]
db2_2['TitleCleaned1'].replace('', 'NoData', inplace=True)
db2_2['TitleCleaned1'] = db2_2['TitleCleaned1'].fillna('NoData')
db2_21 = db2_2 [db2_2.TitleCleaned1.str.contains('NoData')]
db2_22 = db2_2 [~db2_2.TitleCleaned1.str.contains('NoData')]
db2_21['TitleCleaned1'] = db2_21['NoData']
db2_2 = db2_21.append([db2_22])
db2 = db2_1.append([db2_2])
del db2_1
del db2_2
del db2_21
del db2_22
del db2['NoData']
db2['TitleCleaned1'] = db2['TitleCleaned1'].str.strip()
################################
total_vod_1399_Ghadimi_v2['title_first'] = total_vod_1399_Ghadimi_v2['title_first'].str.strip()


db2['Yes_No'] = db2['Yes_No'].astype(str)
db2_yes = db2[db2.Yes_No.str.contains("Yes")]
db2_No = db2[~db2.Yes_No.str.contains("Yes")]

print(db2.loc[25166, 'TitleCleaned1'])
print(total_vod_1399_Ghadimi_v2.loc[4, 'title_first'])


if total_vod_1399_Ghadimi_v2.loc[4, 'season'] == db2.loc[25166, 'Season']:
    print('ok')

db2_yes = db2.query('Yes_No == "Yes"')
db2_No = db2.query('Yes_No != "Yes"')

db2_No['number'] = db2_No['TitleCleaned1'].str.extract('(\d+)', expand=False)
db2_No['number'] = db2_No['number'].fillna('NOData')

db2_No_numberY = db2_No.query('number != "NOData"')
db2_No_nemberN = db2_No.query('number == "NOData"')


db2_No_nemberN_new = pd.DataFrame()
db2_No_nemberN_new['TitleCleaned1'] = db2_No_numberY['TitleCleaned1']

db2_No_nemberN_new.to_excel('db2_No_nemberN_new.xlsx', index= False)







