
import pandas as pd
import numpy as np
import pyodbc
from pyodbc import *
import time
start = time.time()
from EditDB2 import *
################################ GET DATA FROM DATABASE 1 #################################
conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
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

################################ AIO #################################
vod_aio_raw = pd.read_excel(r'E:\python codes\VOD\vod_data\1400\aio_vod\AioEsfand1400.xlsx')
vod_aio = vod_aio_raw.copy()

#del vod_aio['id']
del vod_aio['viewer']
vod_aio = vod_aio.rename(columns={"title_fa":"Title"})
vod_aio = vod_aio.rename(columns={"publish_date":"ReleaseDateGeorgian"})
vod_aio = vod_aio.rename(columns={"imdb_rank":"Imdb"})
vod_aio = vod_aio.rename(columns={"country_fa":"Country"})
vod_aio = vod_aio.rename(columns={"genre":"Genres"})
vod_aio = vod_aio.rename(columns={"cast":"Casts"})

del vod_aio_step3['Visit']
del vod_aio_step3['Epizode']
vod_aio_merge_step3 = pd.merge(vod_aio_step3, vod_aio, on = ['Title'])

vod_aio_merge_step3 = vod_aio_merge_step3.reset_index()
del vod_aio_merge_step3['index']

vod_aio_merge_step3['ReleaseDateGeorgian'] = vod_aio_merge_step3['ReleaseDateGeorgian'].astype('str')
vod_aio_merge_step3['ReleaseDateGeorgian'] = vod_aio_merge_step3['ReleaseDateGeorgian'].str[0:4]
vod_aio_merge_step3['ReleaseDateGeorgian'] = vod_aio_merge_step3['ReleaseDateGeorgian'].astype('int')
vod_aio_merge_step3.insert(12, 'ReleaseDateJalali', '')
for i in range(0, len(vod_aio_merge_step3)):
    print(i)
    if vod_aio_merge_step3.loc[i, 'ReleaseDateGeorgian'] < 1500:
        vod_aio_merge_step3.loc[i, 'ReleaseDateJalali'] = vod_aio_merge_step3.loc[i, 'ReleaseDateGeorgian']
        vod_aio_merge_step3.loc[i, 'ReleaseDateGeorgian'] = ''

vod_aio_merge_step3['Country'] = vod_aio_merge_step3['Country'].str.replace(',', '،')
vod_aio_merge_step3['Genres'] = vod_aio_merge_step3['Genres'].str.replace(',', '،')
vod_aio_merge_step3['Casts'] = vod_aio_merge_step3['Casts'].str.replace(',', '،')

vod_aio_merge_step3['Country'] = vod_aio_merge_step3['Country'].str.replace('ي', 'ی')
vod_aio_merge_step3['Country'] = vod_aio_merge_step3['Country'].str.replace('ؤ','و')
vod_aio_merge_step3['Country'] = vod_aio_merge_step3['Country'].str.replace('ك','ک')
vod_aio_merge_step3['Country'] = vod_aio_merge_step3['Country'].str.strip()

vod_aio_merge_step3['Genres'] = vod_aio_merge_step3['Genres'].str.replace('ي', 'ی')
vod_aio_merge_step3['Genres'] = vod_aio_merge_step3['Genres'].str.replace('ؤ','و')
vod_aio_merge_step3['Genres'] = vod_aio_merge_step3['Genres'].str.replace('ك','ک')
vod_aio_merge_step3['Genres'] = vod_aio_merge_step3['Genres'].str.strip() 

vod_aio_merge_step3['Casts'] = vod_aio_merge_step3['Casts'].str.replace('ي', 'ی')
vod_aio_merge_step3['Casts'] = vod_aio_merge_step3['Casts'].str.replace('ؤ','و')
vod_aio_merge_step3['Casts'] = vod_aio_merge_step3['Casts'].str.replace('ك','ک')
vod_aio_merge_step3['Casts'] = vod_aio_merge_step3['Casts'].str.strip() 

vod_aio_merge_step3.insert(13, 'Director', '')
vod_aio_merge_step3.insert(14, 'EnglishName', '')
vod_aio_merge_step3.insert(15, 'AgeRange', '')
vod_aio_merge_step3.insert(16, 'Language', '')
vod_aio_merge_step3.insert(17, 'Runtime', '')
vod_aio_merge_step3.insert(18, 'Producer', '')
vod_aio_merge_step3.insert(19, 'Writer', '')
vod_aio_merge_step3.insert(20, 'Composer', '')
vod_aio_merge_step3.insert(21, 'Editor', '')
vod_aio_merge_step3.insert(22, 'DubbedSubtitle', '')
vod_aio_merge_step3.insert(23, 'Cameraman', '')
vod_aio_merge_step3.insert(24, 'Singer', '')

vod_aio_merge_step3.replace('nan', '', inplace=True)
vod_aio_merge_step3 = vod_aio_merge_step3.fillna('')
vod_aio_merge_step3.drop_duplicates(subset =['IDS'], keep = 'last', inplace = True)

vod_aio_merge2 = pd.merge(vod_aio_merge_step3, db2, on = ['IDS'])
###############

###############
vod_aio_final = vod_aio_merge_step3.copy()
vod_aio_final['ReleaseDateGeorgian'] = vod_aio_final['ReleaseDateGeorgian'].astype(str)
vod_aio_final['Imdb'] = vod_aio_final['Imdb'].astype(float)
vod_aio_final = vod_aio_final.reset_index()
del vod_aio_final['index']

vod_aio_final['Genres'] = vod_aio_final['Genres'].str.replace('گفت و گو', 'گفتگو')
for i in range(0, len(vod_aio_final)):
    print(i)
    if vod_aio_final.loc[i, 'Imdb'] == 1:
        vod_aio_final.loc[i, 'Imdb'] = ''

vod_aio_final.dtypes

edit_db2_in = vod_aio_final.copy()
edit_db2_out = EditDB2(edit_db2_in)
vod_aio_final = edit_db2_out.copy()
################################
drivers = pyodbc.drivers()

conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

for index, row in vod_aio_final.iterrows():
    print(index)
    cursor.execute("INSERT INTO VOD.dbo.DB2_VOD (ID,IDS,Title,TitleCleaned1,TitleCleaned2,Season,FilmSerial,Director,EnglishName,AgeRange,Casts,Genres,Imdb,Language,ReleaseDateGeorgian,ReleaseDateJalali,Runtime,Country,Producer,Writer,Composer,Editor,DubbedSubtitle,Singer,Cameraman) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", row.ID,row.IDS,row.Title,row.TitleCleaned1,row.TitleCleaned2,row.Season,row.FilmSerial,row.Director,row.EnglishName,row.AgeRange,row.Casts,row.Genres,row.Imdb,row.Language,row.ReleaseDateGeorgian,row.ReleaseDateJalali,row.Runtime,row.Country,row.Producer,row.Writer,row.Composer,row.Editor,row.DubbedSubtitle,row.Singer,row.Cameraman)
    conn.commit()

################################ LENZ #################################
#vod_lenz_raw = pd.read_csv(r'E:\python codes\VOD\vod_data\1400\lenz_vod\LenzAzar1400.csv')
#vod_lenz_crawl = pd.read_csv(r'E:\python codes\VOD\crawl operators\lenz\epg6-20.csv')
######
import psycopg2
import pandas.io.sql as psql
connection = psycopg2.connect(user="postgres",
                                    password="12344321",
                                    host="10.32.141.17",
                                    port="5432",
                                    database="lenz")
cursor = connection.cursor()
vod_lenz_crawl_get = psql.read_sql("SELECT * FROM public.lenz_output", connection)
######
vod_lenz_crawl = vod_lenz_crawl_get.copy()
del vod_lenz_crawl['ID']
del vod_lenz_crawl['content_name']
del vod_lenz_crawl['content_link']
del vod_lenz_crawl['Lenz Rate']
vod_lenz_crawl = vod_lenz_crawl.rename(columns={"content_type":"FilmSerial"})
vod_lenz_crawl = vod_lenz_crawl.rename(columns={"orginal_name":"Title"})
vod_lenz_crawl = vod_lenz_crawl.rename(columns={"finglish_name":"EnglishName"})
vod_lenz_crawl = vod_lenz_crawl.rename(columns={"Epizode_Number":"Epizode"})
vod_lenz_crawl = vod_lenz_crawl.rename(columns={"Directed_by":"DirectedBy"})
vod_lenz_crawl = vod_lenz_crawl.rename(columns={"Release Date Georgian":"ReleaseDateGeorgian"})
vod_lenz_crawl = vod_lenz_crawl.rename(columns={"Release Date Jalali":"ReleaseDateJalali"})
vod_lenz_crawl = vod_lenz_crawl.rename(columns={"Directed by":"Director"})

vod_lenz_raw = vod_lenz_crawl.copy()
#vod_lenz_raw = vod_lenz_raw.fillna('')

vod_lenz_raw['Casts'] = vod_lenz_raw['Casts'].str.replace('}', '')
vod_lenz_raw['Casts'] = vod_lenz_raw['Casts'].str.replace('{', '')
vod_lenz_raw['Casts'] = vod_lenz_raw['Casts'].str.replace('"', '')
vod_lenz_raw['Casts'] = vod_lenz_raw['Casts'].str.replace(',', '،')
vod_lenz_raw['Casts'] = vod_lenz_raw['Casts'].str.strip()

vod_lenz_raw['Genres'] = vod_lenz_raw['Genres'].str.replace('}', '')
vod_lenz_raw['Genres'] = vod_lenz_raw['Genres'].str.replace('{', '')
vod_lenz_raw['Genres'] = vod_lenz_raw['Genres'].str.replace('"', '')
vod_lenz_raw['Genres'] = vod_lenz_raw['Genres'].str.replace(',', '،')
vod_lenz_raw['Genres'] = vod_lenz_raw['Genres'].str.strip()

vod_lenz_raw['Director'] = vod_lenz_raw['Director'].str.strip()
vod_lenz_raw['Language'] = vod_lenz_raw['Language'].str.strip()
vod_lenz_raw['ReleaseDateGeorgian'] = vod_lenz_raw['ReleaseDateGeorgian'].str.strip()
vod_lenz_raw['ReleaseDateJalali'] = vod_lenz_raw['ReleaseDateJalali'].str.strip()
vod_lenz_raw['EnglishName'] = vod_lenz_raw['EnglishName'].str.strip()

vod_lenz_raw['Cameraman'] = ''
vod_lenz_raw['Composer'] = ''
vod_lenz_raw['DubbedSubtitle'] = ''
vod_lenz_raw['Editor'] = ''
vod_lenz_raw['Producer'] = ''
vod_lenz_raw['Writer'] = ''
vod_lenz_raw['TitleCleaned2'] = ''
vod_lenz_raw['AgeRange'] = ''
vod_lenz_raw['Imdb'] = ''
vod_lenz_raw['Singer'] = ''
del vod_lenz_raw['FilmSerial'] 

db1_dup = db1.copy()
db1_dup.drop_duplicates(subset =['ID'], keep = 'last', inplace = True)
db1_new = pd.DataFrame()
db1_new['FilmSerial'] = db1_dup['FilmSerial']
db1_new['ID'] = db1_dup['ID']
db1_new['IDS'] = db1_dup['IDS']
db1_new['TitleCleaned1'] = db1_dup['TitleCleaned1']
db1_new['Season'] = db1_dup['Season']
db1_new['Title'] = db1_dup['Title']

vod_lenz_merge_db1 = pd.merge(vod_lenz_raw, db1_new, on = ["Title"])
vod_lenz_merge_db1['Country'].replace('', 'NoData', inplace=True)
vod_lenz_merge_db1 = vod_lenz_merge_db1 [~vod_lenz_merge_db1.Country.str.contains('NoData')]
vod_lenz_merge_db1 = vod_lenz_merge_db1.reset_index()
del vod_lenz_merge_db1['index']
############################
vod_lenz = vod_lenz_merge_db1.copy()
edit_db2_in = vod_lenz.copy()
edit_db2_out = EditDB2(edit_db2_in)
vod_lenz_final = edit_db2_out.copy()
vod_lenz_final.dtypes
############################
drivers = pyodbc.drivers()

conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

for index, row in vod_lenz_final.iterrows():
    print(index)
    cursor.execute("INSERT INTO VOD.dbo.DB2_VOD (ID,IDS,Title,TitleCleaned1,TitleCleaned2,Season,FilmSerial,Director,EnglishName,AgeRange,Casts,Genres,Imdb,Language,ReleaseDateGeorgian,ReleaseDateJalali,Runtime,Country,Producer,Writer,Composer,Editor,DubbedSubtitle,Singer,Cameraman) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", row.ID,row.IDS,row.Title,row.TitleCleaned1,row.TitleCleaned2,row.Season,row.FilmSerial,row.Director,row.EnglishName,row.AgeRange,row.Casts,row.Genres,row.Imdb,row.Language,row.ReleaseDateGeorgian,row.ReleaseDateJalali,row.Runtime,row.Country,row.Producer,row.Writer,row.Composer,row.Editor,row.DubbedSubtitle,row.Singer,row.Cameraman)
    conn.commit()

################################ TVA #################################
vod_tva = vod_tva_crawl_output.copy()

del vod_tva['content_link']
del vod_tva['Epizode Number']
del vod_tva['tiva Rate']
del vod_tva['orginal_name']

vod_tva = vod_tva.rename(columns={"content_type":"ContentType"})
vod_tva = vod_tva.rename(columns={"latin_name":"FinglishName"})
vod_tva = vod_tva.rename(columns={"Release Date":"ReleaseDateGeorgian"})
vod_tva = vod_tva.rename(columns={"Directed by":"DirectedBy"})
vod_tva = vod_tva.rename(columns={"imdb":"ImdbRate"})
vod_tva = vod_tva.rename(columns={"title_cleaned_for_crawl":"TitleFirstClean"})

vod_tva['TitleFirstClean'] = vod_tva['TitleFirstClean'].str.strip() 

vod_tva1 = vod_tva.copy()
vod_tva1['TitleFirstClean'] = vod_tva1['TitleFirstClean'].str.replace('0', '۰')
vod_tva1['TitleFirstClean'] = vod_tva1['TitleFirstClean'].str.replace('1', '۱')
vod_tva1['TitleFirstClean'] = vod_tva1['TitleFirstClean'].str.replace('2', '۲')
vod_tva1['TitleFirstClean'] = vod_tva1['TitleFirstClean'].str.replace('3', '۳')
vod_tva1['TitleFirstClean'] = vod_tva1['TitleFirstClean'].str.replace('4', '۴')
vod_tva1['TitleFirstClean'] = vod_tva1['TitleFirstClean'].str.replace('5', '۵')
vod_tva1['TitleFirstClean'] = vod_tva1['TitleFirstClean'].str.replace('6', '۶')
vod_tva1['TitleFirstClean'] = vod_tva1['TitleFirstClean'].str.replace('7', '۷')
vod_tva1['TitleFirstClean'] = vod_tva1['TitleFirstClean'].str.replace('8', '۸')
vod_tva1['TitleFirstClean'] = vod_tva1['TitleFirstClean'].str.replace('9', '۹')
vod_tva1['TitleFirstClean'] = vod_tva1['TitleFirstClean'].str.replace('٠', '۰')
vod_tva1['TitleFirstClean'] = vod_tva1['TitleFirstClean'].str.replace('١', '۱')
vod_tva1['TitleFirstClean'] = vod_tva1['TitleFirstClean'].str.replace('٢', '۲')
vod_tva1['TitleFirstClean'] = vod_tva1['TitleFirstClean'].str.replace('٣', '۳')
vod_tva1['TitleFirstClean'] = vod_tva1['TitleFirstClean'].str.replace('٤', '۴')
vod_tva1['TitleFirstClean'] = vod_tva1['TitleFirstClean'].str.replace('٥', '۵')
vod_tva1['TitleFirstClean'] = vod_tva1['TitleFirstClean'].str.replace('٦', '۶')
vod_tva1['TitleFirstClean'] = vod_tva1['TitleFirstClean'].str.replace('٧', '۷')
vod_tva1['TitleFirstClean'] = vod_tva1['TitleFirstClean'].str.replace('٨', '۸')
vod_tva1['TitleFirstClean'] = vod_tva1['TitleFirstClean'].str.replace('٩', '۹')
vod_tva1['TitleFirstClean'] = vod_tva1['TitleFirstClean'].str.replace('ي', 'ی')
vod_tva1['TitleFirstClean'] = vod_tva1['TitleFirstClean'].str.replace('ؤ','و')
vod_tva1['TitleFirstClean'] = vod_tva1['TitleFirstClean'].str.replace('ك','ک')
vod_tva1['TitleFirstClean'] = vod_tva1['TitleFirstClean'].str.strip()

vod_tva1.drop_duplicates(subset =['TitleFirstClean', 'season'], keep = 'last', inplace = True)

vod_tva1.insert(14, 'ID', '')
vod_tva1.insert(15, 'AgeRange', '')
vod_tva1.insert(16, 'ReleaseDateJalali', '')

vod_tva1['Genres'] = vod_tva1['Genres'].str.replace(',','،')
vod_tva1['ReleaseDateGeorgian'] = vod_tva1['ReleaseDateGeorgian'].str.replace('سال','')
vod_tva1['Country'] = vod_tva1['Country'].str.replace(',','،')
vod_tva1['Country'] = vod_tva1['Country'].str.replace('ایالات متحدهٔ آمریکا','آمریکا')
vod_tva1['Country'] = vod_tva1['Country'].str.replace('روسیه فدرال','روسیه')
vod_tva1['Country'] = vod_tva1['Country'].str.replace('روسی','روسیه')
vod_tva1['Country'] = vod_tva1['Country'].str.replace('هدوستان','هند')
vod_tva1['Country'] = vod_tva1['Country'].str.replace('امارات متحده عربی','امارات')
vod_tva1['DirectedBy'] = vod_tva1['DirectedBy'].str.replace(',','،')
vod_tva1['Casts'] = vod_tva1['Casts'].str.replace(',','،')

vod_tva1['IDS'] = vod_tva1['IDS'].str.strip()
vod_tva1['ContentType'] = vod_tva1['ContentType'].str.strip()
vod_tva1['FinglishName'] = vod_tva1['FinglishName'].str.strip()
vod_tva1['Genres'] = vod_tva1['Genres'].str.strip()
vod_tva1['Runtime'] = vod_tva1['Runtime'].str.strip()
vod_tva1['ReleaseDateGeorgian'] = vod_tva1['ReleaseDateGeorgian'].str.strip()
vod_tva1['Language'] = vod_tva1['Language'].str.strip()
vod_tva1['Country'] = vod_tva1['Country'].str.strip()
vod_tva1['DirectedBy'] = vod_tva1['DirectedBy'].str.strip()
vod_tva1['Casts'] = vod_tva1['Casts'].str.strip()
vod_tva1['ImdbRate'] = vod_tva1['ImdbRate'].str.strip()
vod_tva1['TitleFirstClean'] = vod_tva1['TitleFirstClean'].str.strip()
vod_tva1['title'] = vod_tva1['title'].str.strip()
vod_tva1['season'] = vod_tva1['season'].str.strip()

vod_tva1 = vod_tva1.rename(columns={"season":"Season"})
vod_tva1 = vod_tva1.rename(columns={"title":"Title"})
vod_tva1 = vod_tva1.reset_index()
del vod_tva1['index']

vod_tva1['Cameraman'] = vod_tva1['Cameraman'].str.split().str.join(" ")
vod_tva1['Composer'] = vod_tva1['Composer'].str.split().str.join(" ")
vod_tva1['Director'] = vod_tva1['Director'].str.split().str.join(" ")
vod_tva1['DubbedSubtitle'] = vod_tva1['DubbedSubtitle'].str.split().str.join(" ")
vod_tva1['Editor'] = vod_tva1['Editor'].str.split().str.join(" ")
vod_tva1['Genres'] = vod_tva1['Genres'].str.split().str.join(" ")
vod_tva1['Producer'] = vod_tva1['Producer'].str.split().str.join(" ")
vod_tva1['Writer'] = vod_tva1['Writer'].str.split().str.join(" ")
vod_tva1['Language'] = vod_tva1['Language'].str.split().str.join(" ")
vod_tva1['Country'] = vod_tva1['Country'].str.split().str.join(" ")

####### please wate #######
vod_tva1.dtypes
############################
edit_db2_in = vod_tva1.copy()
edit_db2_out = EditDB2(edit_db2_in)
vod_tva_final = edit_db2_out.copy()
############################
drivers = pyodbc.drivers()

conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

for index, row in vod_tva1.iterrows():
    print(index)
    cursor.execute("INSERT INTO VOD.dbo.DB2_VOD (ID,IDS,Casts,AgeRange,ContentType,DirectedBy,FinglishName,Genres,ImdbRate,Language,ReleaseDateGeorgian,ReleaseDateJalali,Runtime,Season,Title,TitleFirstClean,Country) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", row.ID,row.IDS,row.Casts,row.AgeRange,row.ContentType,row.DirectedBy,row.FinglishName,row.Genres,row.ImdbRate,row.Language,row.ReleaseDateGeorgian,row.ReleaseDateJalali,row.Runtime,row.Season,row.Title,row.TitleFirstClean,row.Country)
    conn.commit()

################################ FILIMO #################################
vod_filimo_raw = pd.read_excel(r'E:\python codes\VOD\vod_data\1400\filimo_vod\FilimoDey1400.xlsx')
vod_filimo_new = vod_filimo_step6.copy()
vod_filimo_new.drop_duplicates(subset =['IDS'], keep = 'last', inplace = True)
vod_filimo_new = vod_filimo_new.reset_index()
del vod_filimo_new['index']

db2_IDS = db2['IDS']
vod_filimo_new_merge_db2 = pd.merge(vod_filimo_new, db2_IDS, on = ['IDS'])
#######
# if vod_filimo_new_merge_db2 != 0
#######

db2_TitleCleaned_Season = pd.DataFrame()
db2_TitleCleaned_Season['TitleCleaned1'] = db2['TitleCleaned1']
db2_TitleCleaned_Season['Season'] = db2['Season']
vod_filimo_new_merge_db3 = pd.merge(vod_filimo_new, db2_TitleCleaned_Season, on = ['TitleCleaned1', 'Season'])
vod_filimo_new_merge_db3.drop_duplicates(subset =['TitleCleaned1', 'Season'], keep = False, inplace = True)

#######
# if vod_filimo_new_merge_db3 != 0
vod_filimo_new_append_db3 = vod_filimo_new.append([vod_filimo_new_merge_db3])
vod_filimo_new_append_db3.drop_duplicates(subset =['TitleCleaned1', 'Season'], keep = False, inplace = True)
#######

vod_filimo_new = vod_filimo_new_append_db3.copy()

del vod_filimo_new['Epizode']
del vod_filimo_new['DateTime']
del vod_filimo_new['Visit']
del vod_filimo_new['ActiveUsers']
del vod_filimo_new['DurationMin']
del vod_filimo_new['Operators']
del vod_filimo_new['Month']
del vod_filimo_new['Year']
#del vod_filimo_new['code_FilmSerial']

del vod_filimo_raw['LinkAddress']
del vod_filimo_raw['Like']
del vod_filimo_raw['Percent']
del vod_filimo_raw['Epizode']
del vod_filimo_raw['FilmSerial']

vod_filimo_raw=vod_filimo_raw.rename(columns={"Year":"ReleaseDateGeorgian"})
vod_filimo_raw['Season'] = vod_filimo_raw['Season'].fillna(0)
vod_filimo_raw['Season'] = vod_filimo_raw['Season'].astype(int).astype(str)
vod_filimo_raw['Season'] = vod_filimo_raw['Season'].apply(lambda x: x.zfill(2))

vod_filimo_new_merge_raw = pd.merge(vod_filimo_new, vod_filimo_raw, on = ['Title', 'Season'])
vod_filimo_new_merge_raw.drop_duplicates(subset =['IDS'], keep = 'last', inplace = True)

vod_filimo_new_merge_raw = vod_filimo_new_merge_raw.reset_index()
del vod_filimo_new_merge_raw['index']
for i in range(0, len(vod_filimo_new_merge_raw)):
    print(i)
    if vod_filimo_new_merge_raw.loc[i, 'ReleaseDateGeorgian'] < 1500:
        vod_filimo_new_merge_raw.loc[i, 'ReleaseDateJalali'] = vod_filimo_new_merge_raw.loc[i, 'ReleaseDateGeorgian']
        vod_filimo_new_merge_raw.loc[i, 'ReleaseDateGeorgian'] = ""

vod_filimo_new_merge_raw = vod_filimo_new_merge_raw.fillna("")

vod_filimo_new_merge_raw['Genres'] = vod_filimo_new_merge_raw['Genres'].str.strip() 
vod_filimo_new_merge_raw['Genres'] = vod_filimo_new_merge_raw['Genres'].str.replace(',', '،')
vod_filimo_new_merge_raw['Genres'] = vod_filimo_new_merge_raw['Genres'].astype(str)
vod_filimo_new_merge_raw['Genres'] = vod_filimo_new_merge_raw['Genres'].map(lambda x: x.lstrip('،'))

vod_filimo_new_merge_raw['Country'] = vod_filimo_new_merge_raw['Country'].str.strip() 
vod_filimo_new_merge_raw['Country'] = vod_filimo_new_merge_raw['Country'].str.replace(',', '،')
vod_filimo_new_merge_raw['Country'] = vod_filimo_new_merge_raw['Country'].str.replace('انگلیس', 'انگلستان')
vod_filimo_new_merge_raw['Country'] = vod_filimo_new_merge_raw['Country'].astype(str)

#vod_filimo_new_merge_raw['ReleaseDateGeorgian'] = vod_filimo_new_merge_raw['ReleaseDateGeorgian'].astype(int).astype(str)
#vod_filimo_new_merge_raw['ReleaseDateJalali'] = vod_filimo_new_merge_raw['ReleaseDateJalali'].astype(int).astype(str)
#vod_filimo_new_merge_raw['ReleaseDateGeorgian'] = vod_filimo_new_merge_raw['ReleaseDateGeorgian'].str.strip()
#vod_filimo_new_merge_raw['ReleaseDateJalali'] = vod_filimo_new_merge_raw['ReleaseDateJalali'].str.strip()

vod_filimo_new_merge_raw['Director'] = vod_filimo_new_merge_raw['Director'].str.strip() 
vod_filimo_new_merge_raw['Director'] = vod_filimo_new_merge_raw['Director'].str.replace(',', '،')
vod_filimo_new_merge_raw['Director'] = vod_filimo_new_merge_raw['Director'].astype(str)
vod_filimo_new_merge_raw['Director'] = vod_filimo_new_merge_raw['Director'].map(lambda x: x.lstrip('،'))


vod_filimo_new_merge_raw['Casts'] = vod_filimo_new_merge_raw['Casts'].str.strip() 
vod_filimo_new_merge_raw['Casts'] = vod_filimo_new_merge_raw['Casts'].str.replace(',', '،')
vod_filimo_new_merge_raw['Casts'] = vod_filimo_new_merge_raw['Casts'].astype(str)
vod_filimo_new_merge_raw['Casts'] = vod_filimo_new_merge_raw['Casts'].map(lambda x: x.lstrip('،'))

vod_filimo_new_merge_raw.insert(22, 'Editor', '')
vod_filimo_new_merge_raw.insert(23, 'Language', '')
#vod_filimo_new_merge_raw.insert(24, 'ReleaseDateJalali', '')

#vod_filimo_new_merge_raw.dtypes
vod_filimo_new = vod_filimo_new_merge_raw.copy()

vod_filimo_new = vod_filimo_new.reset_index()
del vod_filimo_new['index']

vod_filimo_new['Cameraman'] = vod_filimo_new['Cameraman'].str.split().str.join(" ")
vod_filimo_new['Composer'] = vod_filimo_new['Composer'].str.split().str.join(" ")
vod_filimo_new['Director'] = vod_filimo_new['Director'].str.split().str.join(" ")
vod_filimo_new['DubbedSubtitle'] = vod_filimo_new['DubbedSubtitle'].str.split().str.join(" ")
vod_filimo_new['Editor'] = vod_filimo_new['Editor'].str.split().str.join(" ")
vod_filimo_new['Genres'] = vod_filimo_new['Genres'].str.split().str.join(" ")
vod_filimo_new['Producer'] = vod_filimo_new['Producer'].str.split().str.join(" ")
vod_filimo_new['Writer'] = vod_filimo_new['Writer'].str.split().str.join(" ")
vod_filimo_new['Language'] = vod_filimo_new['Language'].str.split().str.join(" ")
vod_filimo_new['Country'] = vod_filimo_new['Country'].str.split().str.join(" ")

####### please wait #######
vod_filimo_new['Imdb'] = vod_filimo_new['Imdb'].astype(str)
vod_filimo_new['ReleaseDateGeorgian'] = vod_filimo_new['ReleaseDateGeorgian'].astype(str)
vod_filimo_new['ReleaseDateJalali'] = vod_filimo_new['ReleaseDateJalali'].astype(str).replace('\.0', '', regex=True)
#vod_filimo_new['ReleaseDateJalali'] = vod_filimo_new['ReleaseDateJalali'].astype(str)
#vod_filimo_new.replace('nan', '', inplace=True)
#vod_filimo_new = vod_filimo_new.fillna('')
vod_filimo_new.dtypes
############################
edit_db2_in = vod_filimo_new.copy()
edit_db2_out = EditDB2(edit_db2_in)
vod_filimo_final = edit_db2_out.copy()
###########################

drivers = pyodbc.drivers()

conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

for index, row in vod_filimo_final.iterrows():
    print(index)
    cursor.execute("INSERT INTO VOD.dbo.DB2_VOD (ID,IDS,Title,TitleCleaned1,TitleCleaned2,Season,FilmSerial,Director,EnglishName,AgeRange,Casts,Genres,Imdb,Language,ReleaseDateGeorgian,ReleaseDateJalali,Runtime,Country,Producer,Writer,Composer,Editor,DubbedSubtitle,Singer,Cameraman) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", row.ID,row.IDS,row.Title,row.TitleCleaned1,row.TitleCleaned2,row.Season,row.FilmSerial,row.Director,row.EnglishName,row.AgeRange,row.Casts,row.Genres,row.Imdb,row.Language,row.ReleaseDateGeorgian,row.ReleaseDateJalali,row.Runtime,row.Country,row.Producer,row.Writer,row.Composer,row.Editor,row.DubbedSubtitle,row.Singer,row.Cameraman)
    conn.commit()

################################ FILMGARDI #################################
vod_filmgardi_DB1 = db1.query("operators == 'فیلم گردی'")
vod_filmgardi_DB1 = vod_filmgardi_DB1.query("month == 'شهریور'")
vod_filmgardi_DB1 = vod_filmgardi_DB1.query("year == '1400'")
vod_filmgardi = pd.read_excel(r'E:\python codes\VOD\vod_data\1400\filmgardi_vod\FilmgardiShahrivar1400.xlsx')

del vod_filmgardi_DB1['DateTime']
del vod_filmgardi_DB1['active_users']
#del vod_filmgardi_DB1['title']
del vod_filmgardi_DB1['duration_min']
del vod_filmgardi_DB1['epizode']
del vod_filmgardi_DB1['month']
del vod_filmgardi_DB1['operators']
del vod_filmgardi_DB1['visit']
del vod_filmgardi_DB1['year']

del vod_filmgardi['link_address']
del vod_filmgardi['like']
del vod_filmgardi['percent']
del vod_filmgardi['producer']
del vod_filmgardi['dubbed']
del vod_filmgardi['writer']
del vod_filmgardi['editor']
del vod_filmgardi['composer']
#del vod_filmgardi_merge['composer']

vod_filmgardi_DB1 = vod_filmgardi_DB1.rename(columns={"season":"Season"})
vod_filmgardi_DB1 = vod_filmgardi_DB1.rename(columns={"title_cleaned":"TitleFirstClean"})
vod_filmgardi_DB1 = vod_filmgardi_DB1.rename(columns={"title":"Title"})

vod_filmgardi = vod_filmgardi.rename(columns={"title":"Title"})
vod_filmgardi = vod_filmgardi.rename(columns={"latin_title":"FinglishName"})
vod_filmgardi = vod_filmgardi.rename(columns={"imdb":"ImdbRate"})
vod_filmgardi = vod_filmgardi.rename(columns={"Genre":"Genres"})
vod_filmgardi = vod_filmgardi.rename(columns={"country":"Country"})
vod_filmgardi = vod_filmgardi.rename(columns={"year":"ReleaseDateGeorgian"})
vod_filmgardi = vod_filmgardi.rename(columns={"season":"Season"})
vod_filmgardi = vod_filmgardi.rename(columns={"director":"DirectedBy"})

new = vod_filmgardi["FilmSerial"].str.split('قسمت', n = 1, expand = True)
vod_filmgardi["Season"]= new[0]
vod_filmgardi['Season'] = vod_filmgardi['Season'].str.replace('فصل', '')
vod_filmgardi['Season'] = vod_filmgardi['Season'].str.replace('.', '')
vod_filmgardi['Season'] = vod_filmgardi['Season'].str.strip()
vod_filmgardi['Season'] = vod_filmgardi['Season'].fillna('00')
vod_filmgardi['Season'] = vod_filmgardi['Season'].apply(lambda x: x.zfill(2))
del vod_filmgardi['FilmSerial']

vod_filmgardi_merge = pd.merge(vod_filmgardi_DB1, vod_filmgardi, on = ['Title', 'Season'])
vod_filmgardi_merge.drop_duplicates(subset =['Title', 'Season'], keep = 'last', inplace = True)

vod_filmgardi_merge = vod_filmgardi_merge.rename(columns={"FilmSerial":"ContentType"})
vod_filmgardi_merge = vod_filmgardi_merge.rename(columns={"language":"Language"})
vod_filmgardi_merge.insert(15, 'FinglishName', '')
vod_filmgardi_merge.insert(16, 'ReleaseDateJalali', '')
#vod_filmgardi_merge['ReleaseDateGeorgian'] = vod_filmgardi_merge['ReleaseDateGeorgian'].str.strip()
#vod_filmgardi_merge['ReleaseDateGeorgian'].replace('', '0', inplace=True)
vod_filmgardi_merge['ReleaseDateGeorgian'] = vod_filmgardi_merge['ReleaseDateGeorgian'].fillna(0)
vod_filmgardi_merge = vod_filmgardi_merge.reset_index()
del vod_filmgardi_merge['index']
vod_filmgardi_merge.loc[402, 'ReleaseDateGeorgian'] = 1384
vod_filmgardi_merge['ReleaseDateGeorgian'] = vod_filmgardi_merge['ReleaseDateGeorgian'].astype(int)

for i in range(0, len(vod_filmgardi_merge)):
    print(i)
    if vod_filmgardi_merge.loc[i, 'ReleaseDateGeorgian'] < 1500:
        vod_filmgardi_merge.loc[i, 'ReleaseDateJalali'] = vod_filmgardi_merge.loc[i, 'ReleaseDateGeorgian']
        vod_filmgardi_merge.loc[i, 'ReleaseDateGeorgian'] = ""

vod_filmgardi_merge['Genres'] = vod_filmgardi_merge['Genres'].str.strip() 
vod_filmgardi_merge['Genres'] = vod_filmgardi_merge['Genres'].str.replace(',', '،')
vod_filmgardi_merge['Genres'] = vod_filmgardi_merge['Genres'].astype(str)
vod_filmgardi_merge['Genres'] = vod_filmgardi_merge['Genres'].map(lambda x: x.lstrip('،'))

vod_filmgardi_merge['Country'] = vod_filmgardi_merge['Country'].str.strip() 
vod_filmgardi_merge['Country'] = vod_filmgardi_merge['Country'].str.replace(',', '،')
vod_filmgardi_merge['Country'] = vod_filmgardi_merge['Country'].str.replace('انگلیس', 'انگلستان')
vod_filmgardi_merge['Country'] = vod_filmgardi_merge['Country'].astype(str)

#vod_filmgardi_merge['ReleaseDateGeorgian'] = vod_filmgardi_merge['ReleaseDateGeorgian'].str.strip()
#vod_filmgardi_merge['ReleaseDateJalali'] = vod_filmgardi_merge['ReleaseDateJalali'].str.strip()

vod_filmgardi_merge['DirectedBy'] = vod_filmgardi_merge['DirectedBy'].str.strip() 
vod_filmgardi_merge['DirectedBy'] = vod_filmgardi_merge['DirectedBy'].str.replace(',', '،')
vod_filmgardi_merge['DirectedBy'] = vod_filmgardi_merge['DirectedBy'].astype(str)
vod_filmgardi_merge['DirectedBy'] = vod_filmgardi_merge['DirectedBy'].map(lambda x: x.lstrip('،'))


vod_filmgardi_merge['Casts'] = vod_filmgardi_merge['Casts'].str.strip() 
vod_filmgardi_merge['Casts'] = vod_filmgardi_merge['Casts'].str.replace(',', '،')
vod_filmgardi_merge['Casts'] = vod_filmgardi_merge['Casts'].astype(str)
vod_filmgardi_merge['Casts'] = vod_filmgardi_merge['Casts'].map(lambda x: x.lstrip('،'))
#vod_filmgardi_merge.to_excel('vod_filmgardi_merge.xlsx', index=False)
#vod_filmgardi_merge2=pd.read_excel(r'E:\python codes\VOD\vod_filmgardi_merge.xlsx')
#vod_filmgardi_merge['Country'] = vod_filmgardi_merge2['Country']

for i in range(194, len(vod_filmgardi_merge)):
    print(i)
    for j in range(0, len(db2)):
        if vod_filmgardi_merge.loc[i, 'IDS'] == db2.loc[j, 'IDS']:
            vod_filmgardi_merge = vod_filmgardi_merge.drop(i)
            break

vod_filmgardi_merge.drop_duplicates(subset =['TitleFirstClean', 'Season', 'ContentType'], keep = 'last', inplace = True)
vod_filmgardi_merge = vod_filmgardi_merge.reset_index()
del vod_filmgardi_merge['index']
for i in range(0, len(vod_filmgardi_merge)):
    print(i)
    for j in range(0, len(db2)):
        if vod_filmgardi_merge.loc[i, 'TitleFirstClean'] == db2.loc[j, 'TitleFirstClean'] and \
           vod_filmgardi_merge.loc[i, 'Season'] == db2.loc[j, 'Season'] and \
           vod_filmgardi_merge.loc[i, 'ContentType'] == db2.loc[j, 'ContentType']:
            vod_filmgardi_merge = vod_filmgardi_merge.drop(i)
            break

vod_filmgardi_merge = vod_filmgardi_merge.reset_index()
del vod_filmgardi_merge['index']


#vod_filmgardi2 = vod_filmgardi_merge.copy()
#vod_filmgardi_merge = vod_filmgardi2.copy()

vod_filmgardi_merge['ImdbRate'] = vod_filmgardi_merge['ImdbRate'].astype(str)


vod_filmgardi_merge['Runtime'] = vod_filmgardi_merge['Runtime'].str.strip()
vod_filmgardi_merge['Runtime'] = vod_filmgardi_merge['Runtime'].astype(str)
vod_filmgardi_merge['Runtime'] = vod_filmgardi_merge['Runtime'].map(lambda x: x.lstrip('.'))
vod_filmgardi_merge['Runtime'].replace('nan', '', inplace=True)

vod_filmgardi_merge['Runtime'] = vod_filmgardi_merge['Runtime'].str.strip()

vod_filmgardi_merge_name = vod_filmgardi_merge['AgeRange']
for i in range(0,len(vod_filmgardi_merge)):
    print(i)
    x_name_content = vod_filmgardi_merge_name[i]
    head, sep, tail = x_name_content.partition('مناسب')
    vod_filmgardi_merge.loc[i, 'AgeRange'] = head

vod_filmgardi_merge['DirectedBy'] = vod_filmgardi_merge['DirectedBy'].str.replace('کارگردان :', '')
vod_filmgardi_merge['DirectedBy'] = vod_filmgardi_merge['DirectedBy'].str.strip()

vod_filmgardi_merge = vod_filmgardi_merge.fillna('')
vod_filmgardi_merge.replace('nan', '', inplace=True)

#vod_filmgardi2 = vod_filmgardi_merge.copy()
#vod_filmgardi_merge = vod_filmgardi2.copy()

vod_filmgardi_final = vod_filmgardi_merge.copy()
vod_filmgardi_final.drop_duplicates(subset =['IDS'], keep = 'last', inplace = True)
####### please wate #######
vod_filmgardi_final['ImdbRate'] = vod_filmgardi_final['ImdbRate'].astype(str)
vod_filmgardi_final.replace('nan', '', inplace=True)
vod_filmgardi_final = vod_filmgardi_final.fillna('')
vod_filmgardi_final.dtypes

vod_filmgardi_final['Cameraman'] = vod_filmgardi_final['Cameraman'].str.split().str.join(" ")
vod_filmgardi_final['Composer'] = vod_filmgardi_final['Composer'].str.split().str.join(" ")
vod_filmgardi_final['Director'] = vod_filmgardi_final['Director'].str.split().str.join(" ")
vod_filmgardi_final['DubbedSubtitle'] = vod_filmgardi_final['DubbedSubtitle'].str.split().str.join(" ")
vod_filmgardi_final['Editor'] = vod_filmgardi_final['Editor'].str.split().str.join(" ")
vod_filmgardi_final['Genres'] = vod_filmgardi_final['Genres'].str.split().str.join(" ")
vod_filmgardi_final['Producer'] = vod_filmgardi_final['Producer'].str.split().str.join(" ")
vod_filmgardi_final['Writer'] = vod_filmgardi_final['Writer'].str.split().str.join(" ")
vod_filmgardi_final['Language'] = vod_filmgardi_final['Language'].str.split().str.join(" ")
vod_filmgardi_final['Country'] = vod_filmgardi_final['Country'].str.split().str.join(" ")

############################
edit_db2_in = vod_filmgardi_final.copy()
edit_db2_out = EditDB2(edit_db2_in)
vod_filmgardi_final = edit_db2_out.copy()
###########################

drivers = pyodbc.drivers()

conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

for index, row in vod_filmgardi_final.iterrows():
    print(index)
    cursor.execute("INSERT INTO VOD.dbo.DB2_VOD (ID,IDS,Casts,AgeRange,ContentType,DirectedBy,FinglishName,Genres,ImdbRate,Language,ReleaseDateGeorgian,ReleaseDateJalali,Runtime,Season,Title,TitleFirstClean,Country) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", row.ID,row.IDS,row.Casts,row.AgeRange,row.ContentType,row.DirectedBy,row.FinglishName,row.Genres,row.ImdbRate,row.Language,row.ReleaseDateGeorgian,row.ReleaseDateJalali,row.Runtime,row.Season,row.Title,row.TitleFirstClean,row.Country)
    conn.commit()

################################################################################
################################################################################
################################################################################
################################################################################
################################################################################
###### delete ######
db1_lenz = db1.query('Operators == "لنز"')
db1_aio = db1.query('Operators == "آیو"')
db1_tva = db1.query('Operators == "تیوا"')
db1_filimo = db1.query('Operators == "فیلیمو"')
db1_filmgardi = db1.query('Operators == "فیلم گردی"')
################################################################################
################################################################################
vod_lenz = db1_lenz.copy()
vod_aio = db1_aio.copy()
vod_tva = db1_tva.copy()
vod_filimo = db1_filimo.copy()
vod_filmgardi = db1_filmgardi.copy()
################################################################################
################################################################################
vod_lenz['TitleCleaned1'] = vod_lenz['Title']

vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('دعای')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('ریاضی')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('گزینه دو')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('vod')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('فارسی')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('انگلیسی')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('عربی')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('علوم')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('روضه')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('مداح')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('-مداح')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('متوسطه')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('ابتدایی')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('خلاصه')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('علوم')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('فارسی')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('کاروان ایران')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('زبان انگلیسی')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('زیست شناسی')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('مدال')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('گل های بازی')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('مطالعات اجتماعی')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('لالیگا')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('برترین لحظات')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('برترینهای')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('برد تاریخی')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('برد سخت')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('لیگ جزیره')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('تکنیک های')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('رالی')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('شکست حریف')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('موتور اسپرت')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('برد مقابل')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('صد گل')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('لیگ قهرمانان')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('فوتبال ساحلی')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('فینالیست شدن')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('قرعه کشی')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('مجله آشپزی')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('مجله اشپزی')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('مرور بازیهای')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('مسابقات')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('شکست برابر')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('مقابل حریف')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('کشتی آزاد')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('المپیک')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('کیلوگرم مقابل')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('گل های')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('گلهای')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('ایرانسل')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('گل برتر')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('آنفیلد')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('لیونل مسی')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('کسب طلای')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('جام جهانی')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('شکست حمیده')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('شکست مرتضی')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('شکست میرزازاده')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('صد سیو')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('ضربات آزاد')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('مستند پرسپولیس')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('پیروزی قاطع')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('پیروزی نفسگیر')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('نماینده تکواندو')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('مجله آشپزی')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('مجله آشپزی')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('بارسلونا')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('کولاک بانوان')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('نیمار')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('گل برتر')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('بسکتبال')]
vod_lenz = vod_lenz [~vod_lenz. Title.str.contains('علی پناه')]
vod_lenz = vod_lenz [~vod_lenz. Title.str.contains('رضا هلالی')]
vod_lenz = vod_lenz [~vod_lenz. Title.str.contains('مهدی حسن آبادی')]
vod_lenz = vod_lenz [~vod_lenz. Title.str.contains('حجت الاسلام')]
vod_lenz = vod_lenz [~vod_lenz. Title.str.contains('قاری بین المللی')]
vod_lenz = vod_lenz [~vod_lenz. Title.str.contains('حاج امیر')]
vod_lenz = vod_lenz [~vod_lenz. Title.str.contains('حاج جواد')]
vod_lenz = vod_lenz [~vod_lenz. Title.str.contains('سید جواد')]
vod_lenz = vod_lenz [~vod_lenz. Title.str.contains('کاکایی')]
vod_lenz = vod_lenz [~vod_lenz. Title.str.contains('احمد ابوالقاسمی')]
vod_lenz = vod_lenz [~vod_lenz. Title.str.contains('حاج مهدی')]
vod_lenz = vod_lenz [~vod_lenz. Title.str.contains('حاج احمد')]
vod_lenz = vod_lenz [~vod_lenz. Title.str.contains('کربلایی')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('رونالدو')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('دربی')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('کارت قرمز')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('کار با توپ')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('یک اتفاق تازه')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('شکست نجاتی')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('نماهنگ')]
vod_lenz = vod_lenz [~vod_lenz. TitleCleaned1.str.contains('test')]

vod_lenz['TitleCleaned1'] = vod_lenz['TitleCleaned1'].str.replace('دوبله', '')
vod_lenz['TitleCleaned1'] = vod_lenz['TitleCleaned1'].str.replace('(', '')
vod_lenz['TitleCleaned1'] = vod_lenz['TitleCleaned1'].str.replace(')', '')
vod_lenz['TitleCleaned1'] = vod_lenz['TitleCleaned1'].str.replace('سریال', '')

vod_lenz = vod_lenz.reset_index()
del vod_lenz['index']

vod_lenz['TitleCleaned1'] = vod_lenz['TitleCleaned1'].astype(str)

vod_lenz['Season'] = ''
vod_lenz['Epizode'] = ''
vod_lenz['FilmSerial'] = ''

vod_lenz101 = vod_lenz[~vod_lenz.TitleCleaned1.str.contains("فصل شکار")]
vod_lenz201 = vod_lenz[vod_lenz.TitleCleaned1.str.contains("فصل شکار")]
vod_lenz102 = vod_lenz101[~vod_lenz101.TitleCleaned1.str.contains("فصل قتل ها")]
vod_lenz202 = vod_lenz101[vod_lenz101.TitleCleaned1.str.contains("فصل قتل ها")]
vod_lenz103 = vod_lenz102[~vod_lenz102.TitleCleaned1.str.contains("فصل موج سواری")]
vod_lenz203 = vod_lenz102[vod_lenz102.TitleCleaned1.str.contains("فصل موج سواری")]
vod_lenz200 = vod_lenz201.append([vod_lenz202, vod_lenz203])
del vod_lenz201
del vod_lenz202
del vod_lenz203
del vod_lenz101
del vod_lenz102

vod_lenz1 = vod_lenz103[vod_lenz103.TitleCleaned1.str.contains("فصل")]
vod_lenz2 = vod_lenz103[~vod_lenz103.TitleCleaned1.str.contains("فصل")]
vod_lenz1[["TitleCleaned1", "Season"]]= vod_lenz1["TitleCleaned1"].str.split("فصل", n = 1, expand = True)

vod_lenz11 = vod_lenz1[vod_lenz1.Season.str.contains("قسمت")]
vod_lenz12 = vod_lenz1[~vod_lenz1.Season.str.contains("قسمت")]
vod_lenz11[["Season", "Epizode"]]= vod_lenz11["Season"].str.split("قسمت", n = 1, expand = True)

vod_lenz104 = vod_lenz11.append([vod_lenz12, vod_lenz2, vod_lenz200])
del vod_lenz1
del vod_lenz2
del vod_lenz11
del vod_lenz12
del vod_lenz200
del vod_lenz103

vod_lenz104['Season'] = vod_lenz104['Season'].str.strip()
vod_lenz104['Epizode'] = vod_lenz104['Epizode'].str.strip()
vod_lenz104['TitleCleaned1'] = vod_lenz104['TitleCleaned1'].str.strip()

#########
vod_lenz1 = vod_lenz104[vod_lenz104.TitleCleaned1.str.contains("قسمت")]
vod_lenz2 = vod_lenz104[~vod_lenz104.TitleCleaned1.str.contains("قسمت")]
vod_lenz1[["TitleCleaned1", "Epizode"]]= vod_lenz1["TitleCleaned1"].str.split("قسمت", n = 1, expand = True)
vod_lenz1['Season'].replace('', '01', inplace=True)
vod_lenz105 = vod_lenz1.append([vod_lenz2])
del vod_lenz1
del vod_lenz2
vod_lenz105['Season'] = vod_lenz105['Season'].str.strip()
vod_lenz105['Epizode'] = vod_lenz105['Epizode'].str.strip()
vod_lenz105['TitleCleaned1'] = vod_lenz105['TitleCleaned1'].str.strip()

#########
vod_lenz105['Epizode'] = vod_lenz105['Epizode'].str.replace('سی ام', '30')
vod_lenz105['Epizode'] = vod_lenz105['Epizode'].str.replace('بیست و نهم', '29')
vod_lenz105['Epizode'] = vod_lenz105['Epizode'].str.replace('بیست و هشتم', '28')
vod_lenz105['Epizode'] = vod_lenz105['Epizode'].str.replace('بیست و هفتم', '27')
vod_lenz105['Epizode'] = vod_lenz105['Epizode'].str.replace('بیست و ششم', '26')
vod_lenz105['Epizode'] = vod_lenz105['Epizode'].str.replace('بیست و پنجم', '25')
vod_lenz105['Epizode'] = vod_lenz105['Epizode'].str.replace('بیست و چهارم', '24')
vod_lenz105['Epizode'] = vod_lenz105['Epizode'].str.replace('بیست و سوم', '23')
vod_lenz105['Epizode'] = vod_lenz105['Epizode'].str.replace('بیست و دوم', '22')
vod_lenz105['Epizode'] = vod_lenz105['Epizode'].str.replace('بیست و یکم', '21')
vod_lenz105['Epizode'] = vod_lenz105['Epizode'].str.replace('بیستم', '20')
vod_lenz105['Epizode'] = vod_lenz105['Epizode'].str.replace('نوزدهم', '19')
vod_lenz105['Epizode'] = vod_lenz105['Epizode'].str.replace('هجدهم', '18')
vod_lenz105['Epizode'] = vod_lenz105['Epizode'].str.replace('هفدهم', '17')
vod_lenz105['Epizode'] = vod_lenz105['Epizode'].str.replace('شانزدهم', '16')
vod_lenz105['Epizode'] = vod_lenz105['Epizode'].str.replace('پانزدهم', '15')
vod_lenz105['Epizode'] = vod_lenz105['Epizode'].str.replace('چهاردهم', '14')
vod_lenz105['Epizode'] = vod_lenz105['Epizode'].str.replace('سیزدهم', '13')
vod_lenz105['Epizode'] = vod_lenz105['Epizode'].str.replace('دوازدهم', '12')
vod_lenz105['Epizode'] = vod_lenz105['Epizode'].str.replace('یازدهم', '11')
vod_lenz105['Epizode'] = vod_lenz105['Epizode'].str.replace('دهم', '10')
vod_lenz105['Epizode'] = vod_lenz105['Epizode'].str.replace('نهم', '09')
vod_lenz105['Epizode'] = vod_lenz105['Epizode'].str.replace('هشتم', '08')
vod_lenz105['Epizode'] = vod_lenz105['Epizode'].str.replace('هفتم', '07')
vod_lenz105['Epizode'] = vod_lenz105['Epizode'].str.replace('ششم', '06')
vod_lenz105['Epizode'] = vod_lenz105['Epizode'].str.replace('پنجم', '05')
vod_lenz105['Epizode'] = vod_lenz105['Epizode'].str.replace('چهارم', '04')
vod_lenz105['Epizode'] = vod_lenz105['Epizode'].str.replace('سوم', '03')
vod_lenz105['Epizode'] = vod_lenz105['Epizode'].str.replace('دوم', '02')
vod_lenz105['Epizode'] = vod_lenz105['Epizode'].str.replace('اول', '01')

#########
vod_lenz1 = vod_lenz105[vod_lenz105.Epizode.str.contains(":")]
vod_lenz2 = vod_lenz105[~vod_lenz105.Epizode.str.contains(":")]
vod_lenz1[["Epizode", "NoData"]]= vod_lenz1["Epizode"].str.split(":", n = 1, expand = True)
del vod_lenz1['NoData']
vod_lenz106 = vod_lenz1.append([vod_lenz2])
del vod_lenz1
del vod_lenz2
vod_lenz106['Season'] = vod_lenz106['Season'].str.strip()
vod_lenz106['Epizode'] = vod_lenz106['Epizode'].str.strip()
vod_lenz106['TitleCleaned1'] = vod_lenz106['TitleCleaned1'].str.strip()

#########
vod_lenz1 = vod_lenz106[vod_lenz106.Epizode.str.contains("-")]
vod_lenz2 = vod_lenz106[~vod_lenz106.Epizode.str.contains("-")]
vod_lenz1[["Epizode", "NoData"]]= vod_lenz1["Epizode"].str.split("-", n = 1, expand = True)
del vod_lenz1['NoData']
vod_lenz107 = vod_lenz1.append([vod_lenz2])
del vod_lenz1
del vod_lenz2
vod_lenz107['Season'] = vod_lenz107['Season'].str.strip()
vod_lenz107['Epizode'] = vod_lenz107['Epizode'].str.strip()
vod_lenz107['TitleCleaned1'] = vod_lenz107['TitleCleaned1'].str.strip()

#########
vod_lenz1 = vod_lenz107[vod_lenz107.Epizode.str.contains("_")]
vod_lenz2 = vod_lenz107[~vod_lenz107.Epizode.str.contains("_")]
vod_lenz1[["Epizode", "NoData"]]= vod_lenz1["Epizode"].str.split("_", n = 1, expand = True)
del vod_lenz1['NoData']
vod_lenz108 = vod_lenz1.append([vod_lenz2])
del vod_lenz1
del vod_lenz2
vod_lenz108['Season'] = vod_lenz108['Season'].str.strip()
vod_lenz108['Epizode'] = vod_lenz108['Epizode'].str.strip()
vod_lenz108['TitleCleaned1'] = vod_lenz108['TitleCleaned1'].str.strip()

#########
vod_lenz1 = vod_lenz108[vod_lenz108.Epizode.str.contains("&")]
vod_lenz2 = vod_lenz108[~vod_lenz108.Epizode.str.contains("&")]
vod_lenz1[["Epizode", "NoData"]]= vod_lenz1["Epizode"].str.split("&", n = 1, expand = True)
del vod_lenz1['NoData']
vod_lenz109 = vod_lenz1.append([vod_lenz2])
del vod_lenz1
del vod_lenz2
vod_lenz109['Season'] = vod_lenz109['Season'].str.strip()
vod_lenz109['Epizode'] = vod_lenz109['Epizode'].str.strip()
vod_lenz109['TitleCleaned1'] = vod_lenz109['TitleCleaned1'].str.strip()

#########
vod_lenz1 = vod_lenz109[vod_lenz109.Epizode.str.contains("بخش")]
vod_lenz2 = vod_lenz109[~vod_lenz109.Epizode.str.contains("بخش")]
vod_lenz1[["Epizode", "NoData"]]= vod_lenz1["Epizode"].str.split("بخش", n = 1, expand = True)
del vod_lenz1['NoData']
vod_lenz110 = vod_lenz1.append([vod_lenz2])
del vod_lenz1
del vod_lenz2
vod_lenz110['Season'] = vod_lenz110['Season'].str.strip()
vod_lenz110['Epizode'] = vod_lenz110['Epizode'].str.strip()
vod_lenz110['TitleCleaned1'] = vod_lenz110['TitleCleaned1'].str.strip()

#########
vod_lenz1 = vod_lenz110[vod_lenz110.Epizode.str.contains("و")]
vod_lenz2 = vod_lenz110[~vod_lenz110.Epizode.str.contains("و")]
vod_lenz1[["Epizode", "NoData"]]= vod_lenz1["Epizode"].str.split("و", n = 1, expand = True)
del vod_lenz1['NoData']
vod_lenz111 = vod_lenz1.append([vod_lenz2])
del vod_lenz1
del vod_lenz2
vod_lenz111['Season'] = vod_lenz111['Season'].str.strip()
vod_lenz111['Epizode'] = vod_lenz111['Epizode'].str.strip()
vod_lenz111['TitleCleaned1'] = vod_lenz111['TitleCleaned1'].str.strip()

#########
vod_lenz1 = vod_lenz111[vod_lenz111.Epizode.str.contains(",")]
vod_lenz2 = vod_lenz111[~vod_lenz111.Epizode.str.contains(",")]
vod_lenz1[["Epizode", "NoData"]]= vod_lenz1["Epizode"].str.split(",", n = 1, expand = True)
del vod_lenz1['NoData']
vod_lenz112 = vod_lenz1.append([vod_lenz2])
del vod_lenz1
del vod_lenz2
vod_lenz112['Season'] = vod_lenz112['Season'].str.strip()
vod_lenz112['Epizode'] = vod_lenz112['Epizode'].str.strip()
vod_lenz112['TitleCleaned1'] = vod_lenz112['TitleCleaned1'].str.strip()

vod_lenz112['Season'] = vod_lenz112['Season'].str.replace('اول', '01')
vod_lenz112['Season'] = vod_lenz112['Season'].str.replace('دوم', '02')
vod_lenz112['Season'] = vod_lenz112['Season'].str.replace('سوم', '03')
vod_lenz112['Season'] = vod_lenz112['Season'].str.replace('چهارم', '04')
vod_lenz112['Season'] = vod_lenz112['Season'].str.replace('پنجم', '05')
vod_lenz112['Season'] = vod_lenz112['Season'].str.replace('ششم', '06')
vod_lenz112['Season'] = vod_lenz112['Season'].str.replace('هفتم', '07')
vod_lenz112['Season'] = vod_lenz112['Season'].str.replace('هشتم', '08')
vod_lenz112['Season'] = vod_lenz112['Season'].str.replace('نهم', '09')
vod_lenz112['Season'] = vod_lenz112['Season'].str.replace('دهم', '10')
vod_lenz112['Season'] = vod_lenz112['Season'].str.replace('یازدهم', '11')
vod_lenz112['Season'] = vod_lenz112['Season'].str.replace('دوازدهم', '12')
vod_lenz112['Season'] = vod_lenz112['Season'].str.replace('سیزدهم', '13')
vod_lenz112['Season'] = vod_lenz112['Season'].str.replace('چهاردهم', '14')
vod_lenz112['Season'] = vod_lenz112['Season'].str.replace('پانزدهم', '15')
vod_lenz112['Season'] = vod_lenz112['Season'].str.replace('شانزدهم', '16')
vod_lenz112['Season'] = vod_lenz112['Season'].str.replace('هفدهم', '17')
vod_lenz112['Season'] = vod_lenz112['Season'].str.replace('هجدهم', '18')
vod_lenz112['Season'] = vod_lenz112['Season'].str.replace('نوزدهم', '19')
vod_lenz112['Season'] = vod_lenz112['Season'].str.replace('بیستم', '20')

vod_lenz112['Season'] = vod_lenz112['Season'].str.replace('ا','01')

vod_lenz112['TitleCleaned1'] = vod_lenz112['TitleCleaned1'].str.replace('0', '۰')
vod_lenz112['TitleCleaned1'] = vod_lenz112['TitleCleaned1'].str.replace('1', '۱')
vod_lenz112['TitleCleaned1'] = vod_lenz112['TitleCleaned1'].str.replace('2', '۲')
vod_lenz112['TitleCleaned1'] = vod_lenz112['TitleCleaned1'].str.replace('3', '۳')
vod_lenz112['TitleCleaned1'] = vod_lenz112['TitleCleaned1'].str.replace('4', '۴')
vod_lenz112['TitleCleaned1'] = vod_lenz112['TitleCleaned1'].str.replace('5', '۵')
vod_lenz112['TitleCleaned1'] = vod_lenz112['TitleCleaned1'].str.replace('6', '۶')
vod_lenz112['TitleCleaned1'] = vod_lenz112['TitleCleaned1'].str.replace('7', '۷')
vod_lenz112['TitleCleaned1'] = vod_lenz112['TitleCleaned1'].str.replace('8', '۸')
vod_lenz112['TitleCleaned1'] = vod_lenz112['TitleCleaned1'].str.replace('9', '۹')
vod_lenz112['TitleCleaned1'] = vod_lenz112['TitleCleaned1'].str.replace('٠', '۰')
vod_lenz112['TitleCleaned1'] = vod_lenz112['TitleCleaned1'].str.replace('١', '۱')
vod_lenz112['TitleCleaned1'] = vod_lenz112['TitleCleaned1'].str.replace('٢', '۲')
vod_lenz112['TitleCleaned1'] = vod_lenz112['TitleCleaned1'].str.replace('٣', '۳')
vod_lenz112['TitleCleaned1'] = vod_lenz112['TitleCleaned1'].str.replace('٤', '۴')
vod_lenz112['TitleCleaned1'] = vod_lenz112['TitleCleaned1'].str.replace('٥', '۵')
vod_lenz112['TitleCleaned1'] = vod_lenz112['TitleCleaned1'].str.replace('٦', '۶')
vod_lenz112['TitleCleaned1'] = vod_lenz112['TitleCleaned1'].str.replace('٧', '۷')
vod_lenz112['TitleCleaned1'] = vod_lenz112['TitleCleaned1'].str.replace('٨', '۸')
vod_lenz112['TitleCleaned1'] = vod_lenz112['TitleCleaned1'].str.replace('٩', '۹')
vod_lenz112['TitleCleaned1'] = vod_lenz112['TitleCleaned1'].str.replace('ي', 'ی')
vod_lenz112['TitleCleaned1'] = vod_lenz112['TitleCleaned1'].str.replace('ؤ','و')
vod_lenz112['TitleCleaned1'] = vod_lenz112['TitleCleaned1'].str.replace('ك','ک')

total_vod_1399_Ghadimi_v2 = pd.read_excel(r'E:\python codes\VOD\kh_ghadimi\total_vod_1399_Ghadimi_v2.xlsx')
total_vod_1399_Ghadimi_v2=total_vod_1399_Ghadimi_v2.rename(columns={"شماره فصل":"season"})
total_vod_1399_Ghadimi_v2 = total_vod_1399_Ghadimi_v2.fillna("NO")
total_vod_1399_Ghadimi_v2['season'] = total_vod_1399_Ghadimi_v2['season'].astype(str)
total_vod_1399_Ghadimi_v2 = total_vod_1399_Ghadimi_v2[~total_vod_1399_Ghadimi_v2.season.str.contains("NO")]
total_vod_1399_Ghadimi_v2['season'] = total_vod_1399_Ghadimi_v2['season'].astype(str).replace('\.0', '', regex=True)
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
del total_vod_1399_Ghadimi_v2['فصل']

vod_lenz112.drop_duplicates(subset =['ID', 'Month', 'Year'], keep = 'last', inplace = True)
vod_lenz113 = pd.merge(vod_lenz112, total_vod_1399_Ghadimi_v2, on = ['TitleCleaned1'])
vod_lenz113['Season'] = vod_lenz113['season']
vod_lenz113['TitleCleaned1'] = vod_lenz113['title_first']
vod_lenz113['Season'] = vod_lenz113['season']
del vod_lenz113['season']
del vod_lenz113['title_first']
vod_lenz114 = vod_lenz112.append([vod_lenz113])
vod_lenz114.drop_duplicates(subset =['ID', 'Month', 'Year'], keep = 'last', inplace = True)

vod_lenz114['Season'] = vod_lenz114['Season'].str.replace('2019-2020', '00')
#######
vod_lenz114['TitleCleaned1'] = vod_lenz114['TitleCleaned1'].str.strip()
vod_lenz114['Season'] = vod_lenz114['Season'].str.strip() 
vod_lenz114['Epizode'] = vod_lenz114['Epizode'].str.strip() 
vod_lenz114['Season'] = vod_lenz114['Season'].apply(lambda x: x.zfill(2))
vod_lenz114 ['Season'].replace('', '00', inplace=True)
vod_lenz114['Epizode'] = vod_lenz114['Epizode'].apply(lambda x: x.zfill(3))
vod_lenz114 ['Epizode'].replace('', '000', inplace=True)

vod_lenz114['Epizode'] = vod_lenz114['Epizode'].str.replace('4 غریبه', '004')
vod_lenz114['Epizode'] = vod_lenz114['Epizode'].str.replace('3 زخم ها', '003')
vod_lenz114['Epizode'] = vod_lenz114['Epizode'].str.replace('2 کله آهنی', '002')
vod_lenz114['Epizode'] = vod_lenz114['Epizode'].str.replace('2 بهای قدرت 4', '002')
vod_lenz114['Epizode'] = vod_lenz114['Epizode'].str.replace('2 بهای قدرت 3', '002')
vod_lenz114['Epizode'] = vod_lenz114['Epizode'].str.replace('2 بهای قدرت 2', '002')
vod_lenz114['Epizode'] = vod_lenz114['Epizode'].str.replace('2 بهای قدرت 1', '002')
vod_lenz114['Epizode'] = vod_lenz114['Epizode'].str.replace('17 قسمت پایانی', '017')
vod_lenz114['Epizode'] = vod_lenz114['Epizode'].str.replace('1 خیزش لاک پشت های نینجا', '001')
vod_lenz114['Epizode'] = vod_lenz114['Epizode'].str.replace('1 آغاز ش', '001')

####### edit of contents #######
vod_lenz114_1 = vod_lenz114 [vod_lenz114.Title.str.contains('فصل 1 قسمت 2 : ایکس ری')]
vod_lenz114_2 = vod_lenz114 [~vod_lenz114.Title.str.contains('فصل 1 قسمت 2 : ایکس ری')]
vod_lenz114_1['TitleCleaned1'] = 'ایکس ری'
vod_lenz114_1['Season'] = '01'
vod_lenz114_1['Epizode'] = '002'
vod_lenz114 = vod_lenz114_1.append([vod_lenz114_2])
###
vod_lenz114_1 = vod_lenz114 [vod_lenz114.Title.str.contains('فصل 1 قسمت 3 : اعتماد')]
vod_lenz114_2 = vod_lenz114 [~vod_lenz114.Title.str.contains('فصل 1 قسمت 3 : اعتماد')]
vod_lenz114_1['TitleCleaned1'] = 'اعتماد'
vod_lenz114_1['Season'] = '01'
vod_lenz114_1['Epizode'] = '003'
vod_lenz114 = vod_lenz114_1.append([vod_lenz114_2])
###
vod_lenz114_1 = vod_lenz114 [vod_lenz114.Title.str.contains('فصل 1 قسمت 4 : تازه کار')]
vod_lenz114_2 = vod_lenz114 [~vod_lenz114.Title.str.contains('فصل 1 قسمت 4 : تازه کار')]
vod_lenz114_1['TitleCleaned1'] = 'تازه کار'
vod_lenz114_1['Season'] = '01'
vod_lenz114_1['Epizode'] = '004'
vod_lenz114 = vod_lenz114_1.append([vod_lenz114_2])
###
vod_lenz114_1 = vod_lenz114 [vod_lenz114.Title.str.contains('فصل شکار')]
vod_lenz114_2 = vod_lenz114 [~vod_lenz114.Title.str.contains('فصل شکار')]
vod_lenz114_1['TitleCleaned1'] = vod_lenz114_1['Title']
vod_lenz114_1['Season'] = ''
vod_lenz114 = vod_lenz114_1.append([vod_lenz114_2])
###
vod_lenz114_1 = vod_lenz114 [vod_lenz114.Title.str.contains('فصل 1 قسمت 2 : ایکس ری')]
vod_lenz114_2 = vod_lenz114 [~vod_lenz114.Title.str.contains('فصل 1 قسمت 2 : ایکس ری')]
vod_lenz114_1['TitleCleaned1'] = 'ایکس ری'
vod_lenz114_1['Season'] = '01'
vod_lenz114_1['Epizode'] = '002'
vod_lenz114 = vod_lenz114_1.append([vod_lenz114_2])
###
vod_lenz114_1 = vod_lenz114 [vod_lenz114.Title.str.contains('فصل قتل ها')]
vod_lenz114_2 = vod_lenz114 [~vod_lenz114.Title.str.contains('فصل قتل ها')]
vod_lenz114_1['TitleCleaned1'] = vod_lenz114_1['Title']
vod_lenz114_1['Season'] = ''
vod_lenz114 = vod_lenz114_1.append([vod_lenz114_2])
###
vod_lenz114_1 = vod_lenz114 [vod_lenz114.Title.str.contains('فصل موج سواری 2')]
vod_lenz114_2 = vod_lenz114 [~vod_lenz114.Title.str.contains('فصل موج سواری 2')]
vod_lenz114_1['TitleCleaned1'] = vod_lenz114_1['Title']
vod_lenz114_1['Season'] = ''
vod_lenz114 = vod_lenz114_1.append([vod_lenz114_2])
###
vod_lenz114_1 = vod_lenz114 [vod_lenz114.Title.str.contains('فصل 1 قسمت 1 : خاموش شدن')]
vod_lenz114_2 = vod_lenz114 [~vod_lenz114.Title.str.contains('فصل 1 قسمت 1 : خاموش شدن')]
vod_lenz114_1['TitleCleaned1'] = 'خاموش شدن'
vod_lenz114_1['Season'] = '01'
vod_lenz114_1['Epizode'] = '001'
vod_lenz114 = vod_lenz114_1.append([vod_lenz114_2])
###
vod_lenz114_1 = vod_lenz114 [vod_lenz114.Title.str.contains('فصل 1 قسمت 5 : راولو')]
vod_lenz114_2 = vod_lenz114 [~vod_lenz114.Title.str.contains('فصل 1 قسمت 5 : راولو')]
vod_lenz114_1['TitleCleaned1'] = 'راولو'
vod_lenz114_1['Season'] = '01'
vod_lenz114_1['Epizode'] = '005'
vod_lenz114 = vod_lenz114_1.append([vod_lenz114_2])
###
vod_lenz114_1 = vod_lenz114 [vod_lenz114.Title.str.contains('فصل 1 قسمت 6 : سوزاندن انسان')]
vod_lenz114_2 = vod_lenz114 [~vod_lenz114.Title.str.contains('فصل 1 قسمت 6 : سوزاندن انسان')]
vod_lenz114_1['TitleCleaned1'] = 'سوزاندن انسان'
vod_lenz114_1['Season'] = '01'
vod_lenz114_1['Epizode'] = '006'
vod_lenz114 = vod_lenz114_1.append([vod_lenz114_2])
###
vod_lenz114_1 = vod_lenz114 [vod_lenz114.Title.str.contains('قسمت نوزدهم -شاعر آئینی مهدی زنگنه')]
vod_lenz114_2 = vod_lenz114 [~vod_lenz114.Title.str.contains('قسمت نوزدهم -شاعر آئینی مهدی زنگنه')]
vod_lenz114_1['TitleCleaned1'] = 'شاعر آئینی مهدی زنگنه'
vod_lenz114_1['Season'] = '01'
vod_lenz114_1['Epizode'] = '019'
vod_lenz114 = vod_lenz114_1.append([vod_lenz114_2])
###
vod_lenz114_1 = vod_lenz114 [vod_lenz114.Title.str.contains('قسمت 1-سیزده نه در')]
vod_lenz114_2 = vod_lenz114 [~vod_lenz114.Title.str.contains('قسمت 1-سیزده نه در')]
vod_lenz114_1['TitleCleaned1'] = 'سیزده نه در'
vod_lenz114_1['Season'] = '01'
vod_lenz114_1['Epizode'] = '001'
vod_lenz114 = vod_lenz114_1.append([vod_lenz114_2])
###
vod_lenz114_1 = vod_lenz114 [vod_lenz114.Title.str.contains('قسمت 2-سیزده نه در')]
vod_lenz114_2 = vod_lenz114 [~vod_lenz114.Title.str.contains('قسمت 2-سیزده نه در')]
vod_lenz114_1['TitleCleaned1'] = 'سیزده نه در'
vod_lenz114_1['Season'] = '01'
vod_lenz114_1['Epizode'] = '002'
vod_lenz114 = vod_lenz114_1.append([vod_lenz114_2])
###
vod_lenz114_1 = vod_lenz114 [vod_lenz114.Title.str.contains('قسمت سی ام-شاعر آئینی محمد عظیمی')]
vod_lenz114_2 = vod_lenz114 [~vod_lenz114.Title.str.contains('قسمت سی ام-شاعر آئینی محمد عظیمی')]
vod_lenz114_1['TitleCleaned1'] = 'شاعر آئینی محمد عظیمی'
vod_lenz114_1['Season'] = '01'
vod_lenz114_1['Epizode'] = '030'
vod_lenz114 = vod_lenz114_1.append([vod_lenz114_2])
###
vod_lenz114_1 = vod_lenz114 [vod_lenz114.Title.str.contains('نوروز رنگی فصل 1 پشت صحنه')]
vod_lenz114_2 = vod_lenz114 [~vod_lenz114.Title.str.contains('نوروز رنگی فصل 1 پشت صحنه')]
vod_lenz114_1['TitleCleaned1'] = 'نوروز رنگی'
vod_lenz114_1['Season'] = '01'
vod_lenz114_1['Epizode'] = '000'
vod_lenz114 = vod_lenz114_1.append([vod_lenz114_2])
###
vod_lenz114_1 = vod_lenz114 [vod_lenz114.Title.str.contains('نوروز رنگی فصل 1 پشت صحنه')]
vod_lenz114_2 = vod_lenz114 [~vod_lenz114.Title.str.contains('نوروز رنگی فصل 1 پشت صحنه')]
vod_lenz114_1['TitleCleaned1'] = 'نوروز رنگی'
vod_lenz114_1['Season'] = '01'
vod_lenz114_1['Epizode'] = '000'
vod_lenz114 = vod_lenz114_1.append([vod_lenz114_2])
del vod_lenz114_1
del vod_lenz114_2
###
vod_lenz114 = vod_lenz114 [~vod_lenz114.Title.str.contains('محمد سرور رجایی')]
vod_lenz114 = vod_lenz114 [~vod_lenz114.Title.str.contains('صابر قدیمی')]
vod_lenz114 = vod_lenz114 [~vod_lenz114.Title.str.contains('سید احمد حسینی')]
vod_lenz114 = vod_lenz114 [~vod_lenz114.Title.str.contains('افشین علا')]
vod_lenz114 = vod_lenz114 [~vod_lenz114.Title.str.contains('الهام فلاح')]
vod_lenz114 = vod_lenz114 [~vod_lenz114.Title.str.contains('محمد سرور رجایی')]
vod_lenz114 = vod_lenz114 [~vod_lenz114.Title.str.contains('محمد سرور رجایی')]
vod_lenz115 = vod_lenz114.copy()
################################
vod_lenz115['Epizode'] = vod_lenz115['Epizode'].str.extract('(\d+)', expand=False)
vod_lenz115['Season'] = vod_lenz115['Season'].apply(lambda x: x.zfill(2))
vod_lenz115['Epizode'] = vod_lenz115['Epizode'].apply(lambda x: x.zfill(3))
vod_lenz115['TitleCleaned1'] = vod_lenz115['TitleCleaned1'].str.replace('زیر خاکی','زیرخاکی')
vod_lenz115 = vod_lenz115.reset_index()
del vod_lenz115['index']
################################
vod_lenz115[["TitleCleaned1", "NoData"]]= vod_lenz115["TitleCleaned1"].str.split(":", n = 1, expand = True)
vod_lenz115['NoData'] = vod_lenz115['NoData'].astype(str)
vod_lenz115_1 = vod_lenz115 [vod_lenz115.NoData.str.contains('None')]
vod_lenz115_2 = vod_lenz115 [~vod_lenz115.NoData.str.contains('None')]
vod_lenz115_2['TitleCleaned1'].replace('', 'NoData', inplace=True)
vod_lenz115_2['TitleCleaned1'] = vod_lenz115_2['TitleCleaned1'].fillna('NoData')
vod_lenz115_21 = vod_lenz115_2 [vod_lenz115_2.TitleCleaned1.str.contains('NoData')]
vod_lenz115_22 = vod_lenz115_2 [~vod_lenz115_2.TitleCleaned1.str.contains('NoData')]
vod_lenz115_21['TitleCleaned1'] = vod_lenz115_21['NoData']
vod_lenz115_2 = vod_lenz115_21.append([vod_lenz115_22])
vod_lenz116 = vod_lenz115_1.append([vod_lenz115_2])
del vod_lenz115_1
del vod_lenz115_2
del vod_lenz115_21
del vod_lenz115_22
del vod_lenz116['NoData']
vod_lenz116['TitleCleaned1'] = vod_lenz116['TitleCleaned1'].str.strip()
################################
vod_lenz116[["TitleCleaned1", "NoData"]]= vod_lenz116["TitleCleaned1"].str.split("-", n = 1, expand = True)
vod_lenz116['NoData'] = vod_lenz116['NoData'].astype(str)
vod_lenz116_1 = vod_lenz116 [vod_lenz116.NoData.str.contains('None')]
vod_lenz116_2 = vod_lenz116 [~vod_lenz116.NoData.str.contains('None')]
vod_lenz116_2['TitleCleaned1'].replace('', 'NoData', inplace=True)
vod_lenz116_2['TitleCleaned1'] = vod_lenz116_2['TitleCleaned1'].fillna('NoData')
vod_lenz116_21 = vod_lenz116_2 [vod_lenz116_2.TitleCleaned1.str.contains('NoData')]
vod_lenz116_22 = vod_lenz116_2 [~vod_lenz116_2.TitleCleaned1.str.contains('NoData')]
vod_lenz116_21['TitleCleaned1'] = vod_lenz116_21['NoData']
vod_lenz116_2 = vod_lenz116_21.append([vod_lenz116_22])
vod_lenz117 = vod_lenz116_1.append([vod_lenz116_2])
del vod_lenz116_1
del vod_lenz116_2
del vod_lenz116_21
del vod_lenz116_22
del vod_lenz117['NoData']
vod_lenz117['TitleCleaned1'] = vod_lenz117['TitleCleaned1'].str.strip()
################################
vod_lenz117 = vod_lenz117 [~vod_lenz117.TitleCleaned1.str.contains('_')]
################################
vod_lenz118 = vod_lenz117.copy()
vod_lenz118.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode', 'Month', 'Year', 'DateTime', 'FilmSerial'], keep = 'last', inplace = True)
################################
vod_lenz118 = vod_lenz118.reset_index()
del vod_lenz118['index']
vod_lenz118_1 = vod_lenz118 [vod_lenz118.Season.str.contains('00')]
vod_lenz118_2 = vod_lenz118 [~vod_lenz118.Season.str.contains('00')]
vod_lenz118_film = vod_lenz118_1 [vod_lenz118.Epizode.str.contains('000')]
vod_lenz118_4 = vod_lenz118_1 [~vod_lenz118.Epizode.str.contains('000')]
vod_lenz118_serial = vod_lenz118_2.append([vod_lenz118_4])
vod_lenz118_film['FilmSerial'] = 'فیلم'
vod_lenz118_film['code_FilmSerial'] = '01'
vod_lenz118_serial['FilmSerial'] = 'سریال'
vod_lenz118_serial['code_FilmSerial'] = '02'
vod_lenz119 = vod_lenz118_film.append([vod_lenz118_serial])
del vod_lenz118_1
del vod_lenz118_2
del vod_lenz118_4
################################
vod_lenz119 = vod_lenz119.sort_values(['Season', 'TitleCleaned1'], ascending=[False, False])
vod_lenz119 = vod_lenz119.reset_index()
del vod_lenz119['index']

id_first = 1000001    # write last id_first + 1
vod_lenz119.loc[0, 'id_first'] = id_first
for i in range(1, len(vod_lenz119)):
    print(i)
    if vod_lenz119.loc[i, 'TitleCleaned1'] == vod_lenz119.loc[i-1, 'TitleCleaned1'] and \
       vod_lenz119.loc[i, 'Season'] == vod_lenz119.loc[i-1, 'Season']:
        vod_lenz119.loc[i, 'id_first'] = id_first
    else:
        id_first = id_first + 1
        vod_lenz119.loc[i, 'id_first'] = id_first

vod_lenz119['id_first'] = vod_lenz119['id_first'].astype(int).astype(str) 
   
vod_lenz119['ID_new'] = vod_lenz119['id_first']+vod_lenz119['code_FilmSerial']+vod_lenz119['Season']+vod_lenz119['Epizode']
vod_lenz119['IDS_new'] = vod_lenz119['id_first']+vod_lenz119['Season']

del vod_lenz119['id_first']
del vod_lenz119['code_FilmSerial']
#vod_lenz115.to_excel('vod_lenz115.xlsx', index=False)
#############################################################
#############################################################
vod_aio['TitleCleaned1'] = vod_aio['Title']
vod_aio['TitleCleaned1'] = vod_aio['TitleCleaned1'].astype(str)
vod_aio['Season'] = ''
vod_aio['Epizode'] = ''
vod_aio['FilmSerial'] = ''

vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('دعای')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('ریاضی')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('گزینه دو')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('vod')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('فارسی')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('انگلیسی')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('عربی')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('علوم')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('روضه')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('مداح')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('-مداح')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('متوسطه')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('ابتدایی')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('خلاصه')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('علوم')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('فارسی')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('کاروان ایران')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('زبان انگلیسی')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('زیست شناسی')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('مدال')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('گل های بازی')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('مطالعات اجتماعی')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('لالیگا')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('برترین لحظات')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('برترینهای')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('برد تاریخی')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('برد سخت')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('لیگ جزیره')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('تکنیک های')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('رالی')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('شکست حریف')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('موتور اسپرت')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('برد مقابل')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('صد گل')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('لیگ قهرمانان')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('فوتبال ساحلی')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('فینالیست شدن')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('قرعه کشی')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('مجله آشپزی')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('مجله اشپزی')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('مرور بازیهای')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('مسابقات')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('شکست برابر')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('مقابل حریف')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('کشتی آزاد')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('المپیک')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('کیلوگرم مقابل')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('گل های')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('گلهای')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('ایرانسل')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('گل برتر')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('آنفیلد')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('لیونل مسی')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('کسب طلای')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('جام جهانی')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('شکست حمیده')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('شکست مرتضی')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('شکست میرزازاده')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('صد سیو')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('ضربات آزاد')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('مستند پرسپولیس')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('پیروزی قاطع')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('پیروزی نفسگیر')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('نماینده تکواندو')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('مجله آشپزی')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('مجله آشپزی')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('بارسلونا')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('کولاک بانوان')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('نیمار')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('گل برتر')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('بسکتبال')]
vod_aio = vod_aio [~vod_aio. Title.str.contains('علی پناه')]
vod_aio = vod_aio [~vod_aio. Title.str.contains('رضا هلالی')]
vod_aio = vod_aio [~vod_aio. Title.str.contains('مهدی حسن آبادی')]
vod_aio = vod_aio [~vod_aio. Title.str.contains('حجت الاسلام')]
vod_aio = vod_aio [~vod_aio. Title.str.contains('قاری بین المللی')]
vod_aio = vod_aio [~vod_aio. Title.str.contains('حاج امیر')]
vod_aio = vod_aio [~vod_aio. Title.str.contains('حاج جواد')]
vod_aio = vod_aio [~vod_aio. Title.str.contains('سید جواد')]
vod_aio = vod_aio [~vod_aio. Title.str.contains('کاکایی')]
vod_aio = vod_aio [~vod_aio. Title.str.contains('احمد ابوالقاسمی')]
vod_aio = vod_aio [~vod_aio. Title.str.contains('حاج مهدی')]
vod_aio = vod_aio [~vod_aio. Title.str.contains('حاج احمد')]
vod_aio = vod_aio [~vod_aio. Title.str.contains('کربلایی')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('رونالدو')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('دربی')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('کارت قرمز')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('کار با توپ')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('یک اتفاق تازه')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('شکست نجاتی')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('نماهنگ')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('test')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('نیمه نهایی')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('هایلایت')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('فوتبال')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('گل های')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('فینال')]
vod_aio = vod_aio [~vod_aio. TitleCleaned1.str.contains('لیگ اروپا')]
####### edit of contents #######
vod_aio101 = vod_aio[~vod_aio.TitleCleaned1.str.contains("فصل کشتن")]
vod_aio_2_1 = vod_aio[vod_aio.TitleCleaned1.str.contains("فصل کشتن")]
vod_aio101 = vod_aio101[~vod_aio.TitleCleaned1.str.contains("فصل موج‌سواری")]
vod_aio_2_2 = vod_aio[vod_aio.TitleCleaned1.str.contains("فصل موج‌سواری")]
vod_aio101 = vod_aio101[~vod_aio.TitleCleaned1.str.contains("فصل موج سواری")]
vod_aio_2_3 = vod_aio[vod_aio.TitleCleaned1.str.contains("فصل موج سواری")]
vod_aio101 = vod_aio101[~vod_aio.TitleCleaned1.str.contains("فصل شکار۴: گرخیده")]
vod_aio_2_4 = vod_aio[vod_aio.TitleCleaned1.str.contains("فصل شکار۴: گرخیده")]
vod_aio101 = vod_aio101[~vod_aio.TitleCleaned1.str.contains("فصل نرگس")]
vod_aio_2_5 = vod_aio[vod_aio.TitleCleaned1.str.contains("فصل نرگس")]
vod_aio101 = vod_aio101[~vod_aio.TitleCleaned1.str.contains("لیگ برتر فصل")]
vod_aio_2_6 = vod_aio[vod_aio.TitleCleaned1.str.contains("لیگ برتر فصل")]
vod_aio101 = vod_aio101[~vod_aio.TitleCleaned1.str.contains("در فصل")]
vod_aio_2_7 = vod_aio[vod_aio.TitleCleaned1.str.contains("در فصل")]

vod_aio_2_combine = vod_aio_2_1.append([vod_aio_2_2,vod_aio_2_3,vod_aio_2_4,vod_aio_2_5,vod_aio_2_6,vod_aio_2_7])

del vod_aio_2_1
del vod_aio_2_2
del vod_aio_2_3
del vod_aio_2_4
del vod_aio_2_5
del vod_aio_2_6
del vod_aio_2_7

############################
vod_aio1 = vod_aio101[vod_aio101.TitleCleaned1.str.contains("فصل")]
vod_aio2 = vod_aio101[~vod_aio101.TitleCleaned1.str.contains("فصل")]
vod_aio1[["TitleCleaned1", "Season"]]= vod_aio1["TitleCleaned1"].str.split("فصل", n = 1, expand = True)

vod_aio11 = vod_aio1[vod_aio1.Season.str.contains("قسمت")]
vod_aio12 = vod_aio1[~vod_aio1.Season.str.contains("قسمت")]
vod_aio11[["Season", "Epizode"]]= vod_aio11["Season"].str.split("قسمت", n = 1, expand = True)

vod_aio102 = vod_aio11.append([vod_aio12, vod_aio2, vod_aio_2_combine])
del vod_aio1
del vod_aio2
del vod_aio11
del vod_aio12
del vod_aio_2_combine
del vod_aio101

vod_aio102['Season'] = vod_aio102['Season'].str.strip()
vod_aio102['Epizode'] = vod_aio102['Epizode'].str.strip()
vod_aio102['TitleCleaned1'] = vod_aio102['TitleCleaned1'].str.strip()

#########
vod_aio1 = vod_aio102[vod_aio102.TitleCleaned1.str.contains("قسمت")]
vod_aio2 = vod_aio102[~vod_aio102.TitleCleaned1.str.contains("قسمت")]
vod_aio1[["TitleCleaned1", "Epizode"]]= vod_aio1["TitleCleaned1"].str.split("قسمت", n = 1, expand = True)
vod_aio1['Season'].replace('', '01', inplace=True)
vod_aio103 = vod_aio1.append([vod_aio2])
del vod_aio1
del vod_aio2
vod_aio103['Season'] = vod_aio103['Season'].str.strip()
vod_aio103['Epizode'] = vod_aio103['Epizode'].str.strip()
vod_aio103['TitleCleaned1'] = vod_aio103['TitleCleaned1'].str.strip()

#########
vod_aio103['Epizode'] = vod_aio103['Epizode'].str.replace('اول', '01')
vod_aio103['Epizode'] = vod_aio103['Epizode'].str.replace('دوم', '02')
vod_aio103['Epizode'] = vod_aio103['Epizode'].str.replace('سوم', '03')
vod_aio103['Epizode'] = vod_aio103['Epizode'].str.replace('چهارم', '04')
vod_aio103['Epizode'] = vod_aio103['Epizode'].str.replace('پنجم', '05')
vod_aio103['Epizode'] = vod_aio103['Epizode'].str.replace('ششم', '06')
vod_aio103['Epizode'] = vod_aio103['Epizode'].str.replace('هفتم', '07')
vod_aio103['Epizode'] = vod_aio103['Epizode'].str.replace('هشتم', '08')
vod_aio103['Epizode'] = vod_aio103['Epizode'].str.replace('نهم', '09')
vod_aio103['Epizode'] = vod_aio103['Epizode'].str.replace('دهم', '10')
vod_aio103['Epizode'] = vod_aio103['Epizode'].str.replace('یازدهم', '11')
vod_aio103['Epizode'] = vod_aio103['Epizode'].str.replace('دوازدهم', '12')
vod_aio103['Epizode'] = vod_aio103['Epizode'].str.replace('سیزدهم', '13')
vod_aio103['Epizode'] = vod_aio103['Epizode'].str.replace('چهاردهم', '14')
vod_aio103['Epizode'] = vod_aio103['Epizode'].str.replace('پانزدهم', '15')
vod_aio103['Epizode'] = vod_aio103['Epizode'].str.replace('شانزدهم', '16')
vod_aio103['Epizode'] = vod_aio103['Epizode'].str.replace('هفدهم', '17')
vod_aio103['Epizode'] = vod_aio103['Epizode'].str.replace('هجدهم', '18')
vod_aio103['Epizode'] = vod_aio103['Epizode'].str.replace('نوزدهم', '19')
vod_aio103['Epizode'] = vod_aio103['Epizode'].str.replace('بیستم', '20')

vod_aio103['Epizode'] = vod_aio103['Epizode'].str.extract('(\d+)', expand=False)
vod_aio103['Epizode'] = vod_aio103['Epizode'].astype(str)
vod_aio103['Epizode'] = vod_aio103['Epizode'].str.replace('nan', '')
vod_aio103['Season'] = vod_aio103['Season'].apply(lambda x: x.zfill(2))
vod_aio103['Epizode'] = vod_aio103['Epizode'].apply(lambda x: x.zfill(3))

vod_aio103['TitleCleaned1'] = vod_aio103['TitleCleaned1'].str.replace('0', '۰')
vod_aio103['TitleCleaned1'] = vod_aio103['TitleCleaned1'].str.replace('1', '۱')
vod_aio103['TitleCleaned1'] = vod_aio103['TitleCleaned1'].str.replace('2', '۲')
vod_aio103['TitleCleaned1'] = vod_aio103['TitleCleaned1'].str.replace('3', '۳')
vod_aio103['TitleCleaned1'] = vod_aio103['TitleCleaned1'].str.replace('4', '۴')
vod_aio103['TitleCleaned1'] = vod_aio103['TitleCleaned1'].str.replace('5', '۵')
vod_aio103['TitleCleaned1'] = vod_aio103['TitleCleaned1'].str.replace('6', '۶')
vod_aio103['TitleCleaned1'] = vod_aio103['TitleCleaned1'].str.replace('7', '۷')
vod_aio103['TitleCleaned1'] = vod_aio103['TitleCleaned1'].str.replace('8', '۸')
vod_aio103['TitleCleaned1'] = vod_aio103['TitleCleaned1'].str.replace('9', '۹')
vod_aio103['TitleCleaned1'] = vod_aio103['TitleCleaned1'].str.replace('٠', '۰')
vod_aio103['TitleCleaned1'] = vod_aio103['TitleCleaned1'].str.replace('١', '۱')
vod_aio103['TitleCleaned1'] = vod_aio103['TitleCleaned1'].str.replace('٢', '۲')
vod_aio103['TitleCleaned1'] = vod_aio103['TitleCleaned1'].str.replace('٣', '۳')
vod_aio103['TitleCleaned1'] = vod_aio103['TitleCleaned1'].str.replace('٤', '۴')
vod_aio103['TitleCleaned1'] = vod_aio103['TitleCleaned1'].str.replace('٥', '۵')
vod_aio103['TitleCleaned1'] = vod_aio103['TitleCleaned1'].str.replace('٦', '۶')
vod_aio103['TitleCleaned1'] = vod_aio103['TitleCleaned1'].str.replace('٧', '۷')
vod_aio103['TitleCleaned1'] = vod_aio103['TitleCleaned1'].str.replace('٨', '۸')
vod_aio103['TitleCleaned1'] = vod_aio103['TitleCleaned1'].str.replace('٩', '۹')
vod_aio103['TitleCleaned1'] = vod_aio103['TitleCleaned1'].str.replace('ي', 'ی')
vod_aio103['TitleCleaned1'] = vod_aio103['TitleCleaned1'].str.replace('ؤ','و')
vod_aio103['TitleCleaned1'] = vod_aio103['TitleCleaned1'].str.replace('ك','ک')
vod_aio103['TitleCleaned1'] = vod_aio103['TitleCleaned1'].str.strip() 

vod_aio103['Epizode'] = vod_aio103['Epizode'].str.replace('۰', '0')
vod_aio103['Epizode'] = vod_aio103['Epizode'].str.replace('۱', '1')
vod_aio103['Epizode'] = vod_aio103['Epizode'].str.replace('۲', '2')
vod_aio103['Epizode'] = vod_aio103['Epizode'].str.replace('۳', '3')
vod_aio103['Epizode'] = vod_aio103['Epizode'].str.replace('۴', '4')
vod_aio103['Epizode'] = vod_aio103['Epizode'].str.replace('۵', '5')
vod_aio103['Epizode'] = vod_aio103['Epizode'].str.replace('۶', '6')
vod_aio103['Epizode'] = vod_aio103['Epizode'].str.replace('۷', '7')
vod_aio103['Epizode'] = vod_aio103['Epizode'].str.replace('۸', '8')
vod_aio103['Epizode'] = vod_aio103['Epizode'].str.replace('۹', '9')
vod_aio103['Epizode'] = vod_aio103['Epizode'].str.replace('٠', '0')
vod_aio103['Epizode'] = vod_aio103['Epizode'].str.replace('١', '1')
vod_aio103['Epizode'] = vod_aio103['Epizode'].str.replace('٢', '2')
vod_aio103['Epizode'] = vod_aio103['Epizode'].str.replace('٣', '3')
vod_aio103['Epizode'] = vod_aio103['Epizode'].str.replace('٤', '4')
vod_aio103['Epizode'] = vod_aio103['Epizode'].str.replace('٥', '5')
vod_aio103['Epizode'] = vod_aio103['Epizode'].str.replace('٦', '6')
vod_aio103['Epizode'] = vod_aio103['Epizode'].str.replace('٧', '7')
vod_aio103['Epizode'] = vod_aio103['Epizode'].str.replace('٨', '8')
vod_aio103['Epizode'] = vod_aio103['Epizode'].str.replace('٩', '9')

vod_aio103['Season'] = vod_aio103['Season'].str.replace('۰', '0')
vod_aio103['Season'] = vod_aio103['Season'].str.replace('۱', '1')
vod_aio103['Season'] = vod_aio103['Season'].str.replace('۲', '2')
vod_aio103['Season'] = vod_aio103['Season'].str.replace('۳', '3')
vod_aio103['Season'] = vod_aio103['Season'].str.replace('۴', '4')
vod_aio103['Season'] = vod_aio103['Season'].str.replace('۵', '5')
vod_aio103['Season'] = vod_aio103['Season'].str.replace('۶', '6')
vod_aio103['Season'] = vod_aio103['Season'].str.replace('۷', '7')
vod_aio103['Season'] = vod_aio103['Season'].str.replace('۸', '8')
vod_aio103['Season'] = vod_aio103['Season'].str.replace('۹', '9')
vod_aio103['Season'] = vod_aio103['Season'].str.replace('٠', '0')
vod_aio103['Season'] = vod_aio103['Season'].str.replace('١', '1')
vod_aio103['Season'] = vod_aio103['Season'].str.replace('٢', '2')
vod_aio103['Season'] = vod_aio103['Season'].str.replace('٣', '3')
vod_aio103['Season'] = vod_aio103['Season'].str.replace('٤', '4')
vod_aio103['Season'] = vod_aio103['Season'].str.replace('٥', '5')
vod_aio103['Season'] = vod_aio103['Season'].str.replace('٦', '6')
vod_aio103['Season'] = vod_aio103['Season'].str.replace('٧', '7')
vod_aio103['Season'] = vod_aio103['Season'].str.replace('٨', '8')
vod_aio103['Season'] = vod_aio103['Season'].str.replace('٩', '9')
##########
vod_aio103[["TitleCleaned1", "NoData"]]= vod_aio103["TitleCleaned1"].str.split(":", n = 1, expand = True)
vod_aio103['NoData'] = vod_aio103['NoData'].astype(str)
vod_aio103_1 = vod_aio103 [vod_aio103.NoData.str.contains('None')]
vod_aio103_2 = vod_aio103 [~vod_aio103.NoData.str.contains('None')]
vod_aio103_2['TitleCleaned1'].replace('', 'NoData', inplace=True)
vod_aio103_2['TitleCleaned1'] = vod_aio103_2['TitleCleaned1'].fillna('NoData')
vod_aio103_21 = vod_aio103_2 [vod_aio103_2.TitleCleaned1.str.contains('NoData')]
vod_aio103_22 = vod_aio103_2 [~vod_aio103_2.TitleCleaned1.str.contains('NoData')]
vod_aio103_21['TitleCleaned1'] = vod_aio103_21['NoData']
vod_aio103_2 = vod_aio103_21.append([vod_aio103_22])
vod_aio104 = vod_aio103_1.append([vod_aio103_2])
del vod_aio103_1
del vod_aio103_2
del vod_aio103_21
del vod_aio103_22
del vod_aio104['NoData']
vod_aio104['TitleCleaned1'] = vod_aio104['TitleCleaned1'].str.strip()

################################
total_vod_1399_Ghadimi_v2 = pd.read_excel(r'E:\python codes\VOD\kh_ghadimi\total_vod_1399_Ghadimi_v2.xlsx')
total_vod_1399_Ghadimi_v2=total_vod_1399_Ghadimi_v2.rename(columns={"شماره فصل":"season"})
total_vod_1399_Ghadimi_v2 = total_vod_1399_Ghadimi_v2.fillna("NO")
total_vod_1399_Ghadimi_v2['season'] = total_vod_1399_Ghadimi_v2['season'].astype(str)
total_vod_1399_Ghadimi_v2 = total_vod_1399_Ghadimi_v2[~total_vod_1399_Ghadimi_v2.season.str.contains("NO")]
total_vod_1399_Ghadimi_v2['season'] = total_vod_1399_Ghadimi_v2['season'].astype(str).replace('\.0', '', regex=True)
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
del total_vod_1399_Ghadimi_v2['فصل']

vod_aio104.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Month', 'Year'], keep = 'last', inplace = True)
vod_aio105 = pd.merge(vod_aio104, total_vod_1399_Ghadimi_v2, on = ['TitleCleaned1'])
vod_aio105['Season'] = vod_aio105['season']
vod_aio105['TitleCleaned1'] = vod_aio105['title_first']
vod_aio105['Season'] = vod_aio105['season']
del vod_aio105['season']
del vod_aio105['title_first']
vod_aio106 = vod_aio104.append([vod_aio105])
vod_aio106.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Month', 'Year'], keep = 'last', inplace = True)

vod_aio106['Epizode'] = vod_aio106['Epizode'].astype(str)
vod_aio106['Season'] = vod_aio106['Season'].apply(lambda x: x.zfill(2))
vod_aio106['Epizode'] = vod_aio106['Epizode'].apply(lambda x: x.zfill(3))

################################
vod_aio106 = vod_aio106.reset_index()
del vod_aio106['index']
vod_aio106_1 = vod_aio106 [vod_aio106.Season.str.contains('00')]
vod_aio106_2 = vod_aio106 [~vod_aio106.Season.str.contains('00')]
vod_aio106_film = vod_aio106_1 [vod_aio106.Epizode.str.contains('000')]
vod_aio106_4 = vod_aio106_1 [~vod_aio106.Epizode.str.contains('000')]
vod_aio106_serial = vod_aio106_2.append([vod_aio106_4])
vod_aio106_film['FilmSerial'] = 'فیلم'
vod_aio106_film['code_FilmSerial'] = '01'
vod_aio106_serial['FilmSerial'] = 'سریال'
vod_aio106_serial['code_FilmSerial'] = '02'
vod_aio107 = vod_aio106_film.append([vod_aio106_serial])
del vod_aio106_1
del vod_aio106_2
del vod_aio106_4
################################
vod_aio108 = vod_aio107.copy()
vod_aio108.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode', 'Month', 'Year', 'FilmSerial', 'code_FilmSerial'], keep = 'last', inplace = True)

db1_data = pd.DataFrame()
db1_data['TitleCleaned1'] = vod_lenz119['TitleCleaned1']
db1_data['Season'] = vod_lenz119['Season']
db1_data['Epizode'] = vod_lenz119['Epizode']
db1_data['ID_new'] = vod_lenz119['ID_new']
db1_data['IDS_new'] = vod_lenz119['IDS_new']

db1_data.drop_duplicates(subset =['ID_new'], keep = 'last', inplace = True)
vod_aio_db1_merge = pd.merge(vod_aio108, db1_data, on = ['TitleCleaned1', 'Season', 'Epizode'])
vod_aio_db1_merge.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode', 'Month', 'Year'], keep = 'last', inplace = True)
vod_aio108['ID_new'] = ''
vod_aio108['IDS_new'] = ''

vod_append = vod_aio_db1_merge.append([vod_aio108])

vod_append = vod_append.reset_index()
del vod_append['index']
vod_append_dup = vod_append.copy()
vod_append_dup.drop_duplicates(subset =['Season', 'TitleCleaned1', 'Epizode'], keep = 'last', inplace = True)
vod_append_dup = vod_append_dup.reset_index()
del vod_append_dup['index']

vod_append_new = pd.DataFrame()
for i in range(0, len(vod_append_dup)):   #len(vod_append_dup)
    print(i)
    TT = vod_append_dup.loc[i, 'TitleCleaned1']
    SS = vod_append_dup.loc[i, 'Season']
    EE = vod_append_dup.loc[i, 'Epizode']
    title = vod_append.query("TitleCleaned1 == @TT")
    title = title.query("Season == @SS")
    title = title.query("Epizode == @EE")
    title = title.reset_index()
    del title['index']
    k = 0
    for j in range(0, len(title)):
        if '10' not in title.loc[j, 'IDS_new']:
            k = k + 1
    if j + 1 == k:
        vod_append_new = title.append([vod_append_new])

#vod_append.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode', 'Month', 'Year'], keep = False, inplace = True)
vod_aio_step1 = vod_aio_db1_merge.copy()

################################
del vod_append_new['ID_new']
del vod_append_new['IDS_new']
del db1_data['Epizode']
vod_append_new = vod_append_new.reset_index()
del vod_append_new['index']
db1_data.drop_duplicates(subset =['IDS_new'], keep = 'last', inplace = True)
vod_aio_db1_merge2 = pd.merge(vod_append_new, db1_data, on = ['TitleCleaned1', 'Season'])

vod_aio_db1_merge2.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode', 'Month', 'Year'], keep = 'last', inplace = True)
vod_append_new['ID_new'] = ''
vod_append_new['IDS_new'] = ''

vod_append2 = vod_aio_db1_merge2.append([vod_append_new])

vod_append2 = vod_append2.reset_index()
del vod_append2['index']
vod_append2_dup = vod_append2.copy()
vod_append2_dup.drop_duplicates(subset =['Season', 'TitleCleaned1'], keep = 'last', inplace = True)
vod_append2_dup = vod_append2_dup.reset_index()
del vod_append2_dup['index']

vod_append2_new = pd.DataFrame()
for i in range(0, len(vod_append2_dup)):   #len(vod_append2_dup)
    print(i)
    TT = vod_append2_dup.loc[i, 'TitleCleaned1']
    SS = vod_append2_dup.loc[i, 'Season']
    title = vod_append2.query("TitleCleaned1 == @TT")
    title = title.query("Season == @SS")
    title = title.reset_index()
    del title['index']
    k = 0
    for j in range(0, len(title)):
        if '10' not in title.loc[j, 'IDS_new']:
            k = k + 1
    if j + 1 == k:
        vod_append2_new = title.append([vod_append2_new])

vod_aio_step2 = vod_aio_db1_merge2.copy()
vod_aio_step2['ID_new'] = vod_aio_step2['ID_new'].str[0:11] + vod_aio_step2['Epizode']

################################
vod_append2_new = vod_append2_new.sort_values(['Season', 'TitleCleaned1'], ascending=[False, False])
vod_append2_new = vod_append2_new.reset_index()
del vod_append2_new['index']

id_first1 = max(vod_lenz119['ID_new'])
id_first2 = id_first1[0:7]
id_first = int(id_first2)+1    # write last id_first + 1
vod_append2_new.loc[0, 'id_first'] = id_first
for i in range(1, len(vod_append2_new)):
    print(i)
    if vod_append2_new.loc[i, 'TitleCleaned1'] == vod_append2_new.loc[i-1, 'TitleCleaned1'] and \
       vod_append2_new.loc[i, 'Season'] == vod_append2_new.loc[i-1, 'Season']:
        vod_append2_new.loc[i, 'id_first'] = id_first
    else:
        id_first = id_first + 1
        vod_append2_new.loc[i, 'id_first'] = id_first

vod_append2_new['id_first'] = vod_append2_new['id_first'].astype(int).astype(str) 
   
vod_append2_new['ID_new'] = vod_append2_new['id_first']+vod_append2_new['code_FilmSerial']+vod_append2_new['Season']+vod_append2_new['Epizode']
vod_append2_new['IDS_new'] = vod_append2_new['id_first']+vod_append2_new['Season']

del vod_append2_new['id_first']
################################
vod_aio109 = vod_aio_step1.append([vod_aio_step2, vod_append2_new])
del vod_aio109['code_FilmSerial']
vod_aio109 = vod_aio109.reset_index()
del vod_aio109['index']

#############################################################
#############################################################
#############################################################
vod_tva['TitleCleaned1'] = vod_tva['Title']
vod_tva['TitleCleaned1'] = vod_tva['TitleCleaned1'].astype(str)
vod_tva['Season'] = ''
vod_tva['Epizode'] = ''
vod_tva['FilmSerial'] = ''

vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('دعای')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('ریاضی')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('گزینه دو')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('vod')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('فارسی')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('انگلیسی')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('عربی')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('علوم')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('روضه')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('مداح')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('-مداح')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('متوسطه')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('ابتدایی')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('خلاصه')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('علوم')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('فارسی')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('کاروان ایران')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('زبان انگلیسی')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('زیست شناسی')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('مدال')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('گل های بازی')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('مطالعات اجتماعی')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('لالیگا')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('برترین لحظات')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('برترینهای')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('برد تاریخی')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('برد سخت')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('لیگ جزیره')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('تکنیک های')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('رالی')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('شکست حریف')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('موتور اسپرت')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('برد مقابل')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('صد گل')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('لیگ قهرمانان')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('فوتبال ساحلی')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('فینالیست شدن')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('قرعه کشی')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('مجله آشپزی')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('مجله اشپزی')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('مرور بازیهای')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('مسابقات')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('شکست برابر')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('مقابل حریف')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('کشتی آزاد')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('المپیک')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('کیلوگرم مقابل')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('گل های')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('گلهای')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('ایرانسل')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('گل برتر')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('آنفیلد')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('لیونل مسی')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('کسب طلای')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('جام جهانی')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('شکست حمیده')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('شکست مرتضی')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('شکست میرزازاده')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('صد سیو')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('ضربات آزاد')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('مستند پرسپولیس')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('پیروزی قاطع')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('پیروزی نفسگیر')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('نماینده تکواندو')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('مجله آشپزی')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('مجله آشپزی')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('بارسلونا')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('کولاک بانوان')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('نیمار')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('گل برتر')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('بسکتبال')]
vod_tva = vod_tva [~vod_tva. Title.str.contains('علی پناه')]
vod_tva = vod_tva [~vod_tva. Title.str.contains('رضا هلالی')]
vod_tva = vod_tva [~vod_tva. Title.str.contains('مهدی حسن آبادی')]
vod_tva = vod_tva [~vod_tva. Title.str.contains('حجت الاسلام')]
vod_tva = vod_tva [~vod_tva. Title.str.contains('قاری بین المللی')]
vod_tva = vod_tva [~vod_tva. Title.str.contains('حاج امیر')]
vod_tva = vod_tva [~vod_tva. Title.str.contains('حاج جواد')]
vod_tva = vod_tva [~vod_tva. Title.str.contains('سید جواد')]
vod_tva = vod_tva [~vod_tva. Title.str.contains('کاکایی')]
vod_tva = vod_tva [~vod_tva. Title.str.contains('احمد ابوالقاسمی')]
vod_tva = vod_tva [~vod_tva. Title.str.contains('حاج مهدی')]
vod_tva = vod_tva [~vod_tva. Title.str.contains('حاج احمد')]
vod_tva = vod_tva [~vod_tva. Title.str.contains('کربلایی')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('رونالدو')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('دربی')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('کارت قرمز')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('کار با توپ')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('یک اتفاق تازه')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('شکست نجاتی')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('نماهنگ')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('test')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('نیمه نهایی')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('هایلایت')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('فوتبال')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('گل های')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('فینال')]
vod_tva = vod_tva [~vod_tva. TitleCleaned1.str.contains('لیگ اروپا')]

#########
vod_tva['TitleCleaned1'] = vod_tva['TitleCleaned1'].str.strip()
vod_tva['Season'] = vod_tva['Season'].str.strip()
vod_tva['Epizode'] = vod_tva['Epizode'].str.strip()

vod_tva101 = vod_tva[~vod_tva.TitleCleaned1.str.contains("فصل شکار")]
vod_tva201 = vod_tva[vod_tva.TitleCleaned1.str.contains("فصل شکار")]
vod_tva102 = vod_tva101[~vod_tva101.TitleCleaned1.str.contains("فصل قتل ها")]
vod_tva202 = vod_tva101[vod_tva101.TitleCleaned1.str.contains("فصل قتل ها")]
vod_tva303 = vod_tva102[~vod_tva102.TitleCleaned1.str.contains("فصل موج سواری")]
vod_tva203 = vod_tva102[vod_tva102.TitleCleaned1.str.contains("فصل موج سواری")]
vod_tva200 = vod_tva201.append([vod_tva202, vod_tva203])
del vod_tva201
del vod_tva202
del vod_tva203
del vod_tva101
del vod_tva102

#########
vod_tva1 = vod_tva303 [vod_tva303.TitleCleaned1.str.contains("\((.*?)\)")]
vod_tva2 = vod_tva303 [~vod_tva303.TitleCleaned1.str.contains("\((.*?)\)")]

vod_tva1[['TitleCleaned1', 'Season']] = vod_tva1.TitleCleaned1.str.strip().str.rsplit('(S', 1, expand=True)
vod_tva1[['Season', 'Epizode']] = vod_tva1.Season.str.strip().str.rsplit('E', 1, expand=True)

#########
vod_tva2['TitleCleaned1'] = vod_tva2['TitleCleaned1'].str.replace('اول', '01')
vod_tva2['TitleCleaned1'] = vod_tva2['TitleCleaned1'].str.replace('دوم', '02')
vod_tva2['TitleCleaned1'] = vod_tva2['TitleCleaned1'].str.replace('سوم', '03')
vod_tva2['TitleCleaned1'] = vod_tva2['TitleCleaned1'].str.replace('چهارم', '04')
vod_tva2['TitleCleaned1'] = vod_tva2['TitleCleaned1'].str.replace('پنجم', '05')
vod_tva2['TitleCleaned1'] = vod_tva2['TitleCleaned1'].str.replace('ششم', '06')
vod_tva2['TitleCleaned1'] = vod_tva2['TitleCleaned1'].str.replace('هفتم', '07')
vod_tva2['TitleCleaned1'] = vod_tva2['TitleCleaned1'].str.replace('هشتم', '08')
vod_tva2['TitleCleaned1'] = vod_tva2['TitleCleaned1'].str.replace('نهم', '09')
vod_tva2['TitleCleaned1'] = vod_tva2['TitleCleaned1'].str.replace('دهم', '10')
vod_tva2['TitleCleaned1'] = vod_tva2['TitleCleaned1'].str.replace('یازدهم', '11')
vod_tva2['TitleCleaned1'] = vod_tva2['TitleCleaned1'].str.replace('دوازدهم', '12')
vod_tva2['TitleCleaned1'] = vod_tva2['TitleCleaned1'].str.replace('سیزدهم', '13')
vod_tva2['TitleCleaned1'] = vod_tva2['TitleCleaned1'].str.replace('چهاردهم', '14')
vod_tva2['TitleCleaned1'] = vod_tva2['TitleCleaned1'].str.replace('پانزدهم', '15')
vod_tva2['TitleCleaned1'] = vod_tva2['TitleCleaned1'].str.replace('شانزدهم', '16')
vod_tva2['TitleCleaned1'] = vod_tva2['TitleCleaned1'].str.replace('هفدهم', '17')
vod_tva2['TitleCleaned1'] = vod_tva2['TitleCleaned1'].str.replace('هجدهم', '18')
vod_tva2['TitleCleaned1'] = vod_tva2['TitleCleaned1'].str.replace('نوزدهم', '19')
vod_tva2['TitleCleaned1'] = vod_tva2['TitleCleaned1'].str.replace('بیستم', '20')

vod_tva2['TitleCleaned1'] = vod_tva2['TitleCleaned1'].str.strip()
vod_tva2_1 = vod_tva2[vod_tva2.TitleCleaned1.str.contains("فصل")]
vod_tva2_2 = vod_tva2[~vod_tva2.TitleCleaned1.str.contains("فصل")]

vod_tva2_1[['TitleCleaned1', 'Season']] = vod_tva2_1.TitleCleaned1.str.strip().str.rsplit('فصل', 1, expand=True)
vod_tva2_1[['Season', 'Epizode']] = vod_tva2_1.Season.str.strip().str.rsplit('قسمت', 1, expand=True)
#vod_tva104 = vod_tva1.append([vod_tva2, vod_tva200])
#########
vod_tva2_1['Epizode'] = vod_tva2_1['Epizode'].str.strip()

vod_tva2_1_1 = vod_tva2_1[vod_tva2_1.Epizode.str.contains(":")]
vod_tva2_1_2 = vod_tva2_1[~vod_tva2_1.Epizode.str.contains(":")]
vod_tva2_1_1[["Epizode", "NoData"]]= vod_tva2_1_1["Epizode"].str.split(":", n = 1, expand = True)
del vod_tva2_1_1['NoData']
vod_tva102 = vod_tva2_1_1.append([vod_tva2_1_2])
del vod_tva2_1_1
del vod_tva2_1_2
vod_tva102['Season'] = vod_tva102['Season'].str.strip()
vod_tva102['Epizode'] = vod_tva102['Epizode'].str.strip()
vod_tva102['TitleCleaned1'] = vod_tva102['TitleCleaned1'].str.strip()

#########
vod_tva2_1_1 = vod_tva102[vod_tva102.Epizode.str.contains("&")]
vod_tva2_1_2 = vod_tva102[~vod_tva102.Epizode.str.contains("&")]
vod_tva2_1_1[["Epizode", "NoData"]]= vod_tva2_1_1["Epizode"].str.split("&", n = 1, expand = True)
del vod_tva2_1_1['NoData']
vod_tva103 = vod_tva2_1_1.append([vod_tva2_1_2])
del vod_tva2_1_1
del vod_tva2_1_2
vod_tva103['Season'] = vod_tva103['Season'].str.strip()
vod_tva103['Epizode'] = vod_tva103['Epizode'].str.strip()
vod_tva103['TitleCleaned1'] = vod_tva103['TitleCleaned1'].str.strip()

#########
vod_tva2_1_1 = vod_tva103[vod_tva103.Epizode.str.contains("و")]
vod_tva2_1_2 = vod_tva103[~vod_tva103.Epizode.str.contains("و")]
vod_tva2_1_1[["Epizode", "NoData"]]= vod_tva2_1_1["Epizode"].str.split("و", n = 1, expand = True)
del vod_tva2_1_1['NoData']
vod_tva104 = vod_tva2_1_1.append([vod_tva2_1_2])
del vod_tva2_1_1
del vod_tva2_1_2
vod_tva104['Season'] = vod_tva104['Season'].str.strip()
vod_tva104['Epizode'] = vod_tva104['Epizode'].str.strip()
vod_tva104['TitleCleaned1'] = vod_tva104['TitleCleaned1'].str.strip()

#########
vod_tva105 = vod_tva200.append([vod_tva104,vod_tva2_2, vod_tva1])
del vod_tva104
del vod_tva2_1
del vod_tva2_2
del vod_tva200
del vod_tva2

#########
vod_tva105['Season'] = vod_tva105['Season'].str.replace('S', '')
vod_tva105['Season'] = vod_tva105['Season'].str.replace(')', '')
vod_tva105['Season'] = vod_tva105['Season'].str.replace('(', '')
vod_tva105['Epizode'] = vod_tva105['Epizode'].str.replace(')', '')
vod_tva105['Epizode'] = vod_tva105['Epizode'].str.replace('(', '')
vod_tva105['Season'] = vod_tva105['Season'].str.replace('(مینی سریال)', '')
vod_tva105['Season'] = vod_tva105['Season'].str.replace('(بروسلی)', '')
vod_tva105['Season'] = vod_tva105['Season'].str.replace('(دوبله)', '')
vod_tva105['Season'] = vod_tva105['Season'].str.replace('(انگلیس)', '')

vod_tva105['TitleCleaned1'] = vod_tva105['TitleCleaned1'].str.strip()
vod_tva105['Season'] = vod_tva105['Season'].str.strip()
vod_tva105['Epizode'] = vod_tva105['Epizode'].str.strip()

#########
vod_tva105['Epizode'] = vod_tva105['Epizode'].fillna('')
vod_tva105['Season'] = vod_tva105['Season'].fillna('')
vod_tva105['Epizode'] = vod_tva105['Epizode'].str.replace('None', '')
vod_tva105['Season'] = vod_tva105['Season'].str.replace('None', '')
vod_tva105['Epizode'] = vod_tva105['Epizode'].astype(str)
vod_tva105['Epizode'] = vod_tva105['Epizode'].str.replace('nan', '')
vod_tva105['Season'] = vod_tva105['Season'].astype(str)
vod_tva105['Season'] = vod_tva105['Season'].str.replace('nan', '')
vod_tva105['Season'] = vod_tva105['Season'].apply(lambda x: x.zfill(2))
vod_tva105['Epizode'] = vod_tva105['Epizode'].apply(lambda x: x.zfill(3))

vod_tva105['TitleCleaned1'] = vod_tva105['TitleCleaned1'].str.replace('0', '۰')
vod_tva105['TitleCleaned1'] = vod_tva105['TitleCleaned1'].str.replace('1', '۱')
vod_tva105['TitleCleaned1'] = vod_tva105['TitleCleaned1'].str.replace('2', '۲')
vod_tva105['TitleCleaned1'] = vod_tva105['TitleCleaned1'].str.replace('3', '۳')
vod_tva105['TitleCleaned1'] = vod_tva105['TitleCleaned1'].str.replace('4', '۴')
vod_tva105['TitleCleaned1'] = vod_tva105['TitleCleaned1'].str.replace('5', '۵')
vod_tva105['TitleCleaned1'] = vod_tva105['TitleCleaned1'].str.replace('6', '۶')
vod_tva105['TitleCleaned1'] = vod_tva105['TitleCleaned1'].str.replace('7', '۷')
vod_tva105['TitleCleaned1'] = vod_tva105['TitleCleaned1'].str.replace('8', '۸')
vod_tva105['TitleCleaned1'] = vod_tva105['TitleCleaned1'].str.replace('9', '۹')
vod_tva105['TitleCleaned1'] = vod_tva105['TitleCleaned1'].str.replace('٠', '۰')
vod_tva105['TitleCleaned1'] = vod_tva105['TitleCleaned1'].str.replace('١', '۱')
vod_tva105['TitleCleaned1'] = vod_tva105['TitleCleaned1'].str.replace('٢', '۲')
vod_tva105['TitleCleaned1'] = vod_tva105['TitleCleaned1'].str.replace('٣', '۳')
vod_tva105['TitleCleaned1'] = vod_tva105['TitleCleaned1'].str.replace('٤', '۴')
vod_tva105['TitleCleaned1'] = vod_tva105['TitleCleaned1'].str.replace('٥', '۵')
vod_tva105['TitleCleaned1'] = vod_tva105['TitleCleaned1'].str.replace('٦', '۶')
vod_tva105['TitleCleaned1'] = vod_tva105['TitleCleaned1'].str.replace('٧', '۷')
vod_tva105['TitleCleaned1'] = vod_tva105['TitleCleaned1'].str.replace('٨', '۸')
vod_tva105['TitleCleaned1'] = vod_tva105['TitleCleaned1'].str.replace('٩', '۹')
vod_tva105['TitleCleaned1'] = vod_tva105['TitleCleaned1'].str.replace('ي', 'ی')
vod_tva105['TitleCleaned1'] = vod_tva105['TitleCleaned1'].str.replace('ؤ','و')
vod_tva105['TitleCleaned1'] = vod_tva105['TitleCleaned1'].str.replace('ك','ک')
vod_tva105['TitleCleaned1'] = vod_tva105['TitleCleaned1'].str.strip() 

vod_tva105['Epizode'] = vod_tva105['Epizode'].str.replace('۰', '0')
vod_tva105['Epizode'] = vod_tva105['Epizode'].str.replace('۱', '1')
vod_tva105['Epizode'] = vod_tva105['Epizode'].str.replace('۲', '2')
vod_tva105['Epizode'] = vod_tva105['Epizode'].str.replace('۳', '3')
vod_tva105['Epizode'] = vod_tva105['Epizode'].str.replace('۴', '4')
vod_tva105['Epizode'] = vod_tva105['Epizode'].str.replace('۵', '5')
vod_tva105['Epizode'] = vod_tva105['Epizode'].str.replace('۶', '6')
vod_tva105['Epizode'] = vod_tva105['Epizode'].str.replace('۷', '7')
vod_tva105['Epizode'] = vod_tva105['Epizode'].str.replace('۸', '8')
vod_tva105['Epizode'] = vod_tva105['Epizode'].str.replace('۹', '9')
vod_tva105['Epizode'] = vod_tva105['Epizode'].str.replace('٠', '0')
vod_tva105['Epizode'] = vod_tva105['Epizode'].str.replace('١', '1')
vod_tva105['Epizode'] = vod_tva105['Epizode'].str.replace('٢', '2')
vod_tva105['Epizode'] = vod_tva105['Epizode'].str.replace('٣', '3')
vod_tva105['Epizode'] = vod_tva105['Epizode'].str.replace('٤', '4')
vod_tva105['Epizode'] = vod_tva105['Epizode'].str.replace('٥', '5')
vod_tva105['Epizode'] = vod_tva105['Epizode'].str.replace('٦', '6')
vod_tva105['Epizode'] = vod_tva105['Epizode'].str.replace('٧', '7')
vod_tva105['Epizode'] = vod_tva105['Epizode'].str.replace('٨', '8')
vod_tva105['Epizode'] = vod_tva105['Epizode'].str.replace('٩', '9')

vod_tva105['Season'] = vod_tva105['Season'].str.replace('۰', '0')
vod_tva105['Season'] = vod_tva105['Season'].str.replace('۱', '1')
vod_tva105['Season'] = vod_tva105['Season'].str.replace('۲', '2')
vod_tva105['Season'] = vod_tva105['Season'].str.replace('۳', '3')
vod_tva105['Season'] = vod_tva105['Season'].str.replace('۴', '4')
vod_tva105['Season'] = vod_tva105['Season'].str.replace('۵', '5')
vod_tva105['Season'] = vod_tva105['Season'].str.replace('۶', '6')
vod_tva105['Season'] = vod_tva105['Season'].str.replace('۷', '7')
vod_tva105['Season'] = vod_tva105['Season'].str.replace('۸', '8')
vod_tva105['Season'] = vod_tva105['Season'].str.replace('۹', '9')
vod_tva105['Season'] = vod_tva105['Season'].str.replace('٠', '0')
vod_tva105['Season'] = vod_tva105['Season'].str.replace('١', '1')
vod_tva105['Season'] = vod_tva105['Season'].str.replace('٢', '2')
vod_tva105['Season'] = vod_tva105['Season'].str.replace('٣', '3')
vod_tva105['Season'] = vod_tva105['Season'].str.replace('٤', '4')
vod_tva105['Season'] = vod_tva105['Season'].str.replace('٥', '5')
vod_tva105['Season'] = vod_tva105['Season'].str.replace('٦', '6')
vod_tva105['Season'] = vod_tva105['Season'].str.replace('٧', '7')
vod_tva105['Season'] = vod_tva105['Season'].str.replace('٨', '8')
vod_tva105['Season'] = vod_tva105['Season'].str.replace('٩', '9')

##########
vod_tva105[["TitleCleaned1", "NoData"]]= vod_tva105["TitleCleaned1"].str.split(":", n = 1, expand = True)
vod_tva105['NoData'] = vod_tva105['NoData'].astype(str)
vod_tva105_1 = vod_tva105 [vod_tva105.NoData.str.contains('None')]
vod_tva105_2 = vod_tva105 [~vod_tva105.NoData.str.contains('None')]
vod_tva105_2['TitleCleaned1'].replace('', 'NoData', inplace=True)
vod_tva105_2['TitleCleaned1'] = vod_tva105_2['TitleCleaned1'].fillna('NoData')
vod_tva105_21 = vod_tva105_2 [vod_tva105_2.TitleCleaned1.str.contains('NoData')]
vod_tva105_22 = vod_tva105_2 [~vod_tva105_2.TitleCleaned1.str.contains('NoData')]
vod_tva105_21['TitleCleaned1'] = vod_tva105_21['NoData']
vod_tva105_2 = vod_tva105_21.append([vod_tva105_22])
vod_tva106 = vod_tva105_1.append([vod_tva105_2])
del vod_tva105_1
del vod_tva105_2
del vod_tva105_21
del vod_tva105_22
del vod_tva106['NoData']
vod_tva106['TitleCleaned1'] = vod_tva106['TitleCleaned1'].str.strip()
################################
vod_tva106[["TitleCleaned1", "NoData"]]= vod_tva106["TitleCleaned1"].str.split("_", n = 1, expand = True)
vod_tva106['NoData'] = vod_tva106['NoData'].astype(str)
vod_tva106_1 = vod_tva106 [vod_tva106.NoData.str.contains('None')]
vod_tva106_2 = vod_tva106 [~vod_tva106.NoData.str.contains('None')]
vod_tva106_2['TitleCleaned1'].replace('', 'NoData', inplace=True)
vod_tva106_2['TitleCleaned1'] = vod_tva106_2['TitleCleaned1'].fillna('NoData')
vod_tva106_21 = vod_tva106_2 [vod_tva106_2.TitleCleaned1.str.contains('NoData')]
vod_tva106_22 = vod_tva106_2 [~vod_tva106_2.TitleCleaned1.str.contains('NoData')]
vod_tva106_21['TitleCleaned1'] = vod_tva106_21['NoData']
vod_tva106_2 = vod_tva106_21.append([vod_tva106_22])
vod_tva107 = vod_tva106_1.append([vod_tva106_2])
del vod_tva106_1
del vod_tva106_2
del vod_tva106_21
del vod_tva106_22
del vod_tva107['NoData']
vod_tva107['TitleCleaned1'] = vod_tva107['TitleCleaned1'].str.strip()
################################
vod_tva107[["TitleCleaned1", "NoData"]]= vod_tva107["TitleCleaned1"].str.split("-", n = 1, expand = True)
vod_tva107['NoData'] = vod_tva107['NoData'].astype(str)
vod_tva107_1 = vod_tva107 [vod_tva107.NoData.str.contains('None')]
vod_tva107_2 = vod_tva107 [~vod_tva107.NoData.str.contains('None')]
vod_tva107_2['TitleCleaned1'].replace('', 'NoData', inplace=True)
vod_tva107_2['TitleCleaned1'] = vod_tva107_2['TitleCleaned1'].fillna('NoData')
vod_tva107_21 = vod_tva107_2 [vod_tva107_2.TitleCleaned1.str.contains('NoData')]
vod_tva107_22 = vod_tva107_2 [~vod_tva107_2.TitleCleaned1.str.contains('NoData')]
vod_tva107_21['TitleCleaned1'] = vod_tva107_21['NoData']
vod_tva107_2 = vod_tva107_21.append([vod_tva107_22])
vod_tva108 = vod_tva107_1.append([vod_tva107_2])
del vod_tva107_1
del vod_tva107_2
del vod_tva107_21
del vod_tva107_22
del vod_tva108['NoData']
vod_tva108['TitleCleaned1'] = vod_tva108['TitleCleaned1'].str.strip()

################################
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
del total_vod_1399_Ghadimi_v2['فصل']

vod_tva108.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode', 'Month', 'Year'], keep = 'last', inplace = True)
vod_tva109 = pd.merge(vod_tva108, total_vod_1399_Ghadimi_v2, on = ['TitleCleaned1'])
vod_tva109['Season'] = vod_tva109['season']
vod_tva109['TitleCleaned1'] = vod_tva109['title_first']
vod_tva109['Season'] = vod_tva109['season']
del vod_tva109['season']
del vod_tva109['title_first']
vod_tva110 = vod_tva108.append([vod_tva109])
vod_tva110.drop_duplicates(subset =['ID', 'Month', 'Year'], keep = 'last', inplace = True)

vod_tva110['Epizode'] = vod_tva110['Epizode'].astype(str)
vod_tva110['Season'] = vod_tva110['Season'].apply(lambda x: x.zfill(2))
vod_tva110['Epizode'] = vod_tva110['Epizode'].apply(lambda x: x.zfill(3))

################################
vod_tva110 = vod_tva110.reset_index()
del vod_tva110['index']
vod_tva110_1 = vod_tva110 [vod_tva110.Season.str.contains('00')]
vod_tva110_2 = vod_tva110 [~vod_tva110.Season.str.contains('00')]
vod_tva110_film = vod_tva110_1 [vod_tva110.Epizode.str.contains('000')]
vod_tva110_4 = vod_tva110_1 [~vod_tva110.Epizode.str.contains('000')]
vod_tva110_serial = vod_tva110_2.append([vod_tva110_4])
vod_tva110_film['FilmSerial'] = 'فیلم'
vod_tva110_film['code_FilmSerial'] = '01'
vod_tva110_serial['FilmSerial'] = 'سریال'
vod_tva110_serial['code_FilmSerial'] = '02'
vod_tva110 = vod_tva110_film.append([vod_tva110_serial])
del vod_tva110_1
del vod_tva110_2
del vod_tva110_4

################################
vod_tva111 = vod_tva110.copy()
vod_tva111.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode', 'Month', 'Year', 'FilmSerial', 'code_FilmSerial'], keep = 'last', inplace = True)

db1_data = pd.DataFrame()
vod_total_db1 = vod_lenz119.append([vod_aio109])
db1_data['TitleCleaned1'] = vod_total_db1['TitleCleaned1']
db1_data['Season'] = vod_total_db1['Season']
db1_data['Epizode'] = vod_total_db1['Epizode']
db1_data['ID_new'] = vod_total_db1['ID_new']
db1_data['IDS_new'] = vod_total_db1['IDS_new']

db1_data.drop_duplicates(subset =['ID_new'], keep = 'last', inplace = True)
vod_tva_db1_merge = pd.merge(vod_tva111, db1_data, on = ['TitleCleaned1', 'Season', 'Epizode'])
vod_tva_db1_merge.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode', 'Month', 'Year'], keep = 'last', inplace = True)
vod_tva111['ID_new'] = ''
vod_tva111['IDS_new'] = ''

vod_append = vod_tva_db1_merge.append([vod_tva111])

vod_append = vod_append.reset_index()
del vod_append['index']
vod_append_dup = vod_append.copy()
vod_append_dup.drop_duplicates(subset =['Season', 'TitleCleaned1', 'Epizode'], keep = 'last', inplace = True)
vod_append_dup = vod_append_dup.reset_index()
del vod_append_dup['index']

vod_append_new = pd.DataFrame()
for i in range(0, len(vod_append_dup)):   #len(vod_append_dup)
    print(i)
    TT = vod_append_dup.loc[i, 'TitleCleaned1']
    SS = vod_append_dup.loc[i, 'Season']
    EE = vod_append_dup.loc[i, 'Epizode']
    title = vod_append.query("TitleCleaned1 == @TT")
    title = title.query("Season == @SS")
    title = title.query("Epizode == @EE")
    title = title.reset_index()
    del title['index']
    k = 0
    for j in range(0, len(title)):
        if '10' not in title.loc[j, 'IDS_new']:
            k = k + 1
    if j + 1 == k:
        vod_append_new = title.append([vod_append_new])

#vod_append.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode', 'Month', 'Year'], keep = False, inplace = True)
vod_tva_step1 = vod_tva_db1_merge.copy()

################################
del vod_append_new['ID_new']
del vod_append_new['IDS_new']
del db1_data['Epizode']
vod_append_new = vod_append_new.reset_index()
del vod_append_new['index']
db1_data.drop_duplicates(subset =['IDS_new'], keep = 'last', inplace = True)
vod_tva_db1_merge2 = pd.merge(vod_append_new, db1_data, on = ['TitleCleaned1', 'Season'])

vod_tva_db1_merge2.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode', 'Month', 'Year'], keep = 'last', inplace = True)
vod_append_new['ID_new'] = ''
vod_append_new['IDS_new'] = ''

vod_append2 = vod_tva_db1_merge2.append([vod_append_new])

vod_append2 = vod_append2.reset_index()
del vod_append2['index']
vod_append2_dup = vod_append2.copy()
vod_append2_dup.drop_duplicates(subset =['Season', 'TitleCleaned1'], keep = 'last', inplace = True)
vod_append2_dup = vod_append2_dup.reset_index()
del vod_append2_dup['index']

vod_append2_new = pd.DataFrame()
for i in range(0, len(vod_append2_dup)):   #len(vod_append2_dup)
    print(i)
    TT = vod_append2_dup.loc[i, 'TitleCleaned1']
    SS = vod_append2_dup.loc[i, 'Season']
    title = vod_append2.query("TitleCleaned1 == @TT")
    title = title.query("Season == @SS")
    title = title.reset_index()
    del title['index']
    k = 0
    for j in range(0, len(title)):
        if '10' not in title.loc[j, 'IDS_new']:
            k = k + 1
    if j + 1 == k:
        vod_append2_new = title.append([vod_append2_new])

vod_tva_step2 = vod_tva_db1_merge2.copy()
vod_tva_step2['ID_new'] = vod_tva_step2['ID_new'].str[0:11] + vod_tva_step2['Epizode']

################################
vod_append2_new = vod_append2_new.sort_values(['Season', 'TitleCleaned1'], ascending=[False, False])
vod_append2_new = vod_append2_new.reset_index()
del vod_append2_new['index']

id_first1 = max(vod_aio109['ID_new'])
id_first2 = id_first1[0:7]
id_first = int(id_first2)+1    # write last id_first + 1
vod_append2_new.loc[0, 'id_first'] = id_first
for i in range(1, len(vod_append2_new)):
    print(i)
    if vod_append2_new.loc[i, 'TitleCleaned1'] == vod_append2_new.loc[i-1, 'TitleCleaned1'] and \
       vod_append2_new.loc[i, 'Season'] == vod_append2_new.loc[i-1, 'Season']:
        vod_append2_new.loc[i, 'id_first'] = id_first
    else:
        id_first = id_first + 1
        vod_append2_new.loc[i, 'id_first'] = id_first

vod_append2_new['id_first'] = vod_append2_new['id_first'].astype(int).astype(str) 
   
vod_append2_new['ID_new'] = vod_append2_new['id_first']+vod_append2_new['code_FilmSerial']+vod_append2_new['Season']+vod_append2_new['Epizode']
vod_append2_new['IDS_new'] = vod_append2_new['id_first']+vod_append2_new['Season']

del vod_append2_new['id_first']
################################
vod_tva112 = vod_tva_step1.append([vod_tva_step2, vod_append2_new])
del vod_tva112['code_FilmSerial']
vod_tva112 = vod_tva112.reset_index()
del vod_tva112['index']

#############################################################
#############################################################
#############################################################
vod_filimo['TitleCleaned1'] = vod_filimo['Title']
vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].astype(str)
vod_filimo['FilmSerial'] = ''

vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('دعای')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('ریاضی')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('گزینه دو')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('vod')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('فارسی')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('انگلیسی')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('عربی')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('علوم')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('روضه')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('مداح')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('-مداح')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('متوسطه')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('ابتدایی')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('خلاصه')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('علوم')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('فارسی')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('کاروان ایران')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('زبان انگلیسی')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('زیست شناسی')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('مدال')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('گل های بازی')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('مطالعات اجتماعی')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('لالیگا')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('برترین لحظات')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('برترینهای')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('برد تاریخی')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('برد سخت')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('لیگ جزیره')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('تکنیک های')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('رالی')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('شکست حریف')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('موتور اسپرت')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('برد مقابل')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('صد گل')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('لیگ قهرمانان')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('فوتبال ساحلی')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('فینالیست شدن')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('قرعه کشی')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('مجله آشپزی')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('مجله اشپزی')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('مرور بازیهای')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('مسابقات')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('شکست برابر')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('مقابل حریف')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('کشتی آزاد')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('المپیک')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('کیلوگرم مقابل')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('گل های')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('گلهای')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('ایرانسل')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('گل برتر')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('آنفیلد')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('لیونل مسی')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('کسب طلای')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('جام جهانی')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('شکست حمیده')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('شکست مرتضی')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('شکست میرزازاده')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('صد سیو')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('ضربات آزاد')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('مستند پرسپولیس')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('پیروزی قاطع')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('پیروزی نفسگیر')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('نماینده تکواندو')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('مجله آشپزی')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('مجله آشپزی')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('بارسلونا')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('کولاک بانوان')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('نیمار')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('گل برتر')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('بسکتبال')]
vod_filimo = vod_filimo [~vod_filimo. Title.str.contains('علی پناه')]
vod_filimo = vod_filimo [~vod_filimo. Title.str.contains('رضا هلالی')]
vod_filimo = vod_filimo [~vod_filimo. Title.str.contains('مهدی حسن آبادی')]
vod_filimo = vod_filimo [~vod_filimo. Title.str.contains('حجت الاسلام')]
vod_filimo = vod_filimo [~vod_filimo. Title.str.contains('قاری بین المللی')]
vod_filimo = vod_filimo [~vod_filimo. Title.str.contains('حاج امیر')]
vod_filimo = vod_filimo [~vod_filimo. Title.str.contains('حاج جواد')]
vod_filimo = vod_filimo [~vod_filimo. Title.str.contains('سید جواد')]
vod_filimo = vod_filimo [~vod_filimo. Title.str.contains('کاکایی')]
vod_filimo = vod_filimo [~vod_filimo. Title.str.contains('احمد ابوالقاسمی')]
vod_filimo = vod_filimo [~vod_filimo. Title.str.contains('حاج مهدی')]
vod_filimo = vod_filimo [~vod_filimo. Title.str.contains('حاج احمد')]
vod_filimo = vod_filimo [~vod_filimo. Title.str.contains('کربلایی')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('رونالدو')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('دربی')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('کارت قرمز')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('کار با توپ')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('یک اتفاق تازه')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('شکست نجاتی')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('نماهنگ')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('test')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('نیمه نهایی')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('هایلایت')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('فوتبال')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('گل های')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('فینال')]
vod_filimo = vod_filimo [~vod_filimo. TitleCleaned1.str.contains('لیگ اروپا')]
vod_filimo = vod_filimo [~vod_filimo.Title.str.contains('بوندسلیگا')]
vod_filimo = vod_filimo [~vod_filimo.Title.str.contains('بازی های اروپایی')]
vod_filimo = vod_filimo [~vod_filimo.Title.str.contains('لیگ برتر انگلیس')]
vod_filimo = vod_filimo [~vod_filimo.Title.str.contains('لیگ جزیره')]
vod_filimo = vod_filimo [~vod_filimo.Title.str.contains('یورو 2020')]
vod_filimo = vod_filimo [~vod_filimo.Title.str.contains('لالیگا')]
vod_filimo = vod_filimo [~vod_filimo.Title.str.contains('لیگ اسپانیا')]
vod_filimo = vod_filimo [~vod_filimo.Title.str.contains('لیگ آلمان')]
vod_filimo = vod_filimo [~vod_filimo.Title.str.contains('لیگ ایتالیا')]
vod_filimo = vod_filimo [~vod_filimo.Title.str.contains('سری آ')]

#########
vod_filimo['TitleCleaned1'] = vod_filimo['TitleCleaned1'].str.strip()
vod_filimo['Season'] = vod_filimo['Season'].str.strip()
vod_filimo['Epizode'] = vod_filimo['Epizode'].str.strip()

vod_filimo100 = vod_filimo[~vod_filimo.TitleCleaned1.str.contains("فصل شکار")]
vod_filimo201 = vod_filimo[vod_filimo.TitleCleaned1.str.contains("فصل شکار")]
vod_filimo102 = vod_filimo100[~vod_filimo100.TitleCleaned1.str.contains("فصل قتل ها")]
vod_filimo202 = vod_filimo100[vod_filimo100.TitleCleaned1.str.contains("فصل قتل ها")]
vod_filimo303 = vod_filimo102[~vod_filimo102.TitleCleaned1.str.contains("فصل موج سواری")]
vod_filimo203 = vod_filimo102[vod_filimo102.TitleCleaned1.str.contains("فصل موج سواری")]
vod_filimo401 = vod_filimo303[~vod_filimo303.TitleCleaned1.str.contains("سالاد فصل")]
vod_filimo402 = vod_filimo303[vod_filimo303.TitleCleaned1.str.contains("سالاد فصل")]
vod_filimo501 = vod_filimo401[~vod_filimo401.TitleCleaned1.str.contains("فصل باران های موسمی")]
vod_filimo502 = vod_filimo401[vod_filimo401.TitleCleaned1.str.contains("فصل باران های موسمی")]
vod_filimo601 = vod_filimo501[~vod_filimo501.TitleCleaned1.str.contains("فصل باران های موسمی")]
vod_filimo602 = vod_filimo501[vod_filimo501.TitleCleaned1.str.contains("فصل باران های موسمی")]
vod_filimo101 = vod_filimo601[~vod_filimo601.TitleCleaned1.str.contains("فصل سیب")]
vod_filimo702 = vod_filimo601[vod_filimo601.TitleCleaned1.str.contains("فصل سیب")]

vod_filimo200 = vod_filimo201.append([vod_filimo202, vod_filimo203, vod_filimo402, vod_filimo502, vod_filimo602, vod_filimo702])
del vod_filimo201
del vod_filimo202
del vod_filimo203
del vod_filimo100
del vod_filimo102
del vod_filimo401
del vod_filimo501
del vod_filimo601
del vod_filimo303
del vod_filimo402
del vod_filimo502
del vod_filimo602
del vod_filimo702

vod_filimo102 = vod_filimo101.append([vod_filimo200])
#########
vod_filimo102['TitleCleaned1'] = vod_filimo102['Title']
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('سریال', '')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('فیلم', '')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('انیمیشن', '')

vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('او وی ای', '')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('با زیرنویس مخصوص ناشنوایان', '')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('مخصوص نابینایان', '')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('مخصوص ناشنوایان', '')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('لالیگا', '')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('بوندسلیگا', '')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('سری آ', '')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('لیگ جزیره', '')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('ویژه نوروز', '')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('به روایت محمدرضا احمدی', '')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('تنت', '')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('جدید و برگزیده', '')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('اره ۹', '')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('ماه و ستاره', '')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('سرقت خودرو', '')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('ایدز', '')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('(', '')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace(')', '')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.strip()

#########
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.strip()

vod_filimo1 = vod_filimo102[vod_filimo102.TitleCleaned1.str.contains(":")]
vod_filimo2 = vod_filimo102[~vod_filimo102.TitleCleaned1.str.contains(":")]
vod_filimo1[["TitleCleaned1", "NoData"]]= vod_filimo1["TitleCleaned1"].str.split(":", n = 1, expand = True)
del vod_filimo1['NoData']
vod_filimo102 = vod_filimo1.append([vod_filimo2])
del vod_filimo1
del vod_filimo2
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.strip()

#########
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('0', '۰')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('1', '۱')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('2', '۲')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('3', '۳')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('4', '۴')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('5', '۵')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('6', '۶')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('7', '۷')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('8', '۸')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('9', '۹')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('٠', '۰')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('١', '۱')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('٢', '۲')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('٣', '۳')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('٤', '۴')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('٥', '۵')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('٦', '۶')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('٧', '۷')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('٨', '۸')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('٩', '۹')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('ي', 'ی')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('ؤ','و')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.replace('ك','ک')
vod_filimo102['TitleCleaned1'] = vod_filimo102['TitleCleaned1'].str.strip() 

##########
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
del total_vod_1399_Ghadimi_v2['فصل']

vod_filimo102.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode', 'Month', 'Year'], keep = 'last', inplace = True)
vod_filimo103 = pd.merge(vod_filimo102, total_vod_1399_Ghadimi_v2, on = ['TitleCleaned1'])
vod_filimo103['Season'] = vod_filimo103['season']
vod_filimo103['TitleCleaned1'] = vod_filimo103['title_first']
vod_filimo103['Season'] = vod_filimo103['season']
del vod_filimo103['season']
del vod_filimo103['title_first']
vod_filimo104 = vod_filimo102.append([vod_filimo103])
vod_filimo104.drop_duplicates(subset =['ID', 'Month', 'Year'], keep = 'last', inplace = True)

vod_filimo104['Epizode'] = vod_filimo104['Epizode'].astype(str)
vod_filimo104['Season'] = vod_filimo104['Season'].apply(lambda x: x.zfill(2))
vod_filimo104['Epizode'] = vod_filimo104['Epizode'].apply(lambda x: x.zfill(3))

################################
vod_filimo104 = vod_filimo104.reset_index()
del vod_filimo104['index']
vod_filimo104_1 = vod_filimo104 [vod_filimo104.Season.str.contains('00')]
vod_filimo104_2 = vod_filimo104 [~vod_filimo104.Season.str.contains('00')]
vod_filimo104_film = vod_filimo104_1 [vod_filimo104.Epizode.str.contains('000')]
vod_filimo104_4 = vod_filimo104_1 [~vod_filimo104.Epizode.str.contains('000')]
vod_filimo104_serial = vod_filimo104_2.append([vod_filimo104_4])
vod_filimo104_film['FilmSerial'] = 'فیلم'
vod_filimo104_film['code_FilmSerial'] = '01'
vod_filimo104_serial['FilmSerial'] = 'سریال'
vod_filimo104_serial['code_FilmSerial'] = '02'
vod_filimo1055 = vod_filimo104_film.append([vod_filimo104_serial])
del vod_filimo104_1
del vod_filimo104_2
del vod_filimo104_4

################################
vod_filimo105 = vod_filimo1055.copy()
vod_filimo105.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode', 'Month', 'Year', 'FilmSerial', 'code_FilmSerial'], keep = 'last', inplace = True)

db1_data = pd.DataFrame()
vod_total_db1 = vod_lenz119.append([vod_aio109, vod_tva112])
db1_data['TitleCleaned1'] = vod_total_db1['TitleCleaned1']
db1_data['Season'] = vod_total_db1['Season']
db1_data['Epizode'] = vod_total_db1['Epizode']
db1_data['ID_new'] = vod_total_db1['ID_new']
db1_data['IDS_new'] = vod_total_db1['IDS_new']

db1_data.drop_duplicates(subset =['ID_new'], keep = 'last', inplace = True)
vod_filimo_db1_merge = pd.merge(vod_filimo105, db1_data, on = ['TitleCleaned1', 'Season', 'Epizode'])
vod_filimo_db1_merge.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode', 'Month', 'Year'], keep = 'last', inplace = True)
vod_filimo105['ID_new'] = ''
vod_filimo105['IDS_new'] = ''

vod_append = vod_filimo_db1_merge.append([vod_filimo105])

vod_append = vod_append.reset_index()
del vod_append['index']
vod_append_dup = vod_append.copy()
vod_append_dup.drop_duplicates(subset =['Season', 'TitleCleaned1', 'Epizode'], keep = 'last', inplace = True)
vod_append_dup = vod_append_dup.reset_index()
del vod_append_dup['index']

vod_append_new = pd.DataFrame()
for i in range(0, len(vod_append_dup)):   #len(vod_append_dup)
    print(i)
    TT = vod_append_dup.loc[i, 'TitleCleaned1']
    SS = vod_append_dup.loc[i, 'Season']
    EE = vod_append_dup.loc[i, 'Epizode']
    title = vod_append.query("TitleCleaned1 == @TT")
    title = title.query("Season == @SS")
    title = title.query("Epizode == @EE")
    title = title.reset_index()
    del title['index']
    k = 0
    for j in range(0, len(title)):
        if '10' not in title.loc[j, 'IDS_new']:
            k = k + 1
    if j + 1 == k:
        vod_append_new = title.append([vod_append_new])

#vod_append.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode', 'Month', 'Year'], keep = False, inplace = True)
vod_filimo_step1 = vod_filimo_db1_merge.copy()
print(len(vod_filimo_step1))
################################
del vod_append_new['ID_new']
del vod_append_new['IDS_new']
del db1_data['Epizode']
vod_append_new = vod_append_new.reset_index()
del vod_append_new['index']
db1_data.drop_duplicates(subset =['IDS_new'], keep = 'last', inplace = True)
vod_filimo_db1_merge2 = pd.merge(vod_append_new, db1_data, on = ['TitleCleaned1', 'Season'])

vod_filimo_db1_merge2.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode', 'Month', 'Year'], keep = 'last', inplace = True)
vod_append_new['ID_new'] = ''
vod_append_new['IDS_new'] = ''

vod_append2 = vod_filimo_db1_merge2.append([vod_append_new])

vod_append2 = vod_append2.reset_index()
del vod_append2['index']
vod_append2_dup = vod_append2.copy()
vod_append2_dup.drop_duplicates(subset =['Season', 'TitleCleaned1'], keep = 'last', inplace = True)
vod_append2_dup = vod_append2_dup.reset_index()
del vod_append2_dup['index']

vod_append2_new = pd.DataFrame()
for i in range(0, len(vod_append2_dup)):   #len(vod_append2_dup)
    print(i)
    TT = vod_append2_dup.loc[i, 'TitleCleaned1']
    SS = vod_append2_dup.loc[i, 'Season']
    title = vod_append2.query("TitleCleaned1 == @TT")
    title = title.query("Season == @SS")
    title = title.reset_index()
    del title['index']
    k = 0
    for j in range(0, len(title)):
        if '10' not in title.loc[j, 'IDS_new']:
            k = k + 1
    if j + 1 == k:
        vod_append2_new = title.append([vod_append2_new])

vod_filimo_step2 = vod_filimo_db1_merge2.copy()
vod_filimo_step2['ID_new'] = vod_filimo_step2['ID_new'].str[0:11] + vod_filimo_step2['Epizode']

################################
vod_append2_new = vod_append2_new.sort_values(['Season', 'TitleCleaned1'], ascending=[False, False])
vod_append2_new = vod_append2_new.reset_index()
del vod_append2_new['index']

id_first1 = max(vod_tva112['ID_new'])
id_first2 = id_first1[0:7]
id_first = int(id_first2)+1    # write last id_first + 1
vod_append2_new.loc[0, 'id_first'] = id_first
for i in range(1, len(vod_append2_new)):
    print(i)
    if vod_append2_new.loc[i, 'TitleCleaned1'] == vod_append2_new.loc[i-1, 'TitleCleaned1'] and \
       vod_append2_new.loc[i, 'Season'] == vod_append2_new.loc[i-1, 'Season']:
        vod_append2_new.loc[i, 'id_first'] = id_first
    else:
        id_first = id_first + 1
        vod_append2_new.loc[i, 'id_first'] = id_first

vod_append2_new['id_first'] = vod_append2_new['id_first'].astype(int).astype(str) 
   
vod_append2_new['ID_new'] = vod_append2_new['id_first']+vod_append2_new['code_FilmSerial']+vod_append2_new['Season']+vod_append2_new['Epizode']
vod_append2_new['IDS_new'] = vod_append2_new['id_first']+vod_append2_new['Season']

del vod_append2_new['id_first']
################################
vod_filimo112 = vod_filimo_step1.append([vod_filimo_step2, vod_append2_new])
del vod_filimo112['code_FilmSerial']
vod_filimo112 = vod_filimo112.reset_index()
del vod_filimo112['index']

#############################################################
#############################################################
#############################################################
vod_filmgardi['TitleCleaned1'] = vod_filmgardi['Title']
vod_filmgardi['TitleCleaned1'] = vod_filmgardi['TitleCleaned1'].astype(str)
vod_filmgardi['FilmSerial'] = ''

vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('دعای')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('ریاضی')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('گزینه دو')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('vod')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('فارسی')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('انگلیسی')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('عربی')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('علوم')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('روضه')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('مداح')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('-مداح')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('متوسطه')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('ابتدایی')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('خلاصه')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('علوم')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('فارسی')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('کاروان ایران')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('زبان انگلیسی')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('زیست شناسی')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('مدال')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('گل های بازی')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('مطالعات اجتماعی')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('لالیگا')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('برترین لحظات')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('برترینهای')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('برد تاریخی')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('برد سخت')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('لیگ جزیره')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('تکنیک های')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('رالی')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('شکست حریف')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('موتور اسپرت')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('برد مقابل')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('صد گل')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('لیگ قهرمانان')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('فوتبال ساحلی')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('فینالیست شدن')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('قرعه کشی')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('مجله آشپزی')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('مجله اشپزی')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('مرور بازیهای')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('مسابقات')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('شکست برابر')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('مقابل حریف')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('کشتی آزاد')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('المپیک')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('کیلوگرم مقابل')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('گل های')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('گلهای')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('ایرانسل')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('گل برتر')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('آنفیلد')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('لیونل مسی')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('کسب طلای')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('جام جهانی')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('شکست حمیده')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('شکست مرتضی')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('شکست میرزازاده')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('صد سیو')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('ضربات آزاد')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('مستند پرسپولیس')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('پیروزی قاطع')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('پیروزی نفسگیر')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('نماینده تکواندو')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('مجله آشپزی')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('مجله آشپزی')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('بارسلونا')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('کولاک بانوان')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('نیمار')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('گل برتر')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('بسکتبال')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. Title.str.contains('علی پناه')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. Title.str.contains('رضا هلالی')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. Title.str.contains('مهدی حسن آبادی')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. Title.str.contains('حجت الاسلام')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. Title.str.contains('قاری بین المللی')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. Title.str.contains('حاج امیر')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. Title.str.contains('حاج جواد')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. Title.str.contains('سید جواد')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. Title.str.contains('کاکایی')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. Title.str.contains('احمد ابوالقاسمی')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. Title.str.contains('حاج مهدی')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. Title.str.contains('حاج احمد')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. Title.str.contains('کربلایی')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('رونالدو')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('دربی')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('کارت قرمز')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('کار با توپ')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('یک اتفاق تازه')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('شکست نجاتی')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('نماهنگ')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('test')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('نیمه نهایی')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('هایلایت')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('فوتبال')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('گل های')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('فینال')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi. TitleCleaned1.str.contains('لیگ اروپا')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi.Title.str.contains('بوندسلیگا')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi.Title.str.contains('بازی های اروپایی')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi.Title.str.contains('لیگ برتر انگلیس')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi.Title.str.contains('لیگ جزیره')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi.Title.str.contains('یورو 2020')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi.Title.str.contains('لالیگا')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi.Title.str.contains('لیگ اسپانیا')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi.Title.str.contains('لیگ آلمان')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi.Title.str.contains('لیگ ایتالیا')]
vod_filmgardi = vod_filmgardi [~vod_filmgardi.Title.str.contains('سری آ')]

#########
vod_filmgardi['TitleCleaned1'] = vod_filmgardi['TitleCleaned1'].str.strip()
vod_filmgardi['Season'] = vod_filmgardi['Season'].str.strip()
vod_filmgardi['Epizode'] = vod_filmgardi['Epizode'].str.strip()
vod_filmgardi101 = vod_filmgardi.copy()

#########
vod_filmgardi101['TitleCleaned1'] = vod_filmgardi101['TitleCleaned1'].str.strip()

vod_filmgardi1 = vod_filmgardi101[vod_filmgardi101.TitleCleaned1.str.contains(":")]
vod_filmgardi2 = vod_filmgardi101[~vod_filmgardi101.TitleCleaned1.str.contains(":")]
vod_filmgardi1[["TitleCleaned1", "NoData"]]= vod_filmgardi1["TitleCleaned1"].str.split(":", n = 1, expand = True)
del vod_filmgardi1['NoData']
vod_filmgardi101 = vod_filmgardi1.append([vod_filmgardi2])
del vod_filmgardi1
del vod_filmgardi2
vod_filmgardi101['TitleCleaned1'] = vod_filmgardi101['TitleCleaned1'].str.strip()

#########
vod_filmgardi101['TitleCleaned1'] = vod_filmgardi101['TitleCleaned1'].str.strip()

vod_filmgardi1 = vod_filmgardi101[vod_filmgardi101.TitleCleaned1.str.contains("-")]
vod_filmgardi2 = vod_filmgardi101[~vod_filmgardi101.TitleCleaned1.str.contains("-")]
vod_filmgardi1[["TitleCleaned1", "NoData"]]= vod_filmgardi1["TitleCleaned1"].str.split("-", n = 1, expand = True)
del vod_filmgardi1['NoData']
vod_filmgardi101 = vod_filmgardi1.append([vod_filmgardi2])
del vod_filmgardi1
del vod_filmgardi2
vod_filmgardi101['TitleCleaned1'] = vod_filmgardi101['TitleCleaned1'].str.strip()

#########
vod_filmgardi101['TitleCleaned1'] = vod_filmgardi101['TitleCleaned1'].str.replace('با زیرنویس مخصوص ناشنوایان', '')
vod_filmgardi101['TitleCleaned1'] = vod_filmgardi101['TitleCleaned1'].str.replace('مخصوص نابینایان', '')
vod_filmgardi101['TitleCleaned1'] = vod_filmgardi101['TitleCleaned1'].str.replace('مخصوص ناشنوایان', '')
vod_filmgardi101['TitleCleaned1'] = vod_filmgardi101['TitleCleaned1'].str.replace('لالیگا', '')
vod_filmgardi101['TitleCleaned1'] = vod_filmgardi101['TitleCleaned1'].str.replace('بوندسلیگا', '')
vod_filmgardi101['TitleCleaned1'] = vod_filmgardi101['TitleCleaned1'].str.replace('سری آ', '')
vod_filmgardi101['TitleCleaned1'] = vod_filmgardi101['TitleCleaned1'].str.replace('لیگ جزیره', '')
vod_filmgardi101['TitleCleaned1'] = vod_filmgardi101['TitleCleaned1'].str.replace('ویژه نوروز', '')
vod_filmgardi101['TitleCleaned1'] = vod_filmgardi101['TitleCleaned1'].str.replace('به روایت محمدرضا احمدی', '')
vod_filmgardi101['TitleCleaned1'] = vod_filmgardi101['TitleCleaned1'].str.replace('جدید و برگزیده', '')
vod_filmgardi101['TitleCleaned1'] = vod_filmgardi101['TitleCleaned1'].str.replace('(', '')
vod_filmgardi101['TitleCleaned1'] = vod_filmgardi101['TitleCleaned1'].str.replace(')', '')
vod_filmgardi101['TitleCleaned1'] = vod_filmgardi101['TitleCleaned1'].str.strip()


vod_filmgardi100 = vod_filmgardi101[~vod_filmgardi101.TitleCleaned1.str.contains("فصل شکار")]
vod_filmgardi201 = vod_filmgardi101[vod_filmgardi101.TitleCleaned1.str.contains("فصل شکار")]
vod_filmgardi102 = vod_filmgardi100[~vod_filmgardi100.TitleCleaned1.str.contains("فصل قتل ها")]
vod_filmgardi202 = vod_filmgardi100[vod_filmgardi100.TitleCleaned1.str.contains("فصل قتل ها")]
vod_filmgardi303 = vod_filmgardi102[~vod_filmgardi102.TitleCleaned1.str.contains("فصل موج سواری")]
vod_filmgardi203 = vod_filmgardi102[vod_filmgardi102.TitleCleaned1.str.contains("فصل موج سواری")]
vod_filmgardi401 = vod_filmgardi303[~vod_filmgardi303.TitleCleaned1.str.contains("سالاد فصل")]
vod_filmgardi402 = vod_filmgardi303[vod_filmgardi303.TitleCleaned1.str.contains("سالاد فصل")]
vod_filmgardi501 = vod_filmgardi401[~vod_filmgardi401.TitleCleaned1.str.contains("فصل باران های موسمی")]
vod_filmgardi502 = vod_filmgardi401[vod_filmgardi401.TitleCleaned1.str.contains("فصل باران های موسمی")]
vod_filmgardi601 = vod_filmgardi501[~vod_filmgardi501.TitleCleaned1.str.contains("فصل باران های موسمی")]
vod_filmgardi602 = vod_filmgardi501[vod_filmgardi501.TitleCleaned1.str.contains("فصل باران های موسمی")]
vod_filmgardi110 = vod_filmgardi601[~vod_filmgardi601.TitleCleaned1.str.contains("فصل سیب")]
vod_filmgardi702 = vod_filmgardi601[vod_filmgardi601.TitleCleaned1.str.contains("فصل سیب")]


vod_filmgardi801 = vod_filmgardi110[~vod_filmgardi110.TitleCleaned1.str.contains("حل و فصل نهایی")]
vod_filmgardi802 = vod_filmgardi110[vod_filmgardi110.TitleCleaned1.str.contains("حل و فصل نهایی")]

vod_filmgardi803 = vod_filmgardi801[~vod_filmgardi801.TitleCleaned1.str.contains("فصل خاکستری")]
vod_filmgardi804 = vod_filmgardi801[vod_filmgardi801.TitleCleaned1.str.contains("فصل خاکستری")]

vod_filmgardi805 = vod_filmgardi803[~vod_filmgardi803.TitleCleaned1.str.contains("فصل خون")]
vod_filmgardi806 = vod_filmgardi803[vod_filmgardi803.TitleCleaned1.str.contains("فصل خون")]

vod_filmgardi807 = vod_filmgardi805[~vod_filmgardi805.TitleCleaned1.str.contains("فصل فراموشی فریبا")]
vod_filmgardi808 = vod_filmgardi805[vod_filmgardi805.TitleCleaned1.str.contains("فصل فراموشی فریبا")]

vod_filmgardi809 = vod_filmgardi807[~vod_filmgardi807.TitleCleaned1.str.contains("فصل نرگس")]
vod_filmgardi810 = vod_filmgardi807[vod_filmgardi807.TitleCleaned1.str.contains("فصل نرگس")]

vod_filmgardi811 = vod_filmgardi809[~vod_filmgardi809.TitleCleaned1.str.contains("فصل ما")]
vod_filmgardi812 = vod_filmgardi809[vod_filmgardi809.TitleCleaned1.str.contains("فصل ما")]

vod_filmgardi813 = vod_filmgardi811[~vod_filmgardi811.TitleCleaned1.str.contains("فصل هرس")]
vod_filmgardi814 = vod_filmgardi811[vod_filmgardi811.TitleCleaned1.str.contains("فصل هرس")]

vod_filmgardi815 = vod_filmgardi813[~vod_filmgardi813.TitleCleaned1.str.contains("فصل پنجم")]
vod_filmgardi816 = vod_filmgardi813[vod_filmgardi813.TitleCleaned1.str.contains("فصل پنجم")]

vod_filmgardi817 = vod_filmgardi815[~vod_filmgardi815.TitleCleaned1.str.contains("فصلی از هستی")]
vod_filmgardi818 = vod_filmgardi815[vod_filmgardi815.TitleCleaned1.str.contains("فصلی از هستی")]

vod_filmgardi101 = vod_filmgardi817[~vod_filmgardi817.TitleCleaned1.str.contains("فصل ها")]
vod_filmgardi820 = vod_filmgardi817[vod_filmgardi817.TitleCleaned1.str.contains("فصل ها")]


vod_filmgardi200 = vod_filmgardi201.append([vod_filmgardi202, vod_filmgardi203, vod_filmgardi402, vod_filmgardi502, vod_filmgardi602, vod_filmgardi702, \
                                            vod_filmgardi802, vod_filmgardi804, vod_filmgardi806, vod_filmgardi808, vod_filmgardi810, vod_filmgardi812, \
                                            vod_filmgardi814, vod_filmgardi816, vod_filmgardi818, vod_filmgardi820])
del vod_filmgardi201
del vod_filmgardi202
del vod_filmgardi203
del vod_filmgardi100
del vod_filmgardi102
del vod_filmgardi401
del vod_filmgardi501
del vod_filmgardi601
del vod_filmgardi303
del vod_filmgardi402
del vod_filmgardi502
del vod_filmgardi602
del vod_filmgardi702
del vod_filmgardi110


del vod_filmgardi802
del vod_filmgardi804
del vod_filmgardi806
del vod_filmgardi808
del vod_filmgardi810
del vod_filmgardi812
del vod_filmgardi814
del vod_filmgardi816
del vod_filmgardi818
del vod_filmgardi820

del vod_filmgardi801
del vod_filmgardi803
del vod_filmgardi805
del vod_filmgardi807
del vod_filmgardi809
del vod_filmgardi811
del vod_filmgardi813
del vod_filmgardi815
del vod_filmgardi817

#########
vod_filmgardi101['TitleCleaned1'] = vod_filmgardi101['TitleCleaned1'].str.strip()

vod_filmgardi1 = vod_filmgardi101[vod_filmgardi101.TitleCleaned1.str.contains("فصل")]
vod_filmgardi2 = vod_filmgardi101[~vod_filmgardi101.TitleCleaned1.str.contains("فصل")]
vod_filmgardi1[["TitleCleaned1", "NoData"]]= vod_filmgardi1["TitleCleaned1"].str.split("فصل", n = 1, expand = True)
del vod_filmgardi1['NoData']
vod_filmgardi101 = vod_filmgardi1.append([vod_filmgardi2])
del vod_filmgardi1
del vod_filmgardi2
vod_filmgardi101['TitleCleaned1'] = vod_filmgardi101['TitleCleaned1'].str.strip()

#########
vod_filmgardi102 = vod_filmgardi101.append([vod_filmgardi200])

vod_filmgardi102['TitleCleaned1'] = vod_filmgardi102['TitleCleaned1'].str.replace('0', '۰')
vod_filmgardi102['TitleCleaned1'] = vod_filmgardi102['TitleCleaned1'].str.replace('1', '۱')
vod_filmgardi102['TitleCleaned1'] = vod_filmgardi102['TitleCleaned1'].str.replace('2', '۲')
vod_filmgardi102['TitleCleaned1'] = vod_filmgardi102['TitleCleaned1'].str.replace('3', '۳')
vod_filmgardi102['TitleCleaned1'] = vod_filmgardi102['TitleCleaned1'].str.replace('4', '۴')
vod_filmgardi102['TitleCleaned1'] = vod_filmgardi102['TitleCleaned1'].str.replace('5', '۵')
vod_filmgardi102['TitleCleaned1'] = vod_filmgardi102['TitleCleaned1'].str.replace('6', '۶')
vod_filmgardi102['TitleCleaned1'] = vod_filmgardi102['TitleCleaned1'].str.replace('7', '۷')
vod_filmgardi102['TitleCleaned1'] = vod_filmgardi102['TitleCleaned1'].str.replace('8', '۸')
vod_filmgardi102['TitleCleaned1'] = vod_filmgardi102['TitleCleaned1'].str.replace('9', '۹')
vod_filmgardi102['TitleCleaned1'] = vod_filmgardi102['TitleCleaned1'].str.replace('٠', '۰')
vod_filmgardi102['TitleCleaned1'] = vod_filmgardi102['TitleCleaned1'].str.replace('١', '۱')
vod_filmgardi102['TitleCleaned1'] = vod_filmgardi102['TitleCleaned1'].str.replace('٢', '۲')
vod_filmgardi102['TitleCleaned1'] = vod_filmgardi102['TitleCleaned1'].str.replace('٣', '۳')
vod_filmgardi102['TitleCleaned1'] = vod_filmgardi102['TitleCleaned1'].str.replace('٤', '۴')
vod_filmgardi102['TitleCleaned1'] = vod_filmgardi102['TitleCleaned1'].str.replace('٥', '۵')
vod_filmgardi102['TitleCleaned1'] = vod_filmgardi102['TitleCleaned1'].str.replace('٦', '۶')
vod_filmgardi102['TitleCleaned1'] = vod_filmgardi102['TitleCleaned1'].str.replace('٧', '۷')
vod_filmgardi102['TitleCleaned1'] = vod_filmgardi102['TitleCleaned1'].str.replace('٨', '۸')
vod_filmgardi102['TitleCleaned1'] = vod_filmgardi102['TitleCleaned1'].str.replace('٩', '۹')
vod_filmgardi102['TitleCleaned1'] = vod_filmgardi102['TitleCleaned1'].str.replace('ي', 'ی')
vod_filmgardi102['TitleCleaned1'] = vod_filmgardi102['TitleCleaned1'].str.replace('ؤ','و')
vod_filmgardi102['TitleCleaned1'] = vod_filmgardi102['TitleCleaned1'].str.replace('ك','ک')
vod_filmgardi102['TitleCleaned1'] = vod_filmgardi102['TitleCleaned1'].str.strip() 

##########
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
del total_vod_1399_Ghadimi_v2['فصل']

vod_filmgardi102.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode', 'Month', 'Year'], keep = 'last', inplace = True)
vod_filmgardi103 = pd.merge(vod_filmgardi102, total_vod_1399_Ghadimi_v2, on = ['TitleCleaned1'])
vod_filmgardi103['Season'] = vod_filmgardi103['season']
vod_filmgardi103['TitleCleaned1'] = vod_filmgardi103['title_first']
vod_filmgardi103['Season'] = vod_filmgardi103['season']
del vod_filmgardi103['season']
del vod_filmgardi103['title_first']
vod_filmgardi104 = vod_filmgardi102.append([vod_filmgardi103])
vod_filmgardi104.drop_duplicates(subset =['ID', 'Month', 'Year'], keep = 'last', inplace = True)

vod_filmgardi104['Epizode'] = vod_filmgardi104['Epizode'].astype(str)
vod_filmgardi104['Season'] = vod_filmgardi104['Season'].apply(lambda x: x.zfill(2))
vod_filmgardi104['Epizode'] = vod_filmgardi104['Epizode'].apply(lambda x: x.zfill(3))

################################
vod_filmgardi104 = vod_filmgardi104.reset_index()
del vod_filmgardi104['index']
vod_filmgardi104_1 = vod_filmgardi104 [vod_filmgardi104.Season.str.contains('00')]
vod_filmgardi104_2 = vod_filmgardi104 [~vod_filmgardi104.Season.str.contains('00')]
vod_filmgardi104_film = vod_filmgardi104_1 [vod_filmgardi104.Epizode.str.contains('000')]
vod_filmgardi104_4 = vod_filmgardi104_1 [~vod_filmgardi104.Epizode.str.contains('000')]
vod_filmgardi104_serial = vod_filmgardi104_2.append([vod_filmgardi104_4])
vod_filmgardi104_film['FilmSerial'] = 'فیلم'
vod_filmgardi104_film['code_FilmSerial'] = '01'
vod_filmgardi104_serial['FilmSerial'] = 'سریال'
vod_filmgardi104_serial['code_FilmSerial'] = '02'
vod_filmgardi1055 = vod_filmgardi104_film.append([vod_filmgardi104_serial])
del vod_filmgardi104_1
del vod_filmgardi104_2
del vod_filmgardi104_4

################################
vod_filmgardi105 = vod_filmgardi1055.copy()
vod_filmgardi105.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode', 'Month', 'Year', 'FilmSerial', 'code_FilmSerial'], keep = 'last', inplace = True)

db1_data = pd.DataFrame()
vod_total_db1 = vod_lenz119.append([vod_aio109, vod_tva112, vod_filimo112])
db1_data['TitleCleaned1'] = vod_total_db1['TitleCleaned1']
db1_data['Season'] = vod_total_db1['Season']
db1_data['Epizode'] = vod_total_db1['Epizode']
db1_data['ID_new'] = vod_total_db1['ID_new']
db1_data['IDS_new'] = vod_total_db1['IDS_new']

db1_data.drop_duplicates(subset =['ID_new'], keep = 'last', inplace = True)
vod_filmgardi_db1_merge = pd.merge(vod_filmgardi105, db1_data, on = ['TitleCleaned1', 'Season', 'Epizode'])
vod_filmgardi_db1_merge.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode', 'Month', 'Year'], keep = 'last', inplace = True)
vod_filmgardi105['ID_new'] = ''
vod_filmgardi105['IDS_new'] = ''

vod_append = vod_filmgardi_db1_merge.append([vod_filmgardi105])

vod_append = vod_append.reset_index()
del vod_append['index']
vod_append_dup = vod_append.copy()
vod_append_dup.drop_duplicates(subset =['Season', 'TitleCleaned1', 'Epizode'], keep = 'last', inplace = True)
vod_append_dup = vod_append_dup.reset_index()
del vod_append_dup['index']

vod_append_new = pd.DataFrame()
for i in range(0, len(vod_append_dup)):   #len(vod_append_dup)
    print(i)
    TT = vod_append_dup.loc[i, 'TitleCleaned1']
    SS = vod_append_dup.loc[i, 'Season']
    EE = vod_append_dup.loc[i, 'Epizode']
    title = vod_append.query("TitleCleaned1 == @TT")
    title = title.query("Season == @SS")
    title = title.query("Epizode == @EE")
    title = title.reset_index()
    del title['index']
    k = 0
    for j in range(0, len(title)):
        if '10' not in title.loc[j, 'IDS_new']:
            k = k + 1
    if j + 1 == k:
        vod_append_new = title.append([vod_append_new])

#vod_append.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode', 'Month', 'Year'], keep = False, inplace = True)
vod_filmgardi_step1 = vod_filmgardi_db1_merge.copy()

################################
del vod_append_new['ID_new']
del vod_append_new['IDS_new']
del db1_data['Epizode']
vod_append_new = vod_append_new.reset_index()
del vod_append_new['index']
db1_data.drop_duplicates(subset =['IDS_new'], keep = 'last', inplace = True)
vod_filmgardi_db1_merge2 = pd.merge(vod_append_new, db1_data, on = ['TitleCleaned1', 'Season'])

vod_filmgardi_db1_merge2.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode', 'Month', 'Year'], keep = 'last', inplace = True)
vod_append_new['ID_new'] = ''
vod_append_new['IDS_new'] = ''

vod_append2 = vod_filmgardi_db1_merge2.append([vod_append_new])

vod_append2 = vod_append2.reset_index()
del vod_append2['index']
vod_append2_dup = vod_append2.copy()
vod_append2_dup.drop_duplicates(subset =['Season', 'TitleCleaned1'], keep = 'last', inplace = True)
vod_append2_dup = vod_append2_dup.reset_index()
del vod_append2_dup['index']

vod_append2_new = pd.DataFrame()
for i in range(0, len(vod_append2_dup)):   #len(vod_append2_dup)
    print(i)
    TT = vod_append2_dup.loc[i, 'TitleCleaned1']
    SS = vod_append2_dup.loc[i, 'Season']
    title = vod_append2.query("TitleCleaned1 == @TT")
    title = title.query("Season == @SS")
    title = title.reset_index()
    del title['index']
    k = 0
    for j in range(0, len(title)):
        if '10' not in title.loc[j, 'IDS_new']:
            k = k + 1
    if j + 1 == k:
        vod_append2_new = title.append([vod_append2_new])

vod_filmgardi_step2 = vod_filmgardi_db1_merge2.copy()
vod_filmgardi_step2['ID_new'] = vod_filmgardi_step2['ID_new'].str[0:11] + vod_filmgardi_step2['Epizode']

################################
vod_append2_new = vod_append2_new.sort_values(['Season', 'TitleCleaned1'], ascending=[False, False])
vod_append2_new = vod_append2_new.reset_index()
del vod_append2_new['index']

id_first1 = max(vod_filimo112['ID_new'])
id_first2 = id_first1[0:7]
id_first = int(id_first2)+1    # write last id_first + 1
vod_append2_new.loc[0, 'id_first'] = id_first
for i in range(1, len(vod_append2_new)):
    print(i)
    if vod_append2_new.loc[i, 'TitleCleaned1'] == vod_append2_new.loc[i-1, 'TitleCleaned1'] and \
       vod_append2_new.loc[i, 'Season'] == vod_append2_new.loc[i-1, 'Season']:
        vod_append2_new.loc[i, 'id_first'] = id_first
    else:
        id_first = id_first + 1
        vod_append2_new.loc[i, 'id_first'] = id_first

vod_append2_new['id_first'] = vod_append2_new['id_first'].astype(int).astype(str) 
   
vod_append2_new['ID_new'] = vod_append2_new['id_first']+vod_append2_new['code_FilmSerial']+vod_append2_new['Season']+vod_append2_new['Epizode']
vod_append2_new['IDS_new'] = vod_append2_new['id_first']+vod_append2_new['Season']

del vod_append2_new['id_first']
################################
vod_filmgardi112 = vod_filmgardi_step1.append([vod_filmgardi_step2, vod_append2_new])
del vod_filmgardi112['code_FilmSerial']
vod_filmgardi112 = vod_filmgardi112.reset_index()
del vod_filmgardi112['index']

################################
################################
################################
################################
################################
################################
vod_all_data = vod_lenz119.append([vod_aio109, vod_tva112, vod_filimo112, vod_filmgardi112])
vod_all_data = vod_all_data.reset_index()
del vod_all_data['index']
vod_all_data_ID = pd.DataFrame()
vod_all_data_ID['TitleCleaned1'] = vod_all_data['TitleCleaned1']
vod_all_data_ID['Season'] = vod_all_data['Season']
vod_all_data_ID['ID_new'] = vod_all_data['ID_new']
vod_all_data_ID['IDS_new'] = vod_all_data['IDS_new']

db2_merge = pd.merge(db2, vod_all_data_ID, on = ['IDS'])
db2_merge.drop_duplicates(subset =['IDS'], keep = 'last', inplace = True)


db2_merge2 = pd.merge(db2, vod_all_data_ID, on = ['TitleCleaned1', 'Season'])
db2_merge2.drop_duplicates(subset =['IDS'], keep = 'last', inplace = True)



vod_filimo105.to_excel('vod_filimo105.xlsx', index = False)

a = vod_all_data[vod_all_data.TitleCleaned1.str.contains("ماتریکس")]
a = a[a.Month.str.contains("آبان")]
a = a.query("Season == '01'")
a = a.query("Epizode == '013'")
a = a.query("Year == '1400'")
a = a.query("Operators == 'آیو'")
print(len(a1))

a = vod_filimo112.append([vod_lenz119, vod_aio109, vod_tva112, vod_filmgardi112])

a =  vod_all_data.copy()
a =  vod_append2_new.copy()
a.drop_duplicates(subset =['ID_new', 'Operators', 'Year', 'Month'], keep = 'last', inplace = True)

a1 =  vod_filmgardi_step1.copy()
a2 =  vod_filmgardi_step2.copy()
b = a.loc[a.duplicated(subset =['ID_new', 'Operators', 'Year', 'Month']), :]
a1 = a.loc[a.duplicated(subset =['ID_new', 'Operators', 'Year', 'Month']), :]





