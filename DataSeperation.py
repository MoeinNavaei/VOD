
import pandas as pd
import numpy as np
import pyodbc
from pyodbc import *
import time
start = time.time()

################################## GET DATA ##########################################
#vod_aio_final
#vod_tva1
#vod_lenz_final
#vod_filimo_final
#vod_filmgardi_final
data_indut = vod_aio_final.copy()
Genres_primary = data_indut.copy()
Countries_primary = data_indut.copy()
Casts_primary = data_indut.copy()
Director_primary = data_indut.copy()
Language_primary = data_indut.copy()
ReleaseDateGeorgian_primary = data_indut.copy()
ReleaseDateJalali_primary = data_indut.copy()
Producer_primary = data_indut.copy()
Writer_primary = data_indut.copy()
Composer_primary = data_indut.copy()
Editor_primary = data_indut.copy()
Singer_primary = data_indut.copy()
Cameraman_primary = data_indut.copy()
Imdb_primary = data_indut.copy()
###################################### GET DATA FROM DB2_VOD #########################################
#drivers = pyodbc.drivers()
#conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
#                      'Server=localhost;'
#                      'Database=VOD;'
#                      'Trusted_Connection=yes;')
#cursor = conn.cursor()
#cursor.execute('SELECT * FROM DB2_VOD')
#records = cursor.fetchall()
#DataFromDB = []
#columnNames = [column[0] for column in cursor.description]
#for record in records:
#    DataFromDB.append( dict( zip( columnNames , record ) ) )
#df_DataFromDB = pd.DataFrame(DataFromDB)
#db2_VOD = df_DataFromDB.copy()
################################## GENRES ##########################################
Genres_primary['Genres'] = Genres_primary['Genres'].str.strip()
Genres_primary['Genres'].replace('', 'nan', inplace=True)
Genres_primary = Genres_primary.fillna('nan')
Genres_primary = Genres_primary[~Genres_primary.Genres.str.contains('nan')]
Genres_primary['Genres'] = Genres_primary['Genres'].str.replace(',', '،')
Genres_primary = Genres_primary.reset_index()
del Genres_primary['index']
Genres = pd.DataFrame()
for i in range(0, len(Genres_primary)):
    print(i)
    Genres_primary1 = Genres_primary.loc[i, 'Genres']
    Genres_primary1 = Genres_primary1.split('،')
    Genres_primary_df = pd.DataFrame({'Genres': Genres_primary1})
    Genres_primary_df.insert(1, 'IDS', '')
    for j in range(0, len(Genres_primary_df)):
        Genres_primary_df.loc[j, 'IDS'] = Genres_primary.loc[i, 'IDS']
    Genres = Genres.append(Genres_primary_df)

Genres['Genres'] = Genres['Genres'].str.strip()
Genres = Genres.reset_index()
del Genres['index']
Genres['Genres'] = Genres['Genres'].str.strip()
Genres['Genres'].replace('', 'nan', inplace=True)
Genres = Genres.fillna('nan')
Genres = Genres[~Genres.Genres.str.contains('nan')]
Genres = Genres.reset_index()
del Genres['index']
#################################### TRANSFER TO DATABASE OF GENRES ########################################
drivers = pyodbc.drivers()

conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

for index, row in Genres.iterrows():
    print(index)
    cursor.execute("INSERT INTO VOD.dbo.Genres (IDS,Genres) values(?,?)", row.IDS,row.Genres)
    conn.commit()
################################## COUNTRIES ##########################################
Countries_primary['Country'] = Countries_primary['Country'].str.strip()
Countries_primary['Country'].replace('', 'nan', inplace=True)
Countries_primary = Countries_primary.fillna('nan')
Countries_primary['Country'] = Countries_primary['Country'].str.replace(',', '،')
Countries_primary = Countries_primary[~Countries_primary.Country.str.contains('nan')]
Countries_primary = Countries_primary.reset_index()
del Countries_primary['index']
Countries = pd.DataFrame()
for i in range(0, len(Countries_primary)):
    print(i)
    Countries_primary1 = Countries_primary.loc[i, 'Country']
    Countries_primary1 = Countries_primary1.split('،')
    Countries_primary_df = pd.DataFrame({'Countries': Countries_primary1})
    Countries_primary_df.insert(1, 'IDS', '')
    for j in range(0, len(Countries_primary_df)):
        Countries_primary_df.loc[j, 'IDS'] = Countries_primary.loc[i, 'IDS']
    Countries = Countries.append(Countries_primary_df)

Countries['Countries'] = Countries['Countries'].str.strip()
Countries = Countries.reset_index()
del Countries['index']
Countries['Countries'] = Countries['Countries'].str.strip()
Countries['Countries'].replace('', 'nan', inplace=True)
Countries = Countries.fillna('nan')
Countries = Countries[~Countries.Countries.str.contains('nan')]
Countries = Countries.reset_index()
del Countries['index']
#################################### TRANSFER TO DATABASE OF COUNTRIES ########################################
drivers = pyodbc.drivers()

conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

for index, row in Countries.iterrows():
    print(index)
    cursor.execute("INSERT INTO VOD.dbo.Countries (IDS,Countries) values(?,?)", row.IDS,row.Countries)
    conn.commit()
################################## CASTS ##########################################
Casts_primary['Casts'] = Casts_primary['Casts'].str.strip()
Casts_primary['Casts'].replace('', 'nan', inplace=True)
Casts_primary = Casts_primary.fillna('nan')
Casts_primary = Casts_primary[~Casts_primary.Casts.str.contains('nan')]
Casts_primary['Casts'] = Casts_primary['Casts'].str.replace(',', '،')
Casts_primary = Casts_primary.reset_index()
del Casts_primary['index']
for i in range(0, len(Casts_primary)):
    print(i)
    if Casts_primary.loc[i, 'Casts'] == "":
        Casts_primary.loc[i, 'Casts'] = 'nan'
Casts_primary = Casts_primary[~Casts_primary.Casts.str.contains('nan')]
Casts_primary = Casts_primary.reset_index()
del Casts_primary['index']
Casts = pd.DataFrame()
for i in range(0, len(Casts_primary)):
    print(i)
    Casts_primary1 = Casts_primary.loc[i, 'Casts']
    Casts_primary1 = Casts_primary1.split('،')
    Casts_primary_df = pd.DataFrame({'Casts': Casts_primary1})
    Casts_primary_df.insert(1, 'IDS', '')
    for j in range(0, len(Casts_primary_df)):
        Casts_primary_df.loc[j, 'IDS'] = Casts_primary.loc[i, 'IDS']
    Casts = Casts.append(Casts_primary_df)

Casts['Casts'] = Casts['Casts'].str.strip()
Casts = Casts.reset_index()
del Casts['index']
Casts['Casts'] = Casts['Casts'].str.strip()
Casts['Casts'].replace('', 'nan', inplace=True)
Casts = Casts.fillna('nan')
Casts = Casts[~Casts.Casts.str.contains('nan')]
Casts = Casts.reset_index()
del Casts['index']
#################################### TRANSFER TO DATABASE OF CASTS ########################################
drivers = pyodbc.drivers()

conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

for index, row in Casts.iterrows():
    print(index)
    cursor.execute("INSERT INTO VOD.dbo.Casts (IDS,Casts) values(?,?)", row.IDS,row.Casts)
    conn.commit()

################################## Director ##########################################
Director_primary['Director'] = Director_primary['Director'].str.strip()
Director_primary['Director'].replace('', 'nan', inplace=True)
Director_primary = Director_primary.fillna('nan')
Director_primary = Director_primary[~Director_primary.Director.str.contains('nan')]
Director_primary['Director'] = Director_primary['Director'].str.replace(',', '،')
Director_primary = Director_primary.reset_index()
del Director_primary['index']
for i in range(0, len(Director_primary)):
    print(i)
    if Director_primary.loc[i, 'Director'] == "":
        Director_primary.loc[i, 'Director'] = 'nan'
Director_primary = Director_primary[~Director_primary.Director.str.contains('nan')]
Director_primary = Director_primary.reset_index()
del Director_primary['index']
Director = pd.DataFrame()
for i in range(0, len(Director_primary)):
    print(i)
    Director_primary1 = Director_primary.loc[i, 'Director']
    Director_primary1 = Director_primary1.split('،')
    Director_primary_df = pd.DataFrame({'Director': Director_primary1})
    Director_primary_df.insert(1, 'IDS', '')
    for j in range(0, len(Director_primary_df)):
        Director_primary_df.loc[j, 'IDS'] = Director_primary.loc[i, 'IDS']
    Director = Director.append(Director_primary_df)

Director['Director'] = Director['Director'].str.strip()
Director = Director.reset_index()
del Director['index']
Director['Director'] = Director['Director'].str.strip()
Director['Director'].replace('', 'nan', inplace=True)
Director = Director.fillna('nan')
Director = Director[~Director.Director.str.contains('nan')]
Director = Director.reset_index()
del Director['index']
#################################### TRANSFER TO DATABASE OF Director ########################################
drivers = pyodbc.drivers()

conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

for index, row in Director.iterrows():
    print(index)
    cursor.execute("INSERT INTO VOD.dbo.Director (IDS,Director) values(?,?)", row.IDS,row.Director)
    conn.commit()

################################## Language ##########################################
Language_primary['Language'] = Language_primary['Language'].str.strip()
Language_primary['Language'].replace('', 'nan', inplace=True)
Language_primary = Language_primary.fillna('nan')
Language_primary = Language_primary[~Language_primary.Language.str.contains('nan')]
Language_primary['Language'] = Language_primary['Language'].str.replace(',', '،')
Language_primary = Language_primary.reset_index()
del Language_primary['index']
for i in range(0, len(Language_primary)):
    print(i)
    if Language_primary.loc[i, 'Language'] == "":
        Language_primary.loc[i, 'Language'] = 'nan'
Language_primary = Language_primary[~Language_primary.Language.str.contains('nan')]
Language_primary = Language_primary.reset_index()
del Language_primary['index']
Language = pd.DataFrame()
for i in range(0, len(Language_primary)):
    print(i)
    Language_primary1 = Language_primary.loc[i, 'Language']
    Language_primary1 = Language_primary1.split('،')
    Language_primary_df = pd.DataFrame({'Language': Language_primary1})
    Language_primary_df.insert(1, 'IDS', '')
    for j in range(0, len(Language_primary_df)):
        Language_primary_df.loc[j, 'IDS'] = Language_primary.loc[i, 'IDS']
    Language = Language.append(Language_primary_df)

Language['Language'] = Language['Language'].str.strip()
Language = Language.reset_index()
del Language['index']
Language['Language'] = Language['Language'].str.strip()
Language['Language'].replace('', 'nan', inplace=True)
Language = Language.fillna('nan')
Language = Language[~Language.Language.str.contains('nan')]
Language = Language.reset_index()
del Language['index']
#################################### TRANSFER TO DATABASE OF Language ########################################
drivers = pyodbc.drivers()

conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

for index, row in Language.iterrows():
    print(index)
    cursor.execute("INSERT INTO VOD.dbo.Language (IDS,Language) values(?,?)", row.IDS,row.Language)
    conn.commit()

################################## ReleaseDateGeorgian ##########################################
ReleaseDateGeorgian_primary['ReleaseDateGeorgian'] = ReleaseDateGeorgian_primary['ReleaseDateGeorgian'].astype(str).replace('\.0', '', regex=True)
ReleaseDateGeorgian_primary['ReleaseDateGeorgian'] = ReleaseDateGeorgian_primary['ReleaseDateGeorgian'].str.strip()
ReleaseDateGeorgian_primary['ReleaseDateGeorgian'].replace('', 'nan', inplace=True)
ReleaseDateGeorgian_primary = ReleaseDateGeorgian_primary.fillna('nan')
ReleaseDateGeorgian_primary = ReleaseDateGeorgian_primary[~ReleaseDateGeorgian_primary.ReleaseDateGeorgian.str.contains('nan')]
ReleaseDateGeorgian_primary['ReleaseDateGeorgian'] = ReleaseDateGeorgian_primary['ReleaseDateGeorgian'].str.replace(',', '،')
ReleaseDateGeorgian_primary = ReleaseDateGeorgian_primary.reset_index()
del ReleaseDateGeorgian_primary['index']
for i in range(0, len(ReleaseDateGeorgian_primary)):
    print(i)
    if ReleaseDateGeorgian_primary.loc[i, 'ReleaseDateGeorgian'] == "":
        ReleaseDateGeorgian_primary.loc[i, 'ReleaseDateGeorgian'] = 'nan'
ReleaseDateGeorgian_primary = ReleaseDateGeorgian_primary[~ReleaseDateGeorgian_primary.ReleaseDateGeorgian.str.contains('nan')]
ReleaseDateGeorgian_primary = ReleaseDateGeorgian_primary.reset_index()
del ReleaseDateGeorgian_primary['index']
ReleaseDateGeorgian = pd.DataFrame()
for i in range(0, len(ReleaseDateGeorgian_primary)):
    print(i)
    ReleaseDateGeorgian_primary1 = ReleaseDateGeorgian_primary.loc[i, 'ReleaseDateGeorgian']
    ReleaseDateGeorgian_primary1 = ReleaseDateGeorgian_primary1.split('،')
    ReleaseDateGeorgian_primary_df = pd.DataFrame({'ReleaseDateGeorgian': ReleaseDateGeorgian_primary1})
    ReleaseDateGeorgian_primary_df.insert(1, 'IDS', '')
    for j in range(0, len(ReleaseDateGeorgian_primary_df)):
        ReleaseDateGeorgian_primary_df.loc[j, 'IDS'] = ReleaseDateGeorgian_primary.loc[i, 'IDS']
    ReleaseDateGeorgian = ReleaseDateGeorgian.append(ReleaseDateGeorgian_primary_df)

ReleaseDateGeorgian['ReleaseDateGeorgian'] = ReleaseDateGeorgian['ReleaseDateGeorgian'].str.strip()
ReleaseDateGeorgian = ReleaseDateGeorgian.reset_index()
del ReleaseDateGeorgian['index']
ReleaseDateGeorgian['ReleaseDateGeorgian'] = ReleaseDateGeorgian['ReleaseDateGeorgian'].str.strip()
ReleaseDateGeorgian['ReleaseDateGeorgian'].replace('', 'nan', inplace=True)
ReleaseDateGeorgian = ReleaseDateGeorgian.fillna('nan')
ReleaseDateGeorgian = ReleaseDateGeorgian[~ReleaseDateGeorgian.ReleaseDateGeorgian.str.contains('nan')]
ReleaseDateGeorgian = ReleaseDateGeorgian.reset_index()
del ReleaseDateGeorgian['index']
#################################### TRANSFER TO DATABASE OF ReleaseDateGeorgian ########################################
drivers = pyodbc.drivers()

conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

for index, row in ReleaseDateGeorgian.iterrows():
    print(index)
    cursor.execute("INSERT INTO VOD.dbo.ReleaseDateGeorgian (IDS,ReleaseDateGeorgian) values(?,?)", row.IDS,row.ReleaseDateGeorgian)
    conn.commit()

################################## ReleaseDateJalali ##########################################
ReleaseDateJalali_primary = ReleaseDateJalali_primary.fillna('0000')
ReleaseDateJalali_primary['ReleaseDateJalali'].replace('', '0000', inplace=True)
ReleaseDateJalali_primary['ReleaseDateJalali'] = ReleaseDateJalali_primary['ReleaseDateJalali'].astype(int).astype(str)
ReleaseDateJalali_primary['ReleaseDateJalali'] = ReleaseDateJalali_primary['ReleaseDateJalali'].str.strip()
ReleaseDateJalali_primary['ReleaseDateJalali'] = ReleaseDateJalali_primary['ReleaseDateJalali'].apply(lambda x: x.zfill(4))
#ReleaseDateJalali_primary['ReleaseDateJalali'].replace('', 'nan', inplace=True)
#ReleaseDateJalali_primary = ReleaseDateJalali_primary.fillna('nan')
ReleaseDateJalali_primary = ReleaseDateJalali_primary[~ReleaseDateJalali_primary.ReleaseDateJalali.str.contains('0000')]
ReleaseDateJalali_primary['ReleaseDateJalali'] = ReleaseDateJalali_primary['ReleaseDateJalali'].str.replace(',', '،')
ReleaseDateJalali_primary = ReleaseDateJalali_primary.reset_index()
del ReleaseDateJalali_primary['index']
for i in range(0, len(ReleaseDateJalali_primary)):
    print(i)
    if ReleaseDateJalali_primary.loc[i, 'ReleaseDateJalali'] == "":
        ReleaseDateJalali_primary.loc[i, 'ReleaseDateJalali'] = 'nan'
ReleaseDateJalali_primary = ReleaseDateJalali_primary[~ReleaseDateJalali_primary.ReleaseDateJalali.str.contains('nan')]
ReleaseDateJalali_primary = ReleaseDateJalali_primary.reset_index()
del ReleaseDateJalali_primary['index']
ReleaseDateJalali = pd.DataFrame()
for i in range(0, len(ReleaseDateJalali_primary)):
    print(i)
    ReleaseDateJalali_primary1 = ReleaseDateJalali_primary.loc[i, 'ReleaseDateJalali']
    ReleaseDateJalali_primary1 = ReleaseDateJalali_primary1.split('،')
    ReleaseDateJalali_primary_df = pd.DataFrame({'ReleaseDateJalali': ReleaseDateJalali_primary1})
    ReleaseDateJalali_primary_df.insert(1, 'IDS', '')
    for j in range(0, len(ReleaseDateJalali_primary_df)):
        ReleaseDateJalali_primary_df.loc[j, 'IDS'] = ReleaseDateJalali_primary.loc[i, 'IDS']
    ReleaseDateJalali = ReleaseDateJalali.append(ReleaseDateJalali_primary_df)

ReleaseDateJalali['ReleaseDateJalali'] = ReleaseDateJalali['ReleaseDateJalali'].str.strip()
ReleaseDateJalali = ReleaseDateJalali.reset_index()
del ReleaseDateJalali['index']
ReleaseDateJalali['ReleaseDateJalali'] = ReleaseDateJalali['ReleaseDateJalali'].str.strip()
ReleaseDateJalali['ReleaseDateJalali'].replace('', 'nan', inplace=True)
ReleaseDateJalali = ReleaseDateJalali.fillna('nan')
ReleaseDateJalali = ReleaseDateJalali[~ReleaseDateJalali.ReleaseDateJalali.str.contains('nan')]
ReleaseDateJalali = ReleaseDateJalali.reset_index()
del ReleaseDateJalali['index']
#################################### TRANSFER TO DATABASE OF ReleaseDateJalali ########################################
drivers = pyodbc.drivers()

conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

for index, row in ReleaseDateJalali.iterrows():
    print(index)
    cursor.execute("INSERT INTO VOD.dbo.ReleaseDateJalali (IDS,ReleaseDateJalali) values(?,?)", row.IDS,row.ReleaseDateJalali)
    conn.commit()

################################## Producer ##########################################
Producer_primary['Producer'] = Producer_primary['Producer'].str.strip()
Producer_primary['Producer'].replace('', 'nan', inplace=True)
Producer_primary = Producer_primary.fillna('nan')
Producer_primary = Producer_primary[~Producer_primary.Producer.str.contains('nan')]
Producer_primary['Producer'] = Producer_primary['Producer'].str.replace(',', '،')
Producer_primary = Producer_primary.reset_index()
del Producer_primary['index']
for i in range(0, len(Producer_primary)):
    print(i)
    if Producer_primary.loc[i, 'Producer'] == "":
        Producer_primary.loc[i, 'Producer'] = 'nan'
Producer_primary = Producer_primary[~Producer_primary.Producer.str.contains('nan')]
Producer_primary = Producer_primary.reset_index()
del Producer_primary['index']
Producer = pd.DataFrame()
for i in range(0, len(Producer_primary)):
    print(i)
    Producer_primary1 = Producer_primary.loc[i, 'Producer']
    Producer_primary1 = Producer_primary1.split('،')
    Producer_primary_df = pd.DataFrame({'Producer': Producer_primary1})
    Producer_primary_df.insert(1, 'IDS', '')
    for j in range(0, len(Producer_primary_df)):
        Producer_primary_df.loc[j, 'IDS'] = Producer_primary.loc[i, 'IDS']
    Producer = Producer.append(Producer_primary_df)

Producer['Producer'] = Producer['Producer'].str.strip()
Producer = Producer.reset_index()
del Producer['index']
Producer['Producer'] = Producer['Producer'].str.strip()
Producer['Producer'].replace('', 'nan', inplace=True)
Producer = Producer.fillna('nan')
Producer = Producer[~Producer.Producer.str.contains('nan')]
Producer = Producer.reset_index()
del Producer['index']
#################################### TRANSFER TO DATABASE OF Producer ########################################
drivers = pyodbc.drivers()

conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

for index, row in Producer.iterrows():
    print(index)
    cursor.execute("INSERT INTO VOD.dbo.Producer (IDS,Producer) values(?,?)", row.IDS,row.Producer)
    conn.commit()

################################## Composer ##########################################
Composer_primary['Composer'] = Composer_primary['Composer'].str.strip()
Composer_primary['Composer'].replace('', 'nan', inplace=True)
Composer_primary = Composer_primary.fillna('nan')
Composer_primary = Composer_primary[~Composer_primary.Composer.str.contains('nan')]
Composer_primary['Composer'] = Composer_primary['Composer'].str.replace(',', '،')
Composer_primary = Composer_primary.reset_index()
del Composer_primary['index']
for i in range(0, len(Composer_primary)):
    print(i)
    if Composer_primary.loc[i, 'Composer'] == "":
        Composer_primary.loc[i, 'Composer'] = 'nan'
Composer_primary = Composer_primary[~Composer_primary.Composer.str.contains('nan')]
Composer_primary = Composer_primary.reset_index()
del Composer_primary['index']
Composer = pd.DataFrame()
for i in range(0, len(Composer_primary)):
    print(i)
    Composer_primary1 = Composer_primary.loc[i, 'Composer']
    Composer_primary1 = Composer_primary1.split('،')
    Composer_primary_df = pd.DataFrame({'Composer': Composer_primary1})
    Composer_primary_df.insert(1, 'IDS', '')
    for j in range(0, len(Composer_primary_df)):
        Composer_primary_df.loc[j, 'IDS'] = Composer_primary.loc[i, 'IDS']
    Composer = Composer.append(Composer_primary_df)

Composer['Composer'] = Composer['Composer'].str.strip()
Composer = Composer.reset_index()
del Composer['index']
Composer['Composer'] = Composer['Composer'].str.strip()
Composer['Composer'].replace('', 'nan', inplace=True)
Composer = Composer.fillna('nan')
Composer = Composer[~Composer.Composer.str.contains('nan')]
Composer = Composer.reset_index()
del Composer['index']
#################################### TRANSFER TO DATABASE OF Composer ########################################
drivers = pyodbc.drivers()

conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

for index, row in Composer.iterrows():
    print(index)
    cursor.execute("INSERT INTO VOD.dbo.Composer (IDS,Composer) values(?,?)", row.IDS,row.Composer)
    conn.commit()

################################## Editor ##########################################
Editor_primary['Editor'] = Editor_primary['Editor'].str.strip()
Editor_primary['Editor'].replace('', 'nan', inplace=True)
Editor_primary = Editor_primary.fillna('nan')
Editor_primary = Editor_primary[~Editor_primary.Editor.str.contains('nan')]
Editor_primary['Editor'] = Editor_primary['Editor'].str.replace(',', '،')
Editor_primary = Editor_primary.reset_index()
del Editor_primary['index']
for i in range(0, len(Editor_primary)):
    print(i)
    if Editor_primary.loc[i, 'Editor'] == "":
        Editor_primary.loc[i, 'Editor'] = 'nan'
Editor_primary = Editor_primary[~Editor_primary.Editor.str.contains('nan')]
Editor_primary = Editor_primary.reset_index()
del Editor_primary['index']
Editor = pd.DataFrame()
for i in range(0, len(Editor_primary)):
    print(i)
    Editor_primary1 = Editor_primary.loc[i, 'Editor']
    Editor_primary1 = Editor_primary1.split('،')
    Editor_primary_df = pd.DataFrame({'Editor': Editor_primary1})
    Editor_primary_df.insert(1, 'IDS', '')
    for j in range(0, len(Editor_primary_df)):
        Editor_primary_df.loc[j, 'IDS'] = Editor_primary.loc[i, 'IDS']
    Editor = Editor.append(Editor_primary_df)

Editor['Editor'] = Editor['Editor'].str.strip()
Editor = Editor.reset_index()
del Editor['index']
Editor['Editor'] = Editor['Editor'].str.strip()
Editor['Editor'].replace('', 'nan', inplace=True)
Editor = Editor.fillna('nan')
Editor = Editor[~Editor.Editor.str.contains('nan')]
Editor = Editor.reset_index()
del Editor['index']
#################################### TRANSFER TO DATABASE OF Editor ########################################
drivers = pyodbc.drivers()

conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

for index, row in Editor.iterrows():
    print(index)
    cursor.execute("INSERT INTO VOD.dbo.Editor (IDS,Editor) values(?,?)", row.IDS,row.Editor)
    conn.commit()

################################## Singer ##########################################
Singer_primary['Singer'] = Singer_primary['Singer'].str.strip()
Singer_primary['Singer'].replace('', 'nan', inplace=True)
Singer_primary = Singer_primary.fillna('nan')
Singer_primary = Singer_primary[~Singer_primary.Singer.str.contains('nan')]
Singer_primary['Singer'] = Singer_primary['Singer'].str.replace(',', '،')
Singer_primary = Singer_primary.reset_index()
del Singer_primary['index']
for i in range(0, len(Singer_primary)):
    print(i)
    if Singer_primary.loc[i, 'Singer'] == "":
        Singer_primary.loc[i, 'Singer'] = 'nan'
Singer_primary = Singer_primary[~Singer_primary.Singer.str.contains('nan')]
Singer_primary = Singer_primary.reset_index()
del Singer_primary['index']
Singer = pd.DataFrame()
for i in range(0, len(Singer_primary)):
    print(i)
    Singer_primary1 = Singer_primary.loc[i, 'Singer']
    Singer_primary1 = Singer_primary1.split('،')
    Singer_primary_df = pd.DataFrame({'Singer': Singer_primary1})
    Singer_primary_df.insert(1, 'IDS', '')
    for j in range(0, len(Singer_primary_df)):
        Singer_primary_df.loc[j, 'IDS'] = Singer_primary.loc[i, 'IDS']
    Singer = Singer.append(Singer_primary_df)

Singer['Singer'] = Singer['Singer'].str.strip()
Singer = Singer.reset_index()
del Singer['index']
Singer['Singer'] = Singer['Singer'].str.strip()
Singer['Singer'].replace('', 'nan', inplace=True)
Singer = Singer.fillna('nan')
Singer = Singer[~Singer.Singer.str.contains('nan')]
Singer = Singer.reset_index()
del Singer['index']
#################################### TRANSFER TO DATABASE OF Singer ########################################
drivers = pyodbc.drivers()

conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

for index, row in Singer.iterrows():
    print(index)
    cursor.execute("INSERT INTO VOD.dbo.Singer (IDS,Singer) values(?,?)", row.IDS,row.Singer)
    conn.commit()

################################## Cameraman ##########################################
Cameraman_primary['Cameraman'] = Cameraman_primary['Cameraman'].str.strip()
Cameraman_primary['Cameraman'].replace('', 'nan', inplace=True)
Cameraman_primary = Cameraman_primary.fillna('nan')
Cameraman_primary = Cameraman_primary[~Cameraman_primary.Cameraman.str.contains('nan')]
Cameraman_primary['Cameraman'] = Cameraman_primary['Cameraman'].str.replace(',', '،')
Cameraman_primary = Cameraman_primary.reset_index()
del Cameraman_primary['index']
for i in range(0, len(Cameraman_primary)):
    print(i)
    if Cameraman_primary.loc[i, 'Cameraman'] == "":
        Cameraman_primary.loc[i, 'Cameraman'] = 'nan'
Cameraman_primary = Cameraman_primary[~Cameraman_primary.Cameraman.str.contains('nan')]
Cameraman_primary = Cameraman_primary.reset_index()
del Cameraman_primary['index']
Cameraman = pd.DataFrame()
for i in range(0, len(Cameraman_primary)):
    print(i)
    Cameraman_primary1 = Cameraman_primary.loc[i, 'Cameraman']
    Cameraman_primary1 = Cameraman_primary1.split('،')
    Cameraman_primary_df = pd.DataFrame({'Cameraman': Cameraman_primary1})
    Cameraman_primary_df.insert(1, 'IDS', '')
    for j in range(0, len(Cameraman_primary_df)):
        Cameraman_primary_df.loc[j, 'IDS'] = Cameraman_primary.loc[i, 'IDS']
    Cameraman = Cameraman.append(Cameraman_primary_df)

Cameraman['Cameraman'] = Cameraman['Cameraman'].str.strip()
Cameraman = Cameraman.reset_index()
del Cameraman['index']
Cameraman['Cameraman'] = Cameraman['Cameraman'].str.strip()
Cameraman['Cameraman'].replace('', 'nan', inplace=True)
Cameraman = Cameraman.fillna('nan')
Cameraman = Cameraman[~Cameraman.Cameraman.str.contains('nan')]
Cameraman = Cameraman.reset_index()
del Cameraman['index']
#################################### TRANSFER TO DATABASE OF Cameraman ########################################
drivers = pyodbc.drivers()

conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

for index, row in Cameraman.iterrows():
    print(index)
    cursor.execute("INSERT INTO VOD.dbo.Cameraman (IDS,Cameraman) values(?,?)", row.IDS,row.Cameraman)
    conn.commit()

################################## Writer ##########################################
Writer_primary['Writer'] = Writer_primary['Writer'].str.strip()
Writer_primary['Writer'].replace('', 'nan', inplace=True)
Writer_primary = Writer_primary.fillna('nan')
Writer_primary = Writer_primary[~Writer_primary.Writer.str.contains('nan')]
Writer_primary['Writer'] = Writer_primary['Writer'].str.replace(',', '،')
Writer_primary = Writer_primary.reset_index()
del Writer_primary['index']
for i in range(0, len(Writer_primary)):
    print(i)
    if Writer_primary.loc[i, 'Writer'] == "":
        Writer_primary.loc[i, 'Writer'] = 'nan'
Writer_primary = Writer_primary[~Writer_primary.Writer.str.contains('nan')]
Writer_primary = Writer_primary.reset_index()
del Writer_primary['index']
Writer = pd.DataFrame()
for i in range(0, len(Writer_primary)):
    print(i)
    Writer_primary1 = Writer_primary.loc[i, 'Writer']
    Writer_primary1 = Writer_primary1.split('،')
    Writer_primary_df = pd.DataFrame({'Writer': Writer_primary1})
    Writer_primary_df.insert(1, 'IDS', '')
    for j in range(0, len(Writer_primary_df)):
        Writer_primary_df.loc[j, 'IDS'] = Writer_primary.loc[i, 'IDS']
    Writer = Writer.append(Writer_primary_df)

Writer['Writer'] = Writer['Writer'].str.strip()
Writer = Writer.reset_index()
del Writer['index']
Writer['Writer'] = Writer['Writer'].str.strip()
Writer['Writer'].replace('', 'nan', inplace=True)
Writer = Writer.fillna('nan')
Writer = Writer[~Writer.Writer.str.contains('nan')]
Writer = Writer.reset_index()
del Writer['index']
#################################### TRANSFER TO DATABASE OF Writer ########################################
drivers = pyodbc.drivers()

conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

for index, row in Writer.iterrows():
    print(index)
    cursor.execute("INSERT INTO VOD.dbo.Writer (IDS,Writer) values(?,?)", row.IDS,row.Writer)
    conn.commit()

################################## Imdb ##########################################
Imdb_primary = data_indut.copy()
Imdb_primary['Imdb'] = Imdb_primary['Imdb'].astype(str)
Imdb_primary['Imdb'] = Imdb_primary['Imdb'].str.strip()
Imdb_primary['Imdb'].replace('', 'nan', inplace=True)
Imdb_primary = Imdb_primary.fillna('nan')
Imdb_primary = Imdb_primary[~Imdb_primary.Imdb.str.contains('nan')]
Imdb_primary = Imdb_primary.reset_index()
del Imdb_primary['index']
for i in range(0, len(Imdb_primary)):
    print(i)
    if Imdb_primary.loc[i, 'Imdb'] == "":
        Imdb_primary.loc[i, 'Imdb'] = 'nan'
Imdb_primary = Imdb_primary[~Imdb_primary.Imdb.str.contains('nan')]
Imdb_primary = Imdb_primary.reset_index()
del Imdb_primary['index']
Imdb = pd.DataFrame()
for i in range(0, len(Imdb_primary)):
    print(i)
    Imdb_primary1 = Imdb_primary.loc[i, 'Imdb']
    Imdb_primary1 = Imdb_primary1.split('،')
    Imdb_primary_df = pd.DataFrame({'Imdb': Imdb_primary1})
    Imdb_primary_df.insert(1, 'IDS', '')
    for j in range(0, len(Imdb_primary_df)):
        Imdb_primary_df.loc[j, 'IDS'] = Imdb_primary.loc[i, 'IDS']
    Imdb = Imdb.append(Imdb_primary_df)

Imdb['Imdb'] = Imdb['Imdb'].str.strip()
Imdb = Imdb.reset_index()
del Imdb['index']
Imdb['Imdb'] = Imdb['Imdb'].str.strip()
Imdb['Imdb'].replace('', 'nan', inplace=True)
Imdb = Imdb.fillna('nan')
Imdb = Imdb[~Imdb.Imdb.str.contains('nan')]
Imdb = Imdb.reset_index()
del Imdb['index']
Imdb['ImdbRound'] = Imdb['Imdb']
Imdb['ImdbRound'] = Imdb['ImdbRound'].astype(float).astype(int)
Imdb['ImdbRound'] = Imdb['ImdbRound'].astype(int).astype(str)
#################################### TRANSFER TO DATABASE OF Imdb ########################################
drivers = pyodbc.drivers()

conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

for index, row in Imdb.iterrows():
    print(index)
    cursor.execute("INSERT INTO VOD.dbo.Imdb (IDS,Imdb,ImdbRound) values(?,?,?)", row.IDS,row.Imdb,row.ImdbRound)
    conn.commit()












