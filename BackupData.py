import pandas as pd
import numpy as np
import pyodbc
from pyodbc import *
from sqlalchemy import create_engine
#engine = create_engine('postgresql://postgres:12344321@10.32.141.17/Moein01',pool_size=20, max_overflow=100,)
#con=engine.connect()
################################ GET DATA FROM DATABASE 1 #################################
print("*******************")
print("step 1 of 17")
print("Get db1_VOD from SQL Server")
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
print("Convert db1_VOD to Excel and CSV")
db1_VOD.to_excel('E:\python codes\VOD\BackupData\db1_VOD.xlsx', index = False)
db1_VOD.to_csv('E:\python codes\VOD\BackupData\db1_VOD.csv', index = False)
print("Sending db1_VOD to PostgreSQL")
engine = create_engine('postgresql://postgres:12344321@10.32.141.17/Moein01',pool_size=20, max_overflow=100,)
con=engine.connect()
db1_VOD.to_sql('DB1_VOD',con,if_exists='replace', index=False)
con.close()

################################ GET DATA FROM DATABASE 2 #################################
print("*******************")
print("step 2 of 17")
print("Get db2_VOD from SQL Server")
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
print("Convert db2_VOD to Excel and CSV")
db2_VOD.to_excel('E:\python codes\VOD\BackupData\db2_VOD.xlsx', index = False)
db2_VOD.to_csv('E:\python codes\VOD\BackupData\db2_VOD.csv', index = False)
print("Sending db2_VOD to PostgreSQL")
engine = create_engine('postgresql://postgres:12344321@10.32.141.17/Moein01',pool_size=20, max_overflow=100,)
con=engine.connect()
db2_VOD.to_sql('DB2_VOD',con,if_exists='replace', index=False)
con.close()

################################ GET DATA FROM Cameraman #################################
print("*******************")
print("step 3 of 17")
print("Get Cameraman from SQL Server")
conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')
cursor = conn.cursor()
cursor.execute('SELECT * FROM Cameraman')
records = cursor.fetchall()
Cameraman = []
columnNames = [column[0] for column in cursor.description]
for record in records:
#    print(record)
    Cameraman.append( dict( zip( columnNames , record ) ) )
Cameraman = pd.DataFrame(Cameraman)
print("Convert Cameraman to Excel and CSV")
Cameraman.to_excel('E:\python codes\VOD\BackupData\Cameraman.xlsx', index = False)
Cameraman.to_csv('E:\python codes\VOD\BackupData\Cameraman.csv', index = False)
print("Sending Cameraman to PostgreSQL")
engine = create_engine('postgresql://postgres:12344321@10.32.141.17/Moein01',pool_size=20, max_overflow=100,)
con=engine.connect()
Cameraman.to_sql('Cameraman',con,if_exists='replace', index=False)
con.close()

################################ GET DATA FROM Casts #################################
print("*******************")
print("step 4 of 17")
print("Get Casts from SQL Server")
conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')
cursor = conn.cursor()
cursor.execute('SELECT * FROM Casts')
records = cursor.fetchall()
Casts = []
columnNames = [column[0] for column in cursor.description]
for record in records:
#    print(record)
    Casts.append( dict( zip( columnNames , record ) ) )
Casts = pd.DataFrame(Casts)
print("Convert Casts to Excel and CSV")
Casts.to_excel('E:\python codes\VOD\BackupData\Casts.xlsx', index = False)
Casts.to_csv('E:\python codes\VOD\BackupData\Casts.csv', index = False)
print("Sending Casts to PostgreSQL")
engine = create_engine('postgresql://postgres:12344321@10.32.141.17/Moein01',pool_size=20, max_overflow=100,)
con=engine.connect()
Casts.to_sql('Casts',con,if_exists='replace', index=False)
con.close()

################################ GET DATA FROM Composer #################################
print("*******************")
print("step 5 of 17")
print("Get Composer from SQL Server")
conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')
cursor = conn.cursor()
cursor.execute('SELECT * FROM Composer')
records = cursor.fetchall()
Composer = []
columnNames = [column[0] for column in cursor.description]
for record in records:
#    print(record)
    Composer.append( dict( zip( columnNames , record ) ) )
Composer = pd.DataFrame(Composer)
print("Convert Composer to Excel and CSV")
Composer.to_excel('E:\python codes\VOD\BackupData\Composer.xlsx', index = False)
Composer.to_csv('E:\python codes\VOD\BackupData\Composer.csv', index = False)
print("Sending Composer to PostgreSQL")
engine = create_engine('postgresql://postgres:12344321@10.32.141.17/Moein01',pool_size=20, max_overflow=100,)
con=engine.connect()
Composer.to_sql('Composer',con,if_exists='replace', index=False)
con.close()

################################ GET DATA FROM Countries #################################
print("*******************")
print("step 6 of 17")
print("Get Countries from SQL Server")
conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')
cursor = conn.cursor()
cursor.execute('SELECT * FROM Countries')
records = cursor.fetchall()
Countries = []
columnNames = [column[0] for column in cursor.description]
for record in records:
#    print(record)
    Countries.append( dict( zip( columnNames , record ) ) )
Countries = pd.DataFrame(Countries)
print("Convert Countries to Excel and CSV")
Countries.to_excel('E:\python codes\VOD\BackupData\Countries.xlsx', index = False)
Countries.to_csv('E:\python codes\VOD\BackupData\Countries.csv', index = False)
print("Sending Countries to PostgreSQL")
engine = create_engine('postgresql://postgres:12344321@10.32.141.17/Moein01',pool_size=20, max_overflow=100,)
con=engine.connect()
Countries.to_sql('Countries',con,if_exists='replace', index=False)
con.close()

################################ GET DATA FROM Director #################################
print("*******************")
print("step 7 of 17")
print("Get Director from SQL Server")
conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')
cursor = conn.cursor()
cursor.execute('SELECT * FROM Director')
records = cursor.fetchall()
Director = []
columnNames = [column[0] for column in cursor.description]
for record in records:
#    print(record)
    Director.append( dict( zip( columnNames , record ) ) )
Director = pd.DataFrame(Director)
print("Convert Director to Excel and CSV")
Director.to_excel('E:\python codes\VOD\BackupData\Director.xlsx', index = False)
Director.to_csv('E:\python codes\VOD\BackupData\Director.csv', index = False)
print("Sending Director to PostgreSQL")
engine = create_engine('postgresql://postgres:12344321@10.32.141.17/Moein01',pool_size=20, max_overflow=100,)
con=engine.connect()
Director.to_sql('Director',con,if_exists='replace', index=False)
con.close()

################################ GET DATA FROM Editor #################################
print("*******************")
print("step 8 of 17")
print("Get Editor from SQL Server")
conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')
cursor = conn.cursor()
cursor.execute('SELECT * FROM Editor')
records = cursor.fetchall()
Editor = []
columnNames = [column[0] for column in cursor.description]
for record in records:
#    print(record)
    Editor.append( dict( zip( columnNames , record ) ) )
Editor = pd.DataFrame(Editor)
print("Convert Editor to Excel and CSV")
Editor.to_excel('E:\python codes\VOD\BackupData\Editor.xlsx', index = False)
Editor.to_csv('E:\python codes\VOD\BackupData\Editor.csv', index = False)
print("Sending Editor to PostgreSQL")
engine = create_engine('postgresql://postgres:12344321@10.32.141.17/Moein01',pool_size=20, max_overflow=100,)
con=engine.connect()
Editor.to_sql('Editor',con,if_exists='replace', index=False)
con.close()

################################ GET DATA FROM Genres #################################
print("*******************")
print("step 9 of 17")
print("Get Genres from SQL Server")
conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')
cursor = conn.cursor()
cursor.execute('SELECT * FROM Genres')
records = cursor.fetchall()
Genres = []
columnNames = [column[0] for column in cursor.description]
for record in records:
#    print(record)
    Genres.append( dict( zip( columnNames , record ) ) )
Genres = pd.DataFrame(Genres)
print("Convert Genres to Excel and CSV")
Genres.to_excel('E:\python codes\VOD\BackupData\Genres.xlsx', index = False)
Genres.to_csv('E:\python codes\VOD\BackupData\Genres.csv', index = False)
print("Sending Genres to PostgreSQL")
engine = create_engine('postgresql://postgres:12344321@10.32.141.17/Moein01',pool_size=20, max_overflow=100,)
con=engine.connect()
Genres.to_sql('Genres',con,if_exists='replace', index=False)
con.close()

################################ GET DATA FROM Imdb #################################
print("*******************")
print("step 10 of 17")
print("Get Imdb from SQL Server")
conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')
cursor = conn.cursor()
cursor.execute('SELECT * FROM Imdb')
records = cursor.fetchall()
Imdb = []
columnNames = [column[0] for column in cursor.description]
for record in records:
#    print(record)
    Imdb.append( dict( zip( columnNames , record ) ) )
Imdb = pd.DataFrame(Imdb)
print("Convert Imdb to Excel and CSV")
Imdb.to_excel('E:\python codes\VOD\BackupData\Imdb.xlsx', index = False)
Imdb.to_csv('E:\python codes\VOD\BackupData\Imdb.csv', index = False)
print("Sending Imdb to PostgreSQL")
engine = create_engine('postgresql://postgres:12344321@10.32.141.17/Moein01',pool_size=20, max_overflow=100,)
con=engine.connect()
Imdb.to_sql('Imdb',con,if_exists='replace', index=False)
con.close()

################################ GET DATA FROM Language #################################
print("*******************")
print("step 11 of 17")
print("Get Language from SQL Server")
conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')
cursor = conn.cursor()
cursor.execute('SELECT * FROM Language')
records = cursor.fetchall()
Language = []
columnNames = [column[0] for column in cursor.description]
for record in records:
#    print(record)
    Language.append( dict( zip( columnNames , record ) ) )
Language = pd.DataFrame(Language)
print("Convert Language to Excel and CSV")
Language.to_excel('E:\python codes\VOD\BackupData\Language.xlsx', index = False)
Language.to_csv('E:\python codes\VOD\BackupData\Language.csv', index = False)
print("Sending Language to PostgreSQL")
engine = create_engine('postgresql://postgres:12344321@10.32.141.17/Moein01',pool_size=20, max_overflow=100,)
con=engine.connect()
Language.to_sql('Language',con,if_exists='replace', index=False)
con.close()

################################ GET DATA FROM Producer #################################
print("*******************")
print("step 12 of 17")
print("Get Producer from SQL Server")
conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')
cursor = conn.cursor()
cursor.execute('SELECT * FROM Producer')
records = cursor.fetchall()
Producer = []
columnNames = [column[0] for column in cursor.description]
for record in records:
#    print(record)
    Producer.append( dict( zip( columnNames , record ) ) )
Producer = pd.DataFrame(Producer)
print("Convert Producer to Excel and CSV")
Producer.to_excel('E:\python codes\VOD\BackupData\Producer.xlsx', index = False)
Producer.to_csv('E:\python codes\VOD\BackupData\Producer.csv', index = False)
print("Sending Producer to PostgreSQL")
engine = create_engine('postgresql://postgres:12344321@10.32.141.17/Moein01',pool_size=20, max_overflow=100,)
con=engine.connect()
Producer.to_sql('Producer',con,if_exists='replace', index=False)
con.close()

################################ GET DATA FROM ReleaseDateGeorgian #################################
print("*******************")
print("step 13 of 17")
print("Get ReleaseDateGeorgian from SQL Server")
conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')
cursor = conn.cursor()
cursor.execute('SELECT * FROM ReleaseDateGeorgian')
records = cursor.fetchall()
ReleaseDateGeorgian = []
columnNames = [column[0] for column in cursor.description]
for record in records:
#    print(record)
    ReleaseDateGeorgian.append( dict( zip( columnNames , record ) ) )
ReleaseDateGeorgian = pd.DataFrame(ReleaseDateGeorgian)
print("Convert ReleaseDateGeorgian to Excel and CSV")
ReleaseDateGeorgian.to_excel('E:\python codes\VOD\BackupData\ReleaseDateGeorgian.xlsx', index = False)
ReleaseDateGeorgian.to_csv('E:\python codes\VOD\BackupData\ReleaseDateGeorgian.csv', index = False)
print("Sending ReleaseDateGeorgian to PostgreSQL")
engine = create_engine('postgresql://postgres:12344321@10.32.141.17/Moein01',pool_size=20, max_overflow=100,)
con=engine.connect()
ReleaseDateGeorgian.to_sql('ReleaseDateGeorgian',con,if_exists='replace', index=False)
con.close()

################################ GET DATA FROM ReleaseDateJalali #################################
print("*******************")
print("step 14 of 17")
print("Get ReleaseDateJalali from SQL Server")
conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')
cursor = conn.cursor()
cursor.execute('SELECT * FROM ReleaseDateJalali')
records = cursor.fetchall()
ReleaseDateJalali = []
columnNames = [column[0] for column in cursor.description]
for record in records:
#    print(record)
    ReleaseDateJalali.append( dict( zip( columnNames , record ) ) )
ReleaseDateJalali = pd.DataFrame(ReleaseDateJalali)
print("Convert ReleaseDateJalali to Excel and CSV")
ReleaseDateJalali.to_excel('E:\python codes\VOD\BackupData\ReleaseDateJalali.xlsx', index = False)
ReleaseDateJalali.to_csv('E:\python codes\VOD\BackupData\ReleaseDateJalali.csv', index = False)
print("Sending ReleaseDateJalali to PostgreSQL")
engine = create_engine('postgresql://postgres:12344321@10.32.141.17/Moein01',pool_size=20, max_overflow=100,)
con=engine.connect()
ReleaseDateJalali.to_sql('ReleaseDateJalali',con,if_exists='replace', index=False)
con.close()

################################ GET DATA FROM Singer #################################
print("*******************")
print("step 15 of 17")
print("Get Singer from SQL Server")
conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')
cursor = conn.cursor()
cursor.execute('SELECT * FROM Singer')
records = cursor.fetchall()
Singer = []
columnNames = [column[0] for column in cursor.description]
for record in records:
#    print(record)
    Singer.append( dict( zip( columnNames , record ) ) )
Singer = pd.DataFrame(Singer)
print("Convert Singer to Excel and CSV")
Singer.to_excel('E:\python codes\VOD\BackupData\Singer.xlsx', index = False)
Singer.to_csv('E:\python codes\VOD\BackupData\Singer.csv', index = False)
print("Sending Singer to PostgreSQL")
engine = create_engine('postgresql://postgres:12344321@10.32.141.17/Moein01',pool_size=20, max_overflow=100,)
con=engine.connect()
Singer.to_sql('Singer',con,if_exists='replace', index=False)
con.close()

################################ GET DATA FROM Writer #################################
print("*******************")
print("step 16 of 17")
print("Get Writer from SQL Server")
conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')
cursor = conn.cursor()
cursor.execute('SELECT * FROM Writer')
records = cursor.fetchall()
Writer = []
columnNames = [column[0] for column in cursor.description]
for record in records:
#    print(record)
    Writer.append( dict( zip( columnNames , record ) ) )
Writer = pd.DataFrame(Writer)
print("Convert Writer to Excel and CSV")
Writer.to_excel('E:\python codes\VOD\BackupData\Writer.xlsx', index = False)
Writer.to_csv('E:\python codes\VOD\BackupData\Writer.csv', index = False)
print("Sending Writer to PostgreSQL")
engine = create_engine('postgresql://postgres:12344321@10.32.141.17/Moein01',pool_size=20, max_overflow=100,)
con=engine.connect()
Writer.to_sql('Writer',con,if_exists='replace', index=False)
con.close()

################################ GET DATA FROM StatisticsMonthOperators #################################
print("*******************")
print("step 17 of 17")
print("Get StatisticsMonthOperators from SQL Server")
conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')
cursor = conn.cursor()
cursor.execute('SELECT * FROM StatisticsMonthOperators')
records = cursor.fetchall()
StatisticsMonthOperators = []
columnNames = [column[0] for column in cursor.description]
for record in records:
#    print(record)
    StatisticsMonthOperators.append( dict( zip( columnNames , record ) ) )
StatisticsMonthOperators = pd.DataFrame(StatisticsMonthOperators)
print("Convert StatisticsMonthOperators to Excel and CSV")
StatisticsMonthOperators.to_excel('E:\python codes\VOD\BackupData\StatisticsMonthOperators.xlsx', index = False)
StatisticsMonthOperators.to_csv('E:\python codes\VOD\BackupData\StatisticsMonthOperators.csv', index = False)
print("Sending StatisticsMonthOperators to PostgreSQL")
engine = create_engine('postgresql://postgres:12344321@10.32.141.17/Moein01',pool_size=20, max_overflow=100,)
con=engine.connect()
StatisticsMonthOperators.to_sql('StatisticsMonthOperators',con,if_exists='replace', index=False)
con.close()

################################################################################################
print("*******************")
print("THE END.")












