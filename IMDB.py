
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.request import urlopen as uReq
from sqlalchemy import create_engine
import pyodbc
from pyodbc import *

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
    db2_VOD.append( dict( zip( columnNames , record ) ) )
db2_VOD = pd.DataFrame(db2_VOD)
db2 = db2_VOD.copy()

################ Transfer Data from db2 to all_data ################
all_data = pd.DataFrame()
all_data['Title'] = db2['TitleCleaned1']
all_data['IDS'] = db2['IDS']
all_data['Season'] = db2['Season']
all_data['EnglishName'] = db2['EnglishName']
all_data = all_data.rename(columns={"Title":"Title_fa"})
all_data['Title'] = ''
all_data_repeat = all_data.copy()
all_data_repeat['EnglishName'].replace('', 'No Data', inplace=True)
all_data_repeat = all_data_repeat[~all_data_repeat.EnglishName.str.contains("No Data")]
all_data['Title'] =all_data['Title_fa']
all_data_repeat['Title'] =all_data_repeat['EnglishName']
all_data = all_data.append([all_data_repeat])
all_data['Season'] = all_data['Season'].str.replace('00', '')
all_data['Season'] = all_data['Season'].str.replace('01', '')
all_data['Title'] = all_data['Title'] + ' ' + all_data['Season']
all_data = all_data.reset_index()
del all_data['index']

################ Get two IMDB URLs from Google ################
driver = webdriver.Chrome(executable_path= r'E:\python codes\VOD\chromedriver.exe')
#list_primary = pd.DataFrame()
#k = 0
for i in range(32786, len(all_data)):  # len(all_data)
    print(i, " : ", all_data.loc[i, 'Title'])
    driver.get('https://www.google.com/')
    time.sleep(2)
    search = driver.find_element_by_xpath('/html/body/div[1]/div[3]/form/div[1]/div[1]/div[1]/div/div[2]/input')
    time.sleep(2)
    Title = all_data.loc[i, 'Title'] + '+' + 'IMDB'
    search.send_keys(Title)
    search.send_keys(Keys.ENTER)
    time.sleep(2)
    link_primary = driver.current_url
    site = requests.get(link_primary)
    soup_primary = BeautifulSoup(site.text, 'html.parser')
    for link in soup_primary.findAll('a'):
        link_url = link.get('href')
        number = 0
        if "www.imdb.com/title/tt" in link_url:
            list_primary.loc[k, 'title_first'] = all_data.loc[i, 'Title']
            list_primary.loc[k, 'IDS'] = all_data.loc[i, 'IDS']
            list_primary.loc[k, 'title_primary'] = link_url
            k = k + 1
            number = number + 1
            if number == 2:
                break
driver.close()

################ Clean of URL and Creating Primary List ################
list_primary['title_primary'] = list_primary['title_primary'].str.replace('https', '')
list_primary['col1'] = list_primary['title_primary'].str.split("/")
for i in range(len(list_primary)):
    print(i)
    for link in list_primary.loc[i, 'col1']:
        if "tt" in link:
            break
    link_final = 'https://www.imdb.com/title/' + link + '/'
    list_primary.loc[i, 'title_primary'] = link_final
    list_primary.loc[i, 'title_code'] = link

del list_primary['col1']
list_primary.drop_duplicates(subset =['title_code'], keep = 'last', inplace = True)
list_primary = list_primary[~list_primary.title_code.str.contains(":")]
list_primary = list_primary[~list_primary.title_code.str.contains("http")]
list_primary = list_primary.reset_index()
del list_primary['index']

################################################################
################################################################
###################### CRAWL STARTING ... ######################
################################################################
################################################################

################ Title & Year & Casts & IMDB & Votes ###################
def meta1(soup_primary, data_output):
    try:
        Meta1Data = soup_primary.find('div', {'class': "TitleBlock__Container-sc-1nlhx7j-0 hglRHk"})
        try:
            TitleOrginal = soup_primary.find('h1', {'data-testid': "hero-title-block__title"})
            TitleOrginal = TitleOrginal.text
            data_output.loc[i, 'TitleOrginal'] = TitleOrginal
        except: pass
        try:
            YearService1 = Meta1Data.find('div', {'class': "TitleBlock__TitleMetaDataContainer-sc-1nlhx7j-2 hWHMKr"})
            YearService = YearService1.findAll('li')
            x = ''
            for x1 in YearService:
                try:
                    x2 = x1.find('a')
                    x = x2.text + '---' + x
                except:
                    x = x1.text + '---' + x
            data_output.loc[i, 'YearAndService'] = x
        except: pass
        try:    
            RateImdb = Meta1Data.find('span', {'class': "AggregateRatingButton__RatingScore-sc-1ll29m0-1 iTLWoV"})
            data_output.loc[i, 'RateImdb'] = RateImdb.text
        except: pass
        try:
            TotalVotes = Meta1Data.find('div', {'class': "AggregateRatingButton__TotalRatingAmount-sc-1ll29m0-3 jkCVKJ"})
            data_output.loc[i, 'TotalVotes'] = TotalVotes.text
        except: pass
        try:
            Runtime1 = soup_primary.find('li', {'data-testid': "title-techspec_runtime"})
            Runtime = Runtime1.find('div', {'class': "ipc-metadata-list-item__content-container"})
            data_output.loc[i, 'Runtime'] = Runtime.text
        except: pass
    except: pass
    return data_output

################ Genres ################
def meta2(soup_primary, data_output):
    try:
        Meta2Data = soup_primary.find('ul', {'class': "ipc-metadata-list ipc-metadata-list--dividers-all Storyline__StorylineMetaDataList-sc-1b58ttw-1 esngIX ipc-metadata-list--base"})
        Genres1 = Meta2Data.find('li', {'data-testid': "storyline-genres"})
        GenresList = ''
        for genre in Genres1.findAll('a'):
            GenresList = genre.text + ',' + GenresList
        data_output.loc[i, 'Genres'] = GenresList
    except: pass
    return data_output

################ Date & Country & Language & Company ################    
def meta3(soup_primary, data_output):
    try:
        Meta3Data = soup_primary.find('section', {'data-testid': "Details"})
        for detail in Meta3Data.findAll('li'):
            try:
                if 'date' in detail.text:
                    detail2 = detail.find('div', {'class': "ipc-metadata-list-item__content-container"})
                    data_output.loc[i, 'ReleaseDate'] = detail2.text
            except: pass
            try:    
                if 'origin' in detail.text:
                    detail2 = detail.find('div', {'class': "ipc-metadata-list-item__content-container"})
                    country = ''
                    try:
                        for j in detail2.findAll('li'):
                            country = j.text + ',' + country
                        data_output.loc[i, 'Country'] = country
                    except:
                        country = detail2.text
                        data_output.loc[i, 'Country'] = country
            except: pass
            try:
                if 'Language' in detail.text:
                    detail2 = detail.find('div', {'class': "ipc-metadata-list-item__content-container"})
                    language = ''
                    try:
                        for j in detail2.findAll('li'):
                            language = j.text + ',' + language
                        data_output.loc[i, 'Language'] = language
                    except:
                        language = detail2.text
                        data_output.loc[i, 'Language'] = language
            except: pass
            try:
                if 'companies' in detail.text:
                    detail2 = detail.find('div', {'class': "ipc-metadata-list-item__content-container"})
                    data_output.loc[i, 'Company'] = detail2.text 
            except: pass
    except: pass
    return data_output   

################ Other Factors ################
from IMDB_meta4 import *    

################ Get Data by Crawl ################    
data_output = pd.DataFrame()
for i in range(196, len(list_primary)):    # len(list_primary)
    print(i)
    data_output.loc[i, 'IDS'] = list_primary.loc[i, 'IDS']
    data_output.loc[i, 'TitleFirst'] = list_primary.loc[i, 'title_first']
    title_primary = list_primary.loc[i, 'title_primary']
    data_output.loc[i, 'Link'] = title_primary 
    site = requests.get(title_primary)
    soup_primary = BeautifulSoup(site.text, 'html.parser')
    title_code = list_primary.loc[i, 'title_code']
    UrlCasts = 'https://www.imdb.com/title/' + title_code + '/fullcredits?ref_=tt_ov_st_sm'
#    print(UrlCasts)
    site1 = requests.get(UrlCasts)
    soup_second = BeautifulSoup(site1.text, 'html.parser')
    data_output = meta1(soup_primary, data_output)
    data_output = meta2(soup_primary, data_output)
    data_output = meta3(soup_primary, data_output)
    data_output = IMDB_meta4(soup_second, data_output, i)



list_primary.to_excel('list_primary.xlsx', index=False)



#print(data_output.loc[209, 'title_primary'])
######################################################
data_total = pd.read_excel(r'E:\python codes\VOD\IMDB\data_total.xlsx')
data_total_repeat = data_total.copy()
data_total_repeat['YearAndService'] = data_total_repeat['YearAndService'].astype(str)

data_total_repeat['Year'] = data_total_repeat['YearAndService'].str.split("---")

for i in range(0, 19730):
    print(i)
    try:
        title = data_total_repeat.loc[i, 'Year']
        for j in range(0, len(title)):
            try:
                if 'm' in title[j]:
                    title[j] = ''
            except: pass
            try:
                if 'h' in title[j]:
                    title[j] = ''
            except: pass
            try:
                if '19' in title[j]:
                    data_total_repeat.loc[i, 'Year_new'] = title[j]
            except: pass
            try:
                if '20' in title[j]:
                    data_total_repeat.loc[i, 'Year_new'] = title[j]
            except: pass
    except: pass

data_total_repeat['Year_new'] = data_total_repeat['Year_new'].astype(str)

data_total_repeat1 = data_total_repeat[data_total_repeat.Year_new.str.contains(",")]
data_total_repeat2 = data_total_repeat[~data_total_repeat.Year_new.str.contains(",")]

data_total_repeat1 = data_total_repeat1.reset_index()
del data_total_repeat1['index']
data_total_repeat1['Year_new'] = data_total_repeat1['Year_new'].str.split(",")
for i in range(0, len(data_total_repeat1)):
    print(i)
    title = data_total_repeat1.loc[i, 'Year_new']
    data_total_repeat1.loc[i, 'Year_new'] = title[1]

data_total_repeat2.loc[15072, 'Year_new'] = '1973'
data_total_repeat2.loc[15073, 'Year_new'] = '1974'
data_total_repeat2.loc[8231, 'Year_new'] = '1989'
data_total_repeat2.loc[8971, 'Year_new'] = '2002'
data_total_repeat2.loc[671, 'Year_new'] = '2008'
data_total_repeat2.loc[8859, 'Year_new'] = '2009'
data_total_repeat2.loc[13354, 'Year_new'] = '2010'
data_total_repeat2.loc[13355, 'Year_new'] = '2010'
data_total_repeat2.loc[13356, 'Year_new'] = '2010'
data_total_repeat2.loc[9516, 'Year_new'] = '2015'
data_total_repeat2.loc[9434, 'Year_new'] = '2018'
data_total_repeat2.loc[9435, 'Year_new'] = '2018'
data_total_repeat2.loc[9436, 'Year_new'] = '2018'
data_total_repeat2.loc[12969, 'Year_new'] = '2019'
data_total_repeat2.loc[17331, 'Year_new'] = '2019'
data_total_repeat2.loc[2579, 'Year_new'] = '2021'
data_total_repeat2.loc[18661, 'Year_new'] = '2021'
data_total_repeat2.loc[18662, 'Year_new'] = '2021'
data_total_repeat2.loc[12392, 'Year_new'] = '2011'
data_total_repeat2.loc[12393, 'Year_new'] = '2012'
data_total_repeat2.loc[673, 'Year_new'] = '2010'
data_total_repeat2.loc[17826, 'Year_new'] = '2002'
data_total_repeat2.loc[9882, 'Year_new'] = '2011'
data_total_repeat2.loc[9884, 'Year_new'] = '2011'
data_total_repeat2.loc[10550, 'Year_new'] = '2012'

data_total_repeat21 = data_total_repeat2[data_total_repeat2.Year_new.str.contains("–")]
data_total_repeat22 = data_total_repeat2[~data_total_repeat2.Year_new.str.contains("–")]
data_total_repeat21 = data_total_repeat21.reset_index()
del data_total_repeat21['index']

for i in range(0, len(data_total_repeat21)):
     x_name_content = data_total_repeat21.loc[i, 'Year_new']
     head, sep, tail = x_name_content.partition('–')
     data_total_repeat21.loc[i, 'Year_new'] = head

data_total_repeat_new = pd.DataFrame()
data_total_repeat_new = data_total_repeat1.append([data_total_repeat21, data_total_repeat22])

data_total_repeat_new['Year_new'] = data_total_repeat_new['Year_new'].str.strip()
data_total_repeat_new = data_total_repeat_new.reset_index()
del data_total_repeat_new['index']















