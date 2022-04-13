
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
###########################################################################################
all_data = pd.DataFrame()
all_data['Title'] = db2['TitleCleaned1']
all_data['IDS'] = db2['IDS']
#all_data.loc[0, 'Title'] = 'نیسان آبی'
#all_data.loc[1, 'Title'] = 'میدان سرخ'
#all_data.loc[2, 'Title'] = 'شهرزاد'
#all_data.loc[3, 'Title'] = 'شب دهم'
#all_data.loc[4, 'Title'] = 'قهرمان'
#all_data.loc[5, 'Title'] = 'درباره الی'
driver = webdriver.Chrome(executable_path= r'E:\python codes\VOD\chromedriver.exe')
list_primary = pd.DataFrame()
k = 0
for i in range(16120, 16140):  # len(all_data)
#    list_primary.loc[i, 'title_first'] = all_data.loc[i, 'Title']
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
#    print(link_url)
driver.close()

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

def meta1(soup_primary, data_output):
    Meta1Data = soup_primary.find('div', {'class': "TitleBlock__Container-sc-1nlhx7j-0 hglRHk"})
    try:
        TitleOrginal = soup_primary.find('h1', {'data-testid': "hero-title-block__title"})
        TitleOrginal = TitleOrginal.text
        data_output.loc[i, 'TitleOrginal'] = TitleOrginal
    except: pass
    try:
        ProductionYear = soup_primary.find('a', {'class': "ipc-link ipc-link--baseAlt ipc-link--inherit-color TitleBlockMetaData__StyledTextLink-sc-12ein40-1 rgaOW"})
        ProductionYear = ProductionYear.text
    #    print(ProductionYear)
        data_output.loc[i, 'ProductionYear'] = ProductionYear
    except: pass
    try:
        CastType = soup_primary.find('ul', {'data-testid': "hero-title-block__metadata"})
    #    print(CastType.text)
        data_output.loc[i, 'CastType'] = CastType.text
    except: pass
    try:    
        RateImdb = Meta1Data.find('span', {'class': "AggregateRatingButton__RatingScore-sc-1ll29m0-1 iTLWoV"})
#       print(RateImdb.text)
        data_output.loc[i, 'RateImdb'] = RateImdb.text
    except: pass
    try:
        TotalVotes = Meta1Data.find('div', {'class': "AggregateRatingButton__TotalRatingAmount-sc-1ll29m0-3 jkCVKJ"})
    #    print(TotalVotes.text)
        data_output.loc[i, 'TotalVotes'] = TotalVotes.text
    except: pass
    return data_output
    
def meta2(soup_primary, data_output):
    Meta2Data = soup_primary.find('ul', {'class': "ipc-metadata-list ipc-metadata-list--dividers-all Storyline__StorylineMetaDataList-sc-1b58ttw-1 esngIX ipc-metadata-list--base"})
    Genres1 = Meta2Data.find('li', {'data-testid': "storyline-genres"})
    GenresList = ''
    for genre in Genres1.findAll('a'):
        GenresList = genre.text + ',' + GenresList
#    print(GenresList)
    data_output.loc[i, 'Genres'] = GenresList
    return data_output
    
def meta3(soup_primary, data_output):
    Meta3Data = soup_primary.find('section', {'data-testid': "Details"})
    for detail in Meta3Data.findAll('li'):
        if 'date' in detail.text:
            detail2 = detail.find('div', {'class': "ipc-metadata-list-item__content-container"})
            data_output.loc[i, 'ReleaseDate'] = detail2.text
#            print(detail2.text)
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
        if 'companies' in detail.text:
            detail2 = detail.find('div', {'class': "ipc-metadata-list-item__content-container"})
            data_output.loc[i, 'Company'] = detail2.text   
    return data_output   

def meta4(soup_second, data_output):
    meta4 = soup_second.find('div', {'id': "fullcredits_content"})
    k = 0
    for factor in meta4.findAll('h4'):
        k = k + 1
    name = meta4.findAll('h4')
    table = meta4.findAll('table')
    for counter in range(0, k):
        name1 = name[counter].text
        table1 = table[counter].text
        ######### Directed #########
        if 'Directed' in name1:
            director = ''
            try:
                for j in table[counter].findAll('a'):
                    director = j.text + ',' + director
                data_output.loc[i, 'Director'] = director
            except:
                director = table1
                data_output.loc[i, 'Director'] = director
        ######### Produced ######### 
        if 'Produced' in name1:
            producer = ''
            try:
                for j in table[counter].findAll('a'):
                    producer = j.text + ',' + producer
                data_output.loc[i, 'Producer'] = producer
            except:
                producer = table1
                data_output.loc[i, 'Producer'] = producer
        ######### Cast #########
        if 'Cast' in name1 and 'Casting' not in name1:
            cast = ''
            for j in table[counter].findAll('tr'):
                jj = j.findAll('td')
                try:
                    cast = jj[1].text + ',' + cast
                except: pass
            data_output.loc[i, 'Casts'] = cast
            if cast == '':
                cast = table1
                data_output.loc[i, 'Casts'] = cast
        ######### Writing #########
        if 'Writing' in name1:
            writer = ''
            try:
                for j in table[counter].findAll('a'):
                    writer = j.text + ',' + writer
                data_output.loc[i, 'Writer'] = writer
            except:
                writer = table1
                data_output.loc[i, 'Writer'] = writer
        ######### Cinematography #########
        if 'Cinematography' in name1:
            cinematography = ''
            try:
                for j in table[counter].findAll('a'):
                    cinematography = j.text + ',' + cinematography
                data_output.loc[i, 'Cinematography'] = cinematography
            except:
                cinematography = table1
                data_output.loc[i, 'Cinematography'] = cinematography
        ######### Editing #########
        if 'Editing' in name1:
            editing = ''
            try:
                for j in table[counter].findAll('a'):
                    editing = j.text + ',' + editing
                data_output.loc[i, 'Editor'] = editing
            except:
                editing = table1
                data_output.loc[i, 'Editor'] = editing
        ######### Costume Design #########
        if 'Costume Design' in name1:
            costumedesign = ''
            try:
                for j in table[counter].findAll('a'):
                    costumedesign = j.text + ',' + costumedesign
                data_output.loc[i, 'CostumeDesign'] = costumedesign
            except:
                costumedesign = table1
                data_output.loc[i, 'CostumeDesign'] = costumedesign
        ######### Makeup Department #########
        if 'Makeup Department' in name1:
            makeupdepartment = ''
            try:
                for j in table[counter].findAll('a'):
                    makeupdepartment = j.text + ',' + makeupdepartment
                data_output.loc[i, 'MakeupDepartment'] = makeupdepartment
            except:
                makeupdepartment = table1
                data_output.loc[i, 'MakeupDepartment'] = makeupdepartment
        ######### Production Management #########
        if 'Production Management' in name1:
            productionmanagement = ''
            try:
                for j in table[counter].findAll('a'):
                    productionmanagement = j.text + ',' + productionmanagement
                data_output.loc[i, 'ProductionManagement'] = productionmanagement
            except:
                productionmanagement = table1
                data_output.loc[i, 'ProductionManagement'] = productionmanagement
    return data_output      
    
data_output = pd.DataFrame()
for i in range(0, len(list_primary)):    # len(list_primary)
    print(i)
    data_output.loc[i, 'IDS'] = list_primary.loc[i, 'IDS']
    data_output.loc[i, 'TitleFirst'] = list_primary.loc[i, 'title_first']
    title_primary = list_primary.loc[i, 'title_primary']
    data_output.loc[i, 'Title'] = title_primary 
    site = requests.get(title_primary)
    soup_primary = BeautifulSoup(site.text, 'html.parser')
    title_code = list_primary.loc[i, 'title_code']
    UrlCasts = 'https://www.imdb.com/title/' + title_code + '/fullcredits?ref_=tt_ov_st_sm'
    print(UrlCasts)
    site1 = requests.get(UrlCasts)
    soup_second = BeautifulSoup(site1.text, 'html.parser')
    data_output = meta1(soup_primary, data_output)
    data_output = meta2(soup_primary, data_output)
    data_output = meta3(soup_primary, data_output)
    data_output = meta4(soup_second, data_output)







#print(list_primary.loc[0, 'title_primary'])








