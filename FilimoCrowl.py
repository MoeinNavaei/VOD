import pandas as pd
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from bs2json import bs2json
import re
from scrapy.crawler import CrawlerProcess
import requests
from urllib.request import urlopen as uReq
import time
from selenium import webdriver
import sys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
start = time.time()

url_filimo='https://www.filimo.com/'

data_output=pd.DataFrame()
data_output=pd.DataFrame(data_output, columns=['title', 'latin_title', 'imdb', 'rate_filimo', 'percent', 'Runtime', 'genre', 'country', 'year', 'subtitle', 'quality', 'Epizode Number', 'season', 'Direct', 'actor', 'producer', 'Composer', 'Singer', 'writer', 'Cameraman'])
site=requests.get(url_filimo)
soup_primary=BeautifulSoup(site.text, 'html.parser')
#print(soup_primary)
j=0
for link in soup_primary.findAll('a'):
    print(j)
    try:
        title_url=link.get('href')
        if "/m/" in title_url:
#            print(title_url)
            title_link=requests.get(title_url)
            soup_title=BeautifulSoup(title_link.text, 'html.parser')
            ############## title & epizode ##############
            try:
        #        title1=soup_title.find('h1', {'class': "movie-title  ui-mt-2x ui-mb-2x ps-relative ui-fw-normal"})
                title_original=soup_title.find('span', {'class': "fa d-block w100 ui-fc-white"})
                data_output.loc[j, 'title']=title_original.text
#                print(title_original.text)
                title_original_split=title_original.text.split()
#                print(total_title)
                for cc in title_original_split:
                    if "-" in cc:
                        title_original_split_dash=title_original.text.split('-')
                        aa=title_original_split_dash[1].split()
                        try:
                            aa1=aa[0]+ " " + aa[1]
                        except: pass
                        try:
                            aa2=aa[2]+ " " + aa[3]
                        except: pass
                        try:
                            for bb in aa1.split():
                                if "قسمت" in bb:
    #                                print(aa1)
                                    data_output.loc[j, 'Epizode Number']=aa1
                        except: pass
                        try:
                            for bb in aa2.split():
                                if "قسمت" in bb:
    #                                print(aa2)
                                    data_output.loc[j, 'Epizode Number']=aa2
                        except: pass
                        try:
                            for bb in aa1.split():
                                if "فصل" in bb:
    #                                print(aa1)
                                    data_output.loc[j, 'season']=aa1
                                    season = aa1
                        except: pass
                        try:
                            for bb in aa2.split():
                                if "فصل" in bb:
    #                                print(aa2)
                                    data_output.loc[j, 'season']=aa2
                                    season = aa2
                        except: pass
                        data_output.loc[j, 'title']=title_original_split_dash[0]
            except: pass
            ############## latin_title ##############
            try:
                title1=soup_title.find('div', {'class': "movie-details d-inline-block md-ui-pb-2x va-top"})
                latin_title=title1.find('span', {'class': "en-force-text d-block w100"})
#                print(latin_title.text)
                data_output.loc[j, 'latin_title']=latin_title.text
            except: pass
            ############## imdb ##############
            try:
                imdb1=soup_title.find('div', {'class': "ds-badge ds-badge--icon ds-badge--brand imdb"})
#                imdb2=imdb1.find('span', {'class': "ds-badge_label"})
                imdb=imdb1.find('span', {'class': "en ui-fc-black ui-fw-bold"})
#                print(imdb.text)
                data_output.loc[j, 'imdb']=imdb.text
            except: pass
            ############## rate_filimo ##############
            try:
                rate_filimo1=soup_title.find('div', {'class': "rating-wrapper"})
                rate_filimo=rate_filimo1.find('span', {'id': "rateCnt"})
#                print(rate_filimo.text)
                data_output.loc[j, 'rate_filimo']=rate_filimo.text
            except: pass
            ############## percent ##############
            try:
                percent1=soup_title.find('div', {'class': "rating-wrapper"})
                percent=percent1.find('span', {'id': "percentNumber"})
#                print(percent.text)
                data_output.loc[j, 'percent']=percent.text
            except: pass
            ############## director ##############
            try:
                director1=soup_title.find('div', {'class': "meta-wrapper ui-pt-4x ui-fs-small"})
                for director in director1.findAll('a'):
#                    print(director.text)
                    data_output.loc[j, 'Direct']=director.text
            except: pass
            ############## genre ##############
            try:
                genre1=soup_title.find('div', {'class': "tags-wrapper"})
                genre2=""
                for title in genre1.findAll('a'):
                    genre3=title.get('title')
                    head, sep, tail = genre3.partition(':')
                    genre2=genre2+','+tail
#                print(genre2)
                data_output.loc[j, 'genre']=genre2
            except: pass
            ############## MetaData ##############
            try:
                MetaData1=soup_title.find('div', {'class': "meta-wrapper ui-pt-4x"})
                MetaData=MetaData1.text.split('-')
                year=re.findall('\d{4}', MetaData1.text )
#                print("year: ", year)
                data_output.loc[j, 'year']=year
#                print(MetaData)
                MetaData = pd.DataFrame({'col': MetaData})
#                print(MetaData)
                for word in range(0, len(MetaData)):
#                    print("word: ", word)
                    meta = MetaData.loc[word, 'col']
#                    print("meta: ", meta)
                    if "دقیقه" in meta:
                        data_output.loc[j, 'Runtime']=meta
                        Runtime = meta
                        Runtime = Runtime.split('\n')
                        Runtime = pd.DataFrame({'col': Runtime})
                        for k in range(0, len(Runtime)):
                            if "دقیقه" in Runtime.loc[k, 'col'].split():
                                data_output.loc[j, 'Runtime'] = Runtime.loc[k, 'col']
                                break
#                        print("Runtime: ", Runtime)
                    if "محصول" in meta:
                        data_output.loc[j, 'country']=meta
                        country = meta
#                        print("country: ", country)
                    if "کیفیت" in meta:
                        data_output.loc[j, 'quality']=meta
                        quality = meta
#                        print("quality: ", quality)
                    if "زیرنویس دارد" in meta:
                        data_output.loc[j, 'subtitle']=meta
                        subtitle = meta
#                        print("subtitle: ", subtitle)
            except: pass
            ############## Year - Country - Genres ##############
#            try:
#                a1 = soup_title.find('div', {'class': "movie-details d-inline-block md-ui-pb-2x va-top"})
#                a2 = pd.DataFrame(a1.text)
#                YGC1=soup_title.find('div', {'class': "meta-wrapper ui-pt-4x"})
#                print(a1.text)
#                data_output.loc[j, 'latin_title']=YGC1.text
#            except: pass
            ############## season ##############
#            try:
#                season1=soup_title.find('h2', {'class': "header-tab"})
#                season=season1.find('span', {'class': "fa"})
##                print(season.text)
#                data_output.loc[j, 'season']=season.text
#            except: pass
            ############## actor ##############
            try:
                k = 0
                actor = pd.DataFrame()
                actor.insert(0, 'col', '')
                actor1=soup_title.find('div', {'class': "actors-list clearfix"})
                for actor_x in actor1.findAll('div', {'class': "actors-item is-iran"}):
                    act = actor_x.text.split()
                    actor.loc[k, 'col'] = act
                    k = k + 1
                actor['col'] = actor['col'].astype(str)
                actor['col'] = actor['col'].str.replace("'", "")
                actor['col'] = actor['col'].str.replace("[", "")
                actor['col'] = actor['col'].str.replace("]", "")
                actor['col'] = actor['col'].str.replace(",", "")
                actor_list = ""
                for i in range(0, len(actor)):
                    actor_list = actor.loc[i, 'col']+"," + actor_list
#                print(actor_list)
                data_output.loc[j, 'actor']=actor_list
            except: pass
          ############## agents ##############
            try:
                k = 0
                df_agent = pd.DataFrame()
                df_agent.insert(0, 'col', '')
                agents1=soup_title.find('div', {'class': "single-section section-crew ui-pt-4x"})
                agents2=agents1.find('div', {'class': "single-section-content"})
                for search in agents2.findAll('li', {'class': "crew-item"}):
                    df_agent.loc[k, 'col'] = search.text
                    k = k + 1
                for i in range(0, len(df_agent)):
                    agent = df_agent.loc[i, 'col']
                    agent_split = agent.split()
                    if "کارگردان" in agent_split:
                        data_output.loc[j, 'Direct'] = agent
                        Direct = agent
                    if "فیلمبرداری" in agent_split:
                        data_output.loc[j, 'Cameraman'] = agent
                        Cameraman = agent
                    if "تهیه‌کننده" in agent_split:
                        data_output.loc[j, 'producer'] = agent
                        producer = agent
                    if "فیلم‌نامه‌نویس" in agent_split:
                        data_output.loc[j, 'writer'] = agent
                        writer = agent
                    if "آهنگساز" in agent_split:
                        data_output.loc[j, 'Composer'] = agent
                        Composer = agent
                    if "خواننده" in agent_split:
                        data_output.loc[j, 'Singer'] = agent
                        Singer = agent
                    if "نویسنده" in agent_split:
                        data_output.loc[j, 'writer'] = agent
                        writer = agent
                del df_agent
            except: pass
            ############## old percent and like ##############
            OPL2=soup_title.find('div', {'class': "single-section section-episodes"})
            OPL1=OPL2.find('div', {'class': "tabs-content"})
            for search in OPL1.findAll('li', {'class': "accordion-item"}):
#                print("search: ", search.text)
                search_rate = search.find('span', {'id': "rateCnt"})
                search_percent = search.find('span', {'id': "percentNumber"})
                search_epizode = search.find('span', {'class': "episode"})
                data_output.loc[j, 'rate_filimo']=search_rate.text
                data_output.loc[j, 'percent']=search_percent.text
                data_output.loc[j, 'Epizode Number']=search_epizode.text
                #*****#
                data_output.loc[j, 'title']=title_original.text
#                data_output.loc[j, 'latin_title']=latin_title.text
#                data_output.loc[j, 'imdb']=imdb.text
#                data_output.loc[j, 'Runtime']=Runtime.text
#                data_output.loc[j, 'genre']=genre2.text
#                data_output.loc[j, 'country']=country.text
#                data_output.loc[j, 'year']=year.text
#                data_output.loc[j, 'subtitle']=subtitle.text
#                data_output.loc[j, 'quality']=quality.text
#                data_output.loc[j, 'season']=season.text
#                data_output.loc[j, 'Direct']=director.text
#                data_output.loc[j, 'actor']=actor.text
#                data_output.loc[j, 'producer']=producer.text
#                data_output.loc[j, 'Composer']=Composer.text
#                data_output.loc[j, 'Singer']=Singer.text
#                data_output.loc[j, 'writer']=writer.text
#                data_output.loc[j, 'Cameraman']=Cameraman.text
                j = j + 1
    except: pass
    j = j + 1
    if j > 100000:
        break

data_output = data_output.reset_index()
del data_output['index']
data_output.insert(20, 'title_cleaned', '')
for i in range(0,len(data_output)):
     x_name_content=data_output.loc[i, 'title']
     head, sep, tail = x_name_content.partition('-')
     data_output.loc[i, 'title_cleaned'] = head
    
data_output_final = pd.DataFrame()
title_list = data_output.drop_duplicates(subset =['title_cleaned'])
title_list = title_list.reset_index()
del title_list['index']
for i in range(0, len(title_list)):
    print(i)
    title_per = title_list.loc[i, 'title_cleaned']
    df_title_list = data_output.query("title_cleaned == @title_per")
    df_title_list = df_title_list.fillna(method='ffill')
    df_title_list = df_title_list.fillna(method='bfill')
    data_output_final = data_output_final.append(df_title_list)

data_output_final = data_output_final.reset_index()
del data_output_final['index']
data_output_final.drop_duplicates(subset =['title', 'latin_title', 'Epizode Number'], keep = 'first', inplace = True)

data_output_final = data_output_final.reset_index()
del data_output_final['index']
#######################################
data_output_final1 = data_output_final.copy()     
####################################### 
data_output_final['imdb'] = data_output_final['imdb'].str.replace('/10', '')
data_output_final['percent'] = data_output_final['percent'].str.replace('%', '')
data_output_final['country'] = data_output_final['country'].str.replace('محصول', '')
data_output_final['year'] = data_output_final['year'].astype(str)
data_output_final['year'] = data_output_final['year'].str.replace(']', '')
data_output_final['year'] = data_output_final['year'].str.replace('[', '')
data_output_final['year'] = data_output_final['year'].str.replace("'", "")

data_output_final['Direct'] = data_output_final['Direct'].str.replace('کارگردان', '')
data_output_final['producer'] = data_output_final['producer'].str.replace('تهیه‌کننده', '')
data_output_final['Composer'] = data_output_final['Composer'].str.replace('آهنگساز', '')
data_output_final['Singer'] = data_output_final['Singer'].str.replace('خواننده', '')
data_output_final['writer'] = data_output_final['writer'].str.replace('نویسنده', '')
data_output_final['Cameraman'] = data_output_final['Cameraman'].str.replace('مدیر فیلمبرداری', '')
data_output_final['writer'] = data_output_final['writer'].str.replace('مدیر فیلم‌نامه‌نویس', '')

data_output_final['imdb'] = data_output_final['imdb'].str.strip() 
data_output_final['percent'] = data_output_final['percent'].str.strip() 
data_output_final['country'] = data_output_final['country'].str.strip() 
data_output_final['year'] = data_output_final['year'].str.strip() 
data_output_final['Direct'] = data_output_final['Direct'].str.strip() 
data_output_final['producer'] = data_output_final['producer'].str.strip() 
data_output_final['Composer'] = data_output_final['Composer'].str.strip() 
data_output_final['Singer'] = data_output_final['Singer'].str.strip() 
data_output_final['writer'] = data_output_final['writer'].str.strip() 
data_output_final['Cameraman'] = data_output_final['Cameraman'].str.strip() 

data_output_final = data_output_final.rename(columns={"Epizode Number":"epizode"})

data_output_final['season'] = data_output_final['season'].str.replace('۰', '0')
data_output_final['season'] = data_output_final['season'].str.replace('۱', '1')
data_output_final['season'] = data_output_final['season'].str.replace('۲', '2')
data_output_final['season'] = data_output_final['season'].str.replace('۳', '3')
data_output_final['season'] = data_output_final['season'].str.replace('۴', '4')
data_output_final['season'] = data_output_final['season'].str.replace('۵', '5')
data_output_final['season'] = data_output_final['season'].str.replace('۶', '6')
data_output_final['season'] = data_output_final['season'].str.replace('۷', '7')
data_output_final['season'] = data_output_final['season'].str.replace('۸', '8')
data_output_final['season'] = data_output_final['season'].str.replace('۹', '9')

data_output_final['epizode'] = data_output_final['epizode'].str.replace('۰', '0')
data_output_final['epizode'] = data_output_final['epizode'].str.replace('۱', '1')
data_output_final['epizode'] = data_output_final['epizode'].str.replace('۲', '2')
data_output_final['epizode'] = data_output_final['epizode'].str.replace('۳', '3')
data_output_final['epizode'] = data_output_final['epizode'].str.replace('۴', '4')
data_output_final['epizode'] = data_output_final['epizode'].str.replace('۵', '5')
data_output_final['epizode'] = data_output_final['epizode'].str.replace('۶', '6')
data_output_final['epizode'] = data_output_final['epizode'].str.replace('۷', '7')
data_output_final['epizode'] = data_output_final['epizode'].str.replace('۸', '8')
data_output_final['epizode'] = data_output_final['epizode'].str.replace('۹', '9')

data_output_final['epizode'] = data_output_final['epizode'].str.replace('پشت صحنه', '')
data_output_final['epizode'] = data_output_final['epizode'].str.replace('(قسمت پایانی)', '')
data_output_final['epizode'] = data_output_final['epizode'].str.replace('(بخش اول)', '')
data_output_final['epizode'] = data_output_final['epizode'].str.replace('(بخش دوم)', '')
data_output_final['epizode'] = data_output_final['epizode'].str.replace('(قسمت آخر فصل اول)', '')
for i in range(0,len(data_output_final)):
    print(i)
    try:
        x_name_content=data_output_final.loc[i, 'epizode']
        head, sep, tail = x_name_content.partition(':')
        data_output_final.loc[i, 'epizode'] = head
    except: pass
data_output_final['epizode'] = data_output_final['epizode'].str.strip()

for i in range(0,len(data_output_final)):
    print(i)
    try:
        x_name_content=data_output_final.loc[i, 'epizode']
        head, sep, tail = x_name_content.partition('(')
        data_output_final.loc[i, 'epizode'] = head
    except: pass
data_output_final['epizode'] = data_output_final['epizode'].str.strip()

for i in range(0,len(data_output_final)):
    print(i)
    try:
        x_name_content=data_output_final.loc[i, 'epizode']
        head, sep, tail = x_name_content.partition('قسمت')
        data_output_final.loc[i, 'epizode'] = tail
        data_output_final.loc[i, 'season'] = head
    except: pass

data_output_final['season'] = data_output_final['season'].str.replace('فصل', '')
data_output_final['epizode'] = data_output_final['epizode'].str.replace(')', '')
data_output_final['epizode'] = data_output_final['epizode'].str.replace('(', '')
data_output_final['epizode'] = data_output_final['epizode'].str.replace('اطلاعیه عدم پخش', '')
data_output_final['epizode'] = data_output_final['epizode'].str.replace('پایان فصل اول', '')
data_output_final['season'] = data_output_final['season'].str.replace('ویژه برنامه محرم', '')
data_output_final['epizode'] = data_output_final['epizode'].str.replace('ویژه برنامه محرم', '')
data_output_final['season'] = data_output_final['season'].str.strip()
data_output_final['epizode'] = data_output_final['epizode'].str.strip()

data_output_final.insert(21, 'ReleaseDateGeorgian', '')
data_output_final = data_output_final.rename(columns={"year":"ReleaseDateJalali"})
data_output_final['ReleaseDateJalali'] = data_output_final['ReleaseDateJalali'].astype(int)
data_output_final = data_output_final.reset_index()
del data_output_final['index']
for i in range(0, len(data_output_final)):
    print(i)
    if data_output_final.loc[i, 'ReleaseDateJalali'] > 1500:
        data_output_final.loc[i, 'ReleaseDateGeorgian'] = data_output_final.loc[i, 'ReleaseDateJalali']
        data_output_final.loc[i, 'ReleaseDateJalali'] = ""

data_output_final['season'] = data_output_final['season'].str.strip()
data_output_final['epizode'] = data_output_final['epizode'].str.strip()
data_output_final['epizode'] = data_output_final['epizode'].astype(str)
data_output_final['epizode'] = data_output_final['epizode'].fillna('0')
data_output_final['epizode'].replace('', '0', inplace=True)
for i in range(0, len(data_output_final)):
    if data_output_final.loc[i, 'epizode'] == '0':
        data_output_final.loc[i, 'season'] = '0'
        data_output_final.loc[i, 'FilmSerial'] = 'فیلم'
    else:
        data_output_final.loc[i, 'FilmSerial'] = 'سریال'

data_output_final['actor'] = data_output_final['actor'].astype(str)
data_output_final['genre'] = data_output_final['genre'].map(lambda x: x.lstrip(','))
data_output_final['actor'] = data_output_final['actor'].map(lambda x: x.rstrip(','))

data_output_final.to_excel('filimo4.xlsx', index = False)



























