import pandas as pd
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from bs2json import bs2json
import re
import requests
from urllib.request import urlopen as uReq
import time
import sys
import json
start = time.time()
########################################
from selenium import webdriver
########################################
url_filmgardi='https://filmgardi.com'
site=requests.get(url_filmgardi)
soup_primary = BeautifulSoup(site.text, 'html.parser')

link_list=pd.read_csv(r'E:\python codes\VOD\filmgardi list\link_list.csv')

title_link = pd.DataFrame()

start = time.time()
for i in range(9124, len(link_list)):     # len(link_list)
    print(i)
#    time.sleep(10)
    site = requests.get(link_list.loc[i, 'LinkAddress'])
    if site:
        title_link.loc[i, 'LinkAddress'] = link_list.loc[i, 'LinkAddress']

title_link = title_link.reset_index()
del title_link['index']
title_link.to_excel('title_link_mehr.xlsx', index=False)
print("--- %s min ---" % round((time.time() - start)/60, 2))



def title(soup_primary):
    info_box = soup_primary.find('div', {'class': 'info-box'})
    try:
        title = info_box.find('h1', {'class': 'title'})
        data_output_FG.loc[i, 'Title'] = title.text
#        print(title.text)
    except: pass
    return data_output_FG

def country(soup_primary):
    info_box = soup_primary.find('div', {'class': 'info-box'})
    try:
        country1 = info_box.find('div', {'class': 'country-name'})
        country = country1.find('a')
        data_output_FG.loc[i, 'Country'] = country.text
        # print(country.text)
    except: pass
    return data_output_FG 

def FilmSerial(soup_primary):
    info_box = soup_primary.find('div', {'class': 'info-box'})
    try:
        FilmSerial = info_box.find('div', {'class': 'serial-info'})
        data_output_FG.loc[i, 'FilmSerial'] = FilmSerial.text
        # print(FilmSerial.text)
    except: pass
    return data_output_FG

def Runtime(soup_primary):
    info_box = soup_primary.find('div', {'class': 'info-box'})
    try:
        Runtime = info_box.find('div', {'class': 'time-info'})
        data_output_FG.loc[i, 'Runtime'] = Runtime.text
        # print(Runtime.text)
    except: pass
    return data_output_FG

def metadata1(soup_primary):
    info_box = soup_primary.find('div', {'class': 'info-box'})
    try:
        meta1 = info_box.find('div', {'class': 'icon-info'})
        for xx in meta1.findAll('div', {'class': 'icon'}):
            xx1 = xx.find('span')
            xx2 = xx.find('a')
            if xx1.get('data-type') == 'calendar':
                data_output_FG.loc[i, 'Year'] = xx2.text
#                print("year")
#                print(xx2.text)
            elif xx1.get('data-type') == 'language':
                data_output_FG.loc[i, 'Language'] = xx2.text
#                print("language")
#                print(xx2.text)
            elif xx1.get('data-type') == 'dubbed':
                data_output_FG.loc[i, 'Dubbed_Subtitle'] = xx2.text
#                print("dubbed")
#                print(xx2.text)
        try:
            imdb = meta1.find('i')
            data_output_FG.loc[i, 'Imdb'] = imdb.text
#            print("imdb")
#            print(imdb.text)
        except: pass
        try:
            AgeRange = meta1.find('div', {'class': 'little-info'}) 
            data_output_FG.loc[i, 'AgeRange'] = AgeRange.text
#            print(AgeRange.text)
        except: pass
    except: pass
    return data_output_FG

def Genre(soup_primary):
    info_box = soup_primary.find('div', {'class': 'info-box'})
    try:
        genre_list = ""
        Genre = info_box.find('div', {'class': 'genre-list'})
        for genre1 in Genre.findAll('a'):
            genre_list = genre_list + "،" + genre1.text
#        print(genre_list)
        data_output_FG.loc[i, 'Genres'] = genre_list
    except: pass
    return data_output_FG

def metadata2(soup_primary):
    try:
        meta2 = soup_primary.find('section', {'class': 'season-container'})
        Casts1 = meta2.find('div', {'class': 'cast-container'})
        Casts = Casts1.find('div', {'class': 'cast-list__container'})
        cast_list = ''
        for cast1 in Casts.findAll('span'):
            cast_list = cast_list + '،' + cast1.text
#        print(cast_list)
        data_output_FG.loc[i, 'Casts'] = cast_list
    except: pass
    try:
        meta2 = soup_primary.find('section', {'class': 'season-container'})
        meta3 = meta2.find('div', {'class': 'detail-column'})
        meta4 = meta3.find('div', {'class': 'first-child'})
        try:
            director1 = meta4.find('div', {'data-type': 'director'})
            director_list = ''
            for director2 in director1.findAll('a'):
                director_list = director_list + '،' + director2.text
            data_output_FG.loc[i, 'Director'] = director_list
#            print(director.text)
        except: pass
        try:
            producer1 = meta4.find('div', {'data-type': 'producer'})
            producer_list = ''
            for producer2 in producer1.findAll('a'):
                producer_list = producer_list + '،' + producer2.text
            data_output_FG.loc[i, 'Producer'] = producer_list
#            print(producer.text)
        except: pass
        try:
            writer1 = meta4.find('div', {'data-type': 'writer'})
            writer_list = ''
            for writer2 in writer1.findAll('a'):
                writer_list = writer_list + '،' + writer2.text
            data_output_FG.loc[i, 'Writer'] = writer_list
#            print(writer.text)
        except: pass
        try:
            composer1 = meta4.find('div', {'data-type': 'composer'})
            composer_list = ''
            for composer2 in composer1.findAll('a'):
                composer_list = composer_list + '،' + composer2.text
            data_output_FG.loc[i, 'Composer'] = composer_list
#            print(composer.text)
        except: pass
        try:
            editor1 = meta4.find('div', {'data-type': 'editor'})
            editor_list = ''
            for editor2 in editor1.findAll('a'):
                editor_list = editor_list + '،' + editor2.text
            data_output_FG.loc[i, 'Editor'] = editor_list
#            print(editor.text)
        except: pass
    except: pass
    return data_output_FG

#link_list = ['https://filmgardi.com/p/1008', 'https://filmgardi.com/p/1010', 'https://filmgardi.com/p/d859']
#link_list=pd.read_excel(r'E:\python codes\VOD\title_link.xlsx')
title_link = title_link.reset_index()
del title_link['index']
data_output_FG = pd.DataFrame()
#start = time.time()
for i in range(21614, len(title_link)):     # len(title_link)
    print(i)
#    link = link_list[i]
    link = title_link.loc[i, 'LinkAddress']
    data_output_FG.loc[i, 'LinkAddress'] = link
    site = requests.get(link)
    soup_primary = BeautifulSoup(site.text, 'html.parser')
    data_output_FG = title(soup_primary)
    data_output_FG = country(soup_primary)
    data_output_FG = FilmSerial(soup_primary)
    data_output_FG = Runtime(soup_primary)
    data_output_FG = metadata1(soup_primary)
    data_output_FG = Genre(soup_primary)
    data_output_FG = metadata2(soup_primary)
print("--- %s min ---" % round((time.time() - start)/60, 2))    

data_output_FG.to_excel('data_output_FG.xlsx', index=False)


#data_output_FG = pd.read_excel(r'E:\python codes\VOD\filmgardi list\data_output_FG.xlsx')
driver = webdriver.Chrome(executable_path= r'E:\python codes\VOD\chromedriver.exe')
for i in range(0, len(title_link)):
    link = data_output_FG.loc[i, 'LinkAddress']
#    print(link)
    driver.get(link)
    try:
        percent = driver.find_element_by_xpath('//div[@class="vote-percent"]')
        data_output_FG.loc[i, 'Percent'] = percent.text
    except: pass
    try:
        like = driver.find_element_by_xpath('//div[@class="vote-total"]')
        data_output_FG.loc[i, 'Like'] = like.text
    except: pass

data_output_FG.to_excel('data_output_FG.xlsx', index=False)





print("***********************************************")  











