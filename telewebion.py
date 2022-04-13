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


url_telewebion = 'https://telewebion.com'
site = requests.get(url_telewebion)
soup_primary = BeautifulSoup(site.text, 'html.parser')

link_list_film = pd.read_csv(r'E:\python codes\VOD\telewebion list\link_list_film.csv')

start = time.time()

title_link__serial_tw = pd.DataFrame()
for i in range(2340, len(link_list_film)):     # len(link_list_film)
    print(i)
#    x = i / 20
#    if x == round(x, 0):
#       time.sleep(600)
    try:
        site = requests.get(link_list_film.loc[i, 'link_address'])
        if site:
            title_link__serial_tw.loc[i, 'link_address'] = link_list_film.loc[i, 'link_address']
    except: pass

#title_link__serial_tw.drop_duplicates(subset =['link_address'], keep = 'last', inplace = True)
title_link__serial_tw = title_link__serial_tw.reset_index()
del title_link__serial_tw['index']
title_link__serial_tw.to_excel('title_link__serial_tw.xlsx', index=False)
print("--- %s min ---" % round((time.time() - start)/60, 2))



def meta2(soup_primary):
    meta2 = soup_primary.find('section', {'class': 'season-container'})
    try:
        Casts = meta2.find('div', {'class': 'cast-container'})
        casts_list = ''
        for casts_per in Casts.findAll('div', {'class': 'cast-name'}):
            casts_list = casts_list + '،' + casts_per.text
        data_output_tw.loc[i, 'Casts'] = casts_list
#        print(casts_list)
    except: pass
    try:
        meta2_details = meta2.find('div', {'class': 'detail-column'}) 
    except: pass
    try:
        Director = meta2_details.find('span', {'data-type': 'director'})
        Director_list = ''
        for Director_per in Director.findAll('a'):
            Director_list = Director_list + '،' + Director_per.text
        data_output_tw.loc[i, 'Director'] = Director_list
#        print(Director_list)
    except: pass
    try:
        producer = meta2_details.find('span', {'data-type': 'producer'})
        producer_list = ''
        for producer_per in producer.findAll('a'):
            producer_list = producer_list + '،' + producer_per.text
        data_output_tw.loc[i, 'producer'] = producer_list
#        print(producer_list)
    except: pass
    try:
        writer = meta2_details.find('span', {'data-type': 'writer'})
        writer_list = ''
        for writer_per in writer.findAll('a'):
            writer_list = writer_list + '،' + writer_per.text
        data_output_tw.loc[i, 'writer'] = writer_list
#        print(writer_list)
    except: pass
    try:
        composer = meta2_details.find('span', {'data-type': 'composer'})
        composer_list = ''
        for composer_per in composer.findAll('a'):
           composer_list = composer_list + '،' + composer_per.text
        data_output_tw.loc[i, 'composer'] = composer_list
#        print(composer_list)
    except: pass
    try:
        editor = meta2_details.find('span', {'data-type': 'director'})
        editor_list = ''
        for editor_per in editor.findAll('a'):
            editor_list = editor_list + '،' + editor_per.text
        data_output_tw.loc[i, 'editor'] = editor_list
#        print(editor_list)
    except: pass
    return data_output_tw

def Genre(soup_primary):
    info_box = soup_primary.find('div', {'class': 'info-box'})
    try:
        genres_list = ''
        Genres = info_box.find('div', {'class': 'genre-list'})
        for genre_per in Genres.findAll('a'):
            genres_list = genres_list + '،' + genre_per.text
        data_output_tw.loc[i, 'Genre'] = genres_list
#        print(genres_list)
    except: pass
    return data_output_tw

def meta1(soup_primary):
    info_box = soup_primary.find('div', {'class': 'info-box'})
    try:
        meta1 = info_box.find('div', {'class': 'icon-info'})
        for Xmeta1 in meta1.findAll('div', {'class': 'icon'}):
            try:
                if Xmeta1.find('span', {'data-type': 'calendar'}):
                    year = Xmeta1.find('a')
                    data_output_tw.loc[i, 'year'] = year.text
    #                print(year.text)
                elif Xmeta1.find('span', {'data-type': 'clock'}):
                    Runtime = Xmeta1.find('i')
                    data_output_tw.loc[i, 'Runtime'] = Runtime.text
    #                print(Runtime.text)
                elif Xmeta1.find('span', {'data-type': 'language'}):
                    language = Xmeta1.find('a')
                    data_output_tw.loc[i, 'language'] = language.text
    #                print(language.text)
                elif Xmeta1.find('span', {'data-type': 'dubbed'}):
                    dubbed = Xmeta1.find('a')
                    data_output_tw.loc[i, 'dubbed'] = dubbed.text
    #                print(dubbed.text)
                elif Xmeta1.find('span', {'data-type': 'imdb'}):
                    imdb = Xmeta1.find('i')
                    data_output_tw.loc[i, 'imdb'] = imdb.text
    #                print(imdb.text)
            except: pass
        try:
            AgeRange1 = meta1.find('div', {'class': 'filter-prop'})
            AgeRange = AgeRange1.find('span', {'class': 'age-info'})
            data_output_tw.loc[i, 'AgeRange'] = AgeRange.text
    #        print(AgeRange.text)
        except: pass
    except: pass
    return data_output_tw

def country(soup_primary):
    info_box = soup_primary.find('div', {'class': 'info-box'})
    try:
        country = info_box.find('a', {'data-type': 'product'})
        data_output_tw.loc[i, 'country'] = country.text
#        print(country.text)
    except: pass
    return data_output_tw

def title(soup_primary):
    info_box = soup_primary.find('div', {'class': 'info-box'})
    try:
        title = info_box.find('div', {'class': 'title'})
        data_output_tw.loc[i, 'title'] = title.text
#        print(title.text)
    except: pass
    return data_output_tw


#link_tw = ['https://vod.telewebion.com/p/ff3e', 'https://vod.telewebion.com/p/567d', 'https://vod.telewebion.com/p/ec4c']
data_output_tw = pd.DataFrame()

for i in range(0, 3):
    print(i)
#    link = link_tw[i]
    link = link_tw.loc[i, 'link_address']
    data_output_tw.loc[i, 'link_address'] = link
    site = requests.get(link)
    soup_primary = BeautifulSoup(site.text, 'html.parser')
    data_output_tw = meta2(soup_primary)
    data_output_tw = Genre(soup_primary)
    data_output_tw = meta1(soup_primary)
    data_output_tw = country(soup_primary)
    data_output_tw = title(soup_primary)











