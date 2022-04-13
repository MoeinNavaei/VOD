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
start = time.time()
#list_title=pd.read_excel(r'E:\python codes\VOD\vod_data\1400\filimo_vod\FilimoMordad1400.xlsx')


#filimo1=pd.read_excel(r'E:\python codes\VOD\vod_data\1400\filimo_vod\data_output_total1.xlsx')
#filimo2=pd.read_excel(r'E:\python codes\VOD\vod_data\1400\filimo_vod\data_output_total2.xlsx')
#filimo = pd.DataFrame()
#filimo = filimo1.append([filimo2])
#filimo.drop_duplicates(subset =['title', 'Epizode Number', 'season', 'year'], keep = 'last', inplace = True)
#filimo.to_excel('filimo.xlsx', index=False)
#filimo = data_output_final.copy()


url_filimo='https://www.filimo.com/'

#data_output=pd.DataFrame()
#data_output=pd.DataFrame(data_output, columns=['link_address', 'title', 'latin_title', 'imdb', 'rate_filimo', 'percent', 'Runtime', 'genre', 'country', 'year', 'subtitle', 'FilmSerial', 'Epizode Number', 'season', 'Direct', 'actor', 'producer', 'Composer', 'Singer', 'writer', 'Cameraman'])
site=requests.get(url_filimo)
soup_primary=BeautifulSoup(site.text, 'html.parser')

site_first = 'https://www.filimo.com/tag/'
tag = pd.DataFrame()
tag.insert(0, 'col', '')
for i in range(10074046, 10080000):
    print(i)
    site_address = site_first + str(i)
    site=requests.get(site_address)
    soup_primary=BeautifulSoup(site.text, 'html.parser')
    for link1 in soup_primary.findAll('li'):
        link_url = link1.get('class')
        if link_url:
            if "categoric-menu-bold" in link_url:
                tag.loc[i, 'col'] = site_address
                break
            else:
                pass
 
tag = tag.reset_index()
del tag['index']

tag1 = pd.DataFrame()
tag1.insert(0, 'col', '')
k = 0
for i in range(0, len(tag)):
    link1 = tag.loc[i, 'col']
    site=requests.get(link1)
    soup_primary=BeautifulSoup(site.text, 'html.parser')
    for link in soup_primary.findAll('a'):
        link_url = link.get('href')
        if "/tag/" in link_url:
            tag1.loc[k, 'col'] = link_url
            k = k + 1
            print("k1: ", k)
        else:
            tag1.loc[k, 'col'] = link1
            k = k + 1
            print("k1: ", k)

tag1.drop_duplicates(subset =['col'], keep = 'last', inplace = True)
tag1 = tag1.reset_index()
del tag1['index']

tag2 = pd.DataFrame()
tag2.insert(0, 'col', '')
k = 0
for i in range(0, len(tag1)):
    link2 = tag1.loc[i, 'col']
    site=requests.get(link2)
    soup_primary=BeautifulSoup(site.text, 'html.parser')
    for link in soup_primary.findAll('a'):
        link_url = link.get('href')
        if "/tag/" in link_url:
            tag2.loc[k, 'col'] = link_url
            k = k + 1
            print("k2: ", k)
        else:
            tag2.loc[k, 'col'] = link2
            k = k + 1
            print("k2: ", k)

tag2.drop_duplicates(subset =['col'], keep = 'last', inplace = True)
tag2 = tag2.reset_index()
del tag2['index']

tag3 = pd.DataFrame()
tag3.insert(0, 'col', '')
k = 0
for i in range(0, len(tag2)):
    link3 = tag2.loc[i, 'col']
    site=requests.get(link3)
    soup_primary=BeautifulSoup(site.text, 'html.parser')
    for link in soup_primary.findAll('a'):
        link_url = link.get('href')
        if "/tag/" in link_url:
            tag3.loc[k, 'col'] = link_url
            k = k + 1
            print("k3: ", k)
        else:
            tag3.loc[k, 'col'] = link3
            k = k + 1
            print("k3: ", k)

tag3.drop_duplicates(subset =['col'], keep = 'last', inplace = True)
tag3 = tag3.reset_index()
del tag3['index']

tag4 = pd.DataFrame()
tag4.insert(0, 'col', '')
k = 0
for i in range(0, len(tag3)):
    link4 = tag3.loc[i, 'col']
    site=requests.get(link4)
    soup_primary=BeautifulSoup(site.text, 'html.parser')
    for link in soup_primary.findAll('a'):
        link_url = link.get('href')
        if "/tag/" in link_url:
            tag4.loc[k, 'col'] = link_url
            k = k + 1
            print("k4: ", k)
        else:
            tag4.loc[k, 'col'] = link4
            k = k + 1
            print("k4: ", k)

tag4.drop_duplicates(subset =['col'], keep = 'last', inplace = True)
tag4 = tag4.reset_index()
del tag4['index']

list_title = pd.DataFrame()
i = 0
for k in range(0, len(tag4)):
    per_link = tag4.loc[k, 'col']
    link_all = requests.get(per_link)
    soup_title = BeautifulSoup(link_all.text, 'html.parser')
    for link_item in soup_title.findAll('div', {'class': "item"}):
        for link_link in link_item.findAll('a'):
            list_title.loc[i, 'link_address'] = link_link.get('href')
            i = i + 1
            print("i: ", i)

list_title = list_title [list_title.link_address.str.contains('/m/')]
list_title = list_title.reset_index()
del list_title['index']

######### old list_title #########
FilimoMordad1400=pd.read_excel(r'E:\python codes\VOD\vod_data\1400\filimo_vod\FilimoMordad1400.xlsx')
FilimoMordad1400_link = FilimoMordad1400['LinkAddress']
FilimoShahrivar1400=pd.read_excel(r'E:\python codes\VOD\vod_data\1400\filimo_vod\FilimoShahrivar1400.xlsx')
FilimoShahrivar1400_link = FilimoShahrivar1400['LinkAddress']
link_list_old = pd.DataFrame()
link_list_old = FilimoMordad1400_link.append([FilimoShahrivar1400_link])
link_list_old.drop_duplicates(subset =['LinkAddress'], keep = 'last', inplace = True)
##################################


def SearchingMetaData(link_address):
    data_output=pd.DataFrame()
    data_output=pd.DataFrame(data_output, columns=['link_address', 'title', 'latin_title', 'imdb', 'rate_filimo', 'percent', 'Runtime', 'genre', 'country', 'year', 'subtitle', 'FilmSerial', 'Epizode Number', 'season', 'Direct', 'actor', 'producer', 'Composer', 'Singer', 'writer', 'Cameraman'])
    j = 0
#    data_output.loc[j, 'link_address'] = list_title.loc[i, 'link_address']
    data_output.loc[j, 'link_address'] = link_address
    title_link=requests.get(link_address)
    soup_title=BeautifulSoup(title_link.text, 'html.parser')
#    print(link_address)
    ############## title & epizode ##############
    try:
        title_original=soup_title.find('h1', {'class': "details_poster-description-title ui-mb-8x"})
        data_output.loc[j, 'title']=title_original.text
        title_original_split=title_original.text.split()
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
                            data_output.loc[j, 'Epizode Number']=aa1
                except: pass
                try:
                    for bb in aa2.split():
                        if "قسمت" in bb:
                            data_output.loc[j, 'Epizode Number']=aa2
                except: pass
                try:
                    for bb in aa1.split():
                        if "فصل" in bb:
                            data_output.loc[j, 'season']=aa1
                            season = aa1
                except: pass
                try:
                    for bb in aa2.split():
                        if "فصل" in bb:
                            data_output.loc[j, 'season']=aa2
                            season = aa2
                except: pass
                data_output.loc[j, 'title']=title_original_split_dash[0]
    except: pass
        ############## latin_title ##############
    try:
        title1=soup_title.find('div', {'class': "en-title ui-fw-normal ui-fs-medium force-text-en ui-mb-4x"})
#        latin_title=title1.find('span', {'class': "en-force-text d-block w100"})
        data_output.loc[j, 'latin_title']=title1.text
    except: pass
        ############## imdb ##############
    try:
           imdb1=soup_title.find('div', {'class': "ds-badge ds-badge--icon ds-badge--brand imdb"})
           imdb=imdb1.find('span', {'class': "en ui-fc-black ui-fw-bold"})
           data_output.loc[j, 'imdb']=imdb.text
    except: pass
        ############## rate_filimo ##############
    try:
        rate_filimo1=soup_title.find('span', {'class': "rate_cnt"})
#        rate_filimo=rate_filimo1.find('span', {'id': "rateCnt"})
        data_output.loc[j, 'rate_filimo']=rate_filimo1.text
    except: pass
############## percent ##############
    try:
        percent1=soup_title.find('span', {'id': "percentNumber"})
#        percent=percent1.find('span', {'id': "percentNumber"})
        data_output.loc[j, 'percent']=percent1.text
    except: pass
        ############## director ##############
    try:
        director1=soup_title.find('div', {'class': "meta-wrapper ui-pt-4x ui-fs-small"})
        for director in director1.findAll('a'):
            data_output.loc[j, 'Direct']=director.text
    except: pass
        ############## genre ##############
    try:
        genre1=soup_title.findAll('li', {'class': "ui-ml-2x"})            
        genre2=""
        for genre_per in genre1:
            genre2=genre2+','+genre_per.text
        data_output.loc[j, 'genre']=genre2
        data_output['genre'] = data_output['genre'].str.strip()
    except: pass
        ############## Runtime ##############
    try:
        Runtime=soup_title.find('div', {'class': "details_poster-description-age ui-fc-primary ui-fi-primary d-inline-flex"})
        data_output.loc[j, 'Runtime']=Runtime.text
        data_output['Runtime'] = data_output['Runtime'].str.strip()
    except: pass
        ############## Director ##############
    try:
        Director=soup_title.find('div', {'class': "details_poster-description-director ui-mb-4x"})
        data_output.loc[j, 'Direct']=Director.text
    except: pass
        ############## MetaData ##############
    try:
        MetaData1=soup_title.find('div', {'class': "details_poster-description-more ui-mb-6x d-flex"})
        MetaData=MetaData1.text.split('-')
        year=re.findall('\d{4}', MetaData1.text )
        data_output.loc[j, 'year']=year
        MetaData = pd.DataFrame({'col': MetaData})
        for word in range(0, len(MetaData)):
            meta = MetaData.loc[word, 'col']
            if "محصول" in meta:
                data_output.loc[j, 'country']=meta
                country = meta
            if "زیرنویس دارد" in meta:
                data_output.loc[j, 'subtitle']=meta
                subtitle = meta
    except: pass
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
        for ii in range(0, len(actor)):
            actor_list = actor.loc[ii, 'col']+"," + actor_list
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
    try:
        OPL2=soup_title.find('div', {'class': "single-section section-episodes"})
        OPL1=OPL2.find('div', {'class': "tabs-content"})
        for search in OPL1.findAll('li', {'class': "accordion-item"}):
            search_rate = search.find('span', {'id': "rateCnt"})
            search_percent = search.find('span', {'id': "percentNumber"})
            search_epizode = search.find('span', {'class': "episode"})
            data_output.loc[j, 'rate_filimo']=search_rate.text
            data_output.loc[j, 'percent']=search_percent.text
            data_output.loc[j, 'Epizode Number']=search_epizode.text
            #*****#
            data_output.loc[j, 'title']=title_original.text
            j = j + 1
    except: pass
        ############## FilmSerial ##############    
#    try:
#        FS = soup_title.find('h2', {'class': "gallery_each-movie_title ui-fw-bold ui-mb-2x ui-fs-pre-title"})
#        data_output.loc[j, 'FilmSerial'] = FS.text
#        data_output['FilmSerial'] = data_output['FilmSerial'].str.replace('داستان', '')
#        data_output['FilmSerial'] = data_output['FilmSerial'].str.strip() 
#        data_output["FilmSerial"]= data_output["FilmSerial"].str.split(" ", n = 1, expand = True)
#
#    except: pass
    j = j + 1
    return data_output

def SearchingMetaData2(link_address2, data_output, season):
    data_output1=pd.DataFrame()
    data_output1=pd.DataFrame(data_output1, columns=['link_address', 'title', 'latin_title', 'imdb', 'rate_filimo', 'percent', 'Runtime', 'genre', 'country', 'year', 'subtitle', 'FilmSerial', 'Epizode Number', 'season', 'Direct', 'actor', 'producer', 'Composer', 'Singer', 'writer', 'Cameraman'])
    title_link2=requests.get(link_address2)
    soup_title2=BeautifulSoup(title_link2.text, 'html.parser')
    j = 0
    for meta in soup_title2.findAll('li', {'class': "accordion-item"}):
        data_output1.loc[j, 'season'] = season
        episode = meta.find('span', {'class': "episode"})
        data_output1.loc[j, 'Epizode Number'] = episode.text
        rate_filimo1=meta.find('span', {'class': "rate_cnt"})
        data_output1.loc[j, 'rate_filimo']=rate_filimo1.text
        percent1=meta.find('span', {'id': "percentNumber"})
        data_output1.loc[j, 'percent']=percent1.text
        data_output1.loc[j, 'link_address'] = link_address2
        data_output1.loc[j, 'title'] = data_output.loc[0, 'title']
        data_output1.loc[j, 'latin_title'] = data_output.loc[0, 'latin_title']
        data_output1.loc[j, 'Runtime'] = data_output.loc[0, 'Runtime']
        data_output1.loc[j, 'genre'] = data_output.loc[0, 'genre']
        data_output1.loc[j, 'country'] = data_output.loc[0, 'country']
        data_output1.loc[j, 'year'] = data_output.loc[0, 'year']
        data_output1.loc[j, 'Direct'] = data_output.loc[0, 'Direct']
        data_output1.loc[j, 'actor'] = data_output.loc[0, 'actor']
        data_output1.loc[j, 'producer'] = data_output.loc[0, 'producer']
        data_output1.loc[j, 'Composer'] = data_output.loc[0, 'Composer']
        data_output1.loc[j, 'Singer'] = data_output.loc[0, 'Singer']
        data_output1.loc[j, 'writer'] = data_output.loc[0, 'writer']
        data_output1.loc[j, 'Cameraman'] = data_output.loc[0, 'Cameraman']
        data_output1.loc[j, 'subtitle'] = data_output.loc[0, 'subtitle']
        data_output1.loc[j, 'imdb'] = data_output.loc[0, 'imdb']
        j = j + 1
    return data_output1

def SearchingMetaData3(link_address, data_output, season, link_address3):
    data_output1=pd.DataFrame()
    data_output1=pd.DataFrame(data_output1, columns=['link_address', 'title', 'latin_title', 'imdb', 'rate_filimo', 'percent', 'Runtime', 'genre', 'country', 'year', 'subtitle', 'FilmSerial', 'Epizode Number', 'season', 'Direct', 'actor', 'producer', 'Composer', 'Singer', 'writer', 'Cameraman'])
    j = 0
    for meta in link_address3.findAll('li', {'class': "episodev2_content-item ui-pb-6x ui-mb-6x"}):
        data_output1.loc[j, 'season'] = season.text
        episode=meta.find('h2', {'class': "episodev2_content-summery-title ui-fw-normal pointer d-transition"})
        data_output1.loc[j, 'Epizode Number'] = episode.text
        rate_filimo1=meta.find('span', {'class': "rate-number-count"})
        data_output1.loc[j, 'rate_filimo']=rate_filimo1.text
        percent1=meta.find('div', {'class': "episodev2_content-summery-detail-rate ui-fc-gray-80 d-transition d-inline-flex ui-br-24 ui-ml-2x ps-relative ui-ai-c"})
        percent=percent1.findAll('span')
        data_output1.loc[j, 'percent']=percent[1].text
        data_output1.loc[j, 'link_address'] = link_address
        data_output1.loc[j, 'title'] = data_output.loc[0, 'title']
        data_output1.loc[j, 'latin_title'] = data_output.loc[0, 'latin_title']
        data_output1.loc[j, 'Runtime'] = data_output.loc[0, 'Runtime']
        data_output1.loc[j, 'genre'] = data_output.loc[0, 'genre']
        data_output1.loc[j, 'country'] = data_output.loc[0, 'country']
        data_output1.loc[j, 'year'] = data_output.loc[0, 'year']
        data_output1.loc[j, 'Direct'] = data_output.loc[0, 'Direct']
        data_output1.loc[j, 'actor'] = data_output.loc[0, 'actor']
        data_output1.loc[j, 'producer'] = data_output.loc[0, 'producer']
        data_output1.loc[j, 'Composer'] = data_output.loc[0, 'Composer']
        data_output1.loc[j, 'Singer'] = data_output.loc[0, 'Singer']
        data_output1.loc[j, 'writer'] = data_output.loc[0, 'writer']
        data_output1.loc[j, 'Cameraman'] = data_output.loc[0, 'Cameraman']
        data_output1.loc[j, 'subtitle'] = data_output.loc[0, 'subtitle']
        data_output1.loc[j, 'imdb'] = data_output.loc[0, 'imdb']
        j = j + 1
    data_output1.drop_duplicates(subset =['title', 'Epizode Number', 'season', 'country'], keep = 'last', inplace = True)
    return data_output1



#list_title1=pd.read_excel(r'E:\python codes\VOD\vod_data\1400\filimo_vod\FilimoMordad1400.xlsx')
#list_title = list_title1.copy()
#list_title.drop_duplicates(subset =['link_address'], keep = 'last', inplace = True)
#list_title = list_title.reset_index()
#del list_title['index']
#del data_output_total
#print(list_title.loc[3079, 'link_address'])
#list_title = pd.read_excel(r'E:\python codes\VOD\vod_data\1400\filimo_vod\FilimoMordad1400.xlsx')
#list_title.drop_duplicates(subset =['link_address'], keep = 'first', inplace = True)
#print(list_title.loc[2912, 'link_address'])
#del list_title
#list_title = list_title1.copy()
list_title = pd.read_excel(r'E:\python codes\VOD\list_title.xlsx')


data_output_total = pd.DataFrame()
for i in range(0, len(list_title)):    #   len(list_title)
    try:
        print(i)
        link_address = list_title.loc[i, 'link_address']
        site=requests.get(link_address)
        soup_second=BeautifulSoup(site.text, 'html.parser')
        if soup_second.findAll('div', {'class': "episodes-container"}):
            data_output = SearchingMetaData(link_address)
            site=requests.get(link_address)
            soup_second=BeautifulSoup(site.text, 'html.parser')
            for link in soup_second.findAll('button'):
                link_address2 = link.get('data-href')
                try:
                    if "/cms/serial/" in link_address2:
                        season = link.get('data-season-number')
                        link_address2 = link_address2.replace("?epiv2", "")
                        data_output1 = SearchingMetaData2(link_address2, data_output, season)
                        data_output_total = data_output1.append([data_output_total])
                    else:
                        link_address3 = soup_second.find('div', {'class': "episodes-container"})
                        season = soup_second.find('span', {'class': "ui-ml-6x sm-ui-ml-3x episodev2_title-part-label episodev2_space-left"})
                        data_output1 = SearchingMetaData3(link_address, data_output, season, link_address3)
                        data_output_total = data_output1.append([data_output_total])
                except: pass
            del data_output
            try:
                del data_output1
            except: pass
        else:
            data_output = SearchingMetaData(link_address)
            data_output_total = data_output.append([data_output_total])
            del data_output
    except: pass

data_output_total_final = data_output_total.copy()
data_output_total_final.drop_duplicates(subset =['title', 'Epizode Number', 'country', 'year'], keep = 'first', inplace = True)
data_output_total_final = data_output_total_final.reset_index()
del data_output_total_final['index']
data_output_total_final.to_excel('data_output_total_final.xlsx', index=False)

data_output_final = data_output_total_final.copy()
data_output_final.drop_duplicates(subset =['title', 'Epizode Number', 'season', 'year'], keep = 'first', inplace = True)

data_output_final['imdb'] = data_output_final['imdb'].str.replace('/10', '')
data_output_final['percent'] = data_output_final['percent'].str.replace('%', '')
data_output_final['country'] = data_output_final['country'].str.replace('محصول', '')
data_output_final['season'] = data_output_final['season'].str.replace('تک فصلی', '1')
data_output_final['year'] = data_output_final['year'].astype(str)
data_output_final['year'] = data_output_final['year'].str.replace('.0', '')

data_output_final['Direct'] = data_output_final['Direct'].str.replace('کارگردان', '')
data_output_final['producer'] = data_output_final['producer'].str.replace('تهیه‌کننده', '')
data_output_final['Composer'] = data_output_final['Composer'].str.replace('آهنگساز', '')
data_output_final['Singer'] = data_output_final['Singer'].str.replace('خواننده', '')
data_output_final['writer'] = data_output_final['writer'].str.replace('نویسنده', '')
data_output_final['Cameraman'] = data_output_final['Cameraman'].str.replace('مدیر فیلمبرداری', '')
data_output_final['writer'] = data_output_final['writer'].str.replace('مدیر فیلم‌نامه‌نویس', '')

data_output_final['title'] = data_output_final['title'].str.strip() 
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

data_output_final.to_excel('data_output_final.xlsx', index = False)

print("--- %s seconds ---" % (time.time() - start))










