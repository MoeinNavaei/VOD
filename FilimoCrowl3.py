
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
#url_filimo='https://www.filimo.com/'
#list_title = pd.read_excel(r'C:\MO\list_title.xlsx')
#site=requests.get(url_filimo)
#soup_primary=BeautifulSoup(site.text, 'html.parser')
site_first = 'https://www.filimo.com/tag/'

list_tags = pd.read_excel(r'E:\python codes\VOD\filimo list\list_tags.xlsx')
#s = requests.session()
#s.cookies.keys()

tag = pd.DataFrame()
tag.insert(0, 'col', '')

start = time.time()
for i in range(10096865, 10099999):
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
print("--- %s seconds ---" % (time.time() - start))
 
tag = tag.reset_index()
del tag['index']

######
tag.to_excel('tag.xlsx', index=False)
#####



tag1 = pd.DataFrame()
tag1.insert(0, 'col', '')
k = 0
for i in range(0, len(tag)):     # len(tag)
    print(i)
    link1 = tag.loc[i, 'col']
    site=requests.get(link1)
    soup_primary=BeautifulSoup(site.text, 'html.parser')
    for link in soup_primary.findAll('a'):
        link_url = link.get('href')
        if "/tag/" in link_url:
            tag1.loc[k, 'col'] = link_url
            k = k + 1
#            print("k1: ", k)
        else:
            tag1.loc[k, 'col'] = link1
            k = k + 1
#            print("k1: ", k)

tag1.drop_duplicates(subset =['col'], keep = 'last', inplace = True)
tag1 = tag1.reset_index()
del tag1['index']

tag2 = pd.DataFrame()
tag2.insert(0, 'col', '')
k = 0
for i in range(0, len(tag1)):
    print(i)
    link2 = tag1.loc[i, 'col']
    site=requests.get(link2)
    soup_primary=BeautifulSoup(site.text, 'html.parser')
    for link in soup_primary.findAll('a'):
        link_url = link.get('href')
        if "/tag/" in link_url:
            tag2.loc[k, 'col'] = link_url
            k = k + 1
#            print("k2: ", k)
        else:
            tag2.loc[k, 'col'] = link2
            k = k + 1
#            print("k2: ", k)

tag2.drop_duplicates(subset =['col'], keep = 'last', inplace = True)
tag2 = tag2.reset_index()
del tag2['index']

tag3 = pd.DataFrame()
tag3.insert(0, 'col', '')
k = 0
for i in range(0, len(tag2)):
    print(i)
    link3 = tag2.loc[i, 'col']
    site=requests.get(link3)
    soup_primary=BeautifulSoup(site.text, 'html.parser')
    for link in soup_primary.findAll('a'):
        link_url = link.get('href')
        if "/tag/" in link_url:
            tag3.loc[k, 'col'] = link_url
            k = k + 1
#            print("k3: ", k)
        else:
            tag3.loc[k, 'col'] = link3
            k = k + 1
#            print("k3: ", k)

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

#tag3.to_excel('tag3.xlsx', index=False)
################# old address #################
ListTotalOld = pd.DataFrame()
ListMehr1400_list = pd.DataFrame()

tag3 = pd.read_excel(r'C:\MO\tag3.xlsx')
ListMordad1400 = pd.read_excel(r'C:\MO\FilimoMordad1400.xlsx')
ListShahrivar1400 = pd.read_excel(r'C:\MO\FilimoShahrivar1400.xlsx')
ListMehr1400 = pd.read_excel(r'C:\MO\FilimoMehr1400.xlsx')
ListMordad1400_list['LinkAddress'] = ListMordad1400['LinkAddress']
ListMordad1400_list = pd.DataFrame(ListMordad1400_list)
ListShahrivar1400_list['LinkAddress'] = ListShahrivar1400['LinkAddress']
ListShahrivar1400_list = pd.DataFrame(ListShahrivar1400_list)

ListMehr1400_list['LinkAddress'] = ListMehr1400['LinkAddress']
ListMehr1400_list = pd.DataFrame(ListMehr1400_list)
ListTotalOld = ListMordad1400_list.append([ListShahrivar1400_list, ListMehr1400_list])
ListTotalOld.drop_duplicates(subset =['LinkAddress'], keep = 'last', inplace = True)
###############################################

list_title = pd.DataFrame()
i = 0
for k in range(0, len(tag3)):
    per_link = tag3.loc[k, 'col']
    link_all = requests.get(per_link)
    soup_title = BeautifulSoup(link_all.text, 'html.parser')
    for link_item in soup_title.findAll('div', {'class': "item"}):
        for link_link in link_item.findAll('a'):
            list_title.loc[i, 'LinkAddress'] = link_link.get('href')
            i = i + 1
            print("i: ", i)

list_title2 = list_title.copy()
list_title = list_title2.copy()

list_title = list_title [list_title.LinkAddress.str.contains('/m/')]
list_title.drop_duplicates(subset =['LinkAddress'], keep = 'last', inplace = True)
list_title = list_title.reset_index()
del list_title['index']


list_title = list_title.append([ListTotalOld])
list_title.drop_duplicates(subset =['LinkAddress'], keep = 'last', inplace = True)
list_title = list_title.reset_index()
del list_title['index']

#list_title.to_excel('list_title.xlsx', index=False)


def SearchingMetaData(LinkAddress):
    data_output=pd.DataFrame()
    data_output=pd.DataFrame(data_output, columns=['LinkAddress', 'Title', 'EnglishName', 'Imdb', 'Like', 'Percent', 'AgeRange', 'Runtime', 'Genres', 'Country', 'Year', 'DubbedSubtitle', 'FilmSerial', 'Epizode', 'Season', 'Director', 'Casts', 'Producer', 'Composer', 'Singer', 'Writer', 'Cameraman'])
    j = 0
    data_output.loc[j, 'LinkAddress'] = LinkAddress
    title_link=requests.get(LinkAddress)
    soup_title=BeautifulSoup(title_link.text, 'html.parser')
    ############## Title & epizode ##############
    try:
        title_original=soup_title.find('div', {'class': "fa-title ui-fw-semibold"})
        data_output.loc[j, 'Title']=title_original.text
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
                            data_output.loc[j, 'Epizode']=aa1
                except: pass
                try:
                    for bb in aa2.split():
                        if "قسمت" in bb:
                            data_output.loc[j, 'Epizode']=aa2
                except: pass
                try:
                    for bb in aa1.split():
                        if "فصل" in bb:
                            data_output.loc[j, 'Season']=aa1
                            season = aa1
                except: pass
                try:
                    for bb in aa2.split():
                        if "فصل" in bb:
                            data_output.loc[j, 'Season']=aa2
                            season = aa2
                except: pass
                data_output.loc[j, 'Title']=title_original_split_dash[0]
    except: pass
        ############## EnglishName ##############
    try:
        title1=soup_title.find('p', {'class': "en-title ui-fw-normal ui-fs-medium force-text-en d-inline-bock"})
#        EnglishName=title1.find('span', {'class': "en-force-text d-block w100"})
        data_output.loc[j, 'EnglishName']=title1.text
    except: pass
        ############## Imdb ##############
    try:
           imdb1=soup_title.find('div', {'class': "ds-badge ds-badge--icon ds-badge--brand imdb"})
           Imdb=imdb1.find('span', {'class': "en ui-fc-black ui-fw-bold"})
           data_output.loc[j, 'Imdb']=Imdb.text
    except: pass
        ############## Like ##############
    try:
        rate_filimo1=soup_title.find('span', {'class': "rate_cnt"})
#        Like=rate_filimo1.find('span', {'id': "rateCnt"})
        data_output.loc[j, 'Like']=rate_filimo1.text
    except: pass
        ############## Percent ##############
    try:
        percent1=soup_title.find('span', {'id': "percentNumber"})
#        Percent=percent1.find('span', {'id': "percentNumber"})
        data_output.loc[j, 'Percent']=percent1.text
    except: pass
        ############## Director ##############
    try:
        director1=soup_title.find('div', {'class': "meta-wrapper ui-pt-4x ui-fs-small"})
        for director in director1.findAll('a'):
            data_output.loc[j, 'Director']=director.text
    except: pass
        ############## Genres ##############
    try:
        genre1=soup_title.findAll('li', {'class': "ui-ml-2x"})            
        genre2=""
        for genre_per in genre1:
            genre2=genre2+','+genre_per.text
        data_output.loc[j, 'Genres']=genre2
        data_output['Genres'] = data_output['Genres'].str.strip()
    except: pass
        ############## AgeRange ##############
    try:
        AgeRange=soup_title.find('div', {'class': "details_poster-description-age ui-fc-primary ui-fi-primary d-inline-flex"})
        data_output.loc[j, 'AgeRange']=AgeRange.text
        data_output['AgeRange'] = data_output['AgeRange'].str.strip()
    except: pass
        ############## Director ##############
    try:
        Director=soup_title.find('div', {'class': "details_poster-description-director ui-mb-4x"})
        data_output.loc[j, 'Director']=Director.text
    except: pass
        ############## MetaData ##############
    try:
        MetaData1=soup_title.find('tr', {'class': "details_poster-description-more ui-mb-6x d-flex"})
        MetaData=MetaData1.text.split('-')
        Year=re.findall('\d{4}', MetaData1.text )
        data_output.loc[j, 'Year']=Year
        MetaData = pd.DataFrame({'col': MetaData})
        for word in range(0, len(MetaData)):
            meta = MetaData.loc[word, 'col']
            if "محصول" in meta:
                data_output.loc[j, 'Country']=meta
                Country = meta
            if "زیرنویس دارد" in meta:
                data_output.loc[j, 'DubbedSubtitle']=meta
                subtitle = meta
            if "دقیقه" in meta:
                data_output.loc[j, 'Runtime']=meta
                Runtime = meta
    except: pass
        ############## Casts ##############
    try:
        k = 0
        actor = pd.DataFrame()
        actor.insert(0, 'col', '')
        actor1=soup_title.find('div', {'class': "actors-list clearfix"})
        if actor1.findAll('div', {'class': "actors-item is-iran"}):
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
            data_output.loc[j, 'Casts']=actor_list
        
        elif actor1.findAll('div', {'class': "actors-item is-foreign"}):
            for actor_x in actor1.findAll('div', {'class': "actors-item is-foreign"}):
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
            data_output.loc[j, 'Casts']=actor_list
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
                data_output.loc[j, 'Director'] = agent
                Direct = agent
            if "فیلمبرداری" in agent_split:
                data_output.loc[j, 'Cameraman'] = agent
                Cameraman = agent
            if "تهیه‌کننده" in agent_split:
                data_output.loc[j, 'Producer'] = agent
                producer = agent
            if "فیلم‌نامه‌نویس" in agent_split:
                data_output.loc[j, 'Writer'] = agent
                writer = agent
            if "آهنگساز" in agent_split:
                data_output.loc[j, 'Composer'] = agent
                Composer = agent
            if "خواننده" in agent_split:
                data_output.loc[j, 'Singer'] = agent
                Singer = agent
            if "نویسنده" in agent_split:
                data_output.loc[j, 'Writer'] = agent
                writer = agent
        del df_agent
    except: pass
        ############## old Percent and like ##############
    try:
        OPL2=soup_title.find('div', {'class': "single-section section-episodes"})
        OPL1=OPL2.find('div', {'class': "tabs-content"})
        for search in OPL1.findAll('li', {'class': "accordion-item"}):
            search_rate = search.find('span', {'id': "rateCnt"})
            search_percent = search.find('span', {'id': "percentNumber"})
            search_epizode = search.find('span', {'class': "episode"})
            data_output.loc[j, 'Like']=search_rate.text
            data_output.loc[j, 'Percent']=search_percent.text
            data_output.loc[j, 'Epizode']=search_epizode.text
            #*****#
            data_output.loc[j, 'Title']=title_original.text
            j = j + 1
    except: pass
    j = j + 1
    return data_output

def SearchingMetaData2(link_address3, data_output, season):
    data_output1=pd.DataFrame()
    data_output1=pd.DataFrame(data_output1, columns=['LinkAddress', 'Title', 'EnglishName', 'Imdb', 'Like', 'Percent', 'AgeRange', 'Runtime', 'Genres', 'Country', 'Year', 'DubbedSubtitle', 'FilmSerial', 'Epizode', 'Season', 'Director', 'Casts', 'Producer', 'Composer', 'Singer', 'Writer', 'Cameraman'])
    title_link2=requests.get(link_address3)
    soup_title2=BeautifulSoup(title_link2.text, 'html.parser')
    j = 0
    for meta in soup_title2.findAll('li', {'class': "accordion-item"}):
        data_output1.loc[j, 'Season'] = season
        episode = meta.find('span', {'class': "episode"})
        data_output1.loc[j, 'Epizode'] = episode.text
        try:
            rate_filimo1=meta.find('span', {'class': "rate_cnt"})
            data_output1.loc[j, 'Like']=rate_filimo1.text
        except: pass
        try:
            percent1=meta.find('span', {'id': "percentNumber"})
            data_output1.loc[j, 'Percent']=percent1.text
        except: pass
        data_output1.loc[j, 'LinkAddress'] = link_address3
        data_output1.loc[j, 'Title'] = data_output.loc[0, 'Title']
        data_output1.loc[j, 'EnglishName'] = data_output.loc[0, 'EnglishName']
        data_output1.loc[j, 'AgeRange'] = data_output.loc[0, 'AgeRange']
        data_output1.loc[j, 'Runtime'] = data_output.loc[0, 'Runtime']
        data_output1.loc[j, 'Genres'] = data_output.loc[0, 'Genres']
        data_output1.loc[j, 'Country'] = data_output.loc[0, 'Country']
        data_output1.loc[j, 'Year'] = data_output.loc[0, 'Year']
        data_output1.loc[j, 'Director'] = data_output.loc[0, 'Director']
        data_output1.loc[j, 'Casts'] = data_output.loc[0, 'Casts']
        data_output1.loc[j, 'Producer'] = data_output.loc[0, 'Producer']
        data_output1.loc[j, 'Composer'] = data_output.loc[0, 'Composer']
        data_output1.loc[j, 'Singer'] = data_output.loc[0, 'Singer']
        data_output1.loc[j, 'Writer'] = data_output.loc[0, 'Writer']
        data_output1.loc[j, 'Cameraman'] = data_output.loc[0, 'Cameraman']
        data_output1.loc[j, 'DubbedSubtitle'] = data_output.loc[0, 'DubbedSubtitle']
        data_output1.loc[j, 'Imdb'] = data_output.loc[0, 'Imdb']
        j = j + 1
    return data_output1



#data_output3 = pd.read_excel(r'C:\MO\data_output2.xlsx')
    

list_title_no = pd.DataFrame()
data_output_total = pd.DataFrame()
#print(list_title.loc[9477, 'LinkAddress'])
start = time.time()
for i in range(0, len(list_title)):    #   len(list_title)
    print(i)
#    time.sleep(60)
    LinkAddress = list_title.loc[i, 'LinkAddress']
    site = requests.get(LinkAddress)
    if site:
        soup_second = BeautifulSoup(site.text, 'html.parser')
        if soup_second.findAll('div', {'class': "episodev2_title d-flex ui-ai-c ui-jc-sb ui-mb-8x"}):
            data_output = SearchingMetaData(LinkAddress)
            link_address33 = LinkAddress.replace("https://www.filimo.com/m/", "")
            link_address33 = link_address33.split('/')
            link_address33 = link_address33[0]
            season_number = 0
            for season_number_i in soup_second.findAll('button', {'data-after-action': 'afterGetEpisodes'}):
                season_number = season_number + 1
            if season_number == 0:
                season = soup_second.find('span', {'class': "ui-ml-6x sm-ui-ml-3x episodev2_title-part-label episodev2_space-left"})
                season = season.text
                link_address3 = 'https://www.filimo.com/cms/serial/episodeSeasons/parent_id/' + link_address33 + '/part/' + '1'
                data_output1 = SearchingMetaData2(link_address3, data_output, season)
                data_output_total = data_output1.append([data_output_total])
            else:
                for k in range(1, season_number + 1):
                    link_address3 = 'https://www.filimo.com/cms/serial/episodeSeasons/parent_id/' + link_address33 + '/part/' + str(k)
                    season = str(k)
                    data_output1 = SearchingMetaData2(link_address3, data_output, season)
                    data_output_total = data_output1.append([data_output_total])
        else:
            data_output = SearchingMetaData(LinkAddress)
            data_output_total = data_output.append([data_output_total])
    else:
        list_title_no.loc[i, 'LinkAddress'] = list_title.loc[i, 'LinkAddress']

print("--- %s min ---" % round((time.time() - start)/60, 2))


data_output_total1 = data_output_total.copy()
data_output_total2 = data_output_total.copy()
data_output_total = data_output_total1.append([data_output_total2])
########### delete ###########
data_output_total = pd.read_excel(r'C:\MO\data_output_total.xlsx')
data_output_total = data_output_total.astype(str)
vod_filimo_nan = data_output_total[data_output_total.Title.str.contains("nan")]
vod_filimo_nan = vod_filimo_nan.reset_index()
del vod_filimo_nan['index']
print(vod_filimo_nan.loc[0, 'LinkAddress'])

for i in range(0, len(vod_filimo_nan)):    # len(vod_filimo_nan)
    print(i)
    LinkAddress = vod_filimo_nan.loc[i, 'LinkAddress']
    link_address33 = LinkAddress.replace("https://www.filimo.com/cms/serial/episodeSeasons/parent_id/", "")
    link_address33 = link_address33.split('/')
    link_address33 = link_address33[0]
    link_address333 = "https://www.filimo.com/m/" + str(link_address33)
    vod_filimo_nan.loc[i, 'LinkAddress'] = link_address333

vod_filimo_nan.drop_duplicates(subset =['LinkAddress'], keep = 'first', inplace = True)
del list_title
list_title = pd.DataFrame()
list_title['LinkAddress'] = vod_filimo_nan['LinkAddress']
list_title = list_title.reset_index()
del list_title['index']

#del a111
#a111 = pd.read_excel(r'C:\MO\data_output_final.xlsx')
#aa = pd.DataFrame()
#aa = data_output_final.append([a111])
#aa.drop_duplicates(subset =['Title', 'Epizode', 'Country', 'Year', 'Like', 'DubbedSubtitle', 'Director'], keep = 'first', inplace = True)
#
#aa.to_excel('aa.xlsx', index=False)
#######################################################







data_output_total.drop_duplicates(subset =['Title', 'Epizode', 'Country', 'Year', 'Like', 'DubbedSubtitle', 'Director'], keep = 'first', inplace = True)

data_output_total.to_excel('data_output_total1.xlsx', index=False)

data_output_final = data_output_total.copy()
data_output_final = data_output_final.reset_index()
del data_output_final['index']


data_output_final['Imdb'] = data_output_final['Imdb'].str.replace('/10', '')
data_output_final['Percent'] = data_output_final['Percent'].str.replace('%', '')
data_output_final['Country'] = data_output_final['Country'].str.replace('محصول', '')
data_output_final['Season'] = data_output_final['Season'].str.replace('تک فصلی', '1')
data_output_final['Year'] = data_output_final['Year'].astype(str).replace('\.0', '', regex=True)

data_output_final['Director'] = data_output_final['Director'].str.replace('کارگردان', '')
data_output_final['Director'] = data_output_final['Director'].str.replace('دستیار', '')
data_output_final['Producer'] = data_output_final['Producer'].str.replace('تهیه‌کننده', '')
data_output_final['Composer'] = data_output_final['Composer'].str.replace('آهنگساز', '')
data_output_final['Singer'] = data_output_final['Singer'].str.replace('خواننده', '')
data_output_final['Writer'] = data_output_final['Writer'].str.replace('نویسنده', '')
data_output_final['Cameraman'] = data_output_final['Cameraman'].str.replace('مدیر فیلمبرداری', '')
data_output_final['Writer'] = data_output_final['Writer'].str.replace('مدیر فیلم‌نامه‌نویس', '')
data_output_final['Writer'] = data_output_final['Writer'].str.replace('فیلم‌نامه‌نویس', '')

data_output_final['Title'] = data_output_final['Title'].str.strip() 
data_output_final['Imdb'] = data_output_final['Imdb'].str.strip() 
data_output_final['Percent'] = data_output_final['Percent'].str.strip() 
data_output_final['Country'] = data_output_final['Country'].str.strip() 
data_output_final['Year'] = data_output_final['Year'].str.strip() 
data_output_final['Director'] = data_output_final['Director'].str.strip() 
data_output_final['Producer'] = data_output_final['Producer'].str.strip() 
data_output_final['Composer'] = data_output_final['Composer'].str.strip() 
data_output_final['Singer'] = data_output_final['Singer'].str.strip() 
data_output_final['Writer'] = data_output_final['Writer'].str.strip() 
data_output_final['Cameraman'] = data_output_final['Cameraman'].str.strip()
data_output_final['DubbedSubtitle'] = data_output_final['DubbedSubtitle'].str.strip()



data_output_final.to_excel('data_output_final.xlsx', index = False)

print("--- %s seconds ---" % (time.time() - start))


##################### delete ###########
data_output_final1['Casts'] = data_output_final1['Casts'].astype(str)
data_output_final1['Casts'] = data_output_final1['Casts'].fillna('nan')
data_output_final11 = data_output_final1[~data_output_final1.Casts.str.contains("nan")]
data_output_final22 = data_output_final1[data_output_final1.Casts.str.contains("nan")]
data_output_final22 = data_output_final22.reset_index()
del data_output_final22['index']






data_output_final221 = data_output_final22[~data_output_final22.LinkAddress.str.contains("part")]
data_output_final222 = data_output_final22[data_output_final22.LinkAddress.str.contains("part")]
data_output_final222 = data_output_final222.reset_index()
del data_output_final222['index']
for i in range(2, len(data_output_final222)):    # len(vod_filimo_nan)
    print(i)
    LinkAddress = data_output_final222.loc[i, 'LinkAddress']
    link_address33 = LinkAddress.replace("https://www.filimo.com/cms/serial/episodeSeasons/parent_id/", "")
    link_address33 = link_address33.split('/')
    link_address33 = link_address33[0]
    link_address333 = "https://www.filimo.com/m/" + str(link_address33)
    data_output_final222.loc[i, 'LinkAddress'] = link_address333


data_output_final22 = data_output_final221.append([data_output_final222])
data_output_final22.sort_values('LinkAddress', axis = 0, ascending = False, inplace = True, na_position ='last')
data_output_final22 = data_output_final22[~data_output_final22.LinkAddress.str.contains("nan")]
data_output_final22 = data_output_final22.reset_index()
del data_output_final22['index']






for i in range(100, len(data_output_final22)):    # len(data_output_final22)
    print(i)
    if data_output_final22.loc[i, 'LinkAddress'] == data_output_final22.loc[i-1, 'LinkAddress']:
        data_output_final22.loc[i, 'Casts'] = data_output_final22.loc[i-1, 'Casts']
    elif data_output_final22.loc[i, 'LinkAddress'] != data_output_final22.loc[i-1, 'LinkAddress']:
        LinkAddress = data_output_final22.loc[i, 'LinkAddress']
    #    print(LinkAddress)
        site = requests.get(LinkAddress)
        soup_title = BeautifulSoup(site.text, 'html.parser')
    #    try:
        k = 0
        actor = pd.DataFrame()
        actor.insert(0, 'col', '')
    #    actor1=soup_title.find('div', {'class': "actors-list clearfix"})
        if soup_title.find('div', {'class': "actors-item is-iran"}):
    #        print('iran')
            for actor_x in soup_title.findAll('div', {'class': "actors-item is-iran"}):
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
            data_output_final22.loc[i, 'Casts']=actor_list
        
        elif soup_title.find('div', {'class': "actors-item is-foreign"}):
    #        print('forigen')
            for actor_x in soup_title.findAll('div', {'class': "actors-item is-foreign"}):
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
            data_output_final22.loc[i, 'Casts']=actor_list
#    except: pass


data_output_final = data_output_final11.append([data_output_final22])
data_output_final = data_output_final.reset_index()
del data_output_final['index']
print(data_output_final22.loc[43, 'LinkAddress'])




















