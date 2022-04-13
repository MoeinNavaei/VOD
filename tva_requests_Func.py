import pandas as pd
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from bs2json import bs2json
import re
import requests
from urllib.request import urlopen as uReq
import time

start = time.time()
#print(vod_tva_for_crawl.loc[20, 'TitleCleaned1'])
url_tva='https://tva.tv'
url_tva_search_query='https://tva.tv/search?query='
#tva_vod=pd.read_excel(r'C:\Users\PC\Desktop\VOD\vod_tva.xlsx')
tva_vod = vod_tva_for_crawl2
main_data = vod_tva_for_crawl2
data_output = pd.DataFrame()
#data_output=pd.DataFrame(data_output, columns=['IDS','TitleForCrawl', 'content_type', 'orginal_name', 'latin_name', 'content_link', 'Genres', 'Runtime', 'Release Date', 'Language', 'Country', 'Directed by', 'Casts', 'Epizode Number', 'tiva Rate','imdb'])
jj=0
counter1=50    # majhol
counter2=10    # finding data
counter3=10    # loop of no data
timeing=2
############## choose of content ##############
def get_link_data(jj, tva_vod):
    length_tva_vod=len(tva_vod)
    for j in range(0, length_tva_vod):
        print(j)
        Order_receipt_vod=tva_vod.loc[j, 'TitleForCrawl']
        try:
            Order_receipt_vod = " ".join(Order_receipt_vod.split())
        except: pass
        try:
            site_primary=requests.get(url_tva_search_query+Order_receipt_vod)
            soup_primary=BeautifulSoup(site_primary.text, 'html.parser')
        except: pass
        for link in soup_primary.findAll('a'):
            title_name=link.get('Title')
            if title_name is not None:
                try:
                    title_name = " ".join(title_name.split())
                except: pass
                if title_name == Order_receipt_vod:
                    href=link.get('href')
                    url_final=url_tva+href
                    data_output.loc[jj, 'TitleForCrawl']=Order_receipt_vod
                    data_output.loc[jj, 'OrginalName']=Order_receipt_vod
                    data_output.loc[jj, 'LinkAddress']=url_final
                    data_output.loc[jj, 'IDS']=tva_vod.loc[j, 'IDS']
                    film_serial=link.get('data-testid')
                    try:
                        if film_serial=="serial_card":
                            data_output.loc[jj, 'FilmSerial']="سریال"
                        if film_serial=="movie_card":
                            data_output.loc[jj, 'FilmSerial']="فیلم"
                        else: pass
                    except: pass
                    link_original=href
                    try:
                        link_original = re.sub('/movies/', '', link_original)
                        link_original = re.sub('/series/', '', link_original)
                        link_original = re.sub('_', ' ', link_original)
                        data_output.loc[jj, 'EnglishName']=link_original
                    except: pass
            jj=jj+1
    return data_output
########################################################################            
def majhol_fun(main_data, data_output):
    data_output_title=data_output['TitleForCrawl']
    data_output_title=data_output_title.to_frame().reset_index()
    del data_output_title['index']
    main_data_copy = main_data.copy()
    del main_data_copy['Title']
    del main_data_copy['Season']
#    del main_data_copy['epizode']
#    del main_data_copy['FilmSerial']
#    del main_data_copy['ID']
    del main_data_copy['IDS']
#    del main_data_copy['TitleForCrawl']
    majhol=pd.DataFrame()
    majhol = data_output_title.append([main_data])
    majhol.drop_duplicates(subset =['TitleForCrawl'], keep = False, inplace = True)
    majhol = majhol.reset_index()
    del majhol['index']
#    majhol=pd.concat([data_output_title, main_data]).drop_duplicates(keep=False)
    length_majhol=len(majhol)
    print("************************************")
    print("length_majhol: ", length_majhol)
    return majhol, length_majhol
########################################################################
def imdb_TvaRate(soup_final, ii):
    for i0 in range(counter2):
        try:
            a1=soup_final.find('div', {'class': "FcGuTzla"})
            if a1.find('span', {'class': "_2lCJpJIr"}):
                a2=a1.find('span', {'class': "_2lCJpJIr"})
                if a2.find('span', {'class': "sc-bdVaJa wgfvk"}).text=="تلویزیون تعاملی تیوا: ":
                    a4=a2.find('span', {'class': "sc-bdVaJa fRsqnq"})
                    data_output.loc[ii, 'TvaRate']=a4.text
                    data_output.loc[ii, 'Imdb']= "no data"
                    break
                elif a2.find('span', {'class': "sc-bdVaJa wgfvk"}).text=="IMDb: ":
                    a4=a2.find('span', {'class': "sc-bdVaJa fRsqnq"})
                    data_output.loc[ii, 'Imdb']=a4.text
                    data_output.loc[ii, 'TvaRate']= "no data"
                    break
                else: pass
            else:
                data_output.loc[ii, 'Imdb']= "no data"
                data_output.loc[ii, 'tiva Rate']= "no data"
        except: 
            data_output.loc[ii, 'Imdb']="no data"
            data_output.loc[ii, 'TvaRate']="no data"
        if data_output.loc[ii, 'Imdb'] != "no data" or data_output.loc[ii, 'TvaRate'] != "no data":
            break
        else: pass
    return data_output
########################################################################
def year_number(soup_final, ii):
    for i1 in range(counter2):
        try:
            year_primary=soup_final.find('div', {'class': "FcGuTzla"})
            for k in range(0, 5):
                try:
                    year2=year_primary.findAll('div', {'class': "sc-bdVaJa dBntLG _3mVZq9kE _16LJPU8a"})[k]
                    year=year2.text.split()
                    for word_year in year:
                        if word_year=="سال":
                            data_output.loc[ii, 'ReleaseDateGeorgian']=year2.text
                            break
                        else:
                             data_output.loc[ii, 'ReleaseDateGeorgian']="no data"
                    if data_output.loc[ii, 'ReleaseDateGeorgian'] !="no data":
                        break
                    else: pass
                except: data_output.loc[ii, 'ReleaseDateGeorgian']="no data"
            if data_output.loc[ii, 'ReleaseDateGeorgian'] !="no data":
                break
            else: pass
        except: pass
    return data_output
########################################################################
def country_content(soup_final, ii):
    for i8 in range(counter2):
        try:
            country=soup_final.find('div', {'itemprop': "countryOfOrigin"})
            data_output.loc[ii, 'Country']=country.text
        except: data_output.loc[ii, 'Country']="no data"
        if data_output.loc[ii, 'Country'] != "no data":
            break
        else: pass
    return data_output
########################################################################
def genre_content(soup_final, ii):
    for i9 in range(counter2):
        try:
            genre=soup_final.find('div', {'itemprop': "genre"})
            data_output.loc[ii, 'Genres']=genre.text
        except: data_output.loc[ii, 'Genres']="no data"
        if data_output.loc[ii, 'Genres'] != "no data":
            break
        else: pass
    return data_output
########################################################################
def RunTime(soup_final, ii):
    for i5 in range(counter2):
        time1=soup_final.find('div', {'class': "FcGuTzla"})
        for i in range(0, 5):
            try:
                time2=time1.findAll('div', {'class': "sc-bdVaJa dBntLG _3mVZq9kE _16LJPU8a"})[i]
                time3=time2.find('span', {'class': "sc-bdVaJa wgfvk"})
                time4=time3.text.split()
                for word in time4:
                    if "دقیقه" in word:
                        data_output.loc[ii, 'Runtime']=time3.text
                        break
                    else:
                        data_output.loc[ii, 'Runtime']='no data'
            except: data_output.loc[ii, 'Runtime']="no data"
            if data_output.loc[ii, 'Runtime'] != "no data":
                break
            else: pass
        if data_output.loc[ii, 'Runtime'] != "no data":
            break
        else: pass
    return data_output
########################################################################
def Language(soup_final, ii):
    for i6 in range(counter2):
        for i in range(0, 5):
            try:
                language_primary1=soup_final.findAll('div', {'class': "_13UojNKD"})[i]
                language2=language_primary1.find('span', {'class': "sc-bdVaJa dtXlgf _1FO5bWHd _1ekVBzV2"})
                if "زبان ها" in language2.text:
                    language=language_primary1.find('span', {'class': "sc-bdVaJa wgfvk"})
                    data_output.loc[ii, 'Language']=language.text
                    break
                else:
                    data_output.loc[ii, 'Language']="no data"
                if data_output.loc[ii, 'Language'] != "no data":
                    break
                else: pass
            except: data_output.loc[ii, 'Language']="no data"
        if data_output.loc[ii, 'Language'] != "no data":
            break
        else: pass
    return data_output
########################################################################
def Directed_By(soup_final, ii):
    for i9 in range(counter2):
        try:
            director=soup_final.find('span', {'itemprop': "director"})
            data_output.loc[ii, 'DirectedBy']=director.text
        except: data_output.loc[ii, 'DirectedBy']="no data"
        if data_output.loc[ii, 'DirectedBy'] != "no data":
            break
        else: pass
    return data_output
########################################################################
def Actor(soup_final, ii):
    for i10 in range(counter2):
        try:
            actor=soup_final.find('span', {'itemprop': "actor"})
            data_output.loc[ii, 'Casts']=actor.text
        except: data_output.loc[ii, 'Casts']="no data"
        if data_output.loc[ii, 'Casts'] != "no data":
            break
        else: pass
    return data_output
########################################################################
########################################################################
########################################################################
data_output=get_link_data(jj, tva_vod)
[majhol, length_majhol]=majhol_fun(main_data, data_output)
########################################################################
for i2 in range(counter1):
    if length_majhol!=0:
        del tva_vod
        tva_vod=pd.DataFrame()
        majhol = majhol.reset_index()
        del majhol['index']
        tva_vod=majhol
        data_output=get_link_data(jj, tva_vod)
        del majhol
        [majhol, length_majhol]=majhol_fun(main_data, data_output)
    else: break
########################################################################
#data_output.to_excel('data_output.xlsx')  #########################################
#data_output=pd.read_excel(r'C:\Users\PC\Desktop\login\data_output.xlsx')#########################################
data_output=data_output.drop_duplicates(subset =['LinkAddress'])
data_output=data_output.reset_index()
del data_output['index']
#del data_output['Unnamed: 0']#########################################
print("*****************************")
length_data_output=len(data_output)
for ii in range(0, length_data_output):
    print(ii)
    time.sleep(3)
    url_title=data_output.loc[ii, 'LinkAddress']
    get_title=requests.get(url_title)
    soup_final=BeautifulSoup(get_title.text, 'html.parser')
    data_output=imdb_TvaRate(soup_final, ii)
    data_output=year_number(soup_final, ii)
    data_output=country_content(soup_final, ii)
    data_output=genre_content(soup_final, ii)
    data_output=RunTime(soup_final, ii)
    data_output=Language(soup_final, ii)
    data_output=Directed_By(soup_final, ii)
    data_output=Actor(soup_final, ii)

#del main_data['title']
#del main_data['season']            
data_output_Merge_main_data = pd.merge(data_output, main_data, on = ['TitleForCrawl'])
del data_output_Merge_main_data['IDS_y']
del data_output_Merge_main_data['title_cleaned_orginal']
data_output_Merge_main_data=data_output_Merge_main_data.rename(columns={"IDS_x":"IDS"})
data_output_Merge_main_data.drop_duplicates(subset =['IDS'], keep = 'first', inplace = True)
data_output_Merge_main_data = data_output_Merge_main_data.replace('no data', '')
vod_tva_crawl_output = data_output_Merge_main_data.copy()
vod_tva_crawl_output.to_excel('data_output.xlsx', index = False)
majhol.to_excel('no_data.xlsx', index = False)
print("--- %s seconds ---" % (time.time() - start))

#[{'content_type': 'فیلم', 'orginal_name': 'آسمان خراش', 'content_name': 'آسمان خراش', 'content_link': 'http://lenz.ir/video/21281', 'Genres': ['اکشن'], 'Runtime': '1 ساعت و 39 دقیقه', 'Release Date': '1397 ( 2018 )', 'Language': 'فارسی', 'Country': 'آمریکا', 'Directed by': 'راوسون مارشال تربر', 'Casts': ['دواین جانسون', 'نو کمبل', 'چین هان', 'رونالد مولر', 'نوآ تیلور', 'بایرون من'], 'Epizode Number': '', 'Lenz Rate': '9.4', 'exception message': ''}]         



