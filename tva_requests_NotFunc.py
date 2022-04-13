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

url_tva='https://tva.tv'
url_tva_search_query='https://tva.tv/search?query='
tva_vod=pd.read_excel(r'C:\Users\PC\Desktop\login\tva_tva.xlsx')
main_data=tva_vod
#tva_vod= pd.read_excel ('/home/armin/imdbproject/crawler_input/tva_vod.xlsx')
all_content_excel=pd.DataFrame()
all_content_excel=pd.DataFrame(all_content_excel, columns=['title_in_excel'])
data_output=pd.DataFrame()
data_output=pd.DataFrame(data_output, columns=['title_in_excel', 'content_type', 'orginal_name', 'latin_name', 'content_link', 'Genres', 'Runtime', 'Release Date', 'Language', 'Country', 'Directed by', 'Casts', 'Epizode Number', 'tiva Rate','imdb'])
jj=0
############## choose of content ##############
def main_program():
    global jj
    length_tva_vod=len(tva_vod)
    for j in range(0,length_tva_vod):
        try:
            print(j)
            print("jj: ", jj)
            Order_receipt_vod=tva_vod.iat[j, 0]
            all_content_excel.loc[j, 'title_in_excel']=Order_receipt_vod
            Order_receipt_vod = " ".join(Order_receipt_vod.split())
            try:
                site_primary=requests.get(url_tva_search_query+Order_receipt_vod)
                soup_primary=BeautifulSoup(site_primary.text, 'html.parser')
            except:
                pass
            ############## search of link of content ##############
            time.sleep(3)
            for link in soup_primary.findAll('a'):
                title_name=link.get('title')
                try:
                    title_name = " ".join(title_name.split())
                except: pass
                ############## confirm link of content and start ... ##############
                if title_name == Order_receipt_vod:
                    data_output.loc[jj, 'title_in_excel']=Order_receipt_vod
                    data_output.loc[jj, 'orginal_name']=Order_receipt_vod
                    film_serial=link.get('data-testid')
                    try:
                        if film_serial=="serial_card":
                            data_output.loc[jj, 'content_type']="سریال"
                        if film_serial=="movie_card":
                            data_output.loc[jj, 'content_type']="فیلم"
                        else: pass
                    except: pass
                    link_primary=link.get('href')
                    if link_primary:
                        print("link_primary: ok")
                    else: print("link_primary: NOT")
                    print("link_primary1: ", link_primary)
        #                print("**************************")
                    link_primary_original=link_primary
                    try:
                        link_primary_original = re.sub('/movies/', '', link_primary_original)
                        link_primary_original = re.sub('/series/', '', link_primary_original)
                        link_primary_original = re.sub('_', ' ', link_primary_original)
        #                print("link_primary_original: ", link_primary_original)
                    except:
                        link_primary_original = re.sub('/movies/', '', link_primary_original)
                        link_primary_original = re.sub('/series/', '', link_primary_original)
                        link_primary_original = re.sub('_', ' ', link_primary_original)
                    print("link_primary_original1: ", link_primary_original)
                    print("link_primary2: ", link_primary)            
                    link_final=requests.get(url_tva+link_primary)
                    data_output.loc[jj, 'content_link']=url_tva+link_primary
                    soup_final=BeautifulSoup(link_final.text, 'html.parser')
                    if soup_final:
                        print("soup_final: ok")
                    else:
                        print("soup_final: NOT")
                    test_title=soup_final.find('h2', {'itemprop': "name"})
                    print("test_title: ", test_title.text)
#                        if test_title.text==Order_receipt_vod:
#                            print("Result: ok")
#                        else: print("Result: NOT")
                   ############## name_original ##############
                    data_output.loc[jj, 'latin_name']=link_primary_original
                   ############## imdb ############## 
                    for i in range(10):
                        try:
                            imdb_TvaRate=soup_final.find('div', {'class': "FcGuTzla"})
                            if imdb_TvaRate.find('span', {'class': "sc-bdVaJa wgfvk"}).text=="IMDb: ":
                                imdb=imdb_TvaRate.find('span', {'class': "sc-bdVaJa fRsqnq"})
                                data_output.loc[jj, 'imdb']=imdb.text
                            else: data_output.loc[jj, 'imdb']= "no data"
                            if data_output.loc[jj, 'imdb']!= "no data":
                                break
                            else: pass
                        except: pass
                    for i in range(10):
                        try:
                            imdb_TvaRate=soup_final.find('div', {'class': "FcGuTzla"})
                            if imdb_TvaRate.find('span', {'class': "sc-bdVaJa wgfvk"}).text=="تلویزیون تعاملی تیوا: ":
                                TvaRate=imdb_TvaRate.find('span', {'class': "sc-bdVaJa fRsqnq"})
                                data_output.loc[jj, 'tiva Rate']=TvaRate.text
                            else: data_output.loc[jj, 'tiva Rate']= "no data"
                            if data_output.loc[jj, 'tiva Rate']!= "no data":
                                break
                            else: pass
                        except: pass
                   ############## year ##############
                    for i in range(10):
                        try:
                            year_primary=soup_final.find('div', {'class': "FcGuTzla"})
                            for ii in range(0, 5):
                                try:
                                    year2=year_primary.findAll('div', {'class': "sc-bdVaJa dBntLG _3mVZq9kE _16LJPU8a"})[ii]
                                    year=year2.text.split()
                                    for word_year in year:
                                        if word_year=="سال":
                                            data_output.loc[jj, 'Release Date']=year2.text
                #                            print("year: ", year2.text)
                                            break
                                        elif word_year!="سال":
                                             data_output.loc[jj, 'Release Date']="no data"
                                    if data_output.loc[jj, 'Release Date'] !="no data":
                                        break
                                    else: pass
                                except: pass
                            if data_output.loc[jj, 'Release Date'] !="no data":
                                break
                            else: pass
                        except: pass
                   ############## country ##############
                    try:
                        country=soup_final.find('div', {'itemprop': "countryOfOrigin"})
                        data_output.loc[jj, 'Country']=country.text
        #                print("country: ", country.text)
                    except:
                        country=soup_final.find('div', {'itemprop': "countryOfOrigin"})
                        data_output.loc[jj, 'Country']=country.text
        #                print("country: ", country.text)
                   ############## genre ##############
                    try:
                        genre=soup_final.find('div', {'itemprop': "genre"})
                        data_output.loc[jj, 'Genres']=genre.text
        #                print("genre: ", genre.text)
                    except:
                        genre=soup_final.find('div', {'itemprop': "genre"})
                        data_output.loc[jj, 'Genres']=genre.text
        #                print("genre: ", genre.text)
                   ############## time ##############
                    try:
                        for ii in range(10):
                            time1=soup_final.find('div', {'class': "FcGuTzla"})
                            for i in range(0, 5):
                                try:
                                    time2=time1.findAll('div', {'class': "sc-bdVaJa dBntLG _3mVZq9kE _16LJPU8a"})[i]
                                    time3=time2.find('span', {'class': "sc-bdVaJa wgfvk"})
                                    time4=time3.text.split()
                                    flag=0
                                    for word in time4:
                                        if "دقیقه" in word:
                                            
                                            data_output.loc[jj, 'Runtime']=time3.text
                                            flag=1
                                            if len(time3.text)==0:
                                                data_output.loc[jj, 'Runtime']='no data'
                #                                print("Runtime: ", time3.text)
                                            break
#                                        else:
#                                            data_output.loc[jj, 'Runtime']="no data"
                                    if flag==1:
                                        break
                                    else: data_output.loc[jj, 'Runtime']="no data"
                                except: pass
                            if data_output.loc[jj, 'Runtime']=="no data":
                                pass
                            else: break
                    except: pass
#                    try:
#                        
#                
#                           
#                    except:
#                        data_output.loc[jj, 'Runtime']="data ok"
                   ############## actor ##############
                    try:
                        actor=soup_final.find('span', {'itemprop': "actor"})
                        data_output.loc[jj, 'Casts']=actor.text
                    except: pass
                   ############## director ##############
                    try:
                        director=soup_final.find('span', {'itemprop': "director"})
                        data_output.loc[jj, 'Directed by']=director.text
                    except: pass
                   ############## language ##############
                    try:
                        for ii in range(10):
                            for i in range(0, 5):
                                try:
                                    language_primary1=soup_final.findAll('div', {'class': "_13UojNKD"})[i]
                                    language2=language_primary1.find('span', {'class': "sc-bdVaJa dtXlgf _1FO5bWHd _1ekVBzV2"})
                                    if "زبان ها" in language2.text:
                                        language=language_primary1.find('span', {'class': "sc-bdVaJa wgfvk"})
                                        data_output.loc[jj, 'Language']=language.text
                #                        print("language: ", language.text)
                                        break
                                    else:
                                        data_output.loc[jj, 'Language']="no data"
                                except: pass
                            if data_output.loc[jj, 'Language']=="no data":
                                pass
                            else: break
                    except: pass     
                
                else: pass
                jj=jj+1
#                if data_output.loc[jj, 'latin_name'] is not None:
#                    print("data is: ok")
#                    break
#                else: 
#                    print("data is: blank")
#                    pass
        except: pass
    return data_output

def majhol_fun():
    print("******************************************")
    print("zz_funzz_funzz_funzz_funzz_funzz_funzz_fun")
    data_output_title=data_output['title_in_excel']
    data_output_title=data_output_title.to_frame().reset_index()
    del data_output_title['index']
    majhol=pd.DataFrame()
    majhol=pd.concat([data_output_title, main_data]).drop_duplicates(keep=False)
    print("******************************************")
    print("majhol: ", majhol)
    length_majhol=len(majhol)
    print("length_majhol: ", length_majhol)
    print("******************************************")
    return majhol, length_majhol, data_output_title

def majhol_latin_name_fun():
    print("##########################################")
    print("majhol_latin_name_funmajhol_latin_name_fun")
    data_output_latin_name=pd.DataFrame()
    data_output_latin_name=pd.DataFrame(data_output_latin_name, columns=['title_in_excel', 'latin_name'])
    data_output_latin_name['title_in_excel']=data_output['title_in_excel']
    data_output_latin_name['latin_name']=data_output['latin_name']
    data_output_latin_name1=data_output_latin_name.copy()
    data_output_latin_name1.dropna(inplace=True)
    data_output_latin_name2=pd.concat([data_output_latin_name, data_output_latin_name1]).drop_duplicates(keep=False)
    del data_output_latin_name2['latin_name']
    length_data_output_latin_name=len(data_output_latin_name2)
    print("##########################################")
    print("data_output_latin_name2: ", data_output_latin_name2)
    length_data_output_latin_name=len(data_output_latin_name2)
    print("length_data_output_latin_name: ", length_data_output_latin_name)
    print("##########################################")
    return data_output_latin_name2, length_data_output_latin_name

################################ STARTING ... #####################################
    
main_program()
[majhol, length_majhol, data_output_title]=majhol_fun()


for i in range(1):
    if length_majhol!=0:
        print("******************************************")
        print("******************************************")
        print("******************************************")
        print(length_majhol)
        print("******************************************")
        print("******************************************")
        print("******************************************")
        del tva_vod
        tva_vod=pd.DataFrame()
        tva_vod=majhol
        main_program()
        del majhol
        [majhol, length_majhol, data_output_title]=majhol_fun()
    else: break


[data_output_latin_name2, length_data_output_latin_name]=majhol_latin_name_fun()
for i in range(1):
    if length_data_output_latin_name!=0:
        time.sleep(3)
        print("##########################################")
        print("##########################################")
        print("##########################################")
        print(length_data_output_latin_name)
        print("##########################################")
        print("##########################################")
        print("##########################################")
        del tva_vod
        tva_vod=pd.DataFrame()
        tva_vod=data_output_latin_name2
        main_program()
        del data_output_latin_name2
        [data_output_latin_name2, length_data_output_latin_name]=majhol_latin_name_fun()
    else: break


data_output=data_output.drop_duplicates(subset =['content_link'])
#data_output.to_excel('/home/armin/imdbproject/crawler_output/out_tva.xlsx',index=False)
data_output.to_excel('data_output.xlsx')
print("--- %s seconds ---" % (time.time() - start))

#[{'content_type': 'فیلم', 'orginal_name': 'آسمان خراش', 'content_name': 'آسمان خراش', 'content_link': 'http://lenz.ir/video/21281', 'Genres': ['اکشن'], 'Runtime': '1 ساعت و 39 دقیقه', 'Release Date': '1397 ( 2018 )', 'Language': 'فارسی', 'Country': 'آمریکا', 'Directed by': 'راوسون مارشال تربر', 'Casts': ['دواین جانسون', 'نو کمبل', 'چین هان', 'رونالد مولر', 'نوآ تیلور', 'بایرون من'], 'Epizode Number': '', 'Lenz Rate': '9.4', 'exception message': ''}]         










