
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from bs2json import bs2json
import re
import requests
from urllib.request import urlopen as uReq
import time
import sys
from selenium import webdriver
from sqlalchemy import create_engine
start = time.time()

data_output = pd.DataFrame()


#url_filimo='https://www.manzoom.ir/title/'
#title_code = 4000000
#list_title444 = pd.DataFrame()
#
#for i in range(36276, 999999):
#    print(i)
#    LinkAddress = url_filimo + 'tt' + str(title_code)
#    site = requests.get(LinkAddress)
#    if site:
#        list_title444.loc[i, 'LinkAddress'] = LinkAddress
#    title_code = title_code + 1
#
#list_title444.to_excel('list_title444.xlsx', index=False)

###### Reading List of Links ######
list_title1 = pd.read_excel(r'E:\python codes\VOD\manzom\manzom_final\list_title1.xlsx')
list_title2 = pd.read_excel(r'E:\python codes\VOD\manzom\manzom_final\list_title2.xlsx')
list_title3 = pd.read_excel(r'E:\python codes\VOD\manzom\manzom_final\list_title3.xlsx')
list_title4 = pd.read_excel(r'E:\python codes\VOD\manzom\manzom_final\list_title4.xlsx')
list_title5 = pd.read_excel(r'E:\python codes\VOD\manzom\manzom_final\list_title5.xlsx')
list_title6 = pd.read_excel(r'E:\python codes\VOD\manzom\manzom_final\list_title6.xlsx')
list_title7 = pd.read_excel(r'E:\python codes\VOD\manzom\manzom_final\list_title7.xlsx')
list_title8 = pd.read_excel(r'E:\python codes\VOD\manzom\manzom_final\list_title8.xlsx')
list_title9 = pd.read_excel(r'E:\python codes\VOD\manzom\manzom_final\list_title9.xlsx')
list_title = list_title1.append([list_title2,list_title3,list_title4,list_title5,list_title6,list_title7,list_title8,list_title9])
list_title = list_title.reset_index()
del list_title['index']
####################################

def Title(soup_title):
    if soup_title.find('h1', {'class': "sans_medium inline maznoom-a"}):
        try:
            Title = soup_title.find('h1', {'class': "sans_medium inline maznoom-a"})
            data_output.loc[i, 'Title']=Title.text
#            print("T1: ", Title.text)
        except: pass
    elif soup_title.find('h1', {'class': "sans_medium inline maznoom-a maznoom-c"}):
        try:
            Title = soup_title.find('h1', {'class': "sans_medium inline maznoom-a maznoom-c"})
            data_output.loc[i, 'Title']=Title.text
#            print("T2: ", Title.text)
        except: pass
    elif soup_title.find('h1', {'class': "sans_medium inline maznoom-a maznoom-b"}):
        try:
            Title = soup_title.find('h1', {'class': "sans_medium inline maznoom-a maznoom-b"})
            data_output.loc[i, 'Title']=Title.text
#            print("T3: ", Title.text)
        except: pass
    elif soup_title.find('h1', {'class': "sans_medium inline maznoom-a maznoom-b maznoom-c"}):
        try:
            Title = soup_title.find('h1', {'class': "sans_medium inline maznoom-a maznoom-b maznoom-c"})
            data_output.loc[i, 'Title']=Title.text
#            print("T4: ", Title.text)
        except: pass
    return data_output

def Year(soup_title):
    try:
        Year = soup_title.find('span', {'class': "maznoom-b"})
        data_output.loc[i, 'Year']=Year.text
    except: pass
    return data_output

def Meta1(soup_title):
    try:
        k = 0
        Meta1 = soup_title.find('div', {'class': "flex-row maznoom-d m-t-1"})
        for Meta1_x in Meta1.findAll('div', {'class': "short-info"}):
            k = k + 1
        actor_list = ""
        for ii in range(0, k):
            actor_list = Meta1.findAll('div', {'class': "short-info"})[ii].text+"-" + actor_list
        data_output.loc[i, 'Meta1'] = actor_list
    except: pass
    return data_output

def Meta2(soup_title):
    try:
        k = 0
        Meta2 = soup_title.find('div', {'class': "underline-links maznoom-d"})
        for Meta2_x in Meta2.findAll('div', {'class': "m-t-3"}):
            k = k + 1
        actor_list = ""
        for ii in range(0, k):
            actor_list = Meta2.findAll('div', {'class': "m-t-3"})[ii].text+"-" + actor_list
        data_output.loc[i, 'Meta2'] = actor_list
    except: pass
    return data_output

#def Casts(soup_title):
#    try:
#        k = 0
#        Casts = soup_title.find('div', {'class': "main underline-links"})
#        for Casts_x in Casts.findAll('a', {'class': "actors-item"}):
#            k = k + 1
#        actor_list = ""
#        for ii in range(0, k):
#            actor_list = Casts.findAll('a', {'class': "actors-item"})[ii].text+"-" + actor_list
#        data_output.loc[i, 'Casts'] = actor_list
#    except: pass
#    return data_output

def Staff(soup_title):
    try:
        Staff = soup_title.find('div', {'class': "underline-links maznoom-e m-t-2 m-b-3"})
        Staff1 = Staff.find('a')
        Staff_link = Staff1.get('href')
        Staff_site = requests.get(Staff_link)
        Staff_soup_title = BeautifulSoup(Staff_site.text, 'html.parser')
        driver = webdriver.Chrome(executable_path= r'E:\python codes\VOD\chromedriver.exe')
        driver.get(Staff_link)
        k = 0
        for k1 in Staff_soup_title.find('div', {'class': "slide-down active"}):
            k = k + 1
        for var in range(0, k):
            f = var*2+2
            f1 = var*2+3
            x1 = driver.find_element_by_xpath('//*[@id="main"]/div[1]/div[2]/div/div[2]/div[{}]/div[1]'.format(f))
            x1 = x1.text
            head, sep, tail = x1.partition('(')
            data_output.loc[i, head] = head
            head3, sep3, tail3 = x1.partition('(')
            head4, sep4, tail4 = tail3.partition(')')
            head4 = int(head4)
            avamel= ''
            for var2 in range(1,head4+1):
                try:
                    x2 = driver.find_element_by_xpath('//*[@id="main"]/div[1]/div[2]/div/div[2]/div[{}]/div[{}]'.format(f1, var2))
                    x2 = x2.text
                    head1, sep, tail = x2.partition('(')
                    head1 = head1.split('\n', 1)[0]
                except: pass
                avamel = avamel + ',' + head1
                data_output.loc[i, head] = avamel
    except: pass
    return data_output

data_output = pd.DataFrame()
for i in range(41000, 41001):
    print(i)
    LinkAddress = list_title.loc[i, 'LinkAddress']
    data_output.loc[i, 'LinkAddress'] = LinkAddress
    site = requests.get(LinkAddress)
    soup_title = BeautifulSoup(site.text, 'html.parser')
    data_output = Title(soup_title)
    data_output = Year(soup_title)
    data_output = Meta1(soup_title)
    data_output = Meta2(soup_title)
    data_output = Staff(soup_title)
    engine = create_engine('postgresql://postgres:12344321@10.32.141.17/Moein01',pool_size=20, max_overflow=100,)
    con=engine.connect()
    data_output.to_sql('Manzom',con,if_exists='replace', index=False)
#    con.close()








