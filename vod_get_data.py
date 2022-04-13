import pandas as pd
import time

start = time.time()

print("get data")
########################################################################
tva_farvardin_1399=pd.read_excel(r'C:\Users\PC\Desktop\VOD\vod_data\1399\Tva_vod\farvardin1399.xlsx', sheet_name='Videos')
tva_farvardin_1399.insert(5, 'operator', 'تیوا')
tva_farvardin_1399.insert(6, 'month', 'فروردین 1399')
tva_ordibehesht_1399=pd.read_excel(r'C:\Users\PC\Desktop\VOD\vod_data\1399\Tva_vod\ordibehesht1399.xlsx', sheet_name='Videos')
tva_ordibehesht_1399.insert(5, 'operator', 'تیوا')
tva_ordibehesht_1399.insert(6, 'month', 'اردیبهشت 1399')
tva_khordad_1399=pd.read_excel(r'C:\Users\PC\Desktop\VOD\vod_data\1399\Tva_vod\khordad1399.xlsx', sheet_name='Videos')
tva_khordad_1399.insert(5, 'operator', 'تیوا')
tva_khordad_1399.insert(6, 'month', 'خرداد 1399')
tva_tir_1399=pd.read_excel(r'C:\Users\PC\Desktop\VOD\vod_data\1399\Tva_vod\tir1399.xlsx', sheet_name='Videos')
tva_tir_1399.insert(5, 'operator', 'تیوا')
tva_tir_1399.insert(6, 'month', 'تیر 1399')
tva_mordad_1399=pd.read_excel(r'C:\Users\PC\Desktop\VOD\vod_data\1399\Tva_vod\mordad1399.xlsx', sheet_name='Videos')
tva_mordad_1399.insert(5, 'operator', 'تیوا')
tva_mordad_1399.insert(6, 'month', 'مرداد 1399')
tva_shahrivar_1399=pd.read_excel(r'C:\Users\PC\Desktop\VOD\vod_data\1399\Tva_vod\shahrivar1399.xlsx', sheet_name='Videos')
tva_shahrivar_1399.insert(5, 'operator', 'تیوا')
tva_shahrivar_1399.insert(6, 'month', 'شهریور 1399')
tva_mehr_1399=pd.read_excel(r'C:\Users\PC\Desktop\VOD\vod_data\1399\Tva_vod\mehr1399.xlsx', sheet_name='Videos')
tva_mehr_1399.insert(5, 'operator', 'تیوا')
tva_mehr_1399.insert(6, 'month', 'مهر 1399')
tva_aban_1399=pd.read_excel(r'C:\Users\PC\Desktop\VOD\vod_data\1399\Tva_vod\aban1399.xlsx', sheet_name='Videos')
tva_aban_1399.insert(5, 'operator', 'تیوا')
tva_aban_1399.insert(6, 'month', 'آبان 1399')
tva_azar_1399=pd.read_excel(r'C:\Users\PC\Desktop\VOD\vod_data\1399\Tva_vod\azar1399.xlsx', sheet_name='Videos')
tva_azar_1399.insert(5, 'operator', 'تیوا')
tva_azar_1399.insert(6, 'month', 'آذر 1399')
tva_dey_1399=pd.read_excel(r'C:\Users\PC\Desktop\VOD\vod_data\1399\Tva_vod\dey1399.xlsx', sheet_name='Videos')
tva_dey_1399.insert(5, 'operator', 'تیوا')
tva_dey_1399.insert(6, 'month', 'دی 1399')
tva_bahman_1399=pd.read_excel(r'C:\Users\PC\Desktop\VOD\vod_data\1399\Tva_vod\bahman1399.xlsx', sheet_name='Videos')
tva_bahman_1399.insert(5, 'operator', 'تیوا')
tva_bahman_1399.insert(6, 'month', 'بهمن 1399')
tva_esfand_1399=pd.read_excel(r'C:\Users\PC\Desktop\VOD\vod_data\1399\Tva_vod\esfand1399.xlsx', sheet_name='Videos')
tva_esfand_1399.insert(5, 'operator', 'تیوا')
tva_esfand_1399.insert(6, 'month', 'اسفند 1399')

tva_vod=pd.DataFrame()
tva_vod=tva_farvardin_1399.append([tva_ordibehesht_1399, tva_khordad_1399, tva_tir_1399,
                                   tva_mordad_1399, tva_shahrivar_1399, tva_mehr_1399,
                                   tva_aban_1399, tva_azar_1399, tva_dey_1399,
                                   tva_bahman_1399, tva_esfand_1399])

tva_vod['Video'] = tva_vod['Video'].str.strip()   # remove space
tva_vod['Video'].replace('', 'no', inplace=True)  # write "no" instead of nan
tva_vod=tva_vod[tva_vod['Video'] != 'no']         # remove no

tva_vod.insert(7, 'title_first', '')
tva_vod.insert(8, 'title_second', '')

tva_vod['Video'] = tva_vod['Video'].astype(str)
tva_vod=tva_vod.reset_index()
del tva_vod['index']
tva_vod_prime=tva_vod['Video']
length_tva_vod=len(tva_vod)  
for i in range(0,length_tva_vod):  # length_tva_vod
     print("i1: ", i)
     x_name_content=tva_vod_prime[i]
     head, sep, tail = x_name_content.partition('(')
     tva_vod.iat[i, 7] = head
     tva_vod.iat[i, 8] = tail

tva_vod.insert(9, 'season', '')
tva_vod.insert(10, 'epizode', '')
tva_vod_prime1=tva_vod['title_second']
for i in range(0,length_tva_vod):   # length_tva_vod
     print("i_1: ", i)
     try:
         x_name_content=tva_vod_prime1[i]
         head, sep, tail = x_name_content.partition('E')
         tva_vod.iat[i, 9] = head
         tva_vod.iat[i, 10] = tail
     except: pass

tva_vod['season'] = tva_vod['season'].map(lambda x: x.lstrip('S'))  # delete S
tva_vod['epizode'] = tva_vod['epizode'].map(lambda x: x.rstrip(')'))   # delete )
del tva_vod['title_second']

for i in range(0, length_tva_vod):    # length_tva_vod
    print("i2: ", i)
    try:                              # remove spaces
        txt=tva_vod.loc[i, 'title_first']
        res = " ".join(txt.split())
        tva_vod.loc[i, 'title_first']=res
    except: pass

tva_vod['season'] = tva_vod['season'].str.strip()
tva_vod['epizode'] = tva_vod['epizode'].str.strip()  
tva_vod['title_first'] = tva_vod['title_first'].str.strip()    

tva_vod['season'] = tva_vod['season'].apply(lambda x: x.zfill(2))   # convert 1 digit number to 2 digit number
tva_vod['epizode'] = tva_vod['epizode'].apply(lambda x: x.zfill(3)) # convert 1 digit number to 3 digit number

tva_vod.insert(10, 'all_epizode', '1')
tva_vod['all_epizode']=tva_vod['all_epizode'].astype('int')
#tva_vod.to_excel('tva_vod.xlsx')
########################################################################
lenz_farvardin_1399=pd.read_csv(r'C:\Users\PC\Desktop\VOD\vod_data\1399\lenz_vod\fardardin1399.csv')
lenz_farvardin_1399.insert(5, 'operator', 'لنز')
lenz_farvardin_1399.insert(6, 'month', 'فروردین 1399')
lenz_ordibehesht_1399=pd.read_csv(r'C:\Users\PC\Desktop\VOD\vod_data\1399\lenz_vod\ordibehesht1399.csv')
lenz_ordibehesht_1399.insert(5, 'operator', 'لنز')
lenz_ordibehesht_1399.insert(6, 'month', 'اردیبهشت 1399')
lenz_khordad_1399=pd.read_csv(r'C:\Users\PC\Desktop\VOD\vod_data\1399\lenz_vod\khordad1399.csv')
lenz_khordad_1399.insert(5, 'operator', 'لنز')
lenz_khordad_1399.insert(6, 'month', 'خرداد 1399')
lenz_tir_1399=pd.read_csv(r'C:\Users\PC\Desktop\VOD\vod_data\1399\lenz_vod\tir1399.csv')
lenz_tir_1399.insert(5, 'operator', 'لنز')
lenz_tir_1399.insert(6, 'month', 'تیر 1399')
lenz_mordad_1399=pd.read_csv(r'C:\Users\PC\Desktop\VOD\vod_data\1399\lenz_vod\mordad1399.csv')
lenz_mordad_1399.insert(5, 'operator', 'لنز')
lenz_mordad_1399.insert(6, 'month', 'مرداد 1399')
lenz_shahrivar_1399=pd.read_csv(r'C:\Users\PC\Desktop\VOD\vod_data\1399\lenz_vod\shahrivar1399.csv')
lenz_shahrivar_1399.insert(5, 'operator', 'لنز')
lenz_shahrivar_1399.insert(6, 'month', 'شهریور 1399')
lenz_mehr_1399=pd.read_csv(r'C:\Users\PC\Desktop\VOD\vod_data\1399\lenz_vod\mehr1399.csv')
lenz_mehr_1399.insert(5, 'operator', 'لنز')
lenz_mehr_1399.insert(6, 'month', 'مهر 1399')
lenz_aban_1399=pd.read_csv(r'C:\Users\PC\Desktop\VOD\vod_data\1399\lenz_vod\aban1399.csv')
lenz_aban_1399.insert(5, 'operator', 'لنز')
lenz_aban_1399.insert(6, 'month', 'آبان 1399')
lenz_azar_1399=pd.read_csv(r'C:\Users\PC\Desktop\VOD\vod_data\1399\lenz_vod\azar1399.csv')
lenz_azar_1399.insert(5, 'operator', 'لنز')
lenz_azar_1399.insert(6, 'month', 'آذر 1399')
lenz_dey_1399=pd.read_csv(r'C:\Users\PC\Desktop\VOD\vod_data\1399\lenz_vod\dey1399.csv')
lenz_dey_1399.insert(5, 'operator', 'لنز')
lenz_dey_1399.insert(6, 'month', 'دی 1399')
lenz_bahman_1399=pd.read_csv(r'C:\Users\PC\Desktop\VOD\vod_data\1399\lenz_vod\bahman1399.csv')
lenz_bahman_1399.insert(5, 'operator', 'لنز')
lenz_bahman_1399.insert(6, 'month', 'بهمن 1399')
lenz_esfand_1399=pd.read_csv(r'C:\Users\PC\Desktop\VOD\vod_data\1399\lenz_vod\esfand1399.csv')
lenz_esfand_1399.insert(5, 'operator', 'لنز')
lenz_esfand_1399.insert(6, 'month', 'اسفند 1399')

lenz_vod=pd.DataFrame()
lenz_vod=lenz_farvardin_1399.append([lenz_ordibehesht_1399, lenz_khordad_1399, lenz_tir_1399,
                                   lenz_mordad_1399, lenz_shahrivar_1399, lenz_mehr_1399,
                                   lenz_aban_1399, lenz_azar_1399, lenz_dey_1399,
                                   lenz_bahman_1399, lenz_esfand_1399])
    
lenz_vod['content name'] = lenz_vod['content name'].str.strip()
lenz_vod['content name'].replace('', 'no', inplace=True)
lenz_vod=lenz_vod[lenz_vod['content name'] != 'no']

del lenz_vod['content id']

lenz_vod.insert(7, 'title_first', '')
lenz_vod.insert(8, 'title_second', '')
lenz_vod.insert(9, 'season', '')
lenz_vod.insert(10, 'epizode', '')

lenz_vod['content name'] = lenz_vod['content name'].astype(str)
lenz_vod=lenz_vod.reset_index()
del lenz_vod['index']
lenz_vod_prime=lenz_vod['content name']
length_lenz_vod=len(lenz_vod)

for i in range(0,length_lenz_vod):    # length_lenz_vod
    print("i3: ", i)
    try:
        x_name_content=lenz_vod_prime[i]
        title_split=x_name_content.split()
        for word in title_split:
            if word=="فصل":
                head, sep, tail = x_name_content.partition('فصل')
                lenz_vod.iat[i, 7] = head
                lenz_vod.iat[i, 8] = sep+tail
                season_epizode = sep+tail
                head1, sep1, tail1 = season_epizode.partition('قسمت')
                lenz_vod.iat[i, 9] = head1
                lenz_vod.iat[i, 10] = sep1+tail1
                break
            elif word=="قسمت":
                head, sep, tail = x_name_content.partition('قسمت')
                lenz_vod.iat[i, 7] = head
                lenz_vod.iat[i, 10] = sep+tail
                break
            else:
                lenz_vod.iat[i, 7] = x_name_content
    except: pass

lenz_vod['season'] = lenz_vod['season'].map(lambda x: x.strip('فصل'))   # delete فصل
lenz_vod['epizode'] = lenz_vod['epizode'].map(lambda x: x.strip('قسمت')) # delete قسمت
del lenz_vod['title_second']

for i in range(0, length_lenz_vod):   # length_lenz_vod
    print("i4: ", i)
    try:                              # remove spaces
        txt=lenz_vod.loc[i, 'title_first']
        res = " ".join(txt.split())
        lenz_vod.loc[i, 'title_first']=res
    except: pass

lenz_vod['season'] = lenz_vod['season'].str.strip()    # remove space
lenz_vod['epizode'] = lenz_vod['epizode'].str.strip()  
lenz_vod['title_first'] = lenz_vod['title_first'].str.strip()    

lenz_vod['season'] = lenz_vod['season'].apply(lambda x: x.zfill(2))
lenz_vod['epizode'] = lenz_vod['epizode'].apply(lambda x: x.zfill(3))

lenz_vod.insert(10, 'all_epizode', '1')
lenz_vod['all_epizode']=lenz_vod['all_epizode'].astype('int')
#lenz_vod.to_excel('lenz_vod.xlsx')
########################################################################
tva_vod=tva_vod.rename(columns={"Video":"title"})
tva_vod=tva_vod.rename(columns={"Sessions":"x1"})          # x1 = Sessions
tva_vod=tva_vod.rename(columns={"Users":"x2"})             # x2 = Users
tva_vod=tva_vod.rename(columns={"Traffic (bytes)":"x3"})   # x3 = Traffic (bytes)
tva_vod=tva_vod.rename(columns={"Avg. Duration (sec)":"x4"})   # x4 = Avg. Duration (sec)

lenz_vod=lenz_vod.rename(columns={"content name":"title"})
lenz_vod=lenz_vod.rename(columns={"access users":"x1"})          # x1 = access users
lenz_vod=lenz_vod.rename(columns={"access times":"x2"})             # x2 = access times
lenz_vod=lenz_vod.rename(columns={"access duration (hour)":"x3"})   # x3 = access duration (hour)
lenz_vod=lenz_vod.rename(columns={"average access duration (hour)":"x4"})   # x4 = average access duration (hour)

total_vod=pd.DataFrame()
total_vod=tva_vod.append(lenz_vod)
length_total_vod=len(total_vod)
#total_vod.to_excel('total_vod_append.xlsx')

total_vod_test1=total_vod['epizode'].str.replace(r"\(.*\)","")  # remove ()
total_vod_test2=pd.DataFrame(total_vod_test1)
del total_vod['epizode']
total_vod = total_vod.reset_index()
del total_vod['index']
total_vod_test2 = total_vod_test2.reset_index()
del total_vod_test2['index']
total_vod = pd.merge(total_vod_test2, total_vod, left_index=True, right_index=True)
#total_vod=pd.read_excel(r'C:\Users\PC\Desktop\VOD\vahid.xlsx')


for j in range(0, 2):
    total_vod_counter=total_vod['epizode']
    for i in range(0,len(total_vod)):
         print("i5: ", i)
         try:
             x_name_content=total_vod_counter[i]
             head, sep, tail = x_name_content.partition(':')
             total_vod.loc[i, 'epizode'] = head
         except: pass
         try:
             x_name_content=total_vod_counter[i]
             head, sep, tail = x_name_content.partition('-')
             total_vod.loc[i, 'epizode'] = head
         except: pass
         try:
             x_name_content=total_vod_counter[i]
             head, sep, tail = x_name_content.partition('و')
             total_vod.loc[i, 'epizode'] = head
         except: pass
         try:
             x_name_content=total_vod_counter[i]
             head, sep, tail = x_name_content.partition('بخش')
             total_vod.loc[i, 'epizode'] = head
         except: pass
         try:
             x_name_content=total_vod_counter[i]
             head, sep, tail = x_name_content.partition(',')
             total_vod.loc[i, 'epizode'] = head
         except: pass
         try:
             x_name_content=total_vod_counter[i]
             head, sep, tail = x_name_content.partition('_')
             total_vod.loc[i, 'epizode'] = head
         except: pass
         try:
             x_name_content=total_vod_counter[i]
             head, sep, tail = x_name_content.partition(' ')
             total_vod.loc[i, 'epizode'] = head
         except: pass
         try:
             x_name_content=total_vod_counter[i]
             head, sep, tail = x_name_content.partition('&')
             total_vod.loc[i, 'epizode'] = head
         except: pass

total_vod['epizode'] = total_vod['epizode'].str.strip()
total_vod['epizode'] = total_vod['epizode'].apply(lambda x: x.zfill(3))

total_vod['00'] = total_vod['epizode'].str.contains('00')
total_vod['000'] = total_vod['epizode'].str.contains('000')
total_vod['001'] = total_vod['epizode'].str.contains('001')
total_vod['002'] = total_vod['epizode'].str.contains('002')
total_vod['003'] = total_vod['epizode'].str.contains('003')
total_vod['004'] = total_vod['epizode'].str.contains('004')
total_vod['005'] = total_vod['epizode'].str.contains('005')
total_vod['006'] = total_vod['epizode'].str.contains('006')
total_vod['007'] = total_vod['epizode'].str.contains('007')
total_vod['008'] = total_vod['epizode'].str.contains('008')
total_vod['009'] = total_vod['epizode'].str.contains('009')

total_vod=total_vod.reset_index()
del total_vod['index']
for i in range(len(total_vod)):
    print("i6: ", i)
    if total_vod.loc[i, '00'] == True and total_vod.loc[i, '000'] == False and \
    total_vod.loc[i, '001'] == False and total_vod.loc[i, '002'] == False and \
    total_vod.loc[i, '003'] == False and total_vod.loc[i, '004'] == False and \
    total_vod.loc[i, '005'] == False and total_vod.loc[i, '006'] == False and \
    total_vod.loc[i, '007'] == False and total_vod.loc[i, '008'] == False and \
    total_vod.loc[i, '009'] == False:
        total_vod.loc[i, 'epizode'] = "000"

del total_vod['00']
del total_vod['000']
del total_vod['001']
del total_vod['002']
del total_vod['003']
del total_vod['004']
del total_vod['005']
del total_vod['006']
del total_vod['007']
del total_vod['008']
del total_vod['009']

total_vod_new_1=total_vod.copy()
total_vod_new_1=total_vod_new_1.groupby(['title_first']).sum().reset_index()

total_vod['title_first'] = total_vod['title_first'].str.strip()
total_vod['title_first'].replace('', 'no', inplace=True)
total_vod=total_vod[total_vod['title_first'] != 'no']

total_vod_new_1['title_first'] = total_vod_new_1['title_first'].str.strip()
total_vod_new_1['title_first'].replace('', 'no', inplace=True)
total_vod_new_1=total_vod_new_1[total_vod_new_1['title_first'] != 'no']

total_vod=total_vod.reset_index()
del total_vod['index']

total_vod_new_1=total_vod_new_1.reset_index()
del total_vod_new_1['index']

length_total_vod_new_1=len(total_vod_new_1)
total_vod_new_1.insert(6, 'counter_title_first', '')
total_vod.insert(11, 'counter_title_first', '')
counter_title_first=1000001

for n in range(length_total_vod_new_1):
    print("i7: ", n)
    total_vod_new_1.loc[n, 'counter_title_first']=counter_title_first
    counter_title_first=counter_title_first+1

length_total_vod=len(total_vod)
for i in range(0, length_total_vod):
    print("i8: ", i)
    for ii in range(0, length_total_vod_new_1):
        if total_vod.loc[i , 'title_first']==total_vod_new_1.loc[ii , 'title_first']:
            total_vod.loc[i , 'counter_title_first']=total_vod_new_1.loc[ii, 'counter_title_first']
            break

total_vod_new_2=total_vod.copy()
total_vod_new_2=total_vod_new_2.drop_duplicates(subset =['title_first', 'epizode', 'operator'], keep = 'first')
total_vod_new_2=total_vod_new_2.groupby(['title_first', 'operator']).sum().reset_index()

length_total_vod_new_2=len(total_vod_new_2)
for i in range(length_total_vod):
    print("i9: ", i)
    for ii in range(length_total_vod_new_2):
        if total_vod.loc[i, 'title_first']==total_vod_new_2.loc[ii, 'title_first'] and \
        total_vod.loc[i, 'operator']==total_vod_new_2.loc[ii, 'operator']:
            total_vod.loc[i, 'all_epizode']=total_vod_new_2.loc[ii, 'all_epizode']
            break

for i in range(length_total_vod):
    if total_vod.loc[i, 'title_first'] == "پایتخت 1":
        total_vod.loc[i, 'season'] = "01"
    if total_vod.loc[i, 'title_first'] == "پایتخت 2":
        total_vod.loc[i, 'season'] = "02"
    if total_vod.loc[i, 'title_first'] == "پایتخت 3":
        total_vod.loc[i, 'season'] = "03"
    if total_vod.loc[i, 'title_first'] == "پایتخت 4":
        total_vod.loc[i, 'season'] = "04"
    if total_vod.loc[i, 'title_first'] == "پایتخت 5":
        total_vod.loc[i, 'season'] = "05"
    if total_vod.loc[i, 'title_first'] == "پایتخت 6":
        total_vod.loc[i, 'season'] = "06"

total_vod.insert(12, 'film_serial', '')
for i in range(length_total_vod):
    print("i10: ", i)
    if total_vod.loc[i, 'all_epizode']>1:
        total_vod.loc[i, 'film_serial']="02"
    else:
        total_vod.loc[i, 'film_serial']="01"
        total_vod.loc[i, 'season']="00"
        total_vod.loc[i, 'epizode']="000"

total_vod['counter_title_first']=total_vod['counter_title_first'].astype('str')
total_vod['film_serial']=total_vod['film_serial'].astype('str')
total_vod['season']=total_vod['season'].astype('str')
total_vod['epizode']=total_vod['epizode'].astype('str')
total_vod.insert(13, 'id', '')
total_vod['id']=total_vod['counter_title_first']+total_vod['film_serial']+total_vod['season']+total_vod['epizode']

total_vod.to_excel('total_vod.xlsx')

print("--- %s seconds ---" % (time.time() - start))
#################################################################################
       ############# distinct of tva and lenz for armin #############
#total_vod_armin = total_vod.copy()
#del total_vod_armin['epizode']
#del total_vod_armin['all_epizode']
#del total_vod_armin['month']
#del total_vod_armin['season']
#del total_vod_armin['title']
#del total_vod_armin['x1']
#del total_vod_armin['x2']
#del total_vod_armin['x3']
#del total_vod_armin['x4']
#del total_vod_armin['counter_title_first']
#del total_vod_armin['id']
#total_vod_armin_tva_1 = total_vod_armin.query("operator == 'تیوا'")
#total_vod_armin_lenz_1 = total_vod_armin.query("operator == 'لنز'")
#
#total_vod_armin_tva=total_vod_armin_tva_1.drop_duplicates(subset =['title_first', 'film_serial'], keep = 'first')
#total_vod_armin_lenz=total_vod_armin_lenz_1.drop_duplicates(subset =['title_first', 'film_serial'], keep = 'first')
#
#del total_vod_armin_tva['operator']
#del total_vod_armin_lenz['operator']
#
#total_vod_armin_tva.to_excel('total_vod_armin_tva.xlsx')
#total_vod_armin_lenz.to_excel('total_vod_armin_lenz.xlsx')

#################################################################################
############# conver x1, x2, x3 and x4 to visit, active_user and duration #############
#total_vod=pd.read_excel(r'C:\Users\PC\Desktop\VOD\total_vod.xlsx')
#total_vod_tva = total_vod.query("operator == 'تیوا'")
#total_vod_lenz = total_vod.query("operator == 'لنز'")
#total_vod_tva = total_vod_tva.rename(columns = {"x1" : "visit"})
#total_vod_tva = total_vod_tva.rename(columns = {"x2" : "active_users"})
#total_vod_tva['x4'] = total_vod_tva['x4'] * total_vod_tva['visit']
#total_vod_tva['x4'] = round(total_vod_tva['x4']/60, 0)
#total_vod_tva = total_vod_tva.rename(columns = {"x4" : "duration (min)"})
#del total_vod_tva['x3']
#del total_vod_tva['Unnamed: 0']
#
#total_vod_lenz = total_vod_lenz.rename(columns = {"x1" : "active_users"})
#total_vod_lenz = total_vod_lenz.rename(columns = {"x2" : "visit"})
#total_vod_lenz['x3'] = total_vod_lenz['x3'] * 60
#total_vod_lenz['x3'] = round(total_vod_lenz['x3'], 0)
#total_vod_lenz = total_vod_lenz.rename(columns = {"x3" : "duration (min)"})
#del total_vod_lenz['x4']
#del total_vod_lenz['Unnamed: 0']
#
#total_vod_compelete = pd.DataFrame()
#total_vod_compelete = total_vod_tva.append(total_vod_lenz)
#total_vod_compelete.to_excel('total_vod_compelete.xlsx')
#################################################################################
       ############# edit total_vod_compelete version 1 #############
#total_vod_compelete_1399_ver1=pd.read_excel(r'C:\Users\PC\Desktop\VOD\total_vod_compelete_1399.xlsx')
#total_vod_compelete_1399_ver1['month'] = total_vod_compelete_1399_ver1['month'].str.replace('فروردین 1399', '139901')
#total_vod_compelete_1399_ver1['month'] = total_vod_compelete_1399_ver1['month'].str.replace('اردیبهشت 1399', '139902')
#total_vod_compelete_1399_ver1['month'] = total_vod_compelete_1399_ver1['month'].str.replace('خرداد 1399', '139903')
#total_vod_compelete_1399_ver1['month'] = total_vod_compelete_1399_ver1['month'].str.replace('تیر 1399', '139904')
#total_vod_compelete_1399_ver1['month'] = total_vod_compelete_1399_ver1['month'].str.replace('مرداد 1399', '139905')
#total_vod_compelete_1399_ver1['month'] = total_vod_compelete_1399_ver1['month'].str.replace('شهریور 1399', '139906')
#total_vod_compelete_1399_ver1['month'] = total_vod_compelete_1399_ver1['month'].str.replace('مهر 1399', '139907')
#total_vod_compelete_1399_ver1['month'] = total_vod_compelete_1399_ver1['month'].str.replace('آبان 1399', '139908')
#total_vod_compelete_1399_ver1['month'] = total_vod_compelete_1399_ver1['month'].str.replace('آذر 1399', '139909')
#total_vod_compelete_1399_ver1['month'] = total_vod_compelete_1399_ver1['month'].str.replace('دی 1399', '139910')
#total_vod_compelete_1399_ver1['month'] = total_vod_compelete_1399_ver1['month'].str.replace('بهمن 1399', '139911')
#total_vod_compelete_1399_ver1['month'] = total_vod_compelete_1399_ver1['month'].str.replace('اسفند 1399', '139912')
#
#gozine2 = total_vod_compelete_1399_ver1['title_first'].str.contains(r'گزینه دو', np.nan)
#gozine2 = pd.DataFrame(gozine2)
#for i in range(len(total_vod_compelete_1399_ver1)):
#    print(i)
#    if gozine2.loc[i, 'title_first'] == True:
#        total_vod_compelete_1399_ver1.loc[i, 'title_first'] = "گزینه دو"
#
#del total_vod_compelete_1399_ver1['Unnamed: 0']
#total_vod_compelete_1399_ver1.to_excel('total_vod_compelete_1399_ver1.xlsx')
#################################################################################
         ############# test for clean of epizode #############
#a1=pd.read_excel(r'C:\Users\PC\Desktop\test\vahid.xlsx')
#a2=a1['name'].str.replace(r"\(.*\)","")
#a22=pd.DataFrame(a2)
#del a1['name']
#a3 = pd.merge(a22,a1, left_index=True, right_index=True)
#a11=a3['name']
#for i in range(0,len(a3)):
#     print(i)
#     try:
#         x_name_content=a11[i]
#         head, sep, tail = x_name_content.partition(':')
#         a3.loc[i, 'name'] = head
#     except: pass
#     try:
#         x_name_content=a11[i]
#         head, sep, tail = x_name_content.partition('-')
#         a3.loc[i, 'name'] = head
#     except: pass
#     try:
#         x_name_content=a11[i]
#         head, sep, tail = x_name_content.partition('و')
#         a3.loc[i, 'name'] = head
#     except: pass
#     try:
#         x_name_content=a11[i]
#         head, sep, tail = x_name_content.partition('بخش ')
#         a3.loc[i, 'name'] = head
#     except: pass
#     try:
#         x_name_content=a11[i]
#         head, sep, tail = x_name_content.partition(',')
#         a3.loc[i, 'name'] = head
#     except: pass
#     try:
#         x_name_content=a11[i]
#         head, sep, tail = x_name_content.partition('_')
#         a3.loc[i, 'name'] = head
#     except: pass
#     try:
#         x_name_content=a11[i]
#         head, sep, tail = x_name_content.partition(' ')
#         a3.loc[i, 'name'] = head
#     except: pass
#     try:
#         x_name_content=a11[i]
#         head, sep, tail = x_name_content.partition('&')
#         a3.loc[i, 'name'] = head
#     except: pass
#
#a3['name'] = a3['name'].str.strip()
#a3['name'] = a3['name'].apply(lambda x: x.zfill(3))
#
#a3.insert(1, 'test', '')
#a3['00'] = a3['name'].str.contains('00')
#a3['000'] = a3['name'].str.contains('000')
#a3['001'] = a3['name'].str.contains('001')
#a3['002'] = a3['name'].str.contains('002')
#a3['003'] = a3['name'].str.contains('003')
#a3['004'] = a3['name'].str.contains('004')
#a3['005'] = a3['name'].str.contains('005')
#a3['006'] = a3['name'].str.contains('006')
#a3['007'] = a3['name'].str.contains('007')
#a3['008'] = a3['name'].str.contains('008')
#a3['009'] = a3['name'].str.contains('009')
#
#for i in range(len(a3)):
#    if a3.loc[i, '00'] == True and a3.loc[i, '000'] == False and \
#    a3.loc[i, '001'] == False and a3.loc[i, '002'] == False and \
#    a3.loc[i, '003'] == False and a3.loc[i, '004'] == False and \
#    a3.loc[i, '005'] == False and a3.loc[i, '006'] == False and \
#    a3.loc[i, '007'] == False and a3.loc[i, '008'] == False and \
#    a3.loc[i, '009'] == False:
#        a3.loc[i, 'name'] = "000"
#
#a4=a3
#a4.to_excel('a4.xlsx')
#######################################################################























