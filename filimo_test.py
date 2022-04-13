vod_filimo_star_main=pd.read_excel(r'E:\python codes\VOD\vod_data\1400\filimo_vod\FilimoMehr1400.xlsx')
vod_filimo_star = vod_filimo_star_main.copy()
vod_filimo_star['Title'] = vod_filimo_star['Title'].astype(str)
vod_filimo_star = vod_filimo_star[~vod_filimo_star.Title.str.contains("nan")]

del vod_filimo_star['LinkAddress']
del vod_filimo_star['EnglishName']
del vod_filimo_star['Imdb']
del vod_filimo_star['Runtime']
del vod_filimo_star['Genres']
del vod_filimo_star['Country']
del vod_filimo_star['DubbedSubtitle']
del vod_filimo_star['Director']
del vod_filimo_star['Casts']
del vod_filimo_star['Producer']
del vod_filimo_star['Composer']
del vod_filimo_star['Singer']
del vod_filimo_star['Writer']
del vod_filimo_star['Cameraman']
del vod_filimo_star['Year']
del vod_filimo_star['AgeRange']

vod_filimo_star['Like'] = vod_filimo_star['Like'].astype(str)
vod_filimo_star['Like'] = vod_filimo_star['Like'].str.replace('nan', '0')
vod_filimo_star['Like'] = vod_filimo_star['Like'].fillna('0')
vod_filimo_star_vir = vod_filimo_star[vod_filimo_star.Like.str.contains(",")]
vod_filimo_star_novir = vod_filimo_star[~vod_filimo_star.Like.str.contains(",")]
vod_filimo_star_vir['Like'] = vod_filimo_star_vir['Like'].str.replace(',', '')
vod_filimo_star = vod_filimo_star_vir.append([vod_filimo_star_novir])

vod_filimo_star['Like'] = vod_filimo_star['Like'].str.strip()
vod_filimo_star['Like'] = vod_filimo_star['Like'].astype(int)

vod_filimo_star=vod_filimo_star.rename(columns={"Like":"Visit"})
del vod_filimo_star['Percent']

vod_filimo_star = vod_filimo_star.reset_index()
del vod_filimo_star['index']
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.strip()
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].astype(str)
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('پشت صحنه', '')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('(قسمت پایانی)', '')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('(بخش اول)', '')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('(بخش دوم)', '')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('(قسمت آخر فصل اول)', '')

vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('چهلم', '40')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('سی و نهم', '39')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('سی و هشتم', '38')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('سی و هفتم', '37')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('سی و ششم', '36')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('سی و پنجم', '35')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('سی و چهارم', '34')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('سی و سوم', '33')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('سی و دوم', '32')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('سی و یکم', '31')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('سی ام', '30')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('بیست و نهم', '29+')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('بیست و هشتم', '28')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('بیست و هفتم', '27')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('بیست و ششم', '26')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('بیست و پنجم', '25')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('بیست و چهارم', '24')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('بیست و سوم', '23')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('بیست و دوم', '22')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('بیست و یکم', '21')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('بیستم', '20')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('نوزدهم', '19')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('هجدهم', '18')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('هفدهم', '17')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('شانزدهم', '16')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('پانزدهم', '15')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('چهاردهم', '14')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('سیزدهم', '13')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('دوازدهم', '12')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('یازدهم', '11')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('دهم', '10')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('نهم', '09')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('هشتم', '08')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('هفتم', '07')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('ششم', '06')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('پنجم', '05')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('چهارم', '04')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('سوم', '03')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('دوم', '02')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('اول', '01')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('سی و سه', '33')

for i in range(0,len(vod_filimo_star)):
    print(i)
    try:
        x_name_content=vod_filimo_star.loc[i, 'Epizode']
        head, sep, tail = x_name_content.partition('قسمت')
        vod_filimo_star.loc[i, 'Epizode'] = tail
    except: pass
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.strip()

for i in range(0,len(vod_filimo_star)):
    print(i)
    try:
        x_name_content=vod_filimo_star.loc[i, 'Epizode']
        head, sep, tail = x_name_content.partition(':')
        vod_filimo_star.loc[i, 'Epizode'] = head
    except: pass
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.strip()

for i in range(0,len(vod_filimo_star)):
    print(i)
    try:
        x_name_content=vod_filimo_star.loc[i, 'Epizode']
        head, sep, tail = x_name_content.partition('(')
        vod_filimo_star.loc[i, 'Epizode'] = head
    except: pass
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.strip()

vod_filimo_star['Visit'] = vod_filimo_star['Visit'].fillna(0)
vod_filimo_star['Visit'] = vod_filimo_star['Visit'].astype(int)
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].astype(str)
vod_filimo_star1 = vod_filimo_star [~vod_filimo_star.Epizode.str.contains('و')]
vod_filimo_star2 = vod_filimo_star [vod_filimo_star.Epizode.str.contains('و')]
vod_filimo_star2['Epizode'] = vod_filimo_star2['Epizode'].str.replace('و', ' ')
vod_filimo_star2 = vod_filimo_star2.reset_index()
del vod_filimo_star2['index']
vod_filimo_star2_new = pd.DataFrame()
for i in range(0, len(vod_filimo_star2)):    # len(vod_filimo_star2)
    vod_filimo_star2_per = pd.DataFrame()
    Epizode = vod_filimo_star2.loc[i, 'Epizode']
    epizode_row = vod_filimo_star2.loc[i]
    epizode_row = pd.DataFrame({'ro': epizode_row})
    epizode_row = epizode_row.T
    epizode_row = epizode_row.reset_index()
    del epizode_row['index']
    epizode_split = Epizode.split()
    epizode_split = pd.DataFrame({'col': epizode_split})
    vod_filimo_star2_per = vod_filimo_star2_per.append([epizode_row]*len(epizode_split),ignore_index=True)
    vod_filimo_star2_per.fillna(method='ffill', inplace=True)
    for j in range(0, len(epizode_split)):
        vod_filimo_star2_per.loc[j, 'Epizode'] = epizode_split.loc[j, 'col']
        vod_filimo_star2_per.loc[j, 'Visit'] = round(epizode_row.loc[0, 'Visit']/len(epizode_split), 0)
    vod_filimo_star2_new = vod_filimo_star2_new.append([vod_filimo_star2_per])
    del Epizode
    del epizode_row
    del epizode_split
    del vod_filimo_star2_per

vod_filimo_star =  vod_filimo_star1.append([vod_filimo_star2_new])    

vod_filimo_star['number1'] = vod_filimo_star.Epizode.str.extract('(d)', expand=True)
vod_filimo_star['number2'] = vod_filimo_star.Epizode.str.extract('(dd)', expand=True)
vod_filimo_star['number3'] = vod_filimo_star.Epizode.str.extract('(ddd)', expand=True)
vod_filimo_star = vod_filimo_star.reset_index()
del vod_filimo_star['index']
#vod_filimo_star['epizode'] = vod_filimo_star['epizode'].astype(int, errors='ignore')
vod_filimo_star_types = vod_filimo_star.Epizode.apply(type)
vod_filimo_star_types = pd.DataFrame({'ro': vod_filimo_star_types})
vod_filimo_star['count_numbers'] = vod_filimo_star['Epizode'].str.count('d')
for i in range(0, len(vod_filimo_star)):
    print(i)
    if vod_filimo_star_types.loc[i, 'ro'] != int:
        if vod_filimo_star.loc[i, 'count_numbers'] == 1:
            vod_filimo_star.loc[i, 'Epizode'] = vod_filimo_star.loc[i, 'number1']
        elif vod_filimo_star.loc[i, 'count_numbers'] == 2:
            vod_filimo_star.loc[i, 'Epizode'] = vod_filimo_star.loc[i, 'number2']
        elif vod_filimo_star.loc[i, 'count_numbers'] == 3:
            vod_filimo_star.loc[i, 'Epizode'] = vod_filimo_star.loc[i, 'number3']

del vod_filimo_star['number3']    

vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace(r'\D', '')

del vod_filimo_star['number1']
del vod_filimo_star['number2']
#del vod_filimo_star['number3']
del vod_filimo_star['count_numbers']

total_vod_1399_Ghadimi_v2 = pd.read_excel(r'E:\python codes\VOD\kh_ghadimi\total_vod_1399_Ghadimi_v2.xlsx')
total_vod_1399_Ghadimi_v2=total_vod_1399_Ghadimi_v2.rename(columns={"شماره فصل":"season"})
total_vod_1399_Ghadimi_v2 = total_vod_1399_Ghadimi_v2.fillna("NO")
total_vod_1399_Ghadimi_v2['season'] = total_vod_1399_Ghadimi_v2['season'].astype(str)
total_vod_1399_Ghadimi_v2 = total_vod_1399_Ghadimi_v2[~total_vod_1399_Ghadimi_v2.season.str.contains("NO")]
total_vod_1399_Ghadimi_v2['season'] = total_vod_1399_Ghadimi_v2['season'].astype(str).replace('.0', '', regex=True)

vod_filimo_star['Season'] = vod_filimo_star['Season'].astype(str).replace('.0', '', regex=True)
vod_filimo_star['Season'] = vod_filimo_star['Season'].str.strip() 
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.strip()
vod_filimo_star ['Season'].replace('', '00', inplace=True)
vod_filimo_star['Season'] = vod_filimo_star['Season'].astype(str)
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].astype(str)
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].fillna(0)
vod_filimo_star['Season'] = vod_filimo_star['Season'].fillna(0)
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('nan', '0')
vod_filimo_star['Season'] = vod_filimo_star['Season'].str.replace('nan', '0')
vod_filimo_star['Season'] = vod_filimo_star['Season'].apply(lambda x: x.zfill(2))
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].apply(lambda x: x.zfill(3))
vod_filimo_star ['Epizode'].replace('', '000', inplace=True)
for i in range(0, len(vod_filimo_star)):    # len(vod_filimo_star)
    print(i)
    if vod_filimo_star.loc[i, 'Season'] == '00':
        vod_filimo_star.loc[i, 'FilmSerial'] = 'فیلم'
        vod_filimo_star.loc[i, 'code_FilmSerial'] = '01'
    else:
        vod_filimo_star.loc[i, 'FilmSerial'] = 'سریال'
        vod_filimo_star.loc[i, 'code_FilmSerial'] = '02'

#### save ####
vod_filimo_star_repeat_1 = vod_filimo_star.copy()
vod_filimo_star = vod_filimo_star_repeat_1.copy()
##############
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['Title']
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('سریال', '')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('فیلم', '')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('انیمیشن', '')

vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('او وی ای', '')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('با زیرنویس مخصوص ناشنوایان', '')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('مخصوص نابینایان', '')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('مخصوص ناشنوایان', '')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('لالیگا', '')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('بوندسلیگا', '')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('سری آ', '')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('لیگ جزیره', '')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('ویژه نوروز', '')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('به روایت محمدرضا احمدی', '')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('تنت', '')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('جدید و برگزیده', '')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('اره ۹', '')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('ماه و ستاره', '')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('سرقت خودرو', '')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('ایدز', '')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('(', '')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace(')', '')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.strip()

total_vod_1399_Ghadimi_v2 = total_vod_1399_Ghadimi_v2.reset_index()
del total_vod_1399_Ghadimi_v2['index']
for i in range(0, len(vod_filimo_star)):
    print(i)
    for j in range(0, len(total_vod_1399_Ghadimi_v2)):
        if vod_filimo_star.loc[i, 'Title'] == total_vod_1399_Ghadimi_v2.loc[j, 'title']:
            vod_filimo_star.loc[i, 'TitleCleaned1'] = total_vod_1399_Ghadimi_v2.loc[j, 'title_first']
            vod_filimo_star.loc[i, 'Season'] = total_vod_1399_Ghadimi_v2.loc[j, 'season']
            break
vod_filimo_star['Season'] = vod_filimo_star['Season'].apply(lambda x: x.zfill(2))

vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('0', '۰')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('1', '۱')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('2', '۲')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('3', '۳')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('4', '۴')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('5', '۵')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('6', '۶')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('7', '۷')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('8', '۸')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('9', '۹')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('٠', '۰')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('١', '۱')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('٢', '۲')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('٣', '۳')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('٤', '۴')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('٥', '۵')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('٦', '۶')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('٧', '۷')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('٨', '۸')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('٩', '۹')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('ي', 'ی')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('ؤ','و')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.replace('ك','ک')
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].str.strip()
   
vod_filimo_star['Season'] = vod_filimo_star['Season'].str.replace('۰', '0')
vod_filimo_star['Season'] = vod_filimo_star['Season'].str.replace('۱', '1')
vod_filimo_star['Season'] = vod_filimo_star['Season'].str.replace('۲', '2')
vod_filimo_star['Season'] = vod_filimo_star['Season'].str.replace('۳', '3')
vod_filimo_star['Season'] = vod_filimo_star['Season'].str.replace('۴', '4')
vod_filimo_star['Season'] = vod_filimo_star['Season'].str.replace('۵', '5')
vod_filimo_star['Season'] = vod_filimo_star['Season'].str.replace('۶', '6')
vod_filimo_star['Season'] = vod_filimo_star['Season'].str.replace('۷', '7')
vod_filimo_star['Season'] = vod_filimo_star['Season'].str.replace('۸', '8')
vod_filimo_star['Season'] = vod_filimo_star['Season'].str.replace('۹', '9')

vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('۰', '0')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('۱', '1')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('۲', '2')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('۳', '3')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('۴', '4')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('۵', '5')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('۶', '6')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('۷', '7')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('۸', '8')
vod_filimo_star['Epizode'] = vod_filimo_star['Epizode'].str.replace('۹', '9')

vod_filimo_star['DateTime_year'] = '2021'  # change year
vod_filimo_star['DateTime_month'] = '10'   # change month
vod_filimo_star['DateTime_day'] = '01'
vod_filimo_star['DateTime_time'] = '01'
vod_filimo_star['DateTime'] = vod_filimo_star['DateTime_year']+vod_filimo_star['DateTime_month']+vod_filimo_star['DateTime_day']+vod_filimo_star['DateTime_time']
del vod_filimo_star['DateTime_year']
del vod_filimo_star['DateTime_month']
del vod_filimo_star['DateTime_day']
del vod_filimo_star['DateTime_time']

vod_filimo_star['Title'] = vod_filimo_star['Title'].astype(str)
vod_filimo_star['TitleCleaned1'] = vod_filimo_star['TitleCleaned1'].astype(str)
vod_filimo_star = vod_filimo_star [~vod_filimo_star.Title.str.contains('دعای')]
vod_filimo_star = vod_filimo_star [~vod_filimo_star.Title.str.contains('ریاضی')]
vod_filimo_star = vod_filimo_star [~vod_filimo_star.Title.str.contains('گزینه دو')]
vod_filimo_star = vod_filimo_star [~vod_filimo_star.Title.str.contains('vod')]
vod_filimo_star = vod_filimo_star [~vod_filimo_star.Title.str.startswith('فارسی')]
vod_filimo_star = vod_filimo_star [~vod_filimo_star.Title.str.startswith('انگلیسی')]
vod_filimo_star = vod_filimo_star [~vod_filimo_star.Title.str.startswith('عربی')]
vod_filimo_star = vod_filimo_star [~vod_filimo_star.Title.str.startswith('علوم')]
vod_filimo_star = vod_filimo_star [~vod_filimo_star.Title.str.startswith('روضه')]
vod_filimo_star = vod_filimo_star [~vod_filimo_star.Title.str.startswith('مداح')]
vod_filimo_star = vod_filimo_star [~vod_filimo_star.Title.str.startswith('-مداح')]
vod_filimo_star = vod_filimo_star [~vod_filimo_star.Title.str.startswith('متوسطه')]
vod_filimo_star = vod_filimo_star [~vod_filimo_star.Title.str.startswith('ابتدایی')]

#vod_filimo_star.to_excel('vod_filimo_star.xlsx', index=False)
#vod_filimo_star = vod_filimo_star.groupby(['DateTime', 'code_FilmSerial', 'TitleCleaned1', 'Season', 'Epizode', 'FilmSerial', 'Title']).sum().reset_index()
vod_filimo_star.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode', 'FilmSerial', 'Title'], keep = 'first', inplace = True)

vod_filimo_star.insert(8, 'ID', '')
vod_filimo_star.insert(9, 'IDS', '')
vod_filimo_star.insert(10, 'ActiveUsers', '')
vod_filimo_star.insert(11, 'DurationMin', '')
vod_filimo_star.insert(12, 'Operators', 'فیلیمو')   
vod_filimo_star.insert(13, 'Month', 'مهر')      # change
vod_filimo_star.insert(14, 'Year', '1400')        # change
#### save ####
vod_filimo_star_repeat_2 = vod_filimo_star.copy()
vod_filimo_star = vod_filimo_star_repeat_2.copy()
##############
db1_vod_filimo_star_old = db1.query('Operators == "فیلیمو"')
db1_VOD_unfilimo_old = db1.query('Operators != "فیلیمو"')
#db1_vod_filimo_star_old = Tir.copy()
#db1_vod_filimo_star_old = Tir.append([Mordad])

db1_vod_filimo_star_old = db1_vod_filimo_star_old.reset_index()
del db1_vod_filimo_star_old['index']

db1_data = db1_vod_filimo_star_old.copy()
del db1_data['ActiveUsers']
del db1_data['DateTime']
del db1_data['DurationMin']
del db1_data['Month']
del db1_data['Operators']
del db1_data['Title']
db1_data=db1_data.rename(columns={"Visit":"Visit_old"})
del db1_data['Year']
del db1_data['TitleCleaned2']
del vod_filimo_star['ID']
del vod_filimo_star['IDS']
del vod_filimo_star['FilmSerial']
vod_filimo_star_groupby = vod_filimo_star.copy()
vod_filimo_star_groupby = vod_filimo_star_groupby.groupby(['TitleCleaned1', 'Season', 'Epizode']).sum().reset_index()
del vod_filimo_star['Visit']
vod_filimo_star = pd.merge(vod_filimo_star, vod_filimo_star_groupby, on = ['TitleCleaned1', 'Season', 'Epizode'])
####### compare of TitleCleaned1, Season and Epizode with old filimo = step 1 #######
db1_data = db1_data.groupby(['ID', 'IDS', 'TitleCleaned1', 'Season', 'Epizode']).sum().reset_index()
vod_filimo_star_db1_merge = pd.merge(vod_filimo_star, db1_data, on = ['TitleCleaned1', 'Season', 'Epizode'])
vod_filimo_star_db1_merge.drop_duplicates(subset =['ID'], keep = 'last', inplace = True)
vod_filimo_star_db1_merge.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode'], keep = 'last', inplace = True)
vod_filimo_star_db1_merge['Visit'] = vod_filimo_star_db1_merge['Visit'] - vod_filimo_star_db1_merge['Visit_old']
del vod_filimo_star_db1_merge['Visit_old']
vod_filimo_star_step1 = vod_filimo_star_db1_merge.copy()

vod_filimo_star.insert(12, 'FilmSerial', '')
vod_filimo_star.insert(13, 'ID', '')
vod_filimo_star.insert(14, 'IDS', '')
vod_filimo_star_step22 = vod_filimo_star.append([vod_filimo_star_db1_merge])
vod_filimo_star_step22.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode'], keep = False, inplace = True)
####### compare of TitleCleaned1 and Season with old filimo = step 2 #######
del vod_filimo_star_step22['FilmSerial']
del vod_filimo_star_step22['ID']
del vod_filimo_star_step22['IDS']
del db1_data['Epizode']
vod_filimo_star_db1_merge2 = pd.merge(vod_filimo_star_step22, db1_data, on = ['TitleCleaned1', 'Season'])
vod_filimo_star_db1_merge2.drop_duplicates(subset =['TitleCleaned1', 'Season'], keep = 'last', inplace = True)
vod_filimo_star_db1_merge2 = vod_filimo_star_db1_merge2.reset_index()
del vod_filimo_star_db1_merge2['index']
vod_filimo_star_step2 = vod_filimo_star_db1_merge2.copy()
vod_filimo_star_step2['ID'] = vod_filimo_star_step2['ID'].str[0:11]
vod_filimo_star_step2['ID'] = vod_filimo_star_step2['ID'] + vod_filimo_star_step2['Epizode']
vod_filimo_star_step2['FilmSerial'] = 'سریال'
vod_filimo_star_step2.insert(14, 'TitleCleaned2', '')
####### remain data from new filimo = step 3 #######
vod_filimo_star_step3 = vod_filimo_star_step22.append([vod_filimo_star_step2])
vod_filimo_star_step3.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode'], keep = False, inplace = True)
vod_filimo_star_step3 = vod_filimo_star_step3.reset_index()
del vod_filimo_star_step3['index']
del vod_filimo_star_step3['Visit_old']

##### step 2 #####
vod_filimo_star = vod_filimo_star_step3.copy()
db1_VOD_unfilimo_old = db1_VOD_unfilimo_old.reset_index()
del db1_VOD_unfilimo_old['index']

db1_Visit = db1_VOD_unfilimo_old.copy()
vod_filimo_star_Visit = pd.DataFrame()
vod_filimo_star_Visit['ID'] = db1_Visit['ID']
vod_filimo_star_Visit['Visit_old'] = db1_Visit['Visit']
vod_filimo_star_Visit = vod_filimo_star_Visit.groupby(['ID']).sum().reset_index()

db1_data = db1_VOD_unfilimo_old.copy()
del db1_data['ActiveUsers']
del db1_data['DateTime']
del db1_data['DurationMin']
del db1_data['Month']
del db1_data['Operators']
del db1_data['Title']
del db1_data['Visit']
del db1_data['Year']
del db1_data['TitleCleaned2']
del vod_filimo_star['ID']
del vod_filimo_star['IDS']
del vod_filimo_star['FilmSerial']
db1_data.drop_duplicates(subset =['ID'], keep = 'last', inplace = True)
####### compare of TitleCleaned1, Season and Epizode with old unfilimo = step 4 #######
vod_filimo_star_db1_merge = pd.merge(vod_filimo_star, db1_data, on = ['TitleCleaned1', 'Season', 'Epizode'])
vod_filimo_star_db1_merge.drop_duplicates(subset =['ID'], keep = 'last', inplace = True)
vod_filimo_star_db1_merge.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode'], keep = 'last', inplace = True)
vod_filimo_star_step4 = vod_filimo_star_db1_merge.copy()
####### remain data from new filimo = step 55 #######
vod_filimo_star.insert(13, 'FilmSerial', '')
vod_filimo_star.insert(14, 'ID', '')
vod_filimo_star.insert(15, 'IDS', '')
vod_filimo_star_step55 = vod_filimo_star.append([vod_filimo_star_db1_merge])
vod_filimo_star_step55.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode'], keep = False, inplace = True)
####### compare of TitleCleaned1 and Season with old unfilimo = step 5 #######
del vod_filimo_star_step55['FilmSerial']
del vod_filimo_star_step55['ID']
del vod_filimo_star_step55['IDS']
del vod_filimo_star_step55['TitleCleaned2']
del db1_data['Epizode']
vod_filimo_star_db1_merge2 = pd.merge(vod_filimo_star_step55, db1_data, on = ['TitleCleaned1', 'Season'])
vod_filimo_star_db1_merge2.drop_duplicates(subset =['TitleCleaned1', 'Season'], keep = 'last', inplace = True)
vod_filimo_star_db1_merge2 = vod_filimo_star_db1_merge2.reset_index()
del vod_filimo_star_db1_merge2['index']
vod_filimo_star_step5 = vod_filimo_star_db1_merge2.copy()
vod_filimo_star_step5['ID'] = vod_filimo_star_step5['ID'].str[0:11]
vod_filimo_star_step5['ID'] = vod_filimo_star_step5['ID'] + vod_filimo_star_step5['Epizode']

vod_filimo_star_step55.insert(12, 'FilmSerial', '')
vod_filimo_star_step55.insert(13, 'ID', '')
vod_filimo_star_step55.insert(14, 'IDS', '')
vod_filimo_star_step55.insert(15, 'TitleCleaned2', '')
vod_filimo_star_step5.insert(15, 'TitleCleaned2', '')
####### remain data from new filimo = step 6 #######
vod_filimo_star_step6 = vod_filimo_star_step55.copy()
#vod_filimo_star_step6 = vod_filimo_star_step55.append([vod_filimo_star_step2])
vod_filimo_star_step6.drop_duplicates(subset =['TitleCleaned1', 'Season', 'Epizode'], keep = False, inplace = True)
vod_filimo_star_step6 = vod_filimo_star_step6.reset_index()
del vod_filimo_star_step6['index']

##### data gather #####
vod_filimo_star_step6 = vod_filimo_star_step6.sort_values(['Season', 'TitleCleaned1'], ascending=[False, False])
vod_filimo_star_step6 = vod_filimo_star_step6.reset_index()
del vod_filimo_star_step6['index']
id_first = 1029071    # write last id_first + 1
vod_filimo_star_step6.loc[0, 'id_first'] = id_first
for i in range(1, len(vod_filimo_star_step6)):
    print(i)
    if vod_filimo_star_step6.loc[i, 'TitleCleaned1'] == vod_filimo_star_step6.loc[i-1, 'TitleCleaned1'] and \
       vod_filimo_star_step6.loc[i, 'Season'] == vod_filimo_star_step6.loc[i-1, 'Season']:
        vod_filimo_star_step6.loc[i, 'id_first'] = id_first
    else:
        id_first = id_first + 1
        vod_filimo_star_step6.loc[i, 'id_first'] = id_first

vod_filimo_star_step6['id_first'] = vod_filimo_star_step6['id_first'].astype(int).astype(str) 
       
vod_filimo_star_step6['ID'] = vod_filimo_star_step6['id_first']+vod_filimo_star_step6['code_FilmSerial']+vod_filimo_star_step6['Season']+vod_filimo_star_step6['Epizode']
vod_filimo_star_step6['IDS'] = vod_filimo_star_step6['id_first']+vod_filimo_star_step6['Season']

del vod_filimo_star_step6['id_first']

vod_filimo_star_step6['FilmSerial'] = vod_filimo_star_step6['code_FilmSerial']
vod_filimo_star_step6['FilmSerial'] = vod_filimo_star_step6['FilmSerial'].str.replace('01', 'فیلم')
vod_filimo_star_step6['FilmSerial'] = vod_filimo_star_step6['FilmSerial'].str.replace('02', 'سریال')
    
#### edit ####
vod_filimo_star_step1=vod_filimo_star_step1.rename(columns={"code_FilmSerial":"FilmSerial"})
vod_filimo_star_step1['FilmSerial'] = vod_filimo_star_step1['FilmSerial'].str.replace('01', 'فیلم')
vod_filimo_star_step1['FilmSerial'] = vod_filimo_star_step1['FilmSerial'].str.replace('02', 'سریال')

vod_filimo_star_step1.insert(14, 'TitleCleaned2', '')
del vod_filimo_star_step2['Visit_old']
del vod_filimo_star_step2['code_FilmSerial']
del vod_filimo_star_step4['code_FilmSerial']
del vod_filimo_star_step5['code_FilmSerial']
#del vod_filimo_star_step6['Visit_old']
del vod_filimo_star_step6['code_FilmSerial']
##############

vod_filimo_star_final = vod_filimo_star_step1.append([vod_filimo_star_step2, vod_filimo_star_step4, vod_filimo_star_step5, vod_filimo_star_step6])
#del vod_filimo_star_final['code_FilmSerial']
vod_filimo_star_final.drop_duplicates(subset =['ID'], keep = 'last', inplace = True)

vod_filimo_star_final_merge_visit = pd.merge(vod_filimo_star_final, vod_filimo_star_Visit, on = ['ID'])
vod_filimo_star_final_merge_visit['Visit'] = vod_filimo_star_final_merge_visit['Visit'] - vod_filimo_star_final_merge_visit['Visit_old']

for i in range(0, len(vod_filimo_star_final_merge_visit)):
    print(i)
    if vod_filimo_star_final_merge_visit.loc[i, 'Visit'] < 0:
        vod_filimo_star_final_merge_visit.loc[i, 'Visit'] = 0

del vod_filimo_star_final_merge_visit['Visit_old']

vod_filimo_star_final_append_merge = vod_filimo_star_final.append([vod_filimo_star_final_merge_visit])
vod_filimo_star_final_append_merge.drop_duplicates(subset =['ID'], keep = False, inplace = True)
vod_filimo_star_final_new = vod_filimo_star_final_merge_visit.append([vod_filimo_star_final_append_merge])

vod_filimo_star_final_new['ActiveUsers'] = 0
vod_filimo_star_final_new['DurationMin'] = 0
vod_filimo_star_final_new = vod_filimo_star_final_new.reset_index()
del vod_filimo_star_final_new['index']
vod_filimo_star_final = vod_filimo_star_final_new.copy()
vod_filimo_star_final['DateTime'] = vod_filimo_star_final['DateTime'].astype(str)
vod_filimo_star_final['Visit'] = vod_filimo_star_final['Visit'].astype(int)
vod_filimo_star_final['DurationMin'] = vod_filimo_star_final['DurationMin'].astype(float)
vod_filimo_star_final.replace('nan', '', inplace=True)
vod_filimo_star_final = vod_filimo_star_final.fillna('')
for i in range(0, len(vod_filimo_star_final)):
    print(i)
    if vod_filimo_star_final.loc[i, 'Visit'] < 0:
        vod_filimo_star_final.loc[i, 'Visit'] = 0
vod_filimo_star_final.dtypes
#vod_filimo_star_final.to_excel('Shahrivar.xlsx', index=False)
#Shahrivar = vod_filimo_star_final.copy()

###########################################################
drivers = pyodbc.drivers()
conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()
for index, row in vod_filimo_star_final.iterrows():
    print(index)
    cursor.execute("INSERT INTO VOD.dbo.DB1_VOD (ID,IDS,Title,TitleCleaned1,TitleCleaned2,Season,Epizode,Visit,ActiveUsers,DurationMin,Operators,Month,Year,FilmSerial,DateTime) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", row.ID, row.IDS, row.Title,row.TitleCleaned1,row.TitleCleaned2,row.Season,row.Epizode,row.Visit,row.ActiveUsers,row.DurationMin,row.Operators,row.Month,row.Year,row.FilmSerial,row.DateTime)
    conn.commit()
######## delete ###########################################################

del db1['ID']
del db1['IDS']
del db2['ID']
del db2['IDS']

db1['code_FilmSerial'] = db1['FilmSerial']
db1['code_FilmSerial'] = db1['code_FilmSerial'].str.replace('فیلم', '01')
db1['code_FilmSerial'] = db1['code_FilmSerial'].str.replace('سریال', '02')

db1 = db1.sort_values(['TitleCleaned1', 'Season', 'Epizode'], ascending=[True, True, True])
db1 = db1.reset_index()
del db1['index']
id_first = 1000001
db1.loc[0, 'id_first'] = id_first
for i in range(1, len(db1)):
    print(i)
    if db1.loc[i, 'TitleCleaned1'] == db1.loc[i-1, 'TitleCleaned1'] and \
       db1.loc[i, 'Season'] == db1.loc[i-1, 'Season'] and \
       db1.loc[i, 'FilmSerial'] == db1.loc[i-1, 'FilmSerial']:
           db1.loc[i, 'id_first'] = id_first
    else:
        id_first = id_first + 1
        db1.loc[i, 'id_first'] = id_first

db1['id_first'] = db1['id_first'].astype(int).astype(str)





db1['ID'] = db1['id_first']+db1['code_FilmSerial']+db1['Season']+db1['Epizode']
db1['IDS'] = db1['id_first']+db1['Season']


db1_new['ID'] = db1['ID']
db1_new['IDS'] = db1['IDS']
db1_new['TitleCleaned1'] = db1['TitleCleaned1']
db1_new['Season'] = db1['Season']
db1_new['FilmSerial'] = db1['FilmSerial']

db2_merge_db1_new = pd.merge(db2, db1_new, on = ['TitleCleaned1', 'Season', 'FilmSerial'])
db2_merge_db1_new.drop_duplicates(subset =['IDS'], keep = 'first', inplace = True)
db2_merge_db1_new = db2_merge_db1_new.reset_index()
del db2_merge_db1_new['index']
del db1['id_first']
del db1['code_FilmSerial']

####################################
drivers = pyodbc.drivers()
conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()
for index, row in db1.iterrows():
    print(index)
    cursor.execute("INSERT INTO VOD.dbo.DB1_VOD (ID,IDS,Title,TitleCleaned1,TitleCleaned2,Season,Epizode,Visit,ActiveUsers,DurationMin,Operators,Month,Year,FilmSerial,DateTime) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", row.ID, row.IDS, row.Title,row.TitleCleaned1,row.TitleCleaned2,row.Season,row.Epizode,row.Visit,row.ActiveUsers,row.DurationMin,row.Operators,row.Month,row.Year,row.FilmSerial,row.DateTime)
    conn.commit()

####################################
drivers = pyodbc.drivers()

conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=localhost;'
                      'Database=VOD;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

for index, row in db2_merge_db1_new.iterrows():
    print(index)
    cursor.execute("INSERT INTO VOD.dbo.DB2_VOD (ID,IDS,Title,TitleCleaned1,TitleCleaned2,Season,FilmSerial,Director,EnglishName,AgeRange,Casts,Genres,Imdb,Language,ReleaseDateGeorgian,ReleaseDateJalali,Runtime,Country,Producer,Writer,Composer,Editor,DubbedSubtitle,Singer,Cameraman) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", row.ID,row.IDS,row.Title,row.TitleCleaned1,row.TitleCleaned2,row.Season,row.FilmSerial,row.Director,row.EnglishName,row.AgeRange,row.Casts,row.Genres,row.Imdb,row.Language,row.ReleaseDateGeorgian,row.ReleaseDateJalali,row.Runtime,row.Country,row.Producer,row.Writer,row.Composer,row.Editor,row.DubbedSubtitle,row.Singer,row.Cameraman)
    conn.commit()




