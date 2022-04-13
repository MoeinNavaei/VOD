

def EditDB2(edit_db2_in):
    edit_db2_in = edit_db2_in[~edit_db2_in.Title.str.contains("کنسرت")] 
    edit_db2_in = edit_db2_in[~edit_db2_in.Title.str.contains("تیزر")] 
    edit_db2_in = edit_db2_in[~edit_db2_in.Title.str.contains("کلیپ")] 
    edit_db2_in = edit_db2_in[~edit_db2_in.Genres.str.contains("کلیپ")] 
    edit_db2_in = edit_db2_in[~edit_db2_in.Genres.str.contains("کنسرت")] 
    
    edit_db2_in['Genres'] = edit_db2_in['Genres'].str.replace('زندگی‌نامه', 'زندگینامه')
    edit_db2_in['Genres'] = edit_db2_in['Genres'].str.replace('زندگی نامه', 'زندگینامه')
    edit_db2_in['Genres'] = edit_db2_in['Genres'].str.replace('بیوگرافی', 'زندگینامه')
    edit_db2_in['Genres'] = edit_db2_in['Genres'].str.replace('علمی تخیلی', 'علمی-تخیلی')
    edit_db2_in['Genres'] = edit_db2_in['Genres'].str.replace('کمدی', 'طنز')
    edit_db2_in['Genres'] = edit_db2_in['Genres'].str.replace('کودکان', 'کودک')
    edit_db2_in['Genres'] = edit_db2_in['Genres'].str.replace('ماجرایی', 'ماجراجویی')
    edit_db2_in['Genres'] = edit_db2_in['Genres'].str.replace('فیلم های مستند', 'مستند')
    edit_db2_in['Genres'] = edit_db2_in['Genres'].str.replace('موزیکال', 'موزیک')
    edit_db2_in['Genres'] = edit_db2_in['Genres'].str.replace('موزیک', 'موزیکال')
    edit_db2_in['Genres'] = edit_db2_in['Genres'].str.replace('موسیقی', 'موزیکال')
    edit_db2_in['Genres'] = edit_db2_in['Genres'].str.replace('هیجان‌انگیز', 'هیجان انگیز')
    edit_db2_in['Genres'] = edit_db2_in['Genres'].str.replace('ترسناک', 'وحشت')
    edit_db2_in['Genres'] = edit_db2_in['Genres'].str.replace('بازی و سرگرمی', 'سرگرمی')
    edit_db2_in['Genres'] = edit_db2_in['Genres'].str.replace('سیاسی تاریخی', 'سیاسی، تاریخی')
    edit_db2_in['Genres'] = edit_db2_in['Genres'].str.replace('هاردتاک', 'گفتگو')
    edit_db2_in['Genres'] = edit_db2_in['Genres'].str.replace('تاک شو', 'گفتگو')
    edit_db2_in['Genres'] = edit_db2_in['Genres'].str.replace('پلیسی معمایی', 'پلیسی، معمایی')
    edit_db2_in['Genres'] = edit_db2_in['Genres'].str.replace('خیالی', 'تخیلی')
    edit_db2_in['Genres'] = edit_db2_in['Genres'].str.replace('رمانتیک', 'عاشقانه')
    edit_db2_in['Genres'] = edit_db2_in['Genres'].str.replace('عمومی', 'اجتماعی')
    edit_db2_in['Genres'] = edit_db2_in['Genres'].str.replace('فیلم', '')
    edit_db2_in['Genres'] = edit_db2_in['Genres'].str.replace('ایرانی', '')
    edit_db2_in['Genres'] = edit_db2_in['Genres'].str.replace('محیط زیست', 'مستند')
    edit_db2_in['Genres'] = edit_db2_in['Genres'].str.replace('گانگستری', 'وسترن')
    edit_db2_in['Genres'] = edit_db2_in['Genres'].str.replace('برشی از زندگی', 'زندگینامه')
    edit_db2_in['Genres'] = edit_db2_in['Genres'].str.replace('علمی-پژوهشی', 'ELPA')
    edit_db2_in['Genres'] = edit_db2_in['Genres'].str.replace('علمی-تخیلی', 'ET')
    edit_db2_in['Genres'] = edit_db2_in['Genres'].str.replace('علمی', 'elmi')
    edit_db2_in['Genres'] = edit_db2_in['Genres'].str.replace('پژوهشی', 'pajoheshi')
    edit_db2_in['Genres'] = edit_db2_in['Genres'].str.replace('elmi', 'علمی-پژوهشی')
    edit_db2_in['Genres'] = edit_db2_in['Genres'].str.replace('pajoheshi', 'علمی-پژوهشی')
    edit_db2_in['Genres'] = edit_db2_in['Genres'].str.replace('ELPA', 'علمی-پژوهشی')
    edit_db2_in['Genres'] = edit_db2_in['Genres'].str.replace('پزشکی', 'علمی-پژوهشی')
    edit_db2_in['Genres'] = edit_db2_in['Genres'].str.replace('ET', 'علمی-تخیلی')
    edit_db2_in['Genres'] = edit_db2_in['Genres'].str.replace('فرهنگی', 'مستند')
    edit_db2_in['Genres'] = edit_db2_in['Genres'].str.replace('گردشگری', 'مستند')
    edit_db2_in['Genres'] = edit_db2_in['Genres'].str.replace('دوبله ترکی', '')
    
    edit_db2_in['Language'] = edit_db2_in['Language'].str.replace('(flemish)', '')
    edit_db2_in['Language'] = edit_db2_in['Language'].str.replace('(castilian)', '')
    edit_db2_in['Language'] = edit_db2_in['Language'].str.replace(')', '')
    edit_db2_in['Language'] = edit_db2_in['Language'].str.replace('(', '')
    edit_db2_in['Language'] = edit_db2_in['Language'].str.replace('Spanish', 'اسپانیایی')
    edit_db2_in['Language'] = edit_db2_in['Language'].str.replace('Dutch', 'هلندی')
    edit_db2_in['Language'] = edit_db2_in['Language'].str.replace('غیر فارسی', '')
    edit_db2_in['Language'] = edit_db2_in['Language'].str.replace('کره ای', 'کره‌ای')
    edit_db2_in['Language'] = edit_db2_in['Language'].str.replace('نورسک جدید نروژی', 'نروژی')
    
    edit_db2_in['Country'] = edit_db2_in['Country'].str.replace('آفریقای جنوبی', 'آفریقا جنوبی')
    edit_db2_in['Country'] = edit_db2_in['Country'].str.replace('دیگر کشورها', '')
    edit_db2_in['Country'] = edit_db2_in['Country'].str.replace('سوییس', 'سوئیس')
    edit_db2_in['Country'] = edit_db2_in['Country'].str.replace('لوگزامبورگ', 'لوکزامبورگ')
    edit_db2_in['Country'] = edit_db2_in['Country'].str.replace('نیوزیلند', 'نیوزلند')
    edit_db2_in['Country'] = edit_db2_in['Country'].str.replace('هنگ کنگ', 'هنک کنگ')
    edit_db2_in['Country'] = edit_db2_in['Country'].str.replace('هندوستان', 'هند')
    edit_db2_in['Country'] = edit_db2_in['Country'].str.replace('YU', '')
    
    edit_db2_out = edit_db2_in.copy()
    edit_db2_out = edit_db2_out.reset_index()
    del edit_db2_out['index']

    return edit_db2_out
