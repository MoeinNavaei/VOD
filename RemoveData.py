

def RemoveData(remove_data_in):
    
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('دعای')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('ریاضی')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('گزینه دو')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('vod')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('فارسی')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('انگلیسی')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('عربی')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('علوم')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('روضه')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('مداح')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('-مداح')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('متوسطه')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('ابتدایی')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('خلاصه')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('گل های بازی')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('برترینهای')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('برد تاریخی')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('برد سخت')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('شکست حریف')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('موتور اسپرت')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('برد مقابل')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('فوتبال ساحلی')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('فینالیست شدن')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('مرور بازیهای')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('مسابقات')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('شکست برابر')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('کیلوگرم مقابل')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('گل های')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('گلهای')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('ایرانسل')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('جام جهانی')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('شکست حمیده')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('ضربات آزاد')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('مستند پرسپولیس')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('پیروزی قاطع')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('پیروزی نفسگیر')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('بارسلونا')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('کولاک بانوان')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('نیمار')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('علی پناه')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('رضا هلالی')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('قاری بین المللی')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('حاج امیر')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('کاکایی')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('احمد ابوالقاسمی')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('کربلایی')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('رونالدو')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('دربی')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('کارت قرمز')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('کار با توپ')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('یک اتفاق تازه')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('شکست نجاتی')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('نماهنگ')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('test')]
    
    
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('کاروان ایران')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('زیست شناسی')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('مدال')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('مطالعات اجتماعی')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('لالیگا')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('برترین لحظات')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('لیگ جزیره')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('تکنیک های')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('رالی')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('صد گل')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('لیگ قهرمانان')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('قرعه کشی')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('مجله آشپزی')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('مقابل حریف')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('کشتی آزاد')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('المپیک')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('آنفیلد')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('لیونل مسی')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('کسب طلای')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('شکست مرتضی')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('شکست میرزازاده')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('صد سیو')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('نماینده تکواندو')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('مجله آشپزی')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('گل برتر')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('بسکتبال')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('مهدی حسن آبادی')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('حجت الاسلام')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('حاج جواد')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('سید جواد')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('حاج مهدی')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('حاج احمد')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('لیگ برتر')]
    remove_data_in = remove_data_in [~remove_data_in. Title.str.contains('اشتباهات داوری')]
    
    remove_data_in = remove_data_in [~remove_data_in.Title.str.contains('بوندسلیگا')]
    remove_data_in = remove_data_in [~remove_data_in.Title.str.contains('بازی های اروپایی')]
    remove_data_in = remove_data_in [~remove_data_in.Title.str.contains('لیگ جزیره')]
    remove_data_in = remove_data_in [~remove_data_in.Title.str.contains('یورو 2020')]
    remove_data_in = remove_data_in [~remove_data_in.Title.str.contains('لالیگا')]
    remove_data_in = remove_data_in [~remove_data_in.Title.str.contains('لیگ اسپانیا')]
    remove_data_in = remove_data_in [~remove_data_in.Title.str.contains('لیگ آلمان')]
    remove_data_in = remove_data_in [~remove_data_in.Title.str.contains('لیگ ایتالیا')]
    remove_data_in = remove_data_in [~remove_data_in.Title.str.contains('سری آ')]
    
    remove_data_out = remove_data_in.copy()
    
    
    return remove_data_out























