from datetime import date , datetime, timedelta 




# def d1():
#     data = date.today()
#     if data.isoweekday()==1 :
#         return data-timedelta(days=3)
#     else:
#         return data-timedelta(days=1)




def d1():
    data = date(2021,8,31)
    return data

def day_path():
    day = d1()
    string_day = f"{day.year}/{day.strftime('%m')}/{day.strftime('%d%m%Y/')}"
    return string_day

def day_escriturador():
    day = d1()
    return day


def day_str():
    day = d1().strftime("%d/%m/%Y")
    return day
    
