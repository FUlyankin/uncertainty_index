import requests
import re
from bs4 import BeautifulSoup

import time
import pickle

from multiprocessing.pool import ThreadPool

# Универсальный раздрабливатель на батчи 
def Separator(vect, part):
    n = len(vect)
    vec_parts = [round(n/part)*i for i in range(part)]
    vec_parts.append(n)
    out = [vect[vec_parts[i]:vec_parts[i+1]] for i in range(part)]
    return(out)

# Универсальный Map для скачки массива из статей. Функция по скачке и словарь - аргументы 
def Map(vect, parser_function):
    out = [parser_function(item) for item in vect]
    return(out)

# Универсальный Reduce по объединению статей 
def Reduce(l):
    ll = [ ]
    for item in l:
        ll.extend(item)
    return(ll)

# Универсальный скачиватель 
def MRDownloader(what, parts, parser_function):
    # Map - шаг 
    separatorlist = Separator(what, parts)
    def Mp(what):
        return Map(what, parser_function=parser_function)
    # Скачиваем 
    pool = ThreadPool(parts)
    l = pool.map(Mp, separatorlist)
    # Reduce - шаг
    itog = Reduce(l)
    return(itog)

# Функция, которая забирает текст по конкретной статье и добавляет её в итоговый массив
def page_content(url):
    response = requests.get(url)
    html = response.content
    soup = BeautifulSoup(html,"lxml")

    vvv = soup.findAll("div", { "class" : "news" })[0]
    snippet = vvv.find_all("div", {"class": "news-header__lead"})[0].text
    text = vvv.find_all("div", {"class": "text-content"})[0].text
    return text, snippet

# Конечная функция для сбора статьи
def get_tass_news(item_from_vect):
    url = "http://tass.ru" + item_from_vect['href']
    try:
        text, snippet = page_content(url)
        item_from_vect['text'] = text.strip()
        item_from_vect['snippet'] = snippet.strip()
        return item_from_vect
    except:
        print(url)
        return{ }

with open('tass_titles_2018.pickle', 'rb') as f:
    tass_titles = pickle.load(f)
    
print("Мои данные", len(tass_titles))

itog_news = MRDownloader(tass_titles[:20000], 10, parser_function=get_tass_news)
print("Я скачал", len(itog_news))

itog_news = [itog for itog in itog_news if len(itog.keys()) != 0 ]
print("Не пустых", len(itog_news))

with open('tass_news_2018_part1.pickle', 'wb') as f:
    pickle.dump(itog_news, f)

print("Сохранил данные")
