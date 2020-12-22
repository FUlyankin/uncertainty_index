import requests
from bs4 import BeautifulSoup

import time
import pickle

from joblib import Parallel, delayed

def Separator(vect, part):
    n = len(vect)
    vec_parts = [round(n/part)*i for i in range(part)]
    vec_parts.append(n)
    out = [vect[vec_parts[i]:vec_parts[i+1]] for i in range(part)]
    return(out)

def get_soup(url):
    response = requests.get(url)
    html = response.content
    soup = BeautifulSoup(html,'html.parser')
    return soup

def get_ria_news(item_from_vect):
    url = item_from_vect['href']
    try:
        soup = get_soup(url)
        text = soup.find_all('div', {'class' : 'article__text'})

        item_from_vect['year'] = item_from_vect['date'].split('-')[0]
        item_from_vect['month'] = item_from_vect['date'].split('-')[1]
        item_from_vect['day'] = item_from_vect['date'].split('-')[2]

        item_from_vect['snippet'] = soup.find_all('meta',  {'name' : 'description'})[0].get('content')
        item_from_vect['text'] = ' '.join([tx.text for tx in text])

        # категория
        item_from_vect['category'] = soup.find_all('meta', {'name' : 'analytics:rubric'})[0].get('content')

        # полезная метаинформация 
        item_from_vect['keywords'] = soup.find_all('meta', {'name' : 'keywords'})[0].get('content')
        item_from_vect['tags'] = soup.find_all('meta',  {'name' : 'analytics:tags'})[0].get('content')

        # число показов страницы
        item_from_vect['shows'] = soup.find_all('span', {"class" : "statistic__item m-views"})[0].text

        # хз пригодится ли, но ссылка на титульную картинку 
        item_from_vect['image'] = soup.find_all('meta',  {'property' : 'og:image'})[0].get('content')
        return item_from_vect
    
    except:
        
        print(url)
        return item_from_vect

with open('ria_titles_2020.pickle', 'rb') as f:
    x_2019 = pickle.load(f)
    

i = 0

x_batch = Separator(x_2019[40000:], 200)
print('Надо скачать', len(x_batch), 'батчей')
print('Надо скачать', 200*len(x_batch[0]), 'статей')

result = [ ]
for batch in x_batch:
    
    n_jobs = 2 # параллелим на все ядра
    result_cur = Parallel(n_jobs=n_jobs)(delayed(get_ria_news)(
        text) for text in batch)

    print('Скачал батч номер ' + str(i))
    i += 1
    result.extend(result_cur)

    clean = [itog for itog in result if len(itog.keys()) == 14]
    print('Сейчас у нас есть ' + str(len(clean)) +' новостей')

    bad = [itog for itog in result if len(itog.keys()) < 14]
    print('Сделано ' + str(len(bad)) +' ошибок')


clean = [itog for itog in result if len(itog.keys()) == 14]
print('Сейчас у нас есть ' + str(len(clean)) +' новостей')

bad = [itog for itog in result if len(itog.keys()) < 14]
print('Сделано ' + str(len(bad)) +' ошибок')


with open('ria_news_2020_part2.pickle', 'wb') as f:
    pickle.dump(result, f)
