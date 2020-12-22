from deeppavlov import build_model, configs

from tqdm import tqdm
import pickle

# different model builds:
# http://docs.deeppavlov.ai/en/master/components/classifiers.html

# path to the rusentiment model:   http://text-machine.cs.uml.edu/projects/rusentiment/
CONFIG_PATH = configs.classifiers.rusentiment_cnn

# build model
model_rusentiment = build_model(CONFIG_PATH) #, download=True)

# path to the Twitter model:  http://study.mokoron.com
CONFIG_PATH = configs.classifiers.sentiment_twitter

# build model
model_mokron = build_model(CONFIG_PATH)#, download=True)

# example
sample = ['What is the weather in Boston today?',
          'Люблю маму и папу!!!!а в остальное я так...-влюбляюсь, привязываюсь)))',
          '@ivanenko14 и у меня также, только будильник еще и не выключался.. папу разбудила (']

print(model_mokron(sample))
print(model_rusentiment(sample))

path = '../news_parser/news_data/vk_news/'

file = 'interfax_vk_comments'

# текущие данные  
with open(path + file, 'rb') as f:
    comments = pickle.load(f)

print('Public {} has {} comments'.format(file, len(comments)))

comments_no_predict = [item for item in comments if 'rusentiment_model' not in item]
comments_yes_predict = [item for item in comments if 'rusentiment_model' in item]
print(f"We haven't prediction: {len(comments_no_predict)}, we have prediction: {len(comments_yes_predict)}")

sentiment_comments = [ ]

for i,item in enumerate(comments_no_predict):
    if i % 1000 == 0:
        print(i)
    try:
        item['twitter_mokron_model'] = model_mokron([item['text']])[0]
        item['rusentiment_model'] = model_rusentiment([item['text']])[0]
    except:
        print(item['text'])  
        item['twitter_mokron_model'] = None
        item['rusentiment_model']  = None

    sentiment_comments.append(item)      

# save it
with open('cur_pred', 'wb') as f:
    pickle.dump(sentiment_comments, f)  

comments_yes_predict.extend(sentiment_comments)

print('saving...')

# save it to the same file
with open(path + file +'_sen', 'wb') as f:
    pickle.dump(comments_yes_predict, f)     

print('done')       
