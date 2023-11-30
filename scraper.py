import re
import requests
from bs4 import BeautifulSoup
from collections import namedtuple
from datetime import datetime, timedelta, timezone
import pandas as pd
import json
import arxivscraper.arxivscraper as ax

#function to scrape from urls, here we scrape from two websites that deal with cosmochemistry papers
def scrape (url='https://karmaka.de/?feed=rss2', n_days=7):
    article = namedtuple('article', 'title link desc published')

    soup = BeautifulSoup(requests.get(url).content, 'xml')

    articles = []
    for title, link, desc, pubdate in zip(soup.select('item > title'),
                                   soup.select('item > link'),
                                   soup.select('item > description'),
                                   soup.select('item > pubDate')):
        d = datetime.strptime(pubdate.get_text(strip=True), '%a, %d %b %Y %H:%M:%S %z')
        articles.append(article(title.get_text(strip=True), link.get_text(strip=True), desc.get_text(strip=True), d))

    now = datetime.now(timezone.utc)
    n_days_ago = timedelta(days=n_days)
    latest = []
    for a in articles:
        if now - a.published > n_days_ago:
            continue
        latest.append(a)
        #print(a.title)
        #print(a.link)
        #print(a.desc)
        #print(a.published)
        #print('-' * 80)
    df = pd.DataFrame(latest)
    return df

#scrape from arxiv:astro-ph:EP using arxivscraper
def arxivscrape(n_days=7):
    today = datetime.today().strftime('%Y-%m-%d')
    d = datetime.today() - timedelta(days=7)
    delaydate = d.strftime('%Y-%m-%d')
    scraper = ax.Scraper(category='physics:astro-ph', date_from=delaydate,date_until=today,t=10, filters={'categories':['astro-ph.EP']})
    output = scraper.scrape()
    cols = ('id', 'title', 'categories', 'abstract', 'doi', 'created', 'updated', 'authors')
    df = pd.DataFrame(output,columns=cols)
    d = []
    link = []
    for i in range(len(df)):
        d.append(datetime.strptime(df['created'][i],'%Y-%m-%d'))
        link.append('http://arxiv.org/abs/'+ df['id'][i])
    df.insert(0,'pubdate',d, True)
    df.insert(1,'link',link,True)
    pubdate = datetime.today() - timedelta(days=7)
    result = df[df['pubdate'] > pubdate]
    result = result.reset_index(drop=True)
    dfnew = result.drop(columns=['id','categories','abstract','doi','created','updated'])
    dfnew = dfnew.rename(columns={'authors': 'desc'})
    dfnew = dfnew.rename(columns={'pubdate': 'published'})
    cols = ['title','link','desc','published']
    dfnew = dfnew[cols]
    print(dfnew)
    return dfnew

#merge arxiv and the site you want to list from
def arxiv_merge(df_arxiv,df_cosmo):
    #search for arxiv copies and delete
    for i in range(len(df_arxiv)):
        temp1 = str(df_arxiv['title'][i])
        for j in range(len(df3)):
            temp2 = str(df_cosmo['title'][j])
            if ( temp1.lower() == temp2.lower()):
                df_cosmo.drop(j)
    dff = pd.concat([df_cosmo,df_arxiv])
    dff = dff.reset_index(drop=True)
    return dff

#save to json file for javascript to parse
def to_json(df, filename='jc.json'):
    titles = []
    for i in range(len(df)):
        titles.append(str(df['title'][i]))
    dict_jc = {titles[0]:0}
    for t in titles:
        dict_jc[t] = 0
    # Convert and write JSON object to file
    with open(filename, "w") as outfile: 
        json.dump(dict_jc, outfile)
#generate html table for your dataframe


def to_html(df,links=True,filename='table_jc.html'):
    
    def make_clickable(url, name):
        return '<a href="{}"target="_blank">{}</a>'.format(url,name)
    df2 = df.copy()
    for i in range(len(df2)):
        df2['title'][i] = make_clickable(df2['link'][i],df2['title'][i])
    df2 = df2.drop('link',axis=1)
    df2.to_html(filename,escape=False)
    
url = 'https://karmaka.de/?feed=rss2'
df1 = scrape(url,n_days=7)

url = 'https://cosmochemistry-papers.com/feed/'
df2 = scrape(url,n_days=7)

for i in range(len(df1)) :
    if re.search('OPEN ACCESS', df1['title'][i]):
        df1['title'][i] = df1['title'][i].replace('OPEN ACCESS', '')
#merge the two datasets and drop duplicates        
df3 = pd.concat([df1,df2])
df3 = df3.drop_duplicates(subset='title')
df3 = df3.reset_index(drop=True)
#scrape from arxiv
df_arxiv = arxivscrape(n_days=7)

df_final = arxiv_merge(df_arxiv,df3)
print(df_final)
to_json(df_final,filename='server/jc.json')
to_html(df_final,filename='client/table_jc.html')


            
