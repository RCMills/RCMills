
from airflow import DAG
from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from urllib.error import URLError


import urllib.request
import time
import glob, os
import json

baseURL = 'http://student.mit.edu/catalog/'

urls = ['m1a.html',
        'm1b.html',
        'm1c.html',
        'm2a.html',
        'm2b.html',
        'm2c.html',
        'm3a.html',
        'm3b.html',
        'm4a.html',
        'm4b.html',
        'm4c.html',
        'm4d.html',
        'm4e.html',
        'm4f.html',
        'm4g.html',
        'm5a.html',
        'm5b.html',
        'm6a.html',
        'm6b.html',
        'm6c.html',
        'm7a.html',
        'm8a.html',
        'm8b.html',
        'm9a.html',
        'm9b.html',
        'm10a.html',
        'm10b.html',
        'm10c.html',
        'm11a.html',
        'm11b.html',
        'm11c.html',
        'm12a.html',
        'm12b.html',
        'm12c.html',
        'm14a.html',
        'm14b.html',
        'm15a.html',
        'm15b.html',
        'm15c.html',
        'm16a.html',
        'm16b.html',
        'm18a.html',
        'm18b.html',
        'm20a.html',
        'm22a.html',
        'm22b.html',
        'm22c.html']

#pull course catalog pages
def catalog():

    #define pull(url) helper function
    def pull(url):
        try:
            response = urllib.request.urlopen(url).read()
            data = response.decode('utf-8')
            return data
        except URLError as e:
            #code advise from https://www.programcreek.com/python/example/53027/urllib.error.URLError
            print(e.reason)

    #define store(data, file) helper function
    def store(data, file):
        try:
            #code advise from https://www.tutorialspoint.com/python/os_write.htm
            fil = os.open(file, os.O_RDWR|os.O_CREAT)
            val = os.write(fil, data)
            os.close()
            print(f'wrote: {val} bytes to file: {file}')
        except OSError as e:
            print(f'could not open/write {file} for reason  {e.strerror}')

    for file in urls:
        url = baseURL + file
        data = pull(url)
        store(data, file)
        print(f'pulled ' + file)
        print('---- sleeping ----')
        time.sleep(15)
        
def combine():
    #this step is run after the calls to the catalog have produced a folder full of .html files
    try:
        with open('combo.txt', 'xt') as outfile:
            for file in glob.glob('*.html'):
                with open(file) as infile:
                    outfile.write(infile.read())
    except OSError as e:
        print(f'An error has occured writing the combo text: {e.strerror}')

def titles():
    from bs4 import BeautifulSoup

    def store_json(data, file):
        try:
            with open(file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
                print(f'wrote file {file}')
        except OSError as e:
            print(f'an error occurred storing json {e.strerror}')

    #the combined file was called 'combo.txt'
    try:
        html = open('combo.txt').read()
    except OSError as e:
        print(f'tried to read combo.txt and failed')

    html = html.replace('\n', '').replace('\r', '')

    soup = BeautifulSoup(html, 'html.parser')
    results = soup.find_all('h3')
    titles = []

    for item in results:
        titles.append(item.text)
    
    store_json(titles, 'titles.json')

def clean():
   #complete helper function definition below
   def store_json(data,file):
        try:
            with open(file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)
                    print(f'wrote file {file}')
        except OSError as e:
            print(f'an error occurred storing json {e.strerror}')
      
   with open(titles.json) as file:
       titles = json.load(file)
       # remove punctuation/numbers
       for index, title in enumerate(titles):
           punctuation= '''!()-[]{};:'"\,<>./?@#$%^&*_~1234567890'''
           translationTable= str.maketrans("","",punctuation)
           clean = title.translate(translationTable)
           titles[index] = clean

       # remove one character words
       for index, title in enumerate(titles):
           clean = ' '.join( [word for word in title.split() if len(word)>1] )
           titles[index] = clean

       store_json(titles, 'titles_clean.json')

def count_words():
     from collections import Counter
     def store_json(data,file):
        try:
            with open(file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)
                    print(f'wrote file {file}')
        except OSError as e:
            print(f'an error occurred storing json {e.strerror}')
     
     with open('titles_clean.json') as file:
            titles = json.load(file)
            words = []

            # extract words and flatten
            for title in titles:
                words.extend(title.split())

            # count word frequency
            counts = Counter(words)

            store_json(counts, 'words.json')

with DAG(
   "assignment",
   start_date=days_ago(1),
   schedule_interval="@daily",catchup=False,
) as dag:

# INSTALL BS4 BY HAND THEN CALL FUNCTION

   # ts are tasks
   t0 = BashOperator(
       task_id='task_zero',
       bash_command='pip install beautifulsoup4',
       retries=2
   )

   t1 = PythonOperator(
       task_id='task_one',
       depends_on_past=False,
       python_callable=catalog
   )
   
   t2 = PythonOperator(
       task_id='task_two',
       depends_on_past=True,
       python_callable=combine
   )

   t3 = PythonOperator(
       task_id='task_three',
       depends_on_past=True,
       python_callable=titles
   )

   t4 = PythonOperator(
       task_id='task_four',
       depends_on_past=True,
       python_callable=clean
   )

   t5 = PythonOperator(
       task_id='task_five',
       depends_on_past=True,
       python_callable=count_words
   )


   t0>>t1>>t2>>t3>>t4>>t5