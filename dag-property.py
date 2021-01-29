import requests
import io
from lxml import html
import sqlite3
import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
import os

class PropertiesScrape:
    def __init__(self, min_price, max_price, county, num_beds):
        self.min_price = str(min_price)
        self.max_price = str(max_price)
        self.county = str(county)
        self.num_beds = str(num_beds)
        self.user_agent = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.101 Safari/537.36'
        self.headers = { 'User-Agent' : self.user_agent }
        self.fOut = io.open('/mnt/c/users/user/desktop/python practice/airflow/properties_scrape.csv', 'a+', encoding = 'utf-8')

    def get_content(self, page_number = 1):
        self.web = 'https://www.property.ie/property-for-sale/{}/price_{}-{}/beds_{}/p_{}'.format(  self.county,
                                                                                                    self.min_price,
                                                                                                    self.max_price,
                                                                                                    self.num_beds,
                                                                                                    page_number)
        self.response = requests.get(self.web, headers = self.headers)
        self.content = html.fromstring(self.response.content)
        return self.content

    def string_normalization(self, array):
        self.normalized = ''
        for value in array:
            self.normalized = self.normalized + '|' + value.strip()
        return self.normalized.lstrip('|') + '\n'

    def scrape_data(self):
        self.content = self.get_content()
        self.pages = self.content.xpath('//*[@id="pages"]/a//text()')
        self.pages = [i for i in self.pages if not i.startswith('Next')]
        self.max_page = max(self.pages)

        for i in range(1, int(self.max_page)):
            self.content = self.get_content(page_number = i)

            self.number_rows = len(self.content.xpath('//*[@id="searchresults_container"]/div'))
            for j in range(3, self.number_rows):

                try: self.address = str(self.content.xpath('//*[@id="searchresults_container"]/div[{}]/div[2]/h2/a//text()'.format(str(j)))[0]).replace('\n', '').strip()
                except: self.address = ''

                try: self.price = self.content.xpath('//*[@id="searchresults_container"]/div[{}]/div[3]/h3/text()'.format(str(j)))[0]
                except: self.price = ''

                try: self.website = str(self.content.xpath('//*[@id="searchresults_container"]/div[{}]/div[2]/h2/a//@href'.format(str(j)))[0]).replace('\n', '').strip()
                except: self.website = ''

                self.line = self.string_normalization([self.address, self.price, self.website])
                self.fOut.write(self.line)
        self.fOut.close()

class MyHomeScrape:
    def __init__(self, min_price, max_price, county, num_beds):
        self.min_price = str(min_price)
        self.max_price = str(max_price)
        self.county = str(county)
        self.num_beds = str(num_beds)
        self.user_agent = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.101 Safari/537.36'
        self.headers = { 'User-Agent' : self.user_agent }
        self.fOut = io.open('/mnt/c/users/user/desktop/python practice/airflow/my_home_scrape.csv', 'a+', encoding = 'utf-8')

    def get_content(self, page_number = 1):
        self.web = 'https://www.myhome.ie/residential/{}/property-for-sale?minprice={}&maxprice={}&maxbeds={}&page={}'.format(  self.county,
                                                                                                    self.min_price,
                                                                                                    self.max_price,
                                                                                                    self.num_beds,
                                                                                                    page_number)
        self.response = requests.get(self.web, headers = self.headers)
        self.content = html.fromstring(self.response.content)
        return self.content

    def string_normalization(self, array):
        self.normalized = ''
        for value in array:
            self.normalized = self.normalized + '|' + value.strip()
        return self.normalized.lstrip('|') + '\n'

    def scrape_data(self):
        self.content = self.get_content()
        self.pages = self.content.xpath('//*[@id="content"]/app-residential-search-results/div/div/div[5]/div[2]/mh-pagination/app-desktop-pagination/pagination-controls/pagination-template/ul//text()')
        self.pages_available = []
        for integer in self.pages:
            try: self.pages_available.append(int(integer))
            except: pass
        self.max_pages = max(self.pages_available)

        for i in range(1, self.max_pages):
            self.content = self.get_content(page_number = i)

            self.addresses = self.content.xpath('//*[substring(@id, string-length(@id) - string-length("desktop") +1) = "desktop"]/app-mh-property-listing-card/div/div[2]/div[1]/a//text()')
            self.prices = self.content.xpath('//*[substring(@id, string-length(@id) - string-length("desktop") +1) = "desktop"]/app-mh-property-listing-card/div/div[2]/div[1]/div//text()')
            self.webs = self.content.xpath('//*[substring(@id, string-length(@id) - string-length("desktop") +1) = "desktop"]/app-mh-property-listing-card/div/div[2]/div[1]/a//@href')

            if len(self.addresses) == len(self.prices) and len(self.addresses) == len(self.webs):
                for i in range(len(self.addresses)):
                    self.address, self.price, self.web = self.addresses[i], self.prices[i], self.webs[i]
                    self.line = self.string_normalization([self.address, self.price, self.web])
                    self.fOut.write(self.line)

        self.fOut.close()

class DatabaseManagement:

    def __init__(self):
        self.connection = sqlite3.connect("/mnt/c/users/user/desktop/python practice/airflow/properties.db")
        self.cursor = self.connection.cursor()
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS property (
									address VARCHAR(300),
									price VARCHAR(300),
									web VARCHAR(300));
							""")
        self.connection.commit()
        self.fInput = io.open('/mnt/c/users/user/desktop/python practice/airflow/merger-completed.csv', 'r', encoding = 'utf-8').readlines()

    def database_ingestions(self):
        for line in self.fInput:
            try:
                self.address, self.price, self.web = line.split('|')
                self.query = """ INSERT INTO property VALUES ("{}", "{}", "{}")""".format(self.address, self.price, self.web)
                self.cursor.execute(self.query)
                self.connection.commit()
            except: pass

default_arguments = {
                        'owner' : 'morozovd',
                        'start_date' : days_ago(1),
                        'retries' : 1,
                        'depends_on_past' : False
                    }
dag = DAG(dag_id = 'DAG-PROPERTY-ETL', default_args = default_arguments, schedule_interval = None, catchup = False)
start = DummyOperator(task_id = 'START', dag = dag)
property_one = PythonOperator(task_id = 'WEBSITE-1-SCRAPE-DATA',
                            python_callable = PropertiesScrape(25000, 275000, 'dublin', 3).scrape_data,
                            dag = dag)
property_two = PythonOperator(task_id = 'WEBSITE-2-SCRAPE-DATA',
                            python_callable = MyHomeScrape(25000, 275000, 'dublin', 3).scrape_data,
                            dag = dag)
merging_files = BashOperator(task_id = 'TRANSFORMING-DATA',
                            bash_command = "cat '/mnt/c/users/user/desktop/python practice/airflow/properties_scrape.csv' '/mnt/c/users/user/desktop/python practice/airflow/my_home_scrape.csv' > '/mnt/c/users/user/desktop/python practice/airflow/merger-completed.csv'",
                            dag = dag)
database_ingestions = PythonOperator(task_id = 'INGESTING-DATA-TO-DATABASE',
                            python_callable = DatabaseManagement().database_ingestions,
                            dag = dag)
end = DummyOperator(task_id = 'END', dag = dag)

start >> property_one >> property_two >> merging_files >> database_ingestions >> end
