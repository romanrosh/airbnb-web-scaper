from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import re
import os
import click
import mysql.connector
import logging
import pandas as pd
from datetime import timedelta
import time
import random
from multiprocessing import Pool, cpu_count


class Airbnb:
    def __init__(self):
        self.REGEX_1 = r'\d+'
        self.REGEX_2 = r'rooms\/(\d+)?'
        self.REGEX_HOST = r'^.*[\\\/]([\d]+)'
        self.REGEX_DATE = r'([\d+])'
        self.REGEX_3 = r'[^0-9]'
        self.AIRBNB = 'https://airbnb.com'

        self.USER = 'scrape'
        self.PASSWORD = 'scrape'
        self.DB = 'airbnb'
        self.logger = logging.getLogger('airbnb_logger')
        self.DATE_1 = '2019-01-01'
        self.DATE_2 = '2019-08-01'
        self.DATES = list(map(lambda x: (str(x.date()), str(x.date() + timedelta(days=3))),
                              pd.date_range(self.DATE_1, self.DATE_2).tolist()))[::10]
        self.TRIES = 8
        self.CREATE_DB = 'Create database if not exists airbnb character set utf8 collate utf8_bin;'

        self.CREATE_TABLE_HOST = """CREATE TABLE IF NOT EXISTS hosts(
        host_id INT,reviews INT ,join_date INT,host_url TEXT,PRIMARY KEY(host_id));"""

        self.CREATE_TABLE_LISTING = """CREATE TABLE IF NOT EXISTS listings
        (listing_id INT, max_guests INT,bedrooms INT,beds INT,bathrooms VARCHAR(12),reviews INT,rating_value FLOAT,
        listing_type TEXT,city TEXT,url TEXT,host_id INT, PRIMARY KEY(listing_id));"""

        self.CREATE_TABLE_LISTING_PRICE = """CREATE TABLE IF NOT EXISTS listings_price(
        id INT NOT NULL PRIMARY KEY  AUTO_INCREMENT, listing_id INT, checkin_date VARCHAR(12) ,checkout_date VARCHAR(12)
        , price VARCHAR(5),UNIQUE (listing_id, checkin_date, price));"""

        self.CREATE_TABLE_HOST_LISTING = """CREATE TABLE IF NOT EXISTS hosts_listings(
        host_id INT ,listing_id INT,PRIMARY KEY(host_id, listing_id));"""

        self.INSERT_HOST = """INSERT IGNORE hosts
        (host_id,reviews,join_date,host_url) VALUES(%s,%s,%s,%s);"""

        self.INSERT_LISTING = """INSERT IGNORE listings
        (listing_id ,max_guests ,bedrooms ,beds ,bathrooms ,reviews ,rating_value ,
        listing_type ,city ,url ,host_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""

        self.INSERT_LISTING_PRICE = """INSERT IGNORE listings_price
        (listing_id, checkin_date,checkout_date, price) VALUES (%s,%s,%s,%s);"""

        self.INSERT_HOST_LISTING = """INSERT IGNORE hosts_listings
        (host_id ,listing_id) VALUES (%s,%s);"""

    def create_database(self):
        """create new database called airbnb"""
        cnx = mysql.connector.connect(user=self.USER, password=self.PASSWORD)
        cursor = cnx.cursor()
        cursor.execute(self.CREATE_DB)
        cnx.commit()
        cursor.close()
        cnx.close()

    def create_data_storage(self):
        """create tables in new databse airbnb"""
        cnx = mysql.connector.connect(user=self.USER, password=self.PASSWORD, database=self.DB)
        cursor = cnx.cursor()
        cursor.execute(self.CREATE_TABLE_HOST)
        cursor.execute(self.CREATE_TABLE_LISTING)
        cursor.execute(self.CREATE_TABLE_HOST_LISTING)
        cursor.execute(self.CREATE_TABLE_LISTING_PRICE)
        cnx.commit()
        cursor.close()
        cnx.close()

    @staticmethod
    def get_url(url):
        """create connection and retrieve page to read"""
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        driver = webdriver.Chrome(executable_path=os.path.abspath("chromedriver"), options=chrome_options)
        driver.implicitly_wait(5)
        driver.get(url)
        time.sleep(random.uniform(0.3, 1))
        soup = BeautifulSoup(driver.page_source, 'lxml')
        driver.close()
        return soup

    def retrieve_listings_links(self, url):
        """ function to find the links of all the listings in the first 17 pages"""
        print('retrieving links')
        counter = 1
        scraping_urls = []
        soup = Airbnb.get_url(url)
        while True:
            if counter == 16:
                return scraping_urls
            for link in soup.find_all(attrs={'class': '_v72lrv'}):
                create_link = 'https://airbnb.com' + link.a['href']
                scraping_urls.append(create_link)

            soup = Airbnb.get_url(url + '&items_offset={}'.format(counter * 18))
            counter += 1

    def retrieve_info(self, url):
        """receives a url of a specific listing and scrap the data and inserts it into database , also returns the url
        of the host to be used in the next step"""
        soup = self.get_url(url)

        listing_id = int(re.sub(self.REGEX_3, '', url)[:-1])
        print(listing_id)

        max_guests = int(re.sub(self.REGEX_3, '', soup.find_all(attrs={'class': '_6mxuijo'})[0].text))

        bedrooms = soup.find_all(attrs={'class': '_6mxuijo'})[1].text

        beds = int(re.sub(self.REGEX_3, '', soup.find_all(attrs={'class': '_6mxuijo'})[2].text))

        bathrooms = int(re.sub(self.REGEX_3, '', soup.find_all(attrs={'class': '_6mxuijo'})[3].text))
        reviews = int(soup.find_all(attrs={'class': '_7g6kz31'})[1].text) if int(soup.find_all(attrs={'class': '_7g6kz31'})[1].text) else None

        rating_value = float(soup.find(itemprop="ratingValue").get('content'))

        listing_type = soup.find(attrs={'class': '_1bb2ucx1'}).text

        city = soup.find(attrs={'class': '_1r804a6o'}).text

        host_url = soup.find(attrs={'class': '_1oa3geg'}).get('href')

        host_id = int(re.findall(self.REGEX_HOST, host_url)[0])

        print(listing_id, max_guests, bedrooms, beds, bathrooms, reviews,
              rating_value, listing_type, city, url, host_id)

        cnx = mysql.connector.connect(user=self.USER, password=self.PASSWORD, database=self.DB)
        cursor = cnx.cursor()
        cursor.execute(self.INSERT_LISTING, (listing_id, max_guests, bedrooms, beds, bathrooms, reviews,
                                             rating_value, listing_type, city, url, host_id))

        cursor.execute(self.INSERT_HOST_LISTING, (host_id, listing_id))

        cnx.commit()
        cursor.close()
        cnx.close()
        return host_url, listing_id

    def retrieve_host(self, url):
        """recieves a url of the host of a listing , scraps the data and  insert the host info into database"""
        soup = Airbnb.get_url('https://airbnb.com' + url)
        host_id = int(re.findall(self.REGEX_HOST, url)[0])
        reviews = int(soup.find(attrs={'class': '_e296pg'}).text)

        join_date = int(re.sub(self.REGEX_3, '', soup.find(attrs={'class': 'text-normal'}).text)) if int(
            re.sub(self.REGEX_3, '', soup.find(attrs={'class': 'text-normal'}).text)) else None

        host_url = 'https://www.airbnb.com' + url

        cnx = mysql.connector.connect(user=self.USER, password=self.PASSWORD, database=self.DB)
        cursor = cnx.cursor()
        cursor.execute(self.INSERT_HOST, (host_id, reviews, join_date, host_url))
        print('host_id: ' + str(host_id), )
        cnx.commit()
        cursor.close()
        cnx.close()

    def retrieve_price(self, url, listing_id):
        """"""
        cnx = mysql.connector.connect(user=self.USER, password=self.PASSWORD, database=self.DB)
        cursor = cnx.cursor()
        for dates in self.DATES:
            try:
                temp = url + "&checkin=" + dates[0] + "&checkout=" + dates[1]
                soup = self.get_url(temp)
                price = str(soup.find(attrs={'class': '_doc79r'}).text[1:])
                print(price)
                cursor.execute(self.INSERT_LISTING_PRICE, (listing_id, dates[0], dates[1], price))
            except:
                continue
        cnx.commit()
        cursor.close()
        cnx.close()

    def scrape_one(self, link):
        """main function that will parse over the links and try again if error occures, it will write the error to log
        file, if a listing gets more than 6 errors , it will write its link to the log file and skip it"""

        counter = 0
        print(link)
        while True:
            if counter == self.TRIES:
                self.logger.error("skipped: " + link)
                break
            try:
                host_url, listing_id = self.retrieve_info(link)
                self.retrieve_host(host_url)
                self.retrieve_price(link, listing_id)

            except (IndexError, ValueError, AttributeError) as error:
                counter += 1
                self.logger.warning("failed")
                self.logger.exception(str(error))
                continue
            break


def parse_input(location, adults, children, infants):
    """create link according to commandline input"""
    URL = str()
    if location != '-':
        URL += "https://www.airbnb.com/s/" + location
    else:
        URL += "https://www.airbnb.com/s/" + 'AnyWhere'
    if adults != '-':
        URL += "/homes?adults=" + adults
    if children != '-':
        URL += "&children=" + children
    if infants != '-':
        URL += "&infants=" + infants

    return URL


@click.command()
@click.option('--location', prompt='Location or "-" for anywhere')
@click.option('--adults', prompt='Adults or "-" for any')
@click.option('--children', prompt='Children or "-" for any')
@click.option('--infants', prompt='Infants or "-" for any')
def main(location, adults, children, infants):
    URL = parse_input(location, adults, children, infants)
    airbnb_parser = Airbnb()
    airbnb_parser.create_database()
    airbnb_parser.create_data_storage()

    links_list = airbnb_parser.retrieve_listings_links(URL)
    with Pool(cpu_count() - 1) as p:
        p.starmap(airbnb_parser.scrape_one, zip(links_list))
    # for link in links_list:
    #     airbnb_parser.scrape_one(link)


if __name__ == "__main__":
    main()
