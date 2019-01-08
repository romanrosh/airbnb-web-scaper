import requests
import mysql.connector
import pandas as pd
from datetime import timedelta
import logging

checkin = '2019-02-01' # beginning and ending date to scrape data
checkout = '2019-08-01'
listings = '100' # number of listings per parameter set, it seems API works with up to 100 listings per run per set

URL_LIST = 'http://engine.hotellook.com/api/v2/lookup.json?query='
URL_PRICE = 'http://engine.hotellook.com/api/v2/cache.json?location='

USER = "" # mysql username
PASSWORD = "" # mysql password
DB = 'airbnb'

CREATE_DB = 'Create database if not exists airbnb character set utf8 collate utf8_bin;'

CREATE_TABLE_HOTEL_LIST = """CREATE TABLE IF NOT EXISTS hotel_list
                            (id INT,fullName TEXT,locationId INT,locationName TEXT,label TEXT,location_lat FLOAT,
                            location_lon FLOAT, PRIMARY KEY(id));"""
CREATE_TABLE_HOTEL_PRICE = """CREATE TABLE IF NOT EXISTS hotel_price
                            (hotelId INT,checkin DATE,checkout DATE,today_date DATE,name TEXT,state TEXT,country TEXT,
                            hotelName TEXT,priceFrom FLOAT,locationId INT,geo_lat FLOAT,geo_lon FLOAT,stars INT,priceAvg FLOAT,
                            PRIMARY KEY(hotelId, checkin));"""

INSERT_HOTEL_LIST = """INSERT IGNORE hotel_list
                    (id,fullName,locationId,locationName,label,location_lat,location_lon)
                    VALUES(%s,%s,%s,%s,%s,%s,%s);"""
INSERT_HOTEL_PRICE = """INSERT IGNORE hotel_price
                    (hotelId,checkin,checkout,name,state,country,hotelName,priceFrom,locationId,geo_lat,
                    geo_lon,stars,priceAvg)
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""

SELECT_CITY = """SELECT DISTINCT CITY FROM listings;"""


def request_list(location, listings):
    """retrieval of general hotel listings and locations"""
    list = requests.get(URL_LIST + location + '&lang=en&lookFor=both&limit=' + listings)
    list = list.json()
    cnx = mysql.connector.connect(user=USER, password=PASSWORD, database=DB)
    cursor = cnx.cursor()
    for l in list['results']['hotels']:
        id = l['id']
        fullName = l['fullName']
        locationId = l['locationId']
        locationName = l['locationName']
        label = l['label']
        location_lat = l['location']['lat']
        location_lon = l['location']['lon']
        cursor.execute(CREATE_TABLE_HOTEL_LIST)
        cursor.execute(INSERT_HOTEL_LIST, (id, fullName, locationId, locationName, label, location_lat, location_lon))
        cnx.commit()
    cursor.close()
    cnx.close()


def request_prices(location, checkin, checkout, listings):
    """retrieval of hotel prices dependent on datetime"""
    price = requests.get(
        URL_PRICE + location + '&currency=ils&checkIn=' + checkin + '&checkOut=' + checkout + '&limit=' + listings)
    price = price.json()
    cnx = mysql.connector.connect(user=USER, password=PASSWORD, database=DB)
    cursor = cnx.cursor()
    for p in price:
        name = p['location']['name']
        state = p['location']['state']
        country = p['location']['country']
        hotelId = p['hotelId']
        hotelName = p['hotelName']
        priceFrom = p['priceFrom']
        locationId = p['locationId']
        geo_lat = p['location']['geo']['lat']
        geo_lon = p['location']['geo']['lon']
        stars = p['stars']
        priceAvg = p['priceAvg']
        cursor.execute(CREATE_TABLE_HOTEL_PRICE)
        cursor.execute(INSERT_HOTEL_PRICE, (
            hotelId, checkin, checkout, name, state, country, hotelName, priceFrom, locationId, geo_lat,
            geo_lon, stars, priceAvg))
        cnx.commit()
    cursor.close()
    cnx.close()


def main():
    cnx = mysql.connector.connect(user=USER, password=PASSWORD, database=DB)
    cursor = cnx.cursor()
    cursor.execute(SELECT_CITY)
    cities = list(cursor.fetchall())
    cnx.commit()
    cursor.close()
    cnx.close()
    print(cities)
    for location in cities:
        try:
            print(location[0])
            dates = list(map(lambda x: (str(x.date()), str(x.date() + timedelta(days=2))),
                             pd.date_range(checkin, checkout).tolist()))[::15]
            for in_out_date in dates:
                request_list(location[0], listings)
                request_prices(location[0], in_out_date[0], in_out_date[1], listings)
        except:
            print('Exception occured')
            logging.debug('Error in main function')


if __name__ == "__main__":
    main()
