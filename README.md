# Airbnb_mining and hotels data API for comfortable comparison between 2 sleeping arrangements
Created by Tom Cohen and Roman Rosh

A data scraping program for airbnb and hotel API for data retrieval


Note that the name of the classes used change regularly on airbnb , so you should change them according to the latest airbnb website update.

Airbnb has invested a significant amount of  time to make sure   you cant scrape their website , but this scraper can handle it , it will show you the errors while running but will take care of them.
 
 
This program will uses by default all the cores of your computer , this can be changed in the class initialization.

You will need to include the user and password of mysql  installed on your computer since this program also creates a new database called airbnb.

Requires the chromedriver file , can be downloaded at : http://chromedriver.chromium.org/downloads 
should be put in the same directory as the scraper to avoid unnecessary config

Hotels API connects to hotellookapi and retrieves hotel data, for comparing with AirBnb scraper or any other use. The data is connected separately, the aim is to offer all the options for sleeping arrangements in once database.
