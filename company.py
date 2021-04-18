import os
import numpy as np 
import pandas as pd
from bs4 import BeautifulSoup as bs
import requests
from configurations import wantedRows, letterNumbers, percentageNumbers, dates
from converter import Converter
import pymongo
#from config import password
from tickers import gold, spy
import logging
import datetime

class CompanyInserter:

    def __init__(self, baseurl, connString, tickers):
        self.baseurl = baseurl
        self.connection = pymongo.MongoClient(connString)
        self.tickers = tickers

    def getName(self, html):
        """Gets the string of the currency stock is priced in."""
        soup = bs(html, "lxml")
        results = soup.findAll("h1", {"data-reactid" : "7"})
        if len(results) != 1:
            return False, None
        name = results[0].text.split(' (')[0]
        return True, name

    def getCompanyData(self, ticker):
        data = requests.get(self.baseurl + "%s?p=%s" % (ticker, ticker))
        exists, name = self.getName(data.text)
        if exists:
            print("Inserting:{}".format(name))
            col = self.connection['stocks']['spy']
            col.insert_one({
                'name' : name,
                'ticker' : ticker
            })

if __name__ == "__main__":
    logging.basicConfig(filename="miner.log", filemode='a', level=logging.INFO)
    baseurl = "https://uk.finance.yahoo.com/quote/"
    connString = "mongodb+srv://improve:%s@improveatinvesting.s3ghn.mongodb.net/myFirstDatabase?retryWrites=true&w=majority" % os.environ["DB_PASSWORD"]
    companyInserter = CompanyInserter(baseurl, connString, spy)
    
    for ticker in companyInserter.tickers:
        companyInserter.getCompanyData(ticker)