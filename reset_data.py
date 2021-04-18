import pymongo
import os

connString = os.environ["DB_STRING"]
connection = pymongo.MongoClient(connString)

col = connection['stocks']['gold']
value = {'$unset' : {'last_collected' : ''}}
col.update_many({}, value)

col = connection['stocks']['spy']
col.update_many({}, value)