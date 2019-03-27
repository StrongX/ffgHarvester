#coding:utf-8
#计算匀线

import pymongo

client = pymongo.MongoClient(host='localhost', port=27017)
db = client.ffgHarvester
col = db.stockDaily

def calculateWithDays(days):
	pass