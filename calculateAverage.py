#coding:utf-8
#计算匀线

import pymongo
import pandas as pd
import time
import multiprocessing


client = pymongo.MongoClient(host='localhost', port=27017)
db = client.ffgHarvester
stockDailyCol = db.stockDaily
allStocks = stockDailyCol.find()
allStockDf = pd.DataFrame(allStocks)
processCount = 7


def calculateWithDays(days):
	key = 'ma'+str(days)
	stocks = stockDailyCol.find({key:{'$exists':False}})	#1、筛选出所有未计算多日匀线的日线
	stocksNumber = stocks.count()
	rows = int(stocksNumber/processCount)
	stocks = pd.DataFrame(stocks)
	for x in range(0,processCount):
		if x == processCount-1:
			stocksCut = stocks.iloc[x*rows:stocksNumber]
		else:
			stocksCut = stocks.iloc[x*rows:rows+x*rows]
		p = multiprocessing.Process(target = calculateWithProcess, args = (days,stocksCut))
		p.start()
	

def calculateWithProcess(days,stocks):
	key = 'ma'+str(days)
	for index,daily in stocks.iterrows():	#2、循环每一个日线计算匀线
		stockDf = allStockDf[(allStockDf['ts_code']==daily['ts_code']) & (allStockDf['trade_date']<=daily['trade_date'])].sort_values(by="trade_date",ascending=False).reset_index(drop=True).loc[0:days]
		#3、找出此日线前n天的所有日线
		# if len(maStocks) < days:
		# 	continue  #若数据库数据不够匀线统计则不做统计
		value = stockDf['close'].mean()	#4、计算收盘价均价
		stockDailyCol.update({"_id":daily['_id'],},{"$set":{key:value}}) 	#5、插入计算号的均价。  字段名，如：ma20
		print('股票代码：'+daily['ts_code']+'  交易日期：'+daily['trade_date']+'   '+str(days)+'日均价:'+str(value))

def main():
	calculateWithDays(20)

if __name__ == '__main__':
	main()