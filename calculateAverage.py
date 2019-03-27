#coding:utf-8
#计算匀线

import pymongo
import pandas as pd
import time
import threading

client = pymongo.MongoClient(host='localhost', port=27017)
db = client.ffgHarvester
stockDailyCol = db.stockDaily
threads = [] #创建一个线程列表，用于存放需要执行的子线程
allStocks = stockDailyCol.find()	#1、筛选出所有未计算多日匀线的日线
allStockDf = pd.DataFrame(allStocks)
maxThreadCount = 100

def calculateWithDays(days):
	key = 'ma'+str(days)
	stocks = stockDailyCol.find({key:{'$exists':False}})	#1、筛选出所有未计算多日匀线的日线
	
	for daily in stocks:	#2、循环每一个日线计算匀线
		if len(threads)<=maxThreadCount:
			t = threading.Thread(target=calculateAction, args=(daily,days,key))
			t.setDaemon(True) #将线程声明为守护线程，必须在start() 方法调用之前设置，如果不设置为守护线程程序会被无限挂起
			t.start() #启动子线程
			threads.append(t)
		else:
			while True:
				for t in threads:
					if not t.isAlive():
						threads.remove(t)
				if len(threads)<=maxThreadCount:
					t = threading.Thread(target=calculateAction, args=(daily,days,key))
					t.setDaemon(True) #将线程声明为守护线程，必须在start() 方法调用之前设置，如果不设置为守护线程程序会被无限挂起
					t.start() #启动子线程
					threads.append(t)
					break

def calculateAction(daily,days,key):
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