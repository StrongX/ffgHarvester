#coding:utf-8
#匀线上升：沿20日匀线上升，限定向上偏差和向下偏差

import pymongo
import pandas as pd
import time

client = pymongo.MongoClient(host='localhost', port=27017)
db = client.ffgHarvester
stockDailyCol = db.stockDaily
stockCalendarCol = db.stockCalendar
stockListCol = db.stockList


upOffset = 0.05

downOffset = 0

seriesDays = 60

def analyzeBalanceRise():
	todayStr = time.strftime("%Y%m%d", time.localtime())
	dayList = list(stockCalendarCol.find({"is_open":1,'cal_date':{"$lte":todayStr}}).limit(seriesDays).sort('cal_date', pymongo.DESCENDING)) #倒推获取指定时间的交易日期
	lastCal = dayList[len(dayList)-1]
	lastDayStr = lastCal['cal_date']  #第一个交易日的日期
	stocks = stockDailyCol.find({'trade_date':{'$lte':todayStr},'trade_date':{'$gte':lastDayStr}})
	stocksDF = pd.DataFrame(stocks)
	stockList = stockListCol.find()
	i = 1
	for stock in stockList:
		df = stocksDF[(stocksDF['ts_code'] == stock['ts_code'])]
		allow = True
		for index,row in df.iterrows():
			ma20 = row['ma20']
			close = row['close']
			if close >= ma20:
				if ((close-ma20)/ma20)>upOffset:
					allow = False
					break
			else:
				if ((ma20-close)/ma20)>downOffset:
					allow = False
					break
		if allow:
			print('沿20日匀线上升,股票代码：'+row['ts_code'])
		print('分析第'+str(i)+'支股票')
		i+=1

def main():
	analyzeBalanceRise()

if __name__ == '__main__':
	main()