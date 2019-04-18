#coding:utf-8
#牛回头，前期涨幅很大，回落击穿20日匀线后向上突破20日匀线
#筛选条件，第一天击穿20日匀线，然后连续两天收在20以上。30日累计6个涨停板以上
import pymongo
import pandas as pd
import time

client = pymongo.MongoClient(host='localhost', port=27017)
db = client.ffgHarvester
stockDailyCol = db.stockDaily
stockCalendarCol = db.stockCalendar
stockListCol = db.stockList


seriesDays = 30

def main():
	todayStr = time.strftime("%Y%m%d", time.localtime())
	dayList = list(stockCalendarCol.find({"is_open":1,'cal_date':{"$lte":todayStr}}).limit(seriesDays).sort('cal_date', pymongo.DESCENDING)) #倒推获取指定时间的交易日期
	lastCal = dayList[len(dayList)-1]
	lastDayStr = lastCal['cal_date']  #第一个交易日的日期
	stocks = stockDailyCol.find({'trade_date':{'$lte':todayStr},'trade_date':{'$gte':lastDayStr}})
	stocksDF = pd.DataFrame(stocks)
	stockList = stockListCol.find()
	for stock in stockList:
		df = stocksDF[(stocksDF['ts_code'] == stock['ts_code'])]
		df = df.reset_index(drop=True)   #重建索引
		lastestDF = df.loc[len(df)-3:len(df)].reset_index(drop=True)
		for index,row in lastestDF.iterrows():
			ma20 = row['ma20']
			if(index == 0):
				low = row['low']
				if(low<ma20):
					pass
				else:
					break
			else:
				close = row['close']
				if(close>ma20):
					if(index == 1):
						continue
					else:
						print(dict(row))
				else:
					break


if __name__ == '__main__':
	main()