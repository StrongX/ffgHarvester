#coding:utf-8
#牛回头，前期涨幅很大，回落击穿20日匀线后向上突破20日匀线
#筛选条件，第一天击穿20日匀线，然后连续两天收在20以上。30日累计7个涨停板以上（当日涨幅大于9个点）
import pymongo
import pandas as pd
import time

client = pymongo.MongoClient(host='localhost', port=27017)
db = client.ffgHarvester
stockDailyCol = db.stockDaily
stockCalendarCol = db.stockCalendar
stockListCol = db.stockList


seriesDays = 30
highDays = 7


def findRowStocks(dayStr):
	dayList = list(stockCalendarCol.find({"is_open":1,'cal_date':{"$lte":dayStr}}).limit(seriesDays).sort('cal_date', pymongo.DESCENDING)) #倒推获取指定时间的交易日期
	lastCal = dayList[len(dayList)-1]
	lastDayStr = lastCal['cal_date']  #第一个交易日的日期
	stocks = stockDailyCol.find({'trade_date':{'$lte':dayStr,'$gte':lastDayStr}})
	stocksDF = pd.DataFrame(stocks)
	stockList = stockListCol.find()
	for stock in stockList:
		df = stocksDF[(stocksDF['ts_code'] == stock['ts_code'])]
		df = df.reset_index(drop=True)   #重建索引
		lastestDF = df.loc[len(df)-3:len(df)].reset_index(drop=True)
		for index,row in lastestDF.iterrows():
			ma20 = row['ma20']
			pct_chg = row['pct_chg']
			if(index == 0):
				low = row['low']
				if(low<ma20):
					pass
				else:
					break
			else:
				if(pct_chg<0):
					break
				close = row['close']
				if(close>ma20):
					if(index == 1):
						continue
					else:
						#筛选是否是牛股
						highDf = df[df['pct_chg']>9.0]
						if(len(highDf)>highDays):
							print('股票代码'+row['ts_code']+'    '+str(seriesDays)+'日类累计'+str(len(highDf))+'个交易日涨幅超过9个点'+'    股票交易日'+dayStr)
				else:
					break

def fenchRowStocks(dayStr):#预测牛股 一天跌穿20匀线 第二天收到匀线上，第三天收盘若在匀线上则买入
	dayList = list(stockCalendarCol.find({"is_open":1,'cal_date':{"$lte":dayStr}}).limit(seriesDays).sort('cal_date', pymongo.DESCENDING)) #倒推获取指定时间的交易日期
	lastCal = dayList[len(dayList)-1]
	lastDayStr = lastCal['cal_date']  #第一个交易日的日期
	stocks = stockDailyCol.find({'trade_date':{'$lte':dayStr,'$gte':lastDayStr}})
	stocksDF = pd.DataFrame(stocks)
	stockList = stockListCol.find()
	for stock in stockList:
		df = stocksDF[(stocksDF['ts_code'] == stock['ts_code'])]
		df = df.reset_index(drop=True)   #重建索引
		lastestDF = df.loc[len(df)-2:len(df)].reset_index(drop=True)
		for index,row in lastestDF.iterrows():
			ma20 = row['ma20']
			pct_chg = row['pct_chg']
			if(index == 0):
				low = row['low']
				if(low<ma20):
					pass
				else:
					break
			else:
				if(pct_chg<0):
					break
				close = row['close']
				if(close>ma20):
					#筛选是否是牛股
					highDf = df[df['pct_chg']>9.0]
					if(len(highDf)>highDays):
						print('股票代码'+row['ts_code']+'    '+str(seriesDays)+'日类累计'+str(len(highDf))+'个交易日涨幅超过9个点'+'    股票交易日'+dayStr)
				else:
					break

def main():
	todayStr = time.strftime("%Y%m%d", time.localtime())
	# dayList = list(stockCalendarCol.find({"is_open":1,'cal_date':{"$lte":todayStr}}).limit(10).sort('cal_date', pymongo.DESCENDING)) #倒推获取指定时间的交易日期
	# for day in dayList:
	# 	dayStr = day['cal_date']
	# 	findRowStocks(dayStr)
	findRowStocks(todayStr)
	# fenchRowStocks(todayStr)

if __name__ == '__main__':
	main()











