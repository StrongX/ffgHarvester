#coding:utf-8

#连续涨幅统计
#连续跌幅统计
import pymongo
import time
import pandas as pd
import datetime

client = pymongo.MongoClient(host='localhost', port=27017)
db = client.ffgHarvester
stockCalendarCol = db.stockCalendar
stockListCol = db.stockList
stockDailyCol = db.stockDaily

def riseCalculate(ascending):
	dayCount = int(input("Enter series days:"))
	todayStr = time.strftime("%Y%m%d", time.localtime())
	dayList = list(stockCalendarCol.find({"is_open":1,'cal_date':{"$lte":todayStr}}).limit(dayCount).sort('cal_date', pymongo.DESCENDING)) #倒推获取指定时间的交易日期
	lastCal = dayList[len(dayList)-1]
	lastDayStr = lastCal['cal_date']  #第一个交易日的日期
	firstDayCal = dayList[0]
	firstDayStr = firstDayCal['cal_date'] #最后一个交易日的日期
	isOrNot = input("是否去除30个日内上市的新股？年\n1:yes\n2:no")
	now = datetime.datetime.now()
	newStockDay = (now - datetime.timedelta(days = 30)).strftime('%Y%m%d')
	print(newStockDay)
	if(isOrNot == '1') :
		stockList = list(stockListCol.find({"list_date":{"$lt":newStockDay}}))
	else:
		stockList = list(stockListCol.find())	
	
	stockDf = []
	for stock in stockList:
		stockDf.append(pd.Series(stock))
	stockDf = pd.DataFrame(stockDf)
	count = 0
	calculatedList = []
	stockDailys = list(stockDailyCol.find({'trade_date':{'$lte':firstDayStr},'trade_date':{'$gte':lastDayStr}}))
	for daily in stockDailys:
		calculatedList.append(pd.Series(daily))
	calculateDf = pd.DataFrame(calculatedList).groupby('ts_code')[['pct_chg']].sum()
	res = pd.merge(calculateDf, stockDf, on='ts_code').sort_values(by="pct_chg",ascending=ascending)  #根据涨幅排序
	res = res.reset_index(drop=True)   #重建索引
	while True:
		count = input("look for top count: ")
		print(res.loc[0:int(count)-1,['name','ts_code','pct_chg']])

def main():
	print("1:连续涨幅统计\n2:连续跌幅统计")
	select = input("Enter your input: ") 
	if select == '1':
		riseCalculate(False)
	elif select == '2':
		riseCalculate(True)
if __name__ == '__main__':
	main()

