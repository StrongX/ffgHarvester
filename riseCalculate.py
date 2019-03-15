#coding:utf-8

#连续涨幅统计
#连续跌幅统计
import pymongo
import time
import pandas as pd

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
	stockList = list(stockListCol.find())
	count = 0
	calculatedList = []
	for stock in stockList:
		ts_code = stock['ts_code']
		stockDailys = list(stockDailyCol.find({'ts_code':ts_code,'trade_date':{'$lte':firstDayStr},'trade_date':{'$gte':lastDayStr}}))
		pct_chg = 0.0
		for daily in stockDailys:
			pct_chg+=daily['pct_chg']
		stock['pct_chg'] = pct_chg
		calculatedList.append(pd.Series(stock))
		print('股票名称：'+stock['name']+'  股票代码：'+stock['ts_code']+' '+str(dayCount)+'日累计涨幅：'+'%.2f'%stock['pct_chg']+'%')
		count+=1
		# if count == 10:
		# 	break      #跑的太鸡儿慢了  先跑10支股票  后面优化速度

	df = pd.DataFrame(calculatedList).sort_values(by="pct_chg",ascending=ascending)  #根据涨幅排序
	df = df.reset_index(drop=True)   #重建索引
	while True:
		count = input("look for top count: ")
		print(df.loc[0:int(count)-1,['name','ts_code','pct_chg']])

def main():
	print("1:连续涨幅统计\n2:连续跌幅统计")
	select = input("Enter your input: ")
	if select == '1':
		riseCalculate(False)
	elif select == '2':
		riseCalculate(True)
if __name__ == '__main__':
	main()

