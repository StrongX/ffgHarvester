#coding:utf-8

"""
符号含义示例
$lt小于{'age': {'$lt': 20}}
$gt大于{'age': {'$gt': 20}}
$lte小于等于{'age': {'$lte': 20}}
$gte大于等于{'age': {'$gte': 20}}
$ne不等于{'age': {'$ne': 20}}
$in在范围内{'age': {'$in': [20, 23]}}
$nin不在范围内{'age': {'$nin': [20, 23]}}
"""

import tushare as ts
import pymongo
import time
import sys
client = pymongo.MongoClient(host='localhost', port=27017)
db = client.ffgHarvester
col = db.stockDaily

calendarList = db.stockCalendar.find()

todayStr = time.strftime("%Y%m%d", time.localtime())

ts.set_token('495bd6a4d40acef11e6a222a1632889b27c60938aa9decba468c472b')

pro = ts.pro_api()



def main():
	print('开始获取日线行情')
	for dateObj in calendarList:
		isGetDaily = dateObj['isGetDaily']
		cal_date = dateObj['cal_date']
		if isGetDaily == False:    #若还未获取数据，则获取k线数据
			if cal_date<=todayStr:   #获取当天之前的数据
				is_open = dateObj['is_open']
				if is_open == 1:
					df = pro.query("daily",trade_date=cal_date) #获取当日的全部股票数据
					list = []
					for index,row in df.iterrows():
						obj = dict(row)
						list.append(obj)
						# result = col.update({"ts_code":obj['ts_code'],"trade_date":obj['trade_date']},{"$set":obj},upsert=True)
					if len(list) == 0:
						print('当日股票日线未更新，请稍后再试')
						sys.exit()
					col.insert_many(list)
					db.stockCalendar.update({"_id":cal_date,},{"$set":{"isGetDaily":True}}) #更新日历  标记为已获取
					print('get daily data：'+cal_date)
				else:
					pass    #证监会不开门
			else:
				pass  #未来的日期
		else:
			pass #已获取数据
	print('已更新所有股票日线行情')


if __name__ == '__main__':
	main()