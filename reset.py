#coding:utf-8

import pymongo

client = pymongo.MongoClient(host='localhost', port=27017)
db = client.ffgHarvester
stockDailyCol = db.stockDaily
stockCalendarCol = db.stockCalendar
stockListCol = db.stockList



def main():
	stockCalendarCol.update_many({'cal_date':{"$gt":'20190330'}},{"$set":{"isGetDaily":False}})#重置日历标记位
	stockDailyCol.update_many({'trade_date':{"$gt":'20190330'}},{'$unset':{'ma20':''}},False) #删除计算错误的匀线


if __name__ == '__main__':
	main()