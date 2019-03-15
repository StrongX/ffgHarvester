import tushare as ts
import pymongo
client = pymongo.MongoClient(host='localhost', port=27017)
db = client.ffgHarvester
col = db.stockCalendar

ts.set_token('495bd6a4d40acef11e6a222a1632889b27c60938aa9decba468c472b')

pro = ts.pro_api()

df = pro.query('trade_cal', start_date='20180101', end_date='20201231')

for index,row in df.iterrows():
	obj = dict(row)
	obj['_id'] = obj['cal_date']
	obj['isGetDaily'] = False
	result = col.update({"_id":obj['_id']},{"$set":obj},upsert=True)
	print(result)