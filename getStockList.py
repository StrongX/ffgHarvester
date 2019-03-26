import tushare as ts
import pymongo
client = pymongo.MongoClient(host='localhost', port=27017)
db = client.ffgHarvester
col = db.stockList

ts.set_token('495bd6a4d40acef11e6a222a1632889b27c60938aa9decba468c472b')

pro = ts.pro_api()




def main():
	print('开始查询股票列表')
	data = pro.query('stock_basic', exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')
	for index,row in data.iterrows():
		obj = dict(row)
		obj['_id'] = obj['ts_code']
		result = col.update({"_id":obj['_id']},{"$set":obj},upsert=True)
		if 'upserted' in result.keys():
			print('新增股票：'+result['upserted'])
	print('股票列表更新完毕')

if __name__ == '__main__':
	main()