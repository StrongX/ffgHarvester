#coding:utf-8

import getStockList
import getStockDaily
import calculateAverage
def main():
	getStockList.main()
	getStockDaily.main()
	calculateAverage.calculateWithDays(20)


if __name__ == '__main__':
	main()

