import os
import re
import copy
import math
from collections import OrderedDict 

import arrow
import csv
# import a tasty_trade csv, and emit 1 row for every sell with the
# transaction_type(money_transfer,trade),account,date, symbol, quantity, *stock (0/1), *option (0/1), credit/debit (including fees)
# only works for daily options right now

def create_formatted_csv():
	read_csvs()

def read_csvs():
	path = '../../Downloads/tt/'
	
	for filename in os.listdir(path):
		if '.csv' in filename:
			with open(os.path.join(path, filename)) as csvfile:
				read_csv(csvfile)


def read_csv(csvfile):
	'''assuming the headers are 
	Date/Time
	Transaction Code
	Transaction Subcode
	Security ID
	Symbol
	Buy/Sell
	Open/Close
	Quantity
	Expiration Date
	Strike
	Call/Put
	Price
	Fees
	Amount
	Description
	Account Reference
	'''

	'''creating csv with headers
	x - transaction_type(money_transfer,trade)
	x - account
	x - date
	x - symbol
	x - quantity
	x - stock (0/1)
	x - option (0/1)
	x - p_l (including fees)
	'''
	og_rows = copy_rows(csvfile)

	unique_days = getDays(og_rows)
	# list of dicts, each dict is a csv row
	all_trades = {}
	formatted_rows = []
	for day in unique_days:
		# get account credits and debits and balance changes
		money_movement = moneyMovementForDay(day, og_rows)
		if money_movement:
			formatted_rows.extend(getFormattedRowsForMoneyMoneyMovement(day, money_movement))

		# get options/common trades
		all_trades = getTradesForDay(day, og_rows, all_trades)
		if all_trades:
			# write row
			# we only write out the p/l when the trade is over (quantity 0)
			formatted_rows.extend(getFormattedRowsForTrades(day, all_trades))

			# remove finished trades
			all_trades = remove_completed_trades(all_trades)

	# TODO: persist swing trades for next times
	if all_trades:
		print('*** these trades are still in progress {}'.format(all_trades))

	# output csv
	output_formatted_csv(formatted_rows)
	print('done')

def copy_rows(csvfile):
	reader = csv.DictReader(csvfile)
	rows = []
	for row in reader:
		copied = copy.deepcopy(row)
		rows.append(copied)

	return rows


def getDays(og_rows):
	# get the unique days in the csv
	unique_days = set()

	for row in og_rows:
		mdy = arrow.get(row['Date/Time'], 'MM/DD/YYYY h:mm A').format('MM/DD/YYYY')
		unique_days.add(mdy)

	# sort days ascending
	arrow_days = [arrow.get(u_d, 'MM/DD/YYYY') for u_d in unique_days]
	arrow_days.sort()
	string_days = [a_d.format('MM/DD/YYYY') for a_d in arrow_days]

	print('found {} trading days'.format(len(unique_days)))
	return string_days

def sameDay(mdy, og_row):
	og_mdy = arrow.get(og_row['Date/Time'], 'MM/DD/YYYY h:mm A').format('MM/DD/YYYY')
	
	return mdy == og_mdy

def moneyMovementForDay(day, og_rows):
	money_movement = []

	# get each money movement event for this day
	for row in og_rows:
		# if it's the same day
		if sameDay(day, row):

			if 'money movement' in row['Transaction Code'].lower():
				money_movement.append(getAmount(row))

	return money_movement	

def getTradesForDay(day, og_rows, trades):
	# TODO: support long term swing trades (need a db ;))

	# trades = {}
	# group by symbol (commons or options)
	'''
		{
			symbol:{
				commons: {
					net_amount: float 
					quantity: int
					amount_bought
					amount_sold
				}
				options: {
					net_amount: float 
					quantity: int
					amount_bought
					amount_sold
				}
			}
		}
	'''

	# calc all trades for this day
	for row in og_rows:
		# if it's the same day
		if sameDay(day, row):
			
			# calulate all trades for every symbol
			if 'trade' in row['Transaction Code'].lower():
				symbol = row['Symbol']
	
				if isOption(row):
					# amount with fees
					netAmount = amountWithFees(row)

					# save option trades
					if symbol not in trades:
						trades[symbol] = create_trade_dict()

					trades[symbol]['options']['net_amount'] += netAmount
						
					# update buy and sell totals
					if isPurchase(row):
						trades[symbol]['options']['amount_bought'] += math.fabs(netAmount)
						# increase total holdings
						trades[symbol]['options']['quantity'] += int(row['Quantity'])
					else:
						trades[symbol]['options']['amount_sold'] += math.fabs(netAmount)
						# reduce total holdings
						trades[symbol]['options']['quantity'] -= int(row['Quantity'])

				else:
					# amount with fees
					netAmount = amountWithFees(row)

					# save stock trades
					if symbol not in trades:
						trades[symbol] = create_trade_dict()

					trades[symbol]['commons']['net_amount'] += netAmount
				
					# update buy and sell totals
					if isPurchase(row):
						trades[symbol]['commons']['amount_bought'] += math.fabs(netAmount)
						# increase total holdings
						trades[symbol]['commons']['quantity'] += int(row['Quantity'])
					else:
						trades[symbol]['commons']['amount_sold'] += math.fabs(netAmount)
						# reduce total holdings
						trades[symbol]['commons']['quantity'] -= int(row['Quantity'])
	
	print('calulated all {} trades for {}'.format(len(trades.items()), day))
	return trades

def is_commons_swing_trade(symbol, trade_type):
	return trade_type['commons']['quantity'] != 0

def is_options_swing_trade(symbol, trade_type):
	return trade_type['options']['quantity'] != 0

def get_swing_trades(swing_trades, trades):

	for symbol, trade_type in trades.items():
		if is_commons_swing_trade(symbol, trade_type) or is_options_swing_trade(symbol, trade_type):
			# save most up to date trade info
			swing_trades[symbol] = trade_type

	return swing_trades

def remove_completed_trades(trades):
	symbols_to_delete = []
	for symbol, trade_type in trades.items():
		if not is_commons_swing_trade(symbol, trade_type):
			# trade is complete
			trades[symbol]['commons'] = create_emtpy_common_or_options_dict()

		if not is_options_swing_trade(symbol, trade_type):
			# trade is complete
			trades[symbol]['options'] = create_emtpy_common_or_options_dict()

		if is_trade_type_empty(trade_type):
			symbols_to_delete.append(symbol)

	for symbol in symbols_to_delete:
		trades.pop(symbol, None)

	return trades

def removeSwingTrades(trades):
	# remove trades that are not day trades. TODO support it sometime in the future
	# the quantity should be 0 if it was a day trade
	for symbol, trade_type in trades.items():
		if is_commons_swing_trade(symbol, trade_type):
			print('*****Removing swing trade for {} Commons: {}. Do it manually******'.format(symbol, trade_type['commons']['quantity']))
			trades[symbol]['commons'] = create_emtpy_common_or_options_dict()
			# TODO: save trade_type object

		if is_options_swing_trade(symbol, trade_type):
			print('*****Removing swing trade for {} Options: {}. Do it manually******'.format(symbol, trade_type['options']['quantity']))
			trades[symbol]['options'] = create_emtpy_common_or_options_dict()
			# TODO: save trade_type object

	return trades

def getFormattedRowsForMoneyMoneyMovement(day, money_movement):
	formatted_rows = []
	for event in money_movement:
		formatted_row = {
			'transaction_type': 'money_transfer',
			'account': None,
			'date': day,
			'symbol': None,
			'quantity': None, # doesn't matter
			'stock': None,
			'option': None,
			'p_l': str(round(event, 2)),
			'%': 0
		}
		formatted_rows.append(formatted_row)

	return formatted_rows

def getFormattedRowsForTrades(day, trades):
	formatted_rows = []
	# output rows for each trade symbol p/l for day in trades
	# for all options
	for symbol, trade_type in trades.items(): 
		# print('{} {} {}'.format(symbol, trade_type['options']['quantity'], trade_type['options']['quantity']))
		if trade_type['options']['quantity'] == 0 and trade_type['options']['net_amount'] != 0:
			formatted_row = {
				'transaction_type': 'trade',
				'account': None,
				'date': day,
				'symbol': symbol,
				'quantity': None, # doesn't matter
				'stock': None,
				'option': None,
				'p_l': str(round(trade_type['options']['net_amount'], 2)),
				'%': calculatePercentGain(trade_type['options']['amount_bought'], trade_type['options']['amount_sold'])
			}
			formatted_rows.append(formatted_row)

	# for all commons
	for symbol, trade_type in trades.items():
		if trade_type['commons']['quantity'] == 0 and trade_type['commons']['net_amount'] != 0:
			formatted_row = {
				'transaction_type': 'trade',
				'account': None,
				'date': day,
				'symbol': symbol,
				'quantity': None, # doesn't matter
				'stock': None,
				'option': None,
				'p_l': str(round(trade_type['commons']['net_amount'], 2)),
				'%': calculatePercentGain(trade_type['commons']['amount_bought'], trade_type['commons']['amount_sold'])
			}
			formatted_rows.append(formatted_row)

	return formatted_rows
			
def calculatePercentGain(bought, sold):
	percent_gain = ((sold - bought)/bought) * 100
	
	return str(round(percent_gain, 2))

def create_trade_dict():
	trades = {
		'commons': create_emtpy_common_or_options_dict(),
		'options': create_emtpy_common_or_options_dict()
	}
	return trades

def create_emtpy_common_or_options_dict():
	shell = {
		'net_amount': 0,
		'quantity': 0,
		'amount_bought': 0,
		'amount_sold': 0
	}
	return shell

def is_trade_type_empty(trade_type):
	# common_zeros = [ value for key, value in trade_type['commons'].items() if value == 0]
	# option_zeros = [ value for key, value in trade_type['options'].items() if value == 0]
	# common_zeros.extend(option_zeros)

	# return len(common_zeros) == 0
	return trade_type['commons']['quantity'] == 0 and trade_type['options']['quantity'] == 0

# ======== Row funcs ===========

def amountWithFees(og_row):
	fees = float(og_row['Fees'])
	price = float(og_row['Amount'])

	# if negative, it is a purchase
	isPurchase = price < 0

	amount = math.fabs(price) + fees

	# neg val if purchased
	if isPurchase:
		return amount * -1
	
	return amount

def getAmount(og_row):
	price = float(og_row['Amount'])

	# if negative, it is a purchase
	isPurchase = price < 0

	amount = math.fabs(price)

	# neg val if purchased
	if isPurchase:
		return amount * -1
	
	return amount

def isPurchase(og_row):
	# negative is purchase
	return getAmount(og_row) < 0

def isOption(og_row):
	# is option trade?
	if not og_row['Call/Put']:
		return False
	
	return True

def isCallOption(og_row):
	if isOption(og_row):
		if 'c' in og_row['Call/Put'].lower():
			return True
		else:
			return False
	
	return False

def isPutOption(og_row):
	if isOption(og_row):
		if 'p' in og_row['Call/Put'].lower():
			return True
		else:
			return False

	return False

def output_formatted_csv(formatted_rows):
	print('...creating csv')

	with open('formatted_tt.csv', 'w', newline='') as out_csvfile:
		fieldnames = ['transaction_type','account','date','symbol','quantity','stock','option','p_l', '%']
		writer = csv.DictWriter(out_csvfile, fieldnames=fieldnames)

		writer.writeheader()
		for formatted in formatted_rows:
			writer.writerow(formatted)

	print('finished writing csv')

	'''create a csv with 
	'''

	# save deposits
	# save withdrawls
	# save balance adustments = comes out of account andrew

if __name__ == "__main__":
	create_formatted_csv()



