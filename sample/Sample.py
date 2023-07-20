# -*- coding: utf-8 -*-

from Liquirizia.DataAccessObject import DataAccessObjectHelper

from Liquirizia.DataAccessObject.Implements.AWS.DynamoDB import DataAccessObject, DataAccessObjectConfiguration

if __name__ == '__main__':

	# Set connection
	DataAccessObjectHelper.Set(
		'Sample',
		DataAccessObject,
		DataAccessObjectConfiguration(
			token='YOUR_ACCESS_TOKEN',  # Access Key
			secret='YOUR_ACCESS_TOKEN_SECRET',  # Access Secret Key
			region='YOUR_REGION',  # Region
		)
	)

	# Get connection
	con = DataAccessObjectHelper.Get('Sample')

	# create document table
	con.create(
		'Sample',
		KeySchema=[
			{
				'AttributeName': 'ID',
				'KeyType': 'HASH'
			}
		],
		AttributeDefinitions=[
			{
				'AttributeName': 'ID',
				'AttributeType': 'N'
			}
		],
		BillingMode='PAY_PER_REQUEST'
	)

	# set document
	con.set(
		'Sample',
		Item={
			'ID': 1,
			'Name': '최준호',
			'CountryCode': 'KR',
			'Dept': '연구소',
			'Title': '개발자'
		}
	)

	con.set(
		'Sample',
		Item={
			'ID': 2,
			'Name': '홍승걸',
			'CountryCode': 'KR',
			'Dept': '연구소',
			'Title': '개발자'
		}
	)

	con.set(
		'Sample',
		Item={
			'ID': 3,
			'Name': '허용선',
			'CountryCode': 'KR',
			'Title': 'CTO'
		}
	)

	# get document
	doc = con.get(
		'Sample',
		Key={
			'ID': 1
		}
	)
	print(doc)

	# delete document
	con.remove(
		'Sample',
		Key={
			'ID': 1
		}
	)

	# delete document table
	con.delete('Sample')
