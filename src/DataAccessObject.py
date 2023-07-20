# -*- coding: utf-8 -*-

from Liquirizia.DataAccessObject import DataAccessObject as DataAccessObjectBase
from Liquirizia.DataAccessObject.Properties.Document import Document

from .DataAccessObjectConfiguration import DataAccessObjectConfiguration
from .DataAccessObjectFormatReader import DataAccessObjectFormatReader
from .DataAccessObjectFormatWriter import DataAccessObjectFormatWriter

from Liquirizia.Util.Dictionary import Replace

from time import sleep

import boto3

__all__ = (
	'DataAccessObject'
)


class DataAccessObject(DataAccessObjectBase, Document):
	"""
	Data Access Object Class for DynamoDB of AWS

	TODO :
		* Exception Handling with DataAccessObjectError
	"""

	DataTypeNumeric = 'N'
	DataTypeNumericArray = 'NS'
	DataTypeString = 'S'
	DataTypeStringArray = 'SS'
	DataTypeObject = 'M'

	KeyTypeHash = 'HASH'
	KeyTypeRange = 'RANGE'

	def __init__(self, conf: DataAccessObjectConfiguration, reader=DataAccessObjectFormatReader(), writer=DataAccessObjectFormatWriter()):
		self.conf = conf
		self.reader = reader
		self.writer = writer

		if not isinstance(conf, DataAccessObjectConfiguration):
			raise RuntimeError('{} is not DataAccessConfiguration for DynamoDB')

		self.client = None
		return

	def __del__(self):
		if self.client:
			self.close()
		return

	def connect(self):
		self.client = boto3.resource(
			'dynamodb',
			aws_access_key_id=self.conf.accessKey,
			aws_secret_access_key=self.conf.accessSecretKey,
			region_name=self.conf.region
		)
		return

	def close(self):
		if self.client:
			self.client = None
		return

	def create(self, doc, delay=1000, attempts=5, **kwargs):
		# TODO : verify kwargs for DynamoDB
		#
		# required parameters are following :
		#
		# AttributeDefinitions = [
		#   {
		#     'AttributeName': 'string',
		# 	  'AttributeType': 'S' | 'N' | 'B'
		#   },
		# ],
		# TableName = 'string',
		# KeySchema = [
		#   {
		#     'AttributeName': 'string',
		#     'KeyType': 'HASH' | 'RANGE'
		#   },
		# ],
		# LocalSecondaryIndexes = [
		#   {
		#     'IndexName': 'string',
		#     'KeySchema': [
		#       {
		#         'AttributeName': 'string',
		#         'KeyType': 'HASH' | 'RANGE'
		#       },
		#     ],
		#     'Projection': {
		#       'ProjectionType': 'ALL' | 'KEYS_ONLY' | 'INCLUDE',
		#       'NonKeyAttributes': [
		#         'string',
		#       ]
		#     }
		#   },
		# ],
		# GlobalSecondaryIndexes = [
		#   {
		#     'IndexName': 'string',
		#     'KeySchema': [
		#       {
		#         'AttributeName': 'string',
		#         'KeyType': 'HASH' | 'RANGE'
		#       },
		#     ],
		#     'Projection': {
		#       'ProjectionType': 'ALL' | 'KEYS_ONLY' | 'INCLUDE',
		#       'NonKeyAttributes': [
		#         'string',
		#       ]
		#     },
		#     'ProvisionedThroughput': {
		#       'ReadCapacityUnits': 123,
		#       'WriteCapacityUnits': 123
		#     }
		#   },
		# ],
		# BillingMode = 'PROVISIONED' | 'PAY_PER_REQUEST',
		# ProvisionedThroughput = {
		#   'ReadCapacityUnits': 123,
		#   'WriteCapacityUnits': 123
		# },
		# StreamSpecification = {
		#   'StreamEnabled': True | False,
		#   'StreamViewType': 'NEW_IMAGE' | 'OLD_IMAGE' | 'NEW_AND_OLD_IMAGES' | 'KEYS_ONLY'
		# },
		# SSESpecification = {
		#   'Enabled': True | False,
		#   'SSEType': 'AES256' | 'KMS',
		#   'KMSMasterKeyId': 'string'
		# },
		# Tags = [
		#   {
		#     'Key': 'string',
		#     'Value': 'string'
		#   },
		# ]
		#

		kwargs['TableName'] = doc

		table = self.client.create_table(**kwargs)

		for i in range(0, attempts):
			if table.table_status == 'ACTIVE':
				break
			sleep(delay/1000)

		return table.table_status == 'ACTIVE'

	def set(self, doc, **kwargs):
		# TODO : verify kwargs for DynamoDB
		#
		# required parameters are following :
		#
		# Item = {
		#   'string': 'string' | 123 | Binary(b'bytes') | True | None | set(['string']) | set([123]) | set(
		#   [Binary(b'bytes')]) | [] | {}
		# },
		# Expected = {
		#   'string': {
		#     'Value': 'string' | 123 | Binary(b'bytes') | True | None | set(['string']) | set([123]) | set([Binary(b'bytes')]) | [] | {},
		#     'Exists': True | False,
		#     'ComparisonOperator': 'EQ' | 'NE' | 'IN' | 'LE' | 'LT' | 'GE' | 'GT' | 'BETWEEN' | 'NOT_NULL' | 'NULL' | 'CONTAINS' | 'NOT_CONTAINS' | 'BEGINS_WITH',
		#     'AttributeValueList': [
		#       'string' | 123 | Binary(b'bytes') | True | None | set(['string']) | set([123]) | set([Binary(b'bytes')]) | [] | {},
		#     ]
		#   }
		# },
		# ReturnValues = 'NONE' | 'ALL_OLD' | 'UPDATED_OLD' | 'ALL_NEW' | 'UPDATED_NEW',
		# ReturnConsumedCapacity = 'INDEXES' | 'TOTAL' | 'NONE',
		# ReturnItemCollectionMetrics = 'SIZE' | 'NONE',
		# ConditionalOperator = 'AND' | 'OR',
		# ConditionExpression = Attr('myattribute').eq('myvalue'),
		# ExpressionAttributeNames = {
		#   'string': 'string'
		# },
		# ExpressionAttributeValues = {
		#   'string': 'string' | 123 | Binary(b'bytes') | True | None | set(['string']) | set([123]) | set([Binary(b'bytes')]) | [] | {}
		# }
		#

		kwargs['Item'] = Replace(kwargs['Item'], self.writer)

		table = self.client.Table(doc)
		response = table.put_item(**kwargs)

		# TODO : verify response

		return

	def get(self, doc, **kwargs):
		# TODO : verify kwargs for DynamoDB
		#
		# required parameters are following :
		#
		#
		# Key = {
		#   'string': 'string' | 123 | Binary(b'bytes') | True | None | set(['string']) | set([123]) | set([Binary(b'bytes')]) | [] | {}
		# },
		# AttributesToGet = [
		#   'string',
		# ],
		# ConsistentRead = True | False,
		# ReturnConsumedCapacity = 'INDEXES' | 'TOTAL' | 'NONE',
		# ProjectionExpression = 'string',
		# ExpressionAttributeNames = {
		#   'string': 'string'
		# }
		#

		table = self.client.Table(doc)
		response = table.get_item(**kwargs)

		if 'Item' not in response or not response['Item']:
			return None

		return Replace(response['Item'], self.reader)

	def count(self, doc, **kwargs):
		table = self.client.Table(doc)
		response = table.query(**kwargs)
		
		if 'Count' not in response or not response['Count']:
			return None
		return response['Count']
	
	def query(self, doc, **kwargs):
		# TODO : verify kwargs for DynamoDb
		#
		# required parameters are following :
		#
		# IndexName = 'string',
		# Select = 'ALL_ATTRIBUTES' | 'ALL_PROJECTED_ATTRIBUTES' | 'SPECIFIC_ATTRIBUTES' | 'COUNT',
		# AttributesToGet = [
		#   'string',
		# ],
		# Limit = 123,
		# ConsistentRead = True | False,
		# KeyConditions = {
		#   'string': {
		#     'AttributeValueList': [
		#       'string' | 123 | Binary(b'bytes') | True | None | set(['string']) | set([123]) | set([Binary(b'bytes')]) | [] | {},
		#     ],
		#     'ComparisonOperator': 'EQ' | 'NE' | 'IN' | 'LE' | 'LT' | 'GE' | 'GT' | 'BETWEEN' | 'NOT_NULL' | 'NULL' | 'CONTAINS' | 'NOT_CONTAINS' | 'BEGINS_WITH'
		#   }
		# },
		# QueryFilter = {
		#   'string': {
		#     'AttributeValueList': [
		#       'string' | 123 | Binary(b'bytes') | True | None | set(['string']) | set([123]) | set([Binary(b'bytes')]) | [] | {},
		#     ],
		#     'ComparisonOperator': 'EQ' | 'NE' | 'IN' | 'LE' | 'LT' | 'GE' | 'GT' | 'BETWEEN' | 'NOT_NULL' | 'NULL' | 'CONTAINS' | 'NOT_CONTAINS' | 'BEGINS_WITH'
		#   }
		# },
		# ConditionalOperator = 'AND' | 'OR',
		# ScanIndexForward = True | False,
		# ExclusiveStartKey = {
		#   'string': 'string' | 123 | Binary(b'bytes') | True | None | set(['string']) | set([123]) | set([Binary(b'bytes')]) | [] | {}
		# },
		# ReturnConsumedCapacity = 'INDEXES' | 'TOTAL' | 'NONE',
		# ProjectionExpression = 'string',
		# FilterExpression = Attr('myattribute').eq('myvalue'),
		# KeyConditionExpression = Key('mykey').eq('myvalue'),
		# ExpressionAttributeNames = {
		#   'string': 'string'
		# },
		# ExpressionAttributeValues = {
		# 	'string': 'string' | 123 | Binary(b'bytes') | True | None | set(['string']) | set([123]) | set(
		# 		[Binary(b'bytes')]) | [] | {}
		# }
		#
		
		if kwargs.get('ExclusiveStartKey', None) is not None:
			kwargs['ExclusiveStartKey'] = Replace(kwargs['ExclusiveStartKey'], self.reader)
		
		table = self.client.Table(doc)
		response = table.query(**kwargs)
		
		result = None
		if 'Items' not in response or not response['Items']:
			return result
		elif isinstance(response['Items'], list):
			result = [Replace(item, self.reader) for item in response['Items']]
		else:
			result = Replace(response['Item'], self.reader)
		
		return result

	def remove(self, doc, **kwargs):
		# TODO : verify kwargs for DynamoDB
		#
		# required parameters are following :
		#
		# Key = {
		#   'string': 'string' | 123 | Binary(b'bytes') | True | None | set(['string']) | set([123]) | set([Binary(b'bytes')]) | [] | {}
		# },
		# Expected = {
		#   'string': {
		#     'Value': 'string' | 123 | Binary(b'bytes') | True | None | set(['string']) | set([123]) | set([Binary(b'bytes')]) | [] | {},
		#     'Exists': True | False,
		#     'ComparisonOperator': 'EQ' | 'NE' | 'IN' | 'LE' | 'LT' | 'GE' | 'GT' | 'BETWEEN' | 'NOT_NULL' | 'NULL' | 'CONTAINS' | 'NOT_CONTAINS' | 'BEGINS_WITH',
		#     'AttributeValueList': [
		#       'string' | 123 | Binary(b'bytes') | True | None | set(['string']) | set([123]) | set([Binary(b'bytes')]) | [] | {},
		#     ]
		#   }
		# },
		# ConditionalOperator = 'AND' | 'OR',
		# ReturnValues = 'NONE' | 'ALL_OLD' | 'UPDATED_OLD' | 'ALL_NEW' | 'UPDATED_NEW',
		# ReturnConsumedCapacity = 'INDEXES' | 'TOTAL' | 'NONE',
		# ReturnItemCollectionMetrics = 'SIZE' | 'NONE',
		# ConditionExpression = Attr('myattribute').eq('myvalue'),
		# ExpressionAttributeNames = {
		#   'string': 'string'
		# },
		# ExpressionAttributeValues = {
		#   'string': 'string' | 123 | Binary(b'bytes') | True | None | set(['string']) | set([123]) | set([Binary(b'bytes')]) | [] | {}
		# }
		#

		table = self.client.Table(doc)
		response = table.delete_item(**kwargs)

		# TODO : verify response

		return

	def delete(self, doc):
		# TODO : verify kwargs for DynamoDB

		table = self.client.Table(doc)
		response = table.delete()

		# TODO : verify response

		return
