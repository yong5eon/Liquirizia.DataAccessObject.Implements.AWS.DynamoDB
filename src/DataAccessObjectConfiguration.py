# -*- coding: utf-8 -*-

from Liquirizia.DataAccessObject import DataAccessObjectConfiguration as DataAccessObjectConfigurationBase

__all__ = (
	'DataAccessObjectConfiguration'
)


class DataAccessObjectConfiguration(DataAccessObjectConfigurationBase):
	"""
	Data Access Object Configuration Class for DynamoDB of AWS
	"""

	def __init__(self, token, secret, region, version=None):
		self.accessKey = token
		self.accessSecretKey = secret
		self.region = region
		self.version = version
		return
