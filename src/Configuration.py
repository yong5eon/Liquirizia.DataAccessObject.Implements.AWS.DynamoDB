# -*- coding: utf-8 -*-

from Liquirizia.DataAccessObject import Configuration as BaseConfiguration

__all__ = (
	'Configuration'
)


class Configuration(BaseConfiguration):
	"""Configuration Class for AWS DynamoDB"""

	def __init__(self, token, secret, region, version=None):
		self.accessKey = token
		self.accessSecretKey = secret
		self.region = region
		self.version = version
		return
