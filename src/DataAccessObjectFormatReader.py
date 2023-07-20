# -*- coding: utf-8 -*-

from datetime import datetime, date

from decimal import Decimal

__all__ = (
	'DataAccessObjectFormatReader'
)


class DataAccessObjectFormatReader(object):
	def __call__(self, o):
		if isinstance(o, str):
			try:
				return date.fromisoformat(o)
			except:
				pass
			try:
				return datetime.fromisoformat(o)
			except:
				pass
		if isinstance(o, Decimal):
			o = float(o)
			if o.is_integer():
				return int(o)
			return o
		return o
