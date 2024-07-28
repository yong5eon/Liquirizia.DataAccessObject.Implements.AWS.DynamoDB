# -*- coding: utf-8 -*-

from datetime import datetime, date
from decimal import Decimal

__all__ = (
	'Encoder'
)


class Encoder(object):
	def __call__(self, o):
		if isinstance(o, float):
			# TODO : 부동소수점 문제 해결
			return Decimal(o)
		if isinstance(o, (date, datetime)):
			return o.isoformat()
		return o
