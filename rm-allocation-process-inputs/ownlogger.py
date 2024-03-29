import logging
from Constants import *


class OwnLogger:

	def __init__(self, runid, input):
		self.logger = logging.getLogger()
		self.logger.setLevel(logging.INFO)
		self.runid = runid
		self.input = input.logs[CONST_RUN_AUDIT]

	def info(self, messsage):
		if self.input == DB:
			self.logger.info(f"message:{messsage}  runid:{self.runid}")
		else:
			print(messsage)

	def error(self, errormessage):
		if self.input == DB:
			self.logger.error(f"error:{errormessage} runid:{self.runid}")
		else:
			print(errormessage)
