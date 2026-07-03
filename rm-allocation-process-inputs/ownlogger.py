import logging
from Constants import *


class OwnLogger:

	def __init__(self, runid, input):
		self.logger = logging.getLogger()
		self.logger.setLevel(logging.INFO)
		handler = logging.StreamHandler(sys.stdout)
		handler.setLevel(logging.INFO)
		formatter = logging.Formatter("%(asctime)s - %(levelname)-8s - %(message)s  ")
		handler.setFormatter(formatter)
		self.logger.addHandler(handler)
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
	def warning(self, errormessage):
		if self.input != DB:
			self.logger.error(f"error:{errormessage} runid:{self.runid}")
		else:
			print(errormessage)