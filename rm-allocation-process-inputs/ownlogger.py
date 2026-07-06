import logging
from Constants import *
 
 
class OwnLogger:

	def __init__(self, runid, input):
		self.logger = logging.getLogger()
		self.logger.setLevel(logging.INFO)
		if not self.logger.handlers:
			handler = logging.StreamHandler(sys.stdout)
			handler.setLevel(logging.INFO)
			formatter = logging.Formatter("%(asctime)s - %(levelname)-8s - %(message)s  ")
			handler.setFormatter(formatter)
			self.logger.addHandler(handler)
		self.runid = runid
		self.input = input.logs[CONST_RUN_AUDIT]
		self.current_key = ""

	def set_key(self, key):
		self.current_key = key

	def info(self, messsage):
		key_str = f" [{self.current_key}]" if self.current_key else ""
		if self.input == DB:
			self.logger.info(f"message:{messsage}  runid:{self.runid}{key_str}")
		else:
			print(f"{messsage}{key_str}")

	def error(self, errormessage):
		key_str = f" [{self.current_key}]" if self.current_key else ""
		if self.input == DB:
			self.logger.error(f"error:{errormessage} runid:{self.runid}{key_str}")
		else:
			print(f"{errormessage}{key_str}")

	def warning(self, errormessage):
		key_str = f" [{self.current_key}]" if self.current_key else ""
		if self.input == DB:
			self.logger.warning(f"warning:{errormessage} runid:{self.runid}{key_str}")
		else:
			print(f"warning:{errormessage}{key_str}")
