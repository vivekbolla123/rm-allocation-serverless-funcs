import logging

class OwnLogger:

	def __init__(self, runid):
		self.logger = logging.getLogger()
		self.logger.setLevel(logging.INFO)
		self.runid = runid

	def info(self, message):
			self.logger.info(f"message:{message}  runid:{self.runid}")

	def error(self, errormessage):
			self.logger.error(f"error:{errormessage} runid:{self.runid}")
