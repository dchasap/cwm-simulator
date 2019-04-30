

class Logger:

	def __init__(self, logfile, verbose):
		self.log = open(logfile, 'w')
		self.verbose = verbose

	def write(self, message):
		if self.verbose:
			self.log.write(message)
			self.log.flush()

