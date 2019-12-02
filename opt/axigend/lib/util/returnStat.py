
class ReturnStat:

	def __init__(self):
		self.status='default'

	def setNormalExit(self):
		self.status='normal'

	def setErrorExit(self):
		self.status='error'

	def getStat(self):
		return self.status
