import time,datetime
import subprocess,shlex
import threading
import Queue

from mainDaemon import Daemon

import setting
from main import run
#from reader.main import readRawLog
#from processor.main import processToElastic
#from util.general import runSpecialCommand
#from util.general import waitChildren
#this is an example
#class MyDaemon(Daemon):
#	def run(self):
#		while True:
#			time.sleep(1)



class axigend(Daemon):
	'''
	Monitoring walk Daemon
	'''

	def run(self):
		
		#read config
		startDate=setting.START_DATE
		startPointer=setting.START_POINTER
		endDate=setting.END_DATE
		HOST_LIST=setting.HOST_LIST
		run(startDate,startPointer,endDate,HOST_LIST)

		"""
		readThread={}
		readQueue={}
		processThread={}
		
		stopEvent=threading.Event()
		myChildren=[]
		myReader=[]

		for machine in setting.HOST_LIST:
			readQueue[machine]=Queue.Queue()
			readThread[machine]=threading.Thread(target=readRawLog, args=(machine,startDate,startPointer,endDate,readQueue[machine],stopEvent))
			readThread[machine].start()
			myReader.append(readThread[machine])

			processThread[machine]=threading.Thread(target=processToElastic, args=(machine+'TOP',readQueue[machine]))
			processThread[machine].start()
			myChildren.append(processThread[machine])
			
		while True:
			#monitor threads
			dead=False
			for machine in setting.HOST_LIST:
				if not readThread[machine].isAlive():
					print str(datetime.datetime.now()),"readThread",machine,"DEAD Detected"
					dead=True
					break
				if not processThread[machine].isAlive():
					print str(datetime.datetime.now()),"processThread",machine,"DEAD Detected"
					dead=True
					break
			if dead:
				for machine in setting.HOST_LIST:
					readQueue[machine].queue.clear()
					readQueue[machine].put('broadcast {"action":"break"}')
				waitChildren(myChildren)
				stopEvent.set()
				waitChildren(myReader)
				print str(datetime.datetime.now()),"run() ###runSpecialCommand###"
				runSpecialCommand()
			
			time.sleep(5)
		"""
