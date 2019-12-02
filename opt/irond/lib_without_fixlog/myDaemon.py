import time,datetime
import subprocess,shlex
import threading
import Queue

from mainDaemon import Daemon

import setting
from reader.main import readRawLog
#from processor.main import processToElastic
from processor.main import runMyQueue
from util.general import runSpecialCommand
from util.general import waitChildren
#this is an example
#class MyDaemon(Daemon):
#	def run(self):
#		while True:
#			time.sleep(1)



class irond(Daemon):
	'''
	Monitoring walk Daemon
	'''

	def run(self):
		
		#read config
		startDate=setting.START_DATE
		startPointer=setting.START_POINTER
		endDate=setting.END_DATE
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

			processThread[machine]=threading.Thread(target=runMyQueue, args=(machine+'-msg',readQueue[machine],stopEvent))
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
					readQueue[machine].put('{"action":"break"}')
				waitChildren(myChildren)
				stopEvent.set()
				waitChildren(myReader)
				print str(datetime.datetime.now()),"run() ###return###"
				runSpecialCommand()
				return	#This will stop only this top thread, and never call "atexit"
						#If other threads are still running, the process will stay
			time.sleep(5)
			
