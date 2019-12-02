import time,datetime
import subprocess,shlex
import threading
import Queue

from mainDaemon import Daemon

#import setting
from reader.main import readRawLog
from processor.main import processToElastic
#from util.general import runSpecialCommand
from util.general import waitChildren
from util.returnStat import ReturnStat


def run(startDate,startPointer,endDate,HOST_LIST):

	readThread={}
	readQueue={}
	processThread={}

	readStatus={}
	processStatus={}

	stopEvent=threading.Event()
	myChildren=[]
	myReader=[]

	for machine in HOST_LIST:
		readQueue[machine]=Queue.Queue()
		newStatus=ReturnStat()
		readThread[machine]=threading.Thread(target=readRawLog, args=(machine,startDate,startPointer,endDate,readQueue[machine],stopEvent,newStatus))
		readThread[machine].start()
		myReader.append(readThread[machine])
		readStatus[machine]=newStatus
		
		newStatus=ReturnStat()
		processThread[machine]=threading.Thread(target=processToElastic, args=(machine+'_TOP',readQueue[machine],stopEvent,newStatus))
		processThread[machine].start()
		myChildren.append(processThread[machine])
		processStatus[machine]=newStatus
		
	while True:
		#monitor threads
		dead=False
		for machine in HOST_LIST:
			if not readThread[machine].isAlive():
				if readStatus[machine].getStat()=='error':
					print str(datetime.datetime.now()),"readThread",machine,"DEAD Detected"
					dead=True
					break
			if not processThread[machine].isAlive():
				if processStatus[machine].getStat()=='error':
					print str(datetime.datetime.now()),"processThread",machine,"DEAD Detected"
					dead=True
					break
		if dead:
			for machine in HOST_LIST:
				readQueue[machine].queue.clear()
				readQueue[machine].put('clearbroadcast {"action":"break","clear":true}')
			waitChildren(myChildren)
			stopEvent.set()
			waitChildren(myReader)
			print str(datetime.datetime.now()),"run() ###return###"
			break
		
		else:
			#check if all the process finish successfully
			allend=True
			for machine in HOST_LIST:
				if readThread[machine].isAlive() or processThread[machine].isAlive() or readStatus[machine].getStat()!='normal' or processStatus[machine].getStat()!='normal':
					allend=False
					break
			if allend:
				print str(datetime.datetime.now()),"run() ###allend###"
				return

		time.sleep(5)
