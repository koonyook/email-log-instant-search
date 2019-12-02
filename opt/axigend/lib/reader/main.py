import datetime,time,os,os.path,string,thread
import gzip
import sys
import setting
from util.general import generateDateRange,saveFilePointer
from reader.mapper import translateToJSON

def readRawLog(machine,startDate,startPointer,endDate,targetQueue,stopEvent,myStat):
	'''
	read raw log from only one host in setting range and put in targetQueue line by line
	'''
	print "start readRawLog from",machine
	today=str(datetime.date.today())
	fp = open(setting.PATTERN_FILE,'r')
	patterns=fp.readlines()
	for i in range(len(patterns)):
		patterns[i]=patterns[i].strip()
	fp.close()
	#classify task
	if startDate=="" and endDate=="":
		#deal with current fresh log 
		#loop until the the current file exist
		filepath=""
		currentDate=None
		readLimit=-1	#read until the end of file
		fp=None
		while fp is None:
			filepath,currentDate = changefile(machine)
			if os.path.exists(filepath):
				fp = open(filepath,'r')
				st_results = os.stat(filepath)
				where = st_results[6]
				if startPointer==-1 or startPointer>=where:
					fp.seek(where)	#seek to the end to file
				else:
					fp.seek(startPointer)
					readLimit=10000		#read with limited char
			else:
				time.sleep(5)
	
		rest=""
		while True:
			#last = where
			#where = fp.tell()
			#print "last = " + str(last) +" where = "+str(where)
			line = fp.read(readLimit)
			saveFilePointer(machine,currentDate,fp.tell())
			if not line:	#read to the End of File
				#check whether the new day have already start
				
				if filepath != changefile(machine)[0]:
					#perform last read and clear the rest
					time.sleep(10)	#give some time for the last set of log in that day
					rest+=fp.read(readLimit)
					saveFilePointer(machine,currentDate,fp.tell())
					breakAndPushToQueue(rest,patterns,targetQueue)
					rest=""
					fp.close()
					
					#loop until the new file is ready
					while True:
						fileToCheck,currentDate=changefile(machine)
						if os.path.exists(fileToCheck):
							filepath=fileToCheck
							fp = open(filepath,'r')
							break
						else:
							time.sleep(5)
							continue
					
				else:
					#keep using old file
					pass
			else:
				splitResult=(rest+line).rsplit('\n',1)
				if len(splitResult)==2:
					line,rest=splitResult
					breakAndPushToQueue(line,patterns,targetQueue)	
				else:
					#no newline nothing to print
					rest=splitResult[0]
			#delay between reading
			time.sleep(5)
		fp.close()
	
	elif startDate<=endDate and endDate<today:
		#fixing mode
		readLimit=10000
		for iDate in generateDateRange(startDate,endDate):
			filepathNormal=setting.INPUT_PATH+machine+'/'+string.replace(iDate,'-','/')
			filepathGZ=filepathNormal+'.gz'
			if not (os.path.exists(filepathNormal) or os.path.exists(filepathGZ)):
				#there is no log for this day
				continue
			elif os.path.exists(filepathNormal):
				filepath=filepathNormal
				fp = open(filepath,'r')
			elif os.path.exists(filepathGZ):
				filepath=filepathGZ
				fp=gzip.GzipFile(filepath,'r')
			
			st_results = os.stat(filepath)
			where = st_results[6]
			if startPointer==-1 or startPointer>=where:
				fp.seek(0)  #seek to the end to file
			else:
				fp.seek(startPointer)
			rest=""
			while (not stopEvent.is_set()):
				while targetQueue.qsize() >= setting.MAX_INQUEUE:
					print "READER queue of",machine,"is limited."
					time.sleep(5)

				line = fp.read(readLimit)
				if not line:    #read to the End of File
					breakAndPushToQueue(rest,patterns,targetQueue)
					rest=""
					break
				else:
					splitResult=(rest+line).rsplit('\n',1)
					if len(splitResult)==2:
						line,rest=splitResult
						breakAndPushToQueue(line,patterns,targetQueue)
					else:
						#no newline nothing to print
						rest=splitResult[0]
			fp.close()

			if (stopEvent.is_set()):
				myStat.setErrorExit()
				return

	else:
		print "invalid START_DATE and END_DATE"
		myStat.setErrorExit()
		return

	targetQueue.put('normalbroadcast {"action":"break","clear":false}')
	myStat.setNormalExit()
	print str(datetime.datetime.now())+" READER of",machine,"stop (finish the work)"

def breakAndPushToQueue(text,patterns,targetQueue):
	for line in text.split('\n'):
		line=translateToJSON(line.strip(),patterns)
		if line!=None:
			targetQueue.put(line)

def changefile(machine):
	#for normal operation
	currentDate=str(datetime.date.today())
	return setting.INPUT_PATH+machine+'/'+string.replace(str(datetime.date.today()),'-','/'),currentDate

	#for testing without realtime data condition
	#global dateOffset
	#dateOffset+=1
	#if startDate+datetime.timedelta(days=dateOffset) > datetime.date.today():
	#	print "end of data"
	#	sys.exit()
	#return base+machine+'/'+string.replace(str(startDate+datetime.timedelta(days=dateOffset)),'-','/')


