import datetime
from datetime import date,timedelta
import setting
import subprocess,shlex
import time

def generateDateRange(fromDate,toDate):
	ans=[]
    #verify
	if len(fromDate)!=10 or len(toDate)!=10:
		return []
	else:
		i=0
		iDate=fromDate
		while True:
			ans.append(iDate)
			i+=1
			if iDate==toDate:
				break
			else:
				tmp=iDate.split('-')
				iDate=(date(int(tmp[0]),int(tmp[1]),int(tmp[2]))+timedelta(days=1)).isoformat()
	return ans

def saveFilePointer(machine,idate,position):
	f=open(setting.LOG_PATH+'fileposition-'+machine,'w')
	f.write(idate+' '+str(position))
	f.close()

def runSpecialCommand():
	command=setting.RUN_THIS_COMMAND_WHEN_THREAD_ERROR_IS_FOUND
	print "###################################################"
	print "###################################################"
	print "###################################################"
	result = subprocess.Popen(shlex.split(command))

def waitChildren(children):
	while True:
		time.sleep(5)
		allDie=True
		for aThread in children:
			if aThread.isAlive():
				allDie=False
				break

		if allDie:
			break
