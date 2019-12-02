#copy from ironportProcessor08.py
import sys,os

import threading
import Queue

import memcache
import elasticsearch
import json
import datetime,time

import copy
from copy import deepcopy

import email
from email.header import decode_header, make_header

from util.general import waitChildren
from util.returnStat import ReturnStat
import setting

SUBTHREAD_SIZE= setting.SUBTHREAD_SIZE
CACHE_TIME    = setting.CACHE_TIME #5 minutes
ESconnection  = setting.ES_CONNECTION
MCconnection  = setting.MC_CONNECTION

encodingName={
'windows-874':'cp874',
'x-windows-874':'cp874',
'x-mac-thai':'cp874'
}
#'utf-8':'utf-8',
#'charmap':'charmap',
#'iso2022_jp':'iso2022_jp'

def cleanSubject(message):
	message=message.replace(r"\\\'","'")
	message=message.replace(r"\'","'")
	message=message.replace(r'\"','"')
	return message

def decodeSubject(message):
	message=message.replace(r"\\\\r","")
	message=message.replace(r"\\\\n","")
	message=message.replace(r"\\\\t","")
	message=message.replace(r"\\r","")
	message=message.replace(r"\\n","")
	message=message.replace(r"\\t","")
	message=message.replace(r"?==?",r"?= =?")
	
	message=message.replace(r"\\\'","'")
	message=message.replace(r"\'","'")
	message=message.replace(r'\"','"')
	
	try:
		results=decode_header(message)
	except:
		print "HeaderParseError:"
		print message
		return "HeaderParseError"

	answer=u""
	for result in results:
		if result[1]==None:
			answer+=unicode(result[0])
		else:
			try:
				if result[1] in encodingName:
					answer+=unicode(result[0],encodingName[result[1]],'ignore')
				else:
					answer+=unicode(result[0],result[1],'ignore')
			except LookupError:
				answer+="[LookupError: encoding="+result[1]+"]"
				
	return answer.lower().encode('utf-8') 	#make it lowercase (it will be easier to search)


def screenCache(dataDict):
	take={}
	for akey in dataDict:
		if akey in ['beginTimestamp','hostname','ICID','interface','sourceIP','mx_name','authenUser','authenResult','FMID']:
			take[akey]=dataDict[akey]

	return take

def runSubQueue(threadName, myQueue, stopEvent, mc, pending, myStat):
	currentIndex=None
	checkpoint=1
	topFail=0
	branchFail=0
	
	updateFail=0
	outbound=0
	docmiss=0
	unknownError=0
	
	counter=0
	totalProcessTime=datetime.timedelta(0)
	
	es = elasticsearch.Elasticsearch(ESconnection)
	newline=None
	while (not stopEvent.is_set()):
		if newline!=None:
			line=newline
			newline=None
		else:
			line=myQueue.get()
			#print line

		if line['action']=='break':
			if line['clear']==False:
				print str(datetime.datetime.now()),threadName,"###finish###"
				myStat.setNormalExit()
				return
			else:
				print str(datetime.datetime.now()),threadName,"###return###"
				myStat.setErrorExit()
				return

		roundStartTime=datetime.datetime.now()
		
		if line['action']=='newTop':
			currentIndex=line['indexName']
			#copy data from pvalue to line['value']
			pvalue=pending[line['indexName']+'/'+line['value']['MID']]
			for akey in pvalue:
				line['value'][akey]=pvalue[akey]	#FMID is included 
			
			#initialize some variables
			line['value']['isTop']=True
			line['value']['isTail']=False
			line['value']['isGenerated']=False
			line['value']['isBounce']=False
			line['value']['isRewriter']=False
			line['value']['isRewritten']=False

			line['value']['finishStatus']='?'
			line['value']['firstReject']=[]

			line['value']['RID']=[]
			line['value']['to']=[]
			line['value']['DCID']=[]
			line['value']['rstatus']=[]
			line['value']['reasonMessage']=[]

			line['value']['bouncedCount']=0

			line['value']['SMID']=[]
			line['value']['BMID']=[]
			line['value']['GMID']=[]

			#create
			#startTimer=time.time()
			result=es.create(index=line['indexName'],doc_type=line['type'],refresh=False,body=line['value'],id=line['_id'],routing=line['value']['FMID'])			
			#addTime+=time.time()-startTimer
			#addCounter+=1
			if ('created' in result) and (result['created']):
				mc.set(str(line['indexName']+"/MID/"+line['_id']),screenCache(line['value']),time=CACHE_TIME) #for existence checking and for data copy
				pending.pop(line['indexName']+'/'+line['value']['MID'])
			else:
				pending.pop(line['indexName']+'/'+line['value']['MID'])
				print 'newTop has failed'
				topFail+=1

		elif line['action'] in ['newSplit','newRewrite','newGenerate','newBounce']:
			#copy data to line['value']
			pvalue=pending[line['indexName']+'/'+line['value']['MID']]
			for akey in pvalue:
				line['value'][akey]=pvalue[akey]	#FMID included
			
			#initialize some variables
			line['value']['isTop']=False
			line['value']['isTail']=False
			
			if line['action']=='newBounce':
				line['value']['isBounce']=True
			else:
				line['value']['isBounce']=False
			
			if line['action']=='newGenerate':
				line['value']['isGenerated']=True
			else:
				line['value']['isGenerated']=False
			
			if line['action']=='newRewrite':
				line['value']['isRewriter']=True
			else:
				line['value']['isRewritten']=False

			line['value']['finishStatus']='?'
			line['value']['firstReject']=[]

			line['value']['RID']=[]
			line['value']['to']=[]
			line['value']['DCID']=[]
			line['value']['rstatus']=[]
			line['value']['reasonMessage']=[]

			line['value']['bouncedCount']=0
			
			line['value']['SMID']=[]
			line['value']['BMID']=[]
			line['value']['GMID']=[]

			#create
			#startTimer=time.time()
			#result={}
			#try:
			result=es.create(index=line['indexName'],doc_type=line['type'],refresh=False,body=line['value'],id=line['_id'],parent=line['value']['hostname']+":"+line['value']['parentID'],routing=line['value']['FMID'])
			#except:
			#	print "new MID error:"
			#	print line
			#	print sys.exc_info()

			#addTime+=time.time()-startTimer
			#addCounter+=1
			if ('created' in result) and (result['created']):
				mc.set(str(line['indexName']+"/MID/"+line['_id']),screenCache(line['value']),time=CACHE_TIME) #for existence checking and for data copy
				pending.pop(line['indexName']+'/'+line['value']['MID'])

				#update parent by push new line to it's queue (to avoid VersionConflictEngineException that happened before)
				genLine={'action':'update', 'indexName':line['indexName'], 'type':'mnode', "_id":line['value']['hostname']+":"+line['value']['parentID']}
				genLine['routing']=line['value']['FMID']
				if line['action']=='newSplit':
					genLine['append']={'SMID':line['value']['MID']}
				
				elif line['action']=='newRewrite':
					genLine['value']={'isRewritten':True, 'rewriterID':line['value']['MID']}
				
				elif line['action']=='newGenerate':
					genLine['append']={'GMID':line['value']['MID']}

				elif line['action']=='newBounce':
					genLine['append']={'BMID':line['value']['MID']}
				
				#targetThread=int(line['value']['FMID'])%SUBTHREAD_SIZE
				myQueue.put(genLine)
			
			else:
				pending.pop(line['indexName']+'/'+line['value']['MID'])
				print 'new mnode has failed'
				branchFail+=1
		
		elif line['action']=='update' and line['type']=='mnode':
			#dequeue until the last mnode update
			bulklist=[]
			bulklist.append(line)
			while True:
				try:
					newline=myQueue.get_nowait()
					if newline['action']=='update' and newline['type']=='mnode':
						bulklist.append(newline)
					else:
						break
				except Queue.Empty:
					newline=None
					break
			
			#if len(bulklist)>4:
			#	print "bulksize:",len(bulklist)
			#convert bulklist to query
			bulkbody=[]

			for bline in bulklist:
				updateScript=''
				updateParam={}
				if 'value' in bline:
					for akey in bline['value']:
						updateScript+="ctx._source.%s=%s; "%(akey,akey)
						if akey=='from':
							updateParam[akey]=bline['value'][akey].lower()
						elif akey=='subject':
							updateParam[akey]=cleanSubject(bline['value'][akey])			#original subject
							updateScript+="ctx._source.readableSubject=readableSubject; "
							updateParam['readableSubject']=decodeSubject(bline['value'][akey])			#decode to readable character
						else:
							updateParam[akey]=bline['value'][akey]			#this way is good for sensitive information

				if 'append' in bline:
					for akey in bline['append']:
						updateScript+="ctx._source.%s+=%s; "%(akey,akey)
						if akey=='to':
							updateParam[akey]=bline['append'][akey].lower()
						else:
							updateParam[akey]=bline['append'][akey]

				if 'change' in bline:
					for akey in bline['change']:
						for apos in bline['change'][akey]['position'].split(','):
							#updateScript+="ctx._source.%s[%s]='%s'; "%(akey,apos.strip(),bline['change'][akey]['value'])
							#fix for supporting complicated message
							dummyParameterName=akey+apos.strip()
							updateScript+="ctx._source.%s[%s]=%s; "%(akey,apos.strip(),dummyParameterName)
							updateParam[dummyParameterName]=bline['change'][akey]['value']

						#hack for bouncedCount only
						if akey=='rstatus' and bline['change'][akey]['value']=='bounced':
							updateScript+="ctx._source.bouncedCount+=%d; "%(len(bline['change'][akey]['position'].split(',')),)
			
				bulkbody.append({
					"update":{
						"_id":bline['_id'],
						"_routing":bline['routing'],
						"_type":bline['type'],
						"_index":bline['indexName'],
						"_retry_on_conflict":5
					}
				})
				bulkbody.append({
					"script":updateScript,
					"params":updateParam
				})
			
			bresult=es.bulk(body=bulkbody,refresh=False)
			i=0
			for item in bresult['items']:
				result=item['update']
				
				if 'error' in result:				
					updateFail+=1
					if 'IndexOutOfBoundsException' in result['error']:
						outbound+=1
					elif 'DocumentMissingException' in result['error']:
						docmiss+=1
						print 'DocumentMissingException'
						print bulklist[i]
					else:
						unknownError+=1
						print "unknownError",result
						print bulklist[i]
						print 'update has failed:'
				
				i+=1

			counter+=len(bulklist)-1

			
		#finish process line
		counter+=1
		totalProcessTime+=(datetime.datetime.now()-roundStartTime)
		if counter/1000>checkpoint:
			checkpoint+=1
			allSeconds=float(totalProcessTime.microseconds + (totalProcessTime.seconds + totalProcessTime.days * 24 * 3600) * 10**6) / 10**6
			if allSeconds>0:
				qps=float(counter)/allSeconds
			else:
				qps=-1;
			
			currentTime=str(datetime.datetime.now()).rsplit(":",1)[0]
			print currentTime,currentIndex,threadName,'{0: >9}'.format(counter),"inQ:%s q/s:%.0f\ttopFail:%d\tbranchFail:%d\tupdateFail:%d\tdocmiss:%d\tunknown:%d"%('{0: >7}'.format(myQueue.qsize()),qps,topFail,branchFail,updateFail,docmiss,unknownError)
		
		#if threadName=='10.20.4.111-message.0' and counter>250:
		#	print 2/0

	print str(datetime.datetime.now()),threadName,"###stopEvent###"


def runICIDQueue(threadName, myQueue, stopEvent, mc, icPending, myStat):
	currentIndex=None
	checkpoint=1
	counter=0
	failCount=0
	totalProcessTime=datetime.timedelta(0)

	es = elasticsearch.Elasticsearch(ESconnection)

	while (not stopEvent.is_set()):
		newline=myQueue.get()
		
		roundStartTime=datetime.datetime.now()
		#gather bulklist
		bulklist=[]
		if newline['action']=='newICID' or newline['action']=='update':
			bulklist.append(newline)
		elif newline['action']=='break':
			if newline['clear']==False:
				print str(datetime.datetime.now()),threadName,"###finish###"
				myStat.setNormalExit()
				return
			else:
				print str(datetime.datetime.now()),threadName,"###return###"
				myStat.setErrorExit()
				return

		else:
			print str(datetime.datetime.now()),threadName,"###return###"
			myStat.setErrorExit()
			return
		
		while True:
			try:
				newline=myQueue.get_nowait()
				if newline['action']=='newICID' or newline['action']=='update':
					bulklist.append(newline)
					if len(bulklist)>100:
						break
				else:
					break
			except Queue.Empty:
				break
		
		if 'action' in newline and newline['action']=='break':
			if newline['clear']==True:
				print str(datetime.datetime.now()),threadName,"###return###"
				myStat.setErrorExit()
				return
			else:
				myQueue.put(newline)	#this thread will end in the next round
				pass

		#convert bulklist to bulk query
		bulkbody=[]
		for line in bulklist:
			
			if line['action']=='newICID':
				currentIndex=line['indexName']
				while True:
					try:
						linecopy=deepcopy(line)
						break
					except:		#RuntimeError: dictionary changed size during iteration
						continue
				line=linecopy
				bulkbody.append({
					"index":{
						"_index":line['indexName'],
						"_type":line['type'],
						"_id":line['_id'],
						"refresh":False
					}
				})
				bulkbody.append(line['value'])
				
				
			elif line['action']=='update' and line['type']=='icnode':
				#it's time to update
				updateScript=''
				updateParam={}
					
				for akey in line['value']:
					updateScript+="ctx._source.%s=%s; "%(akey,akey)
					updateParam[akey]=line['value'][akey]
				
				bulkbody.append({
					"update":{
						"_index":line['indexName'],
						"_type":line['type'],
						"_id":line['_id'],
						"refresh":False,
						"_retry_on_conflict":5
					}
				})
				bulkbody.append({
					"script":updateScript,
					"params":updateParam
				})
		
		bresult=es.bulk(body=bulkbody,refresh=False)
		i=0
		for item in bresult['items']:
			if 'index' in item:
				result=item['index']
				if ('status' in result) and (result['status']==201):
					mc.set(str(bulklist[i]['indexName']+"/ICID/"+bulklist[i]['_id']),bulklist[i]['value'],time=CACHE_TIME)
				else:
					print "newICID has failed"
					print "*>>>",result
					failCount+=1
				icPending.pop(bulklist[i]['indexName']+'/'+bulklist[i]['value']['ICID'])

			elif 'update' in item:
				result=item['update']
				if ('status' in result) and (result['status']==200):
					kvalue=bulklist[i]['kvalue']
					for akey in bulklist[i]['value']:
						kvalue[akey]=bulklist[i]['value'][akey]
					mc.set(str(bulklist[i]['indexName']+"/ICID/"+bulklist[i]['_id']),screenCache(kvalue),time=CACHE_TIME)
				else:
					print "update icnode has failed"
					print "*>>>",result
					failCount+=1
			else:
				failCount+=1
				print "a problem about bulk action: not index nor update"
				print item
			i+=1
			
		#finish process line
		counter+=len(bulklist)
		totalProcessTime+=(datetime.datetime.now()-roundStartTime)
		if counter/1000>checkpoint:
			checkpoint+=1
			allSeconds=float(totalProcessTime.microseconds + (totalProcessTime.seconds + totalProcessTime.days * 24 * 3600) * 10**6) / 10**6
			if allSeconds>0:
				qps=float(counter)/allSeconds
			else:
				qps=-1
			
			currentTime=str(datetime.datetime.now()).rsplit(":",1)[0]
			print currentTime,currentIndex,threadName,'{0: >9}'.format(counter),"inQ:%s q/s:%.0f\t\tfailCount:%d"%('{0: >7}'.format(myQueue.qsize()),qps,failCount)

	myStat.setErrorExit()
	print str(datetime.datetime.now()),threadName,"###stopEvent###"

def runDCIDQueue(threadName, myQueue, stopEvent, mc, myStat):
	currentIndex=None
	checkpoint=1
	counter=0
	failCount=0
	totalProcessTime=datetime.timedelta(0)

	es = elasticsearch.Elasticsearch(ESconnection)

	while (not stopEvent.is_set()):
		newline=myQueue.get()
		
		roundStartTime=datetime.datetime.now()
		#gather bulklist
		bulklist=[]
		if newline['action']=='newDCID' or newline['action']=='update':
			bulklist.append(newline)
		elif newline['action']=='break':
			if newline['clear']==False:
				print str(datetime.datetime.now()),threadName,"###finish###"
				myStat.setNormalExit()
				return
			else:
				print str(datetime.datetime.now()),threadName,"###return###"
				myStat.setErrorExit()
				return
		else:
			print str(datetime.datetime.now()),threadName,"###return###"
			myStat.setErrorExit()
			return
		
		while True:
			try:
				newline=myQueue.get_nowait()
				if newline['action']=='newDCID' or newline['action']=='update':
					bulklist.append(newline)
					if len(bulklist)>100:
						break
				else:
					break
			except Queue.Empty:
				break
		
		if 'action' in newline and newline['action']=='break':
			if newline['clear']==True:
				print str(datetime.datetime.now()),threadName,"###return###"
				myStat.setErrorExit()
				return
			else:
				myQueue.put(newline)	#this thread will end in the next round
				pass

		#convert bulklist to bulk query
		bulkbody=[]
		for line in bulklist:
			
			if line['action']=='newDCID':
				currentIndex=line['indexName']
				bulkbody.append({
					"index":{
						"_index":line['indexName'],
						"_type":line['type'],
						"_id":line['_id'],
						"refresh":False
					}
				})
				bulkbody.append(line['value'])
			
			elif line['action']=='update' and line['type']=='dcnode':
				updateScript=''
				updateParam={}
					
				for akey in line['value']:
					updateScript+="ctx._source.%s=%s; "%(akey,akey)
					updateParam[akey]=line['value'][akey]
				
				bulkbody.append({
					"update":{
						"_index":line['indexName'],
						"_type":line['type'],
						"_id":line['_id'],
						"refresh":False,
						"_retry_on_conflict":5
					}
				})
				bulkbody.append({
					"script":updateScript,
					"params":updateParam
				})
				
		bresult=es.bulk(body=bulkbody,refresh=False)
		i=0
		for item in bresult['items']:
			if 'index' in item:
				result=item['index']
				if ('status' not in result) or (result['status']!=201):
					print "newDCID has failed"
					failCount+=1
			elif 'update' in item:
				result=item['update']
			
				if ('status' not in result) or (result['status']!=200):
					#this might be normal failure (I didn't check DCID existence before)
					#print "update dcnode has failed"
					failCount+=1
				
			else:
				failCount+=1
				print "a problem about bulk action: not index nor update"
				print item
			i+=1
			
		#finish process line
		counter+=len(bulklist)
		totalProcessTime+=(datetime.datetime.now()-roundStartTime)
		if counter/1000>checkpoint:
			checkpoint+=1
			allSeconds=float(totalProcessTime.microseconds + (totalProcessTime.seconds + totalProcessTime.days * 24 * 3600) * 10**6) / 10**6
			if allSeconds>0:
				qps=float(counter)/allSeconds
			else:
				qps=-1;
			
			currentTime=str(datetime.datetime.now()).rsplit(":",1)[0]
			print currentTime,currentIndex,threadName,'{0: >9}'.format(counter),"inQ:%s q/s:%.0f\t\tfailCount:%d"%('{0: >7}'.format(myQueue.qsize()),qps,failCount)
	
	myStat.setErrorExit()
	print str(datetime.datetime.now()),threadName,"###stopEvent###"

def runMyQueue(threadName, myQueue, stopEvent, myStat):
	currentIndex=None
	newDCID=0
	newICID=0
	newTop=0
	newElse=0
	updateICID=0
	updateMID=0

	#addTime=0.0
	#addCounter=1
	#searchTime=0.0
	#searchCounter=1
	#updateTime=0.0
	#updateCounter=1
	
	watch=0

	notFoundCount=0
	missCount=0
	counter=0
	totalProcessTime=datetime.timedelta(0)
	
	lastCheckpointTimestamp=datetime.datetime.now()
	inQueueLast=0
	
	time.sleep(1)
	#print "start>>", startTime
	es = elasticsearch.Elasticsearch(ESconnection)
	mc = memcache.Client([MCconnection], debug=0)
	
	#start subthreads beforehand
	icStat=ReturnStat()
	icPending={}
	icQueue=Queue.Queue()
	icThread=threading.Thread(target=runICIDQueue, args=(threadName+'.I',icQueue,stopEvent,mc,icPending,icStat))
	icThread.start()
	
	dcStat=ReturnStat()
	dcQueue=Queue.Queue()
	dcThread=threading.Thread(target=runDCIDQueue, args=(threadName+'.D',dcQueue,stopEvent,mc,dcStat))
	dcThread.start()

	pending={}		# to remember target queue for pending MID
	subQueue=[]
	subThread=[]
	subStat=[]
	for i in range(SUBTHREAD_SIZE):
		newSubStat=ReturnStat()
		newQueue=Queue.Queue()
		subQueue.append(newQueue)
		newThread=threading.Thread(target=runSubQueue, args=(threadName+'.'+str(i),newQueue,stopEvent,mc,pending,newSubStat))
		subThread.append(newThread)
		subStat.append(newSubStat)
		newThread.start()

	while (not stopEvent.is_set()):
		#print "start"

		rawline=myQueue.get()
		roundStartTime=datetime.datetime.now()
		#this line is to escape double quote and to make all string possible for json.loads
		#rawline=rawline.replace("\\", r"\\").replace(r"\"","\\"+r"\"").replace(r"\"}}","\"}}")
		try:
			line=json.loads(rawline)
		except ValueError:
			print "ValueError:"
			print rawline
			continue

		if line['action']=='break':
			
			if line['clear']==True:
				icQueue.queue.clear()
				dcQueue.queue.clear()
			
			icQueue.put(line)
			dcQueue.put(line)

			for aQueue in subQueue:
				if line['clear']==True:
					aQueue.queue.clear()
				aQueue.put(line)
			#time.sleep(5)	
			waitChildren(subThread+[icThread,dcThread])
			
			if line['clear']==True:
				print str(datetime.datetime.now()),threadName,"###return###"
				myStat.setErrorExit()
				return
			else:
				print  str(datetime.datetime.now()),threadName,'###EOF###'
				myStat.setNormalExit()
				return

		result={'ok':False}
		if line['action']=='newDCID' or (line['action']=='update' and line['type']=='dcnode'):
			newDCID+=1
			dcQueue.put(line)

		elif line['action']=='newICID':
			currentIndex=line['indexName']
			newICID+=1
			icPending[line['indexName']+'/'+line['value']['ICID']]=line['value']
			icQueue.put(line)
			#print "heyyyyyy"
		
		elif line['action']=='update' and line['type']=='icnode':
			ICIDexist=False
			updateICID+=1
			#search parent ICID in icPending and update it
			ICID=line['_id'].split(':')[1]
			pendingValue=None
			#this if should be lock (racing -> dictionary change size during iteration in .create)
			if line['indexName']+'/'+ICID in icPending:
				ICIDexist=True
				#update icPending
				try:
					for akey in line['value']:
						icPending[line['indexName']+'/'+ICID][akey]=line['value'][akey]
					pendingValue=icPending[line['indexName']+'/'+ICID]
				except:
					pass

			kvalue=mc.get(str(line['indexName']+"/ICID/"+ICID))
			if kvalue!=None:
				ICIDexist=True
				#update cache
				for akey in line['value']:
					kvalue[akey]=line['value'][akey]
							
				mc.set(str(line['indexName']+"/ICID/"+line['_id']),screenCache(kvalue),time=CACHE_TIME)
			else:
				#cache miss (check existence and update cache)
				missCount+=1
				try:
					getResult=es.get(index=line['indexName'],id=line['_id'],doc_type="icnode",refresh=True)
					kvalue=screenCache(getResult['_source'])
					#update cache
					for akey in line['value']:
						kvalue[akey]=line['value'][akey]
					mc.set(str(getResult['_index']+"/ICID/"+getResult['_id']),kvalue,time=CACHE_TIME)
					ICIDexist=True
				except elasticsearch.exceptions.NotFoundError:
					notFoundCount+=1

			if ICIDexist:
				#this line are allowed to enqueue
				if pendingValue!=None:
					line['kvalue']=pendingValue
				else:
					line['kvalue']=kvalue
				icQueue.put(line)
		
		elif line['action']=='newTop':
			newTop+=1
			pvalue=None
			#search parent ICID in icPending
			#if line['indexName']+'/'+line['value']['parentID'] in icPending:
			try:
				pvalue=deepcopy(icPending[line['indexName']+'/'+line['value']['parentID']])
			#else:
			except KeyError:
				#pending miss
				#search ICID from cache
				
				#startTimer=time.time()
				pvalue=mc.get(str(line['indexName']+"/"+line['value']['parentType']+"/"+line['value']['hostname']+":"+line['value']['parentID']))
				if pvalue==None:
					#cache miss
					missCount+=1
					try:
						getResult=es.get(index=line['indexName'],id=line['value']['hostname']+":"+line['value']['parentID'],doc_type="icnode",refresh=True)
						pvalue=screenCache(getResult['_source'])
						mc.set(str(getResult['_index']+"/ICID/"+getResult['_id']),pvalue,time=CACHE_TIME)
					except elasticsearch.exceptions.NotFoundError:
						#print "newTop cannot find its ICID parent:",line
						watch+=1
						pvalue=None
				#searchTime+=time.time()-startTimer
				#searchCounter+=1

			if pvalue!=None:
				pvalue['FMID']=line['value']['MID']
				targetThread=int(pvalue['FMID'])%SUBTHREAD_SIZE
				pending[line['indexName']+"/"+line['value']['MID']]=pvalue
				subQueue[targetThread].put(line)
				#print "puttttt"
		
		elif line['action'] in ['newSplit','newRewrite','newGenerate','newBounce']:
			newElse+=1
			pvalue=None
			#search parent MID in pending
			#if line['indexName']+'/'+line['value']['parentID'] in pending:
			try:
				#pending hit
				pvalue=pending[line['indexName']+'/'+line['value']['parentID']]
			#else:
			except KeyError:
				#pending miss
				#search parent MID from cache
				startTimer=time.time()
				pvalue=mc.get(str(line['indexName']+"/"+line['value']['parentType']+"/"+line['value']['hostname']+":"+line['value']['parentID']))
				if pvalue==None:
					#cache miss (cannot use es.get because I don't know its routing (FMID))
					missCount+=1
					client=elasticsearch.client.IndicesClient(es)
					client.refresh(line['indexName'])
					searchResult=es.search(index=line['indexName'],doc_type='mnode',size=1,body={
						"query":{
							"constant_score": {
								"filter" : {
									"term" : {"_id": line['value']['hostname']+":"+line['value']['parentID']}    
								}	
							}
						}
					})
					
					if searchResult['hits']['total']==1:
						pvalue=screenCache(searchResult['hits']['hits'][0]['_source'])	#for copy to new child node
						mc.set(str(searchResult['hits']['hits'][0]['_index']+"/MID/"+searchResult['hits']['hits'][0]['_id']),pvalue,time=CACHE_TIME)
					
					else:
						notFoundCount+=1

				#searchTime+=time.time()-startTimer
				#searchCounter+=1

			if pvalue!=None:
				targetThread=int(pvalue['FMID'])%SUBTHREAD_SIZE
				pending[line['indexName']+'/'+line['value']['MID']]=pvalue
				subQueue[targetThread].put(line)
		
		elif line['action']=='update' and line['type']=='mnode':	
			updateMID+=1
			routing=None
			MID=line['_id'].split(':')[1]
			#if line['indexName']+'/'+MID in pending:
			try:	
				#pending hit
				routing=pending[line['indexName']+'/'+MID]['FMID']
			#else:
			except KeyError:
				#pending miss
				kvalue=mc.get(str(line['indexName']+"/MID/"+line['_id']))
				if kvalue!=None:
					#cache hit
					routing=kvalue['FMID']
				else:
					#cache miss
					missCount+=1
					client=elasticsearch.client.IndicesClient(es)
					client.refresh(line['indexName'])
					searchResult=es.search(index=line['indexName'],doc_type='mnode',size=1,body={
						"query":{
							"constant_score": {
								"filter" : {
									"term" : {"_id": line['_id']}    
								}	
							}
						}
					})
					
					if searchResult['hits']['total']==1:
						kvalue=screenCache(searchResult['hits']['hits'][0]['_source'])	
						mc.set(str(searchResult['hits']['hits'][0]['_index']+"/MID/"+searchResult['hits']['hits'][0]['_id']),kvalue,time=CACHE_TIME)
						routing=kvalue['FMID']

			if routing!=None:
				line['routing']=routing
				targetThread=int(line['routing'])%SUBTHREAD_SIZE
				subQueue[targetThread].put(line)
		
		else:
			print line['action'],"should not be found as an action. please check pattern."

		totalProcessTime+=(datetime.datetime.now()-roundStartTime)
		counter+=1
		if counter%4000==0 or (counter<1000 and counter%100==0): # or (counter<10 and counter%2==0):
			allSeconds=float(totalProcessTime.microseconds + (totalProcessTime.seconds + totalProcessTime.days * 24 * 3600) * 10**6) / 10**6
			if allSeconds>0:
				qps=float(counter)/allSeconds
			else:
				qps=-1;
			#addAvg=float(addTime)/addCounter
			#searchAvg=float(searchTime)/searchCounter
			#updateAvg=float(updateTime)/updateCounter
			intervalTime=(datetime.datetime.now()-lastCheckpointTimestamp)
			intervalSec=float(intervalTime.microseconds + (intervalTime.seconds + intervalTime.days * 24 * 3600) * 10**6) / 10**6
			lastCheckpointTimestamp=datetime.datetime.now()
			inQueueNow=myQueue.qsize()
			qChangeRate=float(inQueueNow-inQueueLast)/intervalSec
			inQueueLast=inQueueNow

			currentTime=str(datetime.datetime.now()).rsplit(":",1)[0]
			print currentTime,currentIndex,threadName+".M",'{0: >9}'.format(counter),"inQ:%s q/s:%.0f cps:%+.1f notFound:%d,%d miss:%d(%.2f) pending:%d icp:%d"%('{0: >7}'.format(inQueueNow),qps,qChangeRate,notFoundCount,watch,missCount,float(missCount*100)/counter,len(pending),len(icPending))
			#print "newICID",newICID,"updateICID",updateICID,"newTop",newTop,"newElse",newElse,"updateMID",updateMID
			#print"\r"+str(counter)+"\tq/sec:%.3f\tadd:%.3f\tsearch:%.3f\tupdate:%.3f\tcount(add,update):(%d,%d)"%(qps,addAvg,searchAvg,updateAvg,addCounter,updateCounter) ,
			
			#check all my thread
			dead=False
			for i in range(len(subThread)):
				if (not subThread[i].isAlive()) and subStat[i].getStat()=='error':
					print str(datetime.datetime.now()),"DEAD Detected at a subthread of",threadName
					dead=True
					break
			if (not icThread.isAlive()) and icStat.getStat()=='error':
				print str(datetime.datetime.now()),"DEAD Detected at IC thread of",threadName
				dead=True
			if (not dcThread.isAlive()) and dcStat.getStat()=='error':
				print str(datetime.datetime.now()),"DEAD Detected at DC thread of",threadName
				dead=True

			if dead:
				breakLine={'action':'break','clear':True}
				icQueue.queue.clear()
				icQueue.put(breakLine)
				dcQueue.queue.clear()
				dcQueue.put(breakLine)
				for aQueue in subQueue:
					aQueue.queue.clear()
					aQueue.put(breakLine)
				#time.sleep(5)	
				#runSpecialCommand() #this should be run in myDaemon only
				waitChildren(subThread+[icThread,dcThread])
				print str(datetime.datetime.now()),threadName,"###return###"
				#print "current line",line
				myStat.setErrorExit()
				return
	
	print str(datetime.datetime.now()),threadName,"###stopEvent###"
	myStat.setErrorExit()
	return

"""
def processToElastic(myName,inputQueue,stopEvent):

		queueMap={}			#map from threadName to target queue
		threadMap={}
		#stopEvent=threading.Event()
		counter=0
	#try:
		while (not stopEvent.is_set()):
			raw_line=inputQueue.get()
			#seperate raw_line
			threadName,query=raw_line.split(' ',1)
			
			if threadName not in queueMap:
				if threadName=='break':
					myChildren=[]
					for i in queueMap:
						queueMap[i].queue.clear()
						queueMap[i].put(query)	#this query is {'action':'break'}
						myChildren.append(threadMap[i])
					waitChildren(myChildren)	
					print str(datetime.datetime.now()),myName,"###return###"
					return

				else:
					#create new queue and thread
					newQueue=Queue.Queue()
					queueMap[threadName]=newQueue
					newThread=threading.Thread(target=runMyQueue, args=(threadName,newQueue,stopEvent))
					threadMap[threadName]=newThread
					newThread.start()
			else:
				#put data in that thread
				queueMap[threadName].put(query)
				counter+=1
				if counter%1000==0:
					counter=0
					#check thread
					dead=False
					for i in threadMap:
						if not threadMap[i].isAlive():
							print str(datetime.datetime.now()),i,"DEAD Detected"
							dead=True
							break
					if dead:
						myChildren=[]
						for i in queueMap:
							queueMap[i].queue.clear()
							queueMap[i].put("{'action':'break'}")
							myChildren.append(threadMap[i])
						waitChildren(myChildren)
						print str(datetime.datetime.now()),myName,"###return###"
						#print "currentQuery",query
						return
		
		print str(datetime.datetime.now()),threadName,"###stopEvent###"
						#runSpecialCommand()
	#except (KeyboardInterrupt,):
	#	stopEvent.set()

	#stopEvent.set()
"""
