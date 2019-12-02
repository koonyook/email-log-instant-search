#!/usr/bin/python
import sys

import threading
import Queue

import memcache
import elasticsearch
import json
import datetime,time

import email
from email.header import decode_header, make_header

SUBTHREAD_SIZE=10
CACHE_TIME=300 #5 minutes
ESconnection = "10.20.4.26:9200"
MCconnection = "10.20.4.26:11211"

encodingName={
'windows-874':'cp874',
'x-windows-874':'cp874'
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
		if akey in ['hostname','ICID','interface','sourceIP','mx_name','authenUser','authenResult','FMID']:
			take[akey]=dataDict[akey]

	return take

def runSubQueue(threadName, myQueue, stopEvent, mc, pending):
	notFoundCount=0
	missCount=0
	counter=0
	totalProcessTime=datetime.timedelta(0)
	
	es = elasticsearch.Elasticsearch(ESconnection)
	
	while (not stopEvent.is_set()):
		line=myQueue.get()

		if line['action']=='break':
			break

		roundStartTime=datetime.datetime.now()
		
		if line['action']=='newTop':
			#copy data from pvalue to line['value']
			pvalue=pending[line['value']['MID']]
			for akey in pvalue:
				line['value'][akey]=pvalue[akey]	#FMID is included 
			
			#initialize some variables
			line['value']['isTop']='T'
			line['value']['isTail']='F'
			line['value']['isGenerated']='F'
			line['value']['isBounce']='F'
			line['value']['isRewriter']='F'
			line['value']['isRewritten']='F'

			line['value']['finishStatus']='?'
			line['value']['firstReject']=[]

			line['value']['RID']=[]
			line['value']['to']=[]
			line['value']['DCID']=[]
			line['value']['rstatus']=[]

			line['value']['SMID']=[]
			line['value']['BMID']=[]
			line['value']['GMID']=[]

			#create
			#startTimer=time.time()
			result=es.create(index=line['indexName'],doc_type=line['type'],refresh=False,body=line['value'],id=line['_id'],routing=line['value']['FMID'])			
			#addTime+=time.time()-startTimer
			#addCounter+=1
			if ('ok' in result) and (result['ok']):
				mc.set(str(line['indexName']+"/MID/"+line['_id']),screenCache(line['value']),time=CACHE_TIME) #for existence checking and for data copy
				pending.pop(line['value']['MID'])
			else:
				pending.pop(line['value']['MID'])
				print 'newTop has failed'

		elif line['action'] in ['newSplit','newRewrite','newGenerate','newBounce']:
			#copy data to line['value']
			pvalue=pending[line['value']['MID']]
			for akey in pvalue:
				line['value'][akey]=pvalue[akey]	#FMID included
			
			#initialize some variables
			line['value']['isTop']='F'
			line['value']['isTail']='F'
			
			if line['action']=='newBounce':
				line['value']['isBounce']='T'
			else:
				line['value']['isBounce']='F'
			
			if line['action']=='newGenerate':
				line['value']['isGenerated']='T'
			else:
				line['value']['isGenerated']='F'
			
			if line['action']=='newRewrite':
				line['value']['isRewriter']='T'
			else:
				line['value']['isRewritten']='F'

			line['value']['finishStatus']='?'
			line['value']['firstReject']=[]

			line['value']['RID']=[]
			line['value']['to']=[]
			line['value']['DCID']=[]
			line['value']['rstatus']=[]

			line['value']['SMID']=[]
			line['value']['BMID']=[]
			line['value']['GMID']=[]

			#create
			#startTimer=time.time()
			result=es.create(index=line['indexName'],doc_type=line['type'],refresh=False,body=line['value'],id=line['_id'],parent=line['value']['hostname']+":"+line['value']['parentID'],routing=line['value']['FMID'])
			#addTime+=time.time()-startTimer
			#addCounter+=1
			if ('ok' in result) and (result['ok']):
				mc.set(str(line['indexName']+"/MID/"+line['_id']),screenCache(line['value']),time=CACHE_TIME) #for existence checking and for data copy
				pending.pop(line['value']['MID'])

				#update parent by push new line to it's queue (to avoid VersionConflictEngineException that happened before)
				genLine={'action':'update', 'indexName':line['indexName'], 'type':'mnode', "_id":line['value']['hostname']+":"+line['value']['parentID']}
				genLine['routing']=line['value']['FMID']
				if line['action']=='newSplit':
					genLine['append']={'SMID':line['value']['MID']}
				
				elif line['action']=='newRewrite':
					genLine['value']={'isRewritten':'T', 'rewriterID':line['value']['MID']}
				
				elif line['action']=='newGenerate':
					genLine['append']={'GMID':line['value']['MID']}

				elif line['action']=='newBounce':
					genLine['append']={'BMID':line['value']['MID']}
				
				#targetThread=int(line['value']['FMID'])%SUBTHREAD_SIZE
				myQueue.put(genLine)
			
			else:
				pending.pop(line['value']['MID'])
				print 'new mnode has failed'
		
		elif line['action']=='update' and line['type']=='mnode':
			updateScript=''
			updateParam={}
			if 'value' in line:
				for akey in line['value']:
					updateScript+="ctx._source.%s=%s; "%(akey,akey)
					if akey=='from':
						updateParam[akey]=line['value'][akey].lower()
					elif akey=='subject':
						updateParam[akey]=cleanSubject(line['value'][akey])			#original subject
						updateScript+="ctx._source.readableSubject=readableSubject; "
						updateParam['readableSubject']=decodeSubject(line['value'][akey])			#decode to readable character
					else:
						updateParam[akey]=line['value'][akey]			#this way is good for sensitive information

			if 'append' in line:
				for akey in line['append']:
					updateScript+="ctx._source.%s+=%s; "%(akey,akey)
					if akey=='to':
						updateParam[akey]=line['append'][akey].lower()
					else:
						updateParam[akey]=line['append'][akey]

			if 'change' in line:
				for akey in line['change']:
					for apos in line['change'][akey]['position'].split(','):
						updateScript+="ctx._source.%s[%s]='%s'; "%(akey,apos.strip(),line['change'][akey]['value'])
			
			result={'ok':False}
			
			while True:
				try:
					result=es.update(index=line['indexName'],doc_type=line['type'],id=line['_id'],refresh=False,routing=line['routing'],body={
						"script" : updateScript,
						"params" : updateParam
					})
				except:
					if sys.exc_info()[1][0]==409:
						print '#####retry mnode update#####'
						continue
					else:
						print line
						print "mnode Updating error:", sys.exc_info()
						print 'update has failed'
						break

				if ('ok' not in result) or (not result['ok']):
					print 'update has failed'
				
				break



		#finish process line
		counter+=1
		totalProcessTime+=(datetime.datetime.now()-roundStartTime)
		if counter%100==0:
			allSeconds=float(totalProcessTime.microseconds + (totalProcessTime.seconds + totalProcessTime.days * 24 * 3600) * 10**6) / 10**6
			if allSeconds>0:
				qps=float(counter)/allSeconds
			else:
				qps=-1;
			
			currentTime=str(datetime.datetime.now()).rsplit(":",1)[0]
			print currentTime,threadName,counter,"\tinQ:%d\tq/sec:%.3f\tnotFound:%d\tmiss:%d"%(myQueue.qsize(),qps,notFoundCount,missCount)
	
	print "stop subthread",threadName


def runMyQueue(threadName, myQueue, stopEvent):
	
	newICID=0
	newTop=0
	newElse=0
	updateICID=0
	updateMID=0

	addTime=0.0
	addCounter=1
	searchTime=0.0
	searchCounter=1
	updateTime=0.0
	updateCounter=1
	
	notFoundCount=0
	missCount=0
	counter=0
	totalProcessTime=datetime.timedelta(0)
	time.sleep(1)
	#print "start>>", startTime
	es = elasticsearch.Elasticsearch(ESconnection)
	mc = memcache.Client([MCconnection], debug=0)
	
	#start subthreads beforehand
	pending={}		# to remember target queue for pending MID
	subQueue=[]
	for i in range(SUBTHREAD_SIZE):
		newQueue=Queue.Queue()
		subQueue.append(newQueue)
		newThread=threading.Thread(target=runSubQueue, args=(threadName+'.'+str(i),newQueue,stopEvent,mc,pending))
		newThread.start()

	while (not stopEvent.is_set()):
		#print "start"

		rawline=myQueue.get()
		roundStartTime=datetime.datetime.now()
		#this line is to escape double quote and to make all string possible for json.loads
		rawline=rawline.replace("\\", r"\\").replace(r"\"","\\"+r"\"")
		try:
			line=json.loads(rawline)
		except ValueError:
			print "ValueError:"
			print rawline
			continue

		if line['action']=='break':
			for aQueue in subQueue:
				aQueue.put(line)
			time.sleep(5)	
			break

		result={'ok':False}
			
		if line['action']=='newICID':
			newICID+=1
			startTimer=time.time()
			result=es.create(index=line['indexName'],doc_type=line['type'],refresh=False,body=line['value'],id=line['_id'])
			addTime+=time.time()-startTimer
			addCounter+=1

			if result['ok']:
				# line['_id'] is like "hostname:ICID"
				mc.set(str(line['indexName']+"/ICID/"+line['_id']),line['value'],time=CACHE_TIME)
			else:
				print "newICID has failed"
		
		elif line['action']=='update' and line['type']=='icnode':
			updateICID+=1
			#this line have to be done in main thread
			startTimer=time.time()
			kvalue=mc.get(str(line['indexName']+"/ICID/"+line['_id']))
			if kvalue==None:
				#cache miss
				missCount+=1
				try:
					getResult=es.get(index=line['indexName'],id=line['_id'],doc_type="icnode",refresh=True)
					kvalue=screenCache(getResult['_source'])
					mc.set(str(getResult['_index']+"/ICID/"+getResult['_id']),kvalue,time=CACHE_TIME)
		
				except elasticsearch.exceptions.NotFoundError:
					kvalue=None

			if kvalue==None:
				notFoundCount+=1
			else:
				#it's time to update
				updateScript=''
				updateParam={}
				
				for akey in line['value']:
					updateScript+="ctx._source.%s=%s; "%(akey,akey)
					updateParam[akey]=line['value'][akey]
			
				startTimer=time.time()
				result={'ok':False}
				while True:
					try:
						result=es.update(index=line['indexName'],doc_type=line['type'],id=line['_id'],refresh=False,body={
							"script" : updateScript,
							"params" : updateParam
						})
						
						if ('ok' in result) and (result['ok']):
							#update icnode cache
							for akey in line['value']:
								kvalue[akey]=line['value'][akey]
							
							mc.set(str(line['indexName']+"/ICID/"+line['_id']),screenCache(kvalue),time=CACHE_TIME)
					
					except:
						if sys.exc_info()[1][0]==409:
							print '#####retry update icnode#####'
							continue
						else:
							print line
							print "icnode Updating error:", sys.exc_info()
							print 'update icnode has failed'
							break
				
					if ('ok' not in result) or (not result['ok']):
						print 'update has failed'
					
					break

				updateTime+=time.time()-startTimer
				updateCounter+=1
		
		elif line['action']=='newTop':
			newTop+=1
			#search parent ICID
				
			startTimer=time.time()
			pvalue=mc.get(str(line['indexName']+"/"+line['value']['parentType']+"/"+line['value']['hostname']+":"+line['value']['parentID']))
			if pvalue==None:
				#cache miss
				missCount+=1
				try:
					getResult=es.get(index=line['indexName'],id=line['value']['hostname']+":"+line['value']['parentID'],doc_type="icnode",refresh=True)
					pvalue=screenCache(getResult['_source'])
					mc.set(str(getResult['_index']+"/ICID/"+getResult['_id']),pvalue,time=CACHE_TIME)
		
				except elasticsearch.exceptions.NotFoundError:
					pvalue=None
			
			searchTime+=time.time()-startTimer
			searchCounter+=1

			if pvalue!=None:
				pvalue['FMID']=line['value']['MID']
				targetThread=int(pvalue['FMID'])%SUBTHREAD_SIZE
				pending[line['value']['MID']]=pvalue
				subQueue[targetThread].put(line)
		
		elif line['action'] in ['newSplit','newRewrite','newGenerate','newBounce']:
			newElse+=1
			pvalue=None
			#search parent MID in pending
			if line['value']['parentID'] in pending:
				#pending hit
				pvalue=pending[line['value']['parentID']]
			else:
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

				searchTime+=time.time()-startTimer
				searchCounter+=1

			if pvalue!=None:
				targetThread=int(pvalue['FMID'])%SUBTHREAD_SIZE
				pending[line['value']['MID']]=pvalue
				subQueue[targetThread].put(line)
		
		elif line['action']=='update' and line['type']=='mnode':	
			updateMID+=1
			routing=None
			MID=line['_id'].split(':')[1]
			if MID in pending:
				#pending hit
				routing=pending[MID]['FMID']

			else:
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
		if counter%1000==0 or (counter<1000 and counter%100==0): # or (counter<10 and counter%2==0):
			allSeconds=float(totalProcessTime.microseconds + (totalProcessTime.seconds + totalProcessTime.days * 24 * 3600) * 10**6) / 10**6
			if allSeconds>0:
				qps=float(counter)/allSeconds
			else:
				qps=-1;
			addAvg=float(addTime)/addCounter
			searchAvg=float(searchTime)/searchCounter
			updateAvg=float(updateTime)/updateCounter
			currentTime=str(datetime.datetime.now()).rsplit(":",1)[0]
			print currentTime,threadName,counter,"\tinQ:%d\tq/sec:%.3f\tadd:%.3f\tsearch:%.3f\tupdate:%.3f\tnotFound:%d\tmiss:%d"%(myQueue.qsize(),qps,addAvg,searchAvg,updateAvg,notFoundCount,missCount)
			print "newICID",newICID,"updateICID",updateICID,"newTop",newTop,"newElse",newElse,"updateMID",updateMID
			#print "\r"+str(counter)+"\tq/sec:%.3f\tadd:%.3f\tsearch:%.3f\tupdate:%.3f\tcount(add,update):(%d,%d)"%(qps,addAvg,searchAvg,updateAvg,addCounter,updateCounter) ,
	
	print "stop thread",threadName


if __name__ == "__main__":

	#print deleteIndex("2013-11-05-axigen")
	#print createNewIndex('2013-11-05-axigen')
	
	#print deleteIndex("mmgt-2013-11-21")
	#print createManagementIndex('mmgt-2013-11-21')

	threadMap={}			#map from threadName to target queue
	
	stopEvent=threading.Event()

	try:
		while True:
			try:
				raw_line=raw_input()
			except:
				#wait until all queues are empty
				while True:
					allEmpty=True
					for i in threadMap:
						allEmpty&=threadMap[i].empty()
					if allEmpty:
						break
					else:
						time.sleep(3)

				finishTime=datetime.datetime.now()
				print "stop >>",finishTime
				stopEvent.set()
				
				for aThread in threadMap:
					while not threadMap[aThread].empty():
						threadMap[aThread].get_nowait()

					threadMap[aThread].put('{"action":"break"}')
				
				break
		
			#seperate raw_line
			threadName,query=raw_line.split(' ',1)
			
			if threadName not in threadMap:
				#create new queue and thread
				newQueue=Queue.Queue()
				threadMap[threadName]=newQueue
				newThread=threading.Thread(target=runMyQueue, args=(threadName,newQueue,stopEvent))
				newThread.start()
			#put data in that thread
			threadMap[threadName].put(query)
			
	except (KeyboardInterrupt,):
		stopEvent.set()

	stopEvent.set()
