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

#from util.general import runSpecialCommand
from util.general import waitChildren
from util.returnStat import ReturnStat
import setting

SUBTHREAD_SIZE= setting.SUBTHREAD_SIZE
CACHE_TIME    = setting.CACHE_TIME #5 minutes
ESconnection  = setting.ES_CONNECTION
MCconnection  = setting.MC_CONNECTION

def searchMessage(mc,es,indexName,hostname,idType,idString):
	#indexName='2012-12-31-axigen'
	#idType can be 'ICID', 'MID', 'DCID'
	doc_id=mc.get(str(indexName+"/"+idType+"/"+hostname+":"+idString))
	if doc_id==None:
		client=elasticsearch.client.IndicesClient(es)
		client.refresh(indexName)
		result=es.search(index=indexName,doc_type='message',size=1,body={
			"query": {
				"filtered": {
					"query": {
						"match_all": {}
					},
					"filter": {
						"and": [
							{
								"term": {
									"hostname":hostname
								}
							},
							{
								"term": {
									idType:idString
								}
							}
						]
					}
				}
			}
		})

		if result['hits']['total']==1:
			doc_id=result['hits']['hits'][0]['_id']
			#push in cache
			mc.set(str(indexName+"/ICID/"+hostname+":"+result['hits']['hits'][0]['_source']['ICID']),doc_id,time=CACHE_TIME)
			for mid in result['hits']['hits'][0]['_source']['MID']:
				mc.set(str(indexName+"/MID/"+hostname+":"+mid),doc_id,time=CACHE_TIME)
			for dcid in result['hits']['hits'][0]['_source']['DCID']:
				mc.set(str(indexName+"/DCID/"+hostname+":"+dcid),doc_id,time=CACHE_TIME)

			return doc_id	# _id found
		else:
			return None	# _id not found
	else:
		return doc_id

def searchLoginout(mc,es,indexName,hostname,connectionID):
	
	#search in cache
	doc_id=mc.get(str(indexName+"/loginout/"+hostname+":"+connectionID))
	if doc_id!=None:
		#cache hit
		return doc_id
	else:
		#chace miss
		try:
			getResult=es.get(index=indexName,id=hostname+":"+connectionID,doc_type='loginout',refresh=True)
			doc_id=getResult['_id']
			#update cache
			mc.set(str(indexName+"/loginout/"+hostname+":"+connectionID),doc_id,time=CACHE_TIME)
			return doc_id

		except elasticsearch.NotFoundError:
			return None
		except elasticsearch.TransportError:
			print "TransportError"
			return None
	
	"""
	#indexName='2012-12-31' (for example)
	doc_id=mc.get(str(indexName+"/"+hostname+"/"+connectionID))
	if doc_id==None:
		client=elasticsearch.client.IndicesClient(es)
		client.refresh(indexName)
		result=es.search(index=indexName,doc_type='loginout',size=1,body={
			"query": {
				"filtered": {
					"query": {
						"match_all": {}
					},
					"filter": {
						"and": [
							{
								"term": {
									"hostname":hostname
								}
							},
							{
								"term": {
									"ConnectionID":connectionID
								}
							}
						]
					}
				}
			}
		})

		if result['hits']['total']==1:
			doc_id=result['hits']['hits'][0]['_id']
			#push in cache
			mc.set(str(indexName+"/"+hostname+"/"+connectionID),doc_id,time=CACHE_TIME)

			return doc_id	# _id found
		else:
			return None	# _id not found
	else:
		return doc_id
	"""

def searchDeleted(mc,es,indexName,hostname,connectionID):
	#indexName='2012-12-31' (for example)
	doc_id=mc.get(str(indexName+"/"+hostname+"/"+connectionID))
	if doc_id==None:
		client=elasticsearch.client.IndicesClient(es)
		client.refresh(indexName)
		result=es.search(index=indexName,doc_type='deleted',size=1,body={
			"query": {
				"filtered": {
					"query": {
						"match_all": {}
					},
					"filter": {
						"and": [
							{
								"term": {
									"hostname":hostname
								}
							},
							{
								"term": {
									"ConnectionID":connectionID
								}
							}
						]
					}
				}
			}
		})

		if result['hits']['total']==1:
			doc_id=result['hits']['hits'][0]['_id']
			#push in cache
			mc.set(str(indexName+"/"+hostname+"/"+connectionID),doc_id,time=CACHE_TIME)

			return doc_id	# _id found
		else:
			return None	# _id not found
	else:
		return doc_id

def addDeletedDocument(mc,es,indexName,dataDict):
	#before using this function, you must be sure that is document is new
	#dataDict must contain at least hostname and ConnectionID
	result=es.create(index=indexName,doc_type='deleted',refresh=False,body=dataDict)
	mc.set(str(indexName+"/"+dataDict['hostname']+"/"+dataDict['ConnectionID']),result['_id'],time=CACHE_TIME)	
	return result

def addManagementDocument(mc,es,indexName,dataDict):
	#before using this function, you must be sure that is document is new
	#dataDict must contain everything in the management mapping
	result=es.create(index=indexName,doc_type='management',refresh=False,body=dataDict)
	#big different in adding for management is about not-catching
	return result

def updateDeletedDocument(es,indexName,document_id,dataDict):
	#only updated values allowed in dataDict (must be cleaned before by pop out key values)
	#update by doc feature
	result=es.update(index=indexName,doc_type='deleted',id=documentID,refresh=False,body={
		"doc" : dataDict
	})
	return result

def runLoginoutQueue(threadName, myQueue, stopEvent, mc, loginoutPending, myStat):
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
		if 'type' in newline and newline['type']=='loginout':
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
				if 'type' in newline and newline['type']=='loginout':
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
				myQueue.put(newline)  #this thread will end in the next round
				pass

		#convert bulklist to bulk query
		bulkbody=[]
		for line in bulklist:
			if line['action']=='new':
				while True:
					try:
						linecopy=deepcopy(line)
						break
					except:  #RuntimeError: dictionary changed size during iteration (code from irond)
						continue
				line=linecopy
				bulkbody.append({
					"index":{
						"_index":line['indexName'],
						"_type":line['type'],
						"_id":line['_id'],	#######******
						"refresh":False
					}
				})
				bulkbody.append(line['value'])
				currentIndex=line['indexName']

			elif line['action']=='update':
				updateScript=''
				updateParam={}

				for akey in line['value']:
					updateScript+="ctx._source.%s=%s; "%(akey,akey)		#######*******
					updateParam[akey]=line['value'][akey]

				bulkbody.append({
					"update":{
						"_index":line['indexName'],
						"_type":line['type'],
						"_id":line['_id'],			#####*****
						"refresh":False,
						"_retry_on_conflict":5
					}
				})
				bulkbody.append({
					"script":updateScript,
					"params":updateParam
				})

		bresult=es.bulk(body=bulkbody, refresh=False)
		i=0
		for item in bresult['items']:
			if 'index' in item:
				result=item['index']
				hostname,ConnectionID=bulklist[i]['_id'].split(':')
				if ('status' in result) and (result['status']==201):
					mc.set(str(bulklist[i]['indexName']+"/loginout/"+hostname+":"+ConnectionID),bulklist[i]['_id'],time=CACHE_TIME)
				else:
					failCount+=1

				try:
					loginoutPending.pop(bulklist[i]['indexName']+'/'+ConnectionID)
				except KeyError:
					pass

			elif 'update' in item:
				result=item['update']
				if ('status' in result) and (result['status']==200):
					pass	#you have to update cache if you update to the same document more than once
				else:
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
	
	print str(datetime.datetime.now()),threadName,"###stopEvent###" #should never happen

def runMessageQueue(threadName, myQueue, stopEvent, mc, messagePending, myStat):
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
		if 'type' in newline and newline['type']=='message':
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
				if 'type' in newline and newline['type']=='message':
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
				myQueue.put(newline)
				pass

		#convert bulklist to bulk query
		bulkbody=[]
		for line in bulklist:
			if line['action']=='new':
				while True:
					try:
						linecopy=deepcopy(line)
						break
					except:  #RuntimeError: dictionary changed size during iteration (code from irond)
						continue
				line=linecopy
				bulkbody.append({
					"index":{
						"_index":line['indexName'],
						"_type":line['type'],
						"_id":line['_id'],	#######******
						"refresh":False
					}
				})
				bulkbody.append(line['value'])
				currentIndex=line['indexName']

			elif line['action']=='update':
				updateScript=''
				updateParam={}
				
				if 'value' in line:
					for akey in line['value']:
						updateScript+="ctx._source.%s=%s; "%(akey,akey)		#######*******
						if akey=='from':
							updateParam[akey]=line['value'][akey].lower()
						else:
							updateParam[akey]=line['value'][akey]

				if 'append' in line:
					for akey in line['append']:
						updateScript+="ctx._source.%s+=%s; "%(akey,akey)
						if akey=='to':
							updateParam[akey]=line['append'][akey].lower()
						else:
							updateParam[akey]=line['append'][akey]

				bulkbody.append({
					"update":{
						"_index":line['indexName'],
						"_type":line['type'],
						"_id":line['_id'],			#####*****
						"refresh":False,
						"_retry_on_conflict":5
					}
				})
				bulkbody.append({
					"script":updateScript,
					"params":updateParam
				})

		bresult=es.bulk(body=bulkbody, refresh=False)
		i=0
		for item in bresult['items']:
			if 'index' in item:
				result=item['index']
				hostname,ConnectionID=bulklist[i]['_id'].split(':')
				if ('status' in result) and (result['status']==201):
					mc.set(str(bulklist[i]['indexName']+"/ICID/"+hostname+":"+ConnectionID),bulklist[i]['_id'],time=CACHE_TIME)
				else:
					failCount+=1
				
				try:
					messagePending.pop(bulklist[i]['indexName']+'/'+ConnectionID)
				except KeyError:
					#it was pop before by merging problem of axigen
					pass

			elif 'update' in item:
				result=item['update']
				if ('status' in result) and (result['status']==200):
					if ('append' in bulklist[i]):
						if ('MID' in bulklist[i]['append']):
							mc.set(str(bulklist[i]['indexName']+"/MID/"+hostname+":"+bulklist[i]['append']['MID']),bulklist[i]['_id'],time=CACHE_TIME)
						if ('DCID' in bulklist[i]['append']):
							mc.set(str(bulklist[i]['indexName']+"/DCID/"+hostname+":"+bulklist[i]['append']['DCID']),bulklist[i]['_id'],time=CACHE_TIME)
				else:
					failCount+=1
				
				try:
					if ('append' in bulklist[i]):
						if ('MID' in bulklist[i]['append']):
							messagePending.pop(bulklist[i]['indexName']+'/'+bulklist[i]['append']['MID'])
						if ('DCID' in bulklist[i]['append']):
							messagePending.pop(bulklist[i]['indexName']+'/'+bulklist[i]['append']['DCID'])
				except KeyError:
					#it was pop before by merging problem of axigen
					pass

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
	print str(datetime.datetime.now()),threadName,"###stopEvent###" #should never happen

def runMyQueue(threadName, myQueue, stopEvent, myStat):
	
	addTime=0.0
	addCounter=1
	searchTime=0.0
	searchCounter=1
	updateTime=0.0
	updateCounter=1
	
	notFoundCount=0
	counter=0
	startTime=datetime.datetime.now()
	time.sleep(1)
	#print "start>>", startTime
	es = elasticsearch.Elasticsearch(ESconnection)
	mc = memcache.Client([MCconnection], debug=0)
	
	if 'loginout' in threadName:
		#start subthreads beforehand
		loginoutPending={}
		loginoutQueue=Queue.Queue()
		loginoutStat=ReturnStat()
		loginoutThread=threading.Thread(target=runLoginoutQueue, args=(threadName+'.L',loginoutQueue,stopEvent,mc,loginoutPending,loginoutStat))
		loginoutThread.start()

		while (not stopEvent.is_set()):
			
			rawline=myQueue.get()
			try:
				line=json.loads(rawline)
			except ValueError:
				print "ValueError:"
				print rawline
				continue

			if line['action']=='break':
				
				if line['clear']==True:
					loginoutQueue.queue.clear()
				loginoutQueue.put(line)
				loginoutThread.join()
				print threadName, '###return###'
				if line['clear']==True:
					myStat.setNormalExit()
				else:
					myStat.setErrorExit()
				return

			elif line['action']=='new':
				loginoutPending[line['indexName']+'/'+line['value']['ConnectionID']]=line['_id']
				loginoutQueue.put(line)

			elif line['action']=='update':
				haveToEnqueue=False
				target_id=None
				#search node in loginoutPending
				hostname,ConnectionID=line['_id'].split(':')
				
				if line['indexName']+'/'+ConnectionID in loginoutPending:
					try:
						target_id=loginoutPending[line['indexName']+'/'+ConnectionID]
						haveToEnqueue=True
					except KeyError:
						target_id=searchLoginout(mc,es,line['indexName'],hostname,ConnectionID)
						if target_id!=None:
							haveToEnqueue=True

				else:
					target_id=searchLoginout(mc,es,line['indexName'],hostname,ConnectionID)
					if target_id!=None:
						haveToEnqueue=True

				if haveToEnqueue:
					loginoutQueue.put(line)

			#check thread
			if not loginoutThread.isAlive():
				if loginoutStat.getStat()=='normal':
					myStat.setNormalExit()
				elif loginoutStat.getStat()=='error':
					myStat.setErrorExit()
				print threadName, '###return###' 
				return
	
	elif 'message' in threadName:
		#start subthreads beforehand
		messagePending={}
		messageQueue=Queue.Queue()
		messageStat=ReturnStat()
		messageThread=threading.Thread(target=runMessageQueue, args=(threadName+'-main',messageQueue,stopEvent,mc,messagePending,messageStat))
		messageThread.start()

		while (not stopEvent.is_set()):
			
			rawline=myQueue.get()
			try:
				line=json.loads(rawline)
			except ValueError:
				print "ValueError:"
				print rawline
				continue

			if line['action']=='break':
				
				if line['clear']==True:
					messageQueue.queue.clear()
				messageQueue.put(line)
				messageThread.join()
				print threadName, '###return###'
				if line['clear']==True:
					myStat.setNormalExit()
				else:
					myStat.setErrorExit()
				return

			elif line['action']=='new':
				messagePending[line['indexName']+'/'+line['value']['ICID']]=line['_id']
				messageQueue.put(line)

			elif line['action']=='update':
				haveToEnqueue=False
				target_id=None
				if '_id' in line:
					hostname,ICID=line['_id'].split(':')
					if line['indexName']+'/'+ICID in messagePending:
						try:
							target_id=messagePending[line['indexName']+'/'+ICID]
							haveToEnqueue=True
						
						except KeyError:	#rare error when another thread suddenly pop it out
							target_id=searchMessage(mc,es,line['indexName'],hostname,'ICID',ICID)
							if target_id!=None:
								haveToEnqueue=True
							
					else:
						target_id=searchMessage(mc,es,line['indexName'],hostname,'ICID',ICID)
						if target_id!=None:
							haveToEnqueue=True
				else:
					#have to fill _id in line
					if line['indexName']+'/'+line['keyValue'] in messagePending:
						try:
							target_id=messagePending[line['indexName']+'/'+line['keyValue']]
							haveToEnqueue=True
						
						except KeyError:	#rare error when another thread suddenly pop it out
							target_id=searchMessage(mc,es,line['indexName'],line['hostname'],line['keyType'],line['keyValue'])
							if target_id!=None:
								haveToEnqueue=True

					else:
						target_id=searchMessage(mc,es,line['indexName'],line['hostname'],line['keyType'],line['keyValue'])
						if target_id!=None:
							haveToEnqueue=True

				if haveToEnqueue:
					if 'append' in line:
						if 'MID' in line['append']:
							messagePending[line['indexName']+'/'+line['append']['MID']]=target_id
						if 'DCID' in line['append']:
							messagePending[line['indexName']+'/'+line['append']['DCID']]=target_id

					line['_id']=target_id
					messageQueue.put(line)

			#check thread
			if not messageThread.isAlive():
				if messageStat.getStat()=='normal':
					myStat.setNormalExit()
				elif messageStat.getStat()=='error':
					myStat.setErrorExit()
				print threadName, '###return###' 
				return
	
	else:
		#old process style
		while (not stopEvent.is_set()):
			#print "start"
			
			rawline=myQueue.get()

			try:
				line=json.loads(rawline)
			except ValueError:
				print "ValueError:"
				print rawline
				continue

			if line['action']=='break':
				print threadName, '###return###'
				myStat.setNormalExit()
				return
				#break

			counter+=1
			if counter%1000==0:
				qps=float(counter)/(datetime.datetime.now()-startTime).seconds
				addAvg=float(addTime)/addCounter
				searchAvg=float(searchTime)/searchCounter
				updateAvg=float(updateTime)/updateCounter
				currentTime=str(datetime.datetime.now()).rsplit(":",1)[0]
				print currentTime,line['indexName'],threadName,counter,"\tinQ:%d\tq/sec:%.3f\tadd:%.3f\tsearch:%.3f\tupdate:%.3f\tnotFound:%d"%(myQueue.qsize(),qps,addAvg,searchAvg,updateAvg,notFoundCount)
				#print "\r"+str(counter)+"\tq/sec:%.3f\tadd:%.3f\tsearch:%.3f\tupdate:%.3f\tcount(add,update):(%d,%d)"%(qps,addAvg,searchAvg,updateAvg,addCounter,updateCounter) ,
			action=line.pop('action',None)
			targetType=line.pop('type',None)
			indexName=line.pop('indexName',None)

			result={'ok':False, 'type':'dummy'}
			if action=='new':
				startTimer=time.time()

				if targetType=='deleted':
					result=addDeletedDocument(mc,es,indexName,line['value'])
				
				elif targetType=='management':
					result=addManagementDocument(mc,es,indexName,line['value'])

				else:
					print "type error"
					pass	#type error
				
				if not result['created']:
					print ">>created error>>",threadName, result
					result['ok']=False
				else:
					result['ok']=True

				if ('ok' not in result) or (not result['ok']):
					print threadName, result

				addTime+=time.time()-startTimer
				addCounter+=1
			
			elif action=='update':
				hostname=line['value'].pop('hostname',None)
				if targetType=='deleted':
					connectionID=line['value'].pop('ConnectionID',None)
					startTimer=time.time()
					document_id=searchDeleted(mc,es,indexName,hostname,connectionID)
					searchTime+=time.time()-startTimer
					searchCounter+=1
					
					startTimer=time.time()
					result=updateDeletedDocument(es,indexName,document_id,line['value'])
					updateTime+=time.time()-startTimer
					updateCounter+=1

				else:
					print "type error"
					pass	#type error
				
				#debug
				#if ('ok' not in result):
				#	print ">>",result
				#end debug	
				if not result['ok']:
					print threadName, result
			else:
				pass


def processToElastic(myName,inputQueue,stopEvent,myStat):

		queueMap={}			#map from threadName to target queue
		threadMap={}
		statMap={}

		myChildren=[]

		#stopEvent=threading.Event()
		counter=0
	#try:
		while True:
			raw_line=inputQueue.get()
			#seperate raw_line
			threadName,query=raw_line.split(' ',1)
			
			if threadName not in queueMap:
				if threadName=='clearbroadcast':	#sign of abnormal ending
					#broadcast to all queue
					for aName in queueMap:
						queueMap[aName].queue.clear()
						queueMap[aName].put(query)
					waitChildren(myChildren)
					print  str(datetime.datetime.now()),myName,'###return###'
					myStat.setErrorExit()
					return
				if threadName=='normalbroadcast': 	#sign of file ending (EOF)
					for aName in queueMap:
						queueMap[aName].put(query)
					waitChildren(myChildren)
					print  str(datetime.datetime.now()),myName,'###EOF###'
					myStat.setNormalExit()
					return

				else:
					#create new queue and thread
					newQueue=Queue.Queue()
					queueMap[threadName]=newQueue
					newStat=ReturnStat()
					newThread=threading.Thread(target=runMyQueue, args=(threadName,newQueue,stopEvent,newStat))
					threadMap[threadName]=newThread
					statMap[threadName]=newStat
					newThread.start()
					myChildren.append(newThread)
					queueMap[threadName].put(query)

			else:
				#put data in that thread
				queueMap[threadName].put(query)
				counter+=1
				if counter%1000==0:
					counter=0
					#check thread
					dead=False
					for i in threadMap:
						if not threadMap[i].isAlive() and statMap[i].getStat!='normal':
							dead=True
							break
					if dead:
						for aName in queueMap:
							queueMap[aName].queue.clear()
							queueMap[aName].put('{"action":"break", "clear":true}')
						waitChildren(myChildren)
						print  str(datetime.datetime.now()),myName,'###return###'
						myStat.setErrorExit()
						return

						#runSpecialCommand()
	#except (KeyboardInterrupt,):
	#	stopEvent.set()

	#stopEvent.set()
