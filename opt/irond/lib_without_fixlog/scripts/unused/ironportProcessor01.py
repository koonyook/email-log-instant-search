#!/usr/bin/python
import sys

import threading
import Queue

import memcache
import elasticsearch
import json
import datetime,time

CACHE_TIME=300 #5 minutes
ESconnection = "10.20.4.26:9200"
MCconnection = "10.20.4.26:11211"

def screenCache(dataDict):
	take={}
	for akey in dataDict:
		if akey in ['hostname','ICID','interface','sourceIP','mx_name','authenUser','authenResult','FMID']:
			take[akey]=dataDict[akey]

	return take

def runMyQueue(threadName, myQueue, stopEvent):
	
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

	while (not stopEvent.is_set()):
		#print "start"

		rawline=myQueue.get()
		roundStartTime=datetime.datetime.now()
		rawline=rawline.replace("\\'","'")
		try:
			line=json.loads(rawline)
		except ValueError:
			print "ValueError:"
			print rawline
			continue

		if line['action']=='break':
			break

		counter+=1
		if counter%1000==0 or (counter<1000 and counter%10==0) or (counter<10 and counter%2==0):
			allSeconds=float(totalProcessTime.microseconds + (totalProcessTime.seconds + totalProcessTime.days * 24 * 3600) * 10**6) / 10**6
			if allSeconds>0:
				if counter<1000:
					qps=float(counter)/allSeconds
				else:
					qps=float(1000)/allSeconds
					totalProcessTime=datetime.timedelta(0)
			else:
				qps=-1;
			addAvg=float(addTime)/addCounter
			searchAvg=float(searchTime)/searchCounter
			updateAvg=float(updateTime)/updateCounter
			currentTime=str(datetime.datetime.now()).rsplit(":",1)[0]
			print currentTime,threadName,counter,"\tinQ:%d\tq/s:%.3f\tadd:%.3f\tsearch:%.3f\tupdate:%.3f\tnotFound:%d\tmiss:%d"%(myQueue.qsize(),qps,addAvg,searchAvg,updateAvg,notFoundCount,missCount)
			#print "\r"+str(counter)+"\tq/sec:%.3f\tadd:%.3f\tsearch:%.3f\tupdate:%.3f\tcount(add,update):(%d,%d)"%(qps,addAvg,searchAvg,updateAvg,addCounter,updateCounter) ,
		
		result={'ok':False}
		if line['action'].startswith('new'):
			
			if line['action']=='newICID':
				startTimer=time.time()
				result=es.create(index=line['indexName'],doc_type=line['type'],refresh=False,body=line['value'],id=line['_id'])
				addTime+=time.time()-startTimer
				addCounter+=1

				if result['ok']:
					# line['_id'] is like "hostname:ICID"
					mc.set(str(line['indexName']+"/ICID/"+line['_id']),line['value'],time=CACHE_TIME)
				else:
					print "newICID has failed"

			elif line['action']=='newTop':
				#search parent ICID
				
				startTimer=time.time()
				pvalue=mc.get(str(line['indexName']+"/"+line['value']['parentType']+"/"+line['value']['hostname']+":"+line['value']['parentID']))
				if pvalue==None:
					#cache miss
					missCount+=1
					try:
						getResult=es.get(index=line['indexName'],id=line['value']['hostname']+":"+line['value']['parentID'],doc_type="icnode",refresh="true")
						pvalue=screenCache(getResult['_source'])
						mc.set(str(getResult['_index']+"/ICID/"+getResult['_id']),pvalue,time=CACHE_TIME)
			
					except elasticsearch.exceptions.NotFoundError:
						pvalue=None
				
				searchTime+=time.time()-startTimer
				searchCounter+=1

				if pvalue!=None:
					#copy data to line['value']
					for akey in pvalue:
						line['value'][akey]=pvalue[akey]
					
					#initialize some variables
					line['value']['FMID']=line['value']['MID']
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
					startTimer=time.time()
					result=es.create(index=line['indexName'],doc_type=line['type'],refresh=False,body=line['value'],id=line['_id'],routing=line['value']['FMID'])			
					addTime+=time.time()-startTimer
					addCounter+=1
					if ('ok' in result) and (result['ok']):
						mc.set(str(line['indexName']+"/MID/"+line['_id']),screenCache(line['value']),time=CACHE_TIME) #for existence checking and for data copy
					else:
						print 'newTop has failed'

			elif line['action'] in ['newSplit','newRewrite','newGenerate','newBounce']:
				#search parent MID
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
						pvalue=None
						notFoundCount+=1

				searchTime+=time.time()-startTimer
				searchCounter+=1

				if pvalue!=None:
					#copy data to line['value']
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
					startTimer=time.time()
					result=es.create(index=line['indexName'],doc_type=line['type'],refresh=False,body=line['value'],id=line['_id'],parent=line['value']['hostname']+":"+line['value']['parentID'],routing=line['value']['FMID'])
					addTime+=time.time()-startTimer
					addCounter+=1
					if ('ok' in result) and (result['ok']):
						mc.set(str(line['indexName']+"/MID/"+line['_id']),screenCache(line['value']),time=CACHE_TIME) #for existence checking and for data copy
						
						#update parent
						if line['action']=='newSplit':
							updateScript="ctx._source.SMID+=newSMID; "
							updateParam={"newSMID":line['value']['MID']}
						elif line['action']=='newRewrite':
							updateScript="ctx._source.isRewritten='T'; ctx._source.rewriterID=rewriterID; "
							updateParam={"rewriterID":line['value']['MID']}
						elif line['action']=='newGenerate':
							updateScript="ctx._source.GMID+=newGMID; "
							updateParam={"newGMID":line['value']['MID']}
						elif line['action']=='newBounce':
							updateScript="ctx._source.BMID+=newBMID; "
							updateParam={"newBMID":line['value']['MID']}
						
						startTimer=time.time()
						updateResult=es.update(index=line['indexName'],doc_type='mnode',id=line['value']['hostname']+":"+line['value']['parentID'],refresh=False,routing=line['value']['FMID'],body={
							"script" : updateScript,
							"params" : updateParam
						})
						updateTime+=time.time()-startTimer
						updateCounter+=1

						if ('ok' not in updateResult) or (not updateResult['ok']):
							print "feedback update has failed"
					
					else:
						print 'new mnode has failed'

			else:
				print "type error:",
				print targetType
				pass	#type error			

		elif line['action']=='update':
			# 3 methods
			# value = update directly
			# append = append at the end of that list
			# change = update at specific index in list
			
			#check existence and get routing if need
			startTimer=time.time()
			kvalue=None
			if line['type']=='icnode':
				kvalue=mc.get(str(line['indexName']+"/ICID/"+line['_id']))
				if kvalue==None:
					#cache miss
					missCount+=1
					try:
						getResult=es.get(index=line['indexName'],id=line['_id'],doc_type="icnode",refresh="true")
						kvalue=screenCache(getResult['_source'])
						mc.set(str(getResult['_index']+"/ICID/"+getResult['_id']),kvalue,time=CACHE_TIME)
			
					except elasticsearch.exceptions.NotFoundError:
						kvalue=None

			elif line['type']=='mnode':
				kvalue=mc.get(str(line['indexName']+"/MID/"+line['_id']))
				if kvalue==None:
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
					
					else:
						kvalue=None
			searchTime+=time.time()-startTimer
			searchCounter+=1

			if kvalue==None:
				notFoundCount+=1
			else:
				#it's time to update
				updateScript=''
				updateParam={}
				if 'value' in line:
					for akey in line['value']:
						updateScript+="ctx._source.%s=%s; "%(akey,akey)
						updateParam[akey]=line['value'][akey]			#this way is good for sensitive information

				if 'append' in line:
					for akey in line['append']:
						updateScript+="ctx._source.%s+=%s; "%(akey,akey)
						updateParam[akey]=line['append'][akey]

				if 'change' in line:
					for akey in line['change']:
						for apos in line['change'][akey]['position'].split(','):
							updateScript+="ctx._source.%s[%s]='%s'; "%(akey,apos.strip(),line['change'][akey]['value'])
				
				startTimer=time.time()
				result={'ok':False}
				if line['type']=='mnode':
					try:
						result=es.update(index=line['indexName'],doc_type=line['type'],id=line['_id'],refresh=False,routing=kvalue['FMID'],body={
							"script" : updateScript,
							"params" : updateParam
						})
					except:
						print line
						print "mnode Updating error:", sys.exc_info()[0]

				elif line['type']=='icnode':
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
						print line
						print "icnode Updating error:", sys.exc_info()[0]
				updateTime+=time.time()-startTimer
				updateCounter+=1

				if ('ok' not in result) or (not result['ok']):
					print 'update has failed'
					
		else:
			print line['action'],"should not be found as an action. please check pattern."

		totalProcessTime+=(datetime.datetime.now()-roundStartTime)


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
						time.sleep(5)

				finishTime=datetime.datetime.now()
				print "stop >>",finishTime
				stopEvent.set()
				
				for aThread in threadMap:
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
