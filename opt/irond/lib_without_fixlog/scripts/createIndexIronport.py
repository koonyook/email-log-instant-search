#!/usr/bin/python
import sys,os
libPath=os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(libPath)

import setting

from elasticsearch.exceptions import NotFoundError
import sys

import threading
import Queue

import memcache
import elasticsearch
import json
import datetime,time

CACHE_TIME= setting.CACHE_TIME
connection = setting.ES_CONNECTION

def createNewIndex(indexName):
	#indexName='2012-12-31-ironport' (for example)
	es = elasticsearch.Elasticsearch(connection)
	client=elasticsearch.client.IndicesClient(es)

	result=client.create(index=indexName,body={
		"settings" : {
			"refresh_interval":60
		},
		"mappings" : {
			
			"icnode" : {
				"properties" : {
					"beginTimestamp": {
						"type":"date",
						"index":"not_analyzed"
					},
					"hostname": {
						"type":"string",
						"index":"not_analyzed"
					},
					"ICID":{
						"type":"string",
						"index":"not_analyzed"
					},
					"interface": {
						"type":"string",
						"index":"not_analyzed"
					},
					"sourceIP": {
						"type":"string",
						"index":"not_analyzed"
					},
					"mx_name": {
						"type":"string",
						"index":"not_analyzed"
					},
					"authenUser": {
						"type":"string",
						"index":"not_analyzed"
					},
					"authenResult": {
						"type":"string",
						"index":"not_analyzed"
					}
					
				}
			},
			
			"dcnode" : {
				"properties" : {
					"timestamp": {
						"type":"date",
						"index":"not_analyzed"
					},
					"hostname": {
						"type":"string",
						"index":"not_analyzed"
					},
					"DCID":{
						"type":"string",
						"index":"not_analyzed"
					},
					"interface": {
						"type":"string",
						"index":"not_analyzed"
					},
					"address": {
						"type":"string",
						"index":"not_analyzed"
					},
					"port": {
						"type":"string",
						"index":"not_analyzed"
					},
					"closeTimestamp": {
						"type":"date",
						"index":"not_analyzed"
					}
				}
			},

			"mnode" : {
				"_parent":{
					"type":"mnode"
				},
				"_routing": {
					"required": True	#its path shoud be FMID
				},
				"properties" : {
					
					"MID":{
						"type":"string",
						"index":"not_analyzed"
					},

					#inheritable property from icnode
					"beginTimestamp": {
						"type":"date",
						"index":"not_analyzed"
					},
					"hostname": {
						"type":"string",
						"index":"not_analyzed"
					},
					"ICID":{
						"type":"string",
						"index":"not_analyzed"
					},
					"interface": {
						"type":"string",
						"index":"no"
					},
					"sourceIP": {
						"type":"string",
						"index":"not_analyzed"
					},
					"mx_name": {
						"type":"string",
						"index":"not_analyzed"
					},
					"authenUser": {
						"type":"string",
						"index":"not_analyzed"
					},
					"authenResult": {
						"type":"string",
						"index":"not_analyzed"
					},
					
					#inheritable property from parent mnode
					"FMID": {
						"type":"string",
						"index":"not_analyzed"
					},
					
					#mnode type
					"isTop": {			
						"type":"boolean",	#know at creation
					},
					"isTail": {				
						"type":"boolean",	#default = F
					},
					"isGenerated": {
						"type":"boolean",	#know at creation
					},
					"isBounce": {
						"type":"boolean",	#know at creation
					},
					"isRewriter": {
						"type":"boolean",	#know at creation
					},
					"isRewritten": {
						"type":"boolean",	#default = F
					},

					#my property
					"parentType": {
						"type":"string",
						"index":"not_analyzed"
					},
					"parentID": {
						"type":"string",
						"index":"not_analyzed"
					},

					"finishStatus": {
						"type":"string",
						"index":"not_analyzed"	#default='?' then it be changed to 'done' or 'aborted'
					},
					
					
					#general property
					"timestamp": {
						"type":"date",
						"index":"not_analyzed"
					},
					"from": {
						"type":"string",
						"index":"not_analyzed"
					},
					"MSGID":{
						"type":"string",
						"index":"not_analyzed"
					},
					"subject":{
						"type":"string",
						"index":"not_analyzed"
					},
					"readableSubject":{
						"type":"string",
						"index":"not_analyzed"
					},
					"size": {
						"type":"integer",
						"index":"not_analyzed"
					},
					"antivirus": {
						"type":"string",
						"index":"not_analyzed"	#negative or positive
					},
					"spam": {
						"type":"string",
						"index":"not_analyzed"	#negative or positive
					},
					"dropMessage":{
						"type":"string",
						"index":"no"
					},
					
					"firstReject":{
						"type":"string",
						"index":"no"	#list of string
					},

					#these set have to go together (append at the same time while updating)
					"RID":{
						"type":"integer",
						"index":"no"	#list of RID (this list must be in correct order 0, 1, 2, 3, ... )
					},
					"to":{
						"type":"string",
						"index":"not_analyzed"	#list of recipients
					},
					"DCID":{
						"type":"string",
						"index":"no"	#list of DCID (default = '?')
					},
					"rstatus":{
						"type":"string",
						"index":"not_analyzed"	#list of status (default = '?')
					},
					"reasonMessage":{
						"type":"string",
						"index":"no"	#list of special message (default = '')
					},
					
					#this is for bounced counting
					"bouncedCount":{
						"type":"integer",
						"index":"not_analyzed"
					},

					#for child tracking
					"rewriterID":{
						"type":"string",
						"index":"no"	#MID of rewriter
					},
					"SMID":{
						"type":"string",
						"index":"no"	#list of split MID
					},
					"BMID":{
						"type":"string",
						"index":"no"	#list of bounce MID
					},
					"GMID":{
						"type":"string",
						"index":"no"	#list of generated MID
					}
					
				}
			}
			
		}
	})
	
	return result

def deleteIndex(indexName):
	#indexName='2012-12-31-ironport' (for example)
	es = elasticsearch.Elasticsearch(connection)
	client=elasticsearch.client.IndicesClient(es)
	
	result=client.delete(index=indexName)

	return result

	
if __name__ == "__main__":

	#print deleteIndex("2013-11-21-ironport")
	#print createNewIndex('2013-11-21-ironport')
	print sys.argv[1]
	print createNewIndex(sys.argv[1])
	
	#for date in range(31):
	#	try:
	#		print deleteIndex("2013-12-%02d-ironport"%(date+1))			
	#	except NotFoundError:
	#		print "newly create"
	#	
	#	print createNewIndex("2013-12-%02d-ironport"%(date+1))			

	#print deleteIndex("2013-11-05")
	#print createNewIndex('2013-11-05')


	
