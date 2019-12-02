#!/usr/bin/python

#import time,datetime
#import subprocess,shlex
#import threading
#import Queue

#from mainDaemon import Daemon

#import setting
#from reader.main import readRawLog
#from processor.main import processToElastic
#from util.general import runSpecialCommand
#from util.general import waitChildren
#from util.returnStat import ReturnStat
from main import run

#read config
startDate="2014-04-01"
startPointer=0
endDate="2014-04-30"
HOST_LIST=[
 "?.?.?.?",      #data
 "?.?.?.?",      #data
#"?.?.?.?",       #management log
]

#end of config
run(startDate,startPointer,endDate,HOST_LIST)
print "FINISH",startDate,"to",endDate

