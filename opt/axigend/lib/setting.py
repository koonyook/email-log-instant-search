'''
	this file assign CONSTANT_VALUE only

	usage:
		import setting
		walk setting.MAIN_PATH
'''
LOG_PATH="/var/log/"

MAIN_PATH="/opt/axigend/lib/"			#end with '/' because this is only way to allow path to be "/"

INPUT_PATH='/exports/'
PATTERN_FILE='/opt/axigend/lib/axigen-json.pattern'

SUBTHREAD_SIZE=2
CACHE_TIME=300 #5 minutes

MAX_INQUEUE=10000

ES_CONNECTION = "?.?.?.?:9200"
MC_CONNECTION = "?.?.?.?:11211"


#configuration
HOST_LIST=[
	"?.?.?.?",		#axmail3
	"?.?.?.?",		#axmail5
	"?.?.?.?"		#management log
]

RUN_THIS_COMMAND_WHEN_THREAD_ERROR_IS_FOUND = "service axigend stop"

#normal condition, start at that moment and keep working
START_DATE=""		#let it blank
START_POINTER=-1		#set to zero to start at the beginning of current day, # set to a number to start at the byte you know
END_DATE=""			#let it blank

##fix log, both date must be in the past
#START_DATE="2014-01-05"
#START_POINTER=436576876
#END_DATE="2014-01-07"

KEEP_PERIOD=366 #days
