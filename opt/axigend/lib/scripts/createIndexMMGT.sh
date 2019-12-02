#!/bin/bash

if [ -z $1 ]; then
	d=`date --date 'tomorrow' '+%Y-%m-%d'`
else
    d=$1
fi

echo /opt/axigend/lib/scripts/createIndexManagement $d-mmgt
/opt/axigend/lib/scripts/createIndexManagement $d-mmgt
