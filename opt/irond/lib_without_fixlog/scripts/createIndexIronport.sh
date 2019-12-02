#!/bin/bash

if [ -z $1 ]; then
	d=`date --date 'tomorrow' '+%Y-%m-%d'`
else
    d=$1
fi

echo /opt/irond/lib/scripts/createIndexIronport.py $d-ironport
/opt/irond/lib/scripts/createIndexIronport.py $d-ironport
