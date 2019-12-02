#!/bin/bash
file="/root/bgscript/running"

if ! [ -a "$file" ]
then
	touch $file
	echo "start..."
	/root/bgscript/bigExport.py
	echo "finish..."
	rm -f $file
else
	echo "$file is already exist."
fi
