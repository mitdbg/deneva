#!/bin/bash
USERNAME=rhardin
HOSTS="$1"
SCRIPT="$2"
count=0
date --rfc-3339=ns
for HOSTNAME in ${HOSTS}; do
	ssh -n -o BatchMode=yes -o StrictHostKeyChecking=no -l ${USERNAME} ${HOSTNAME}.csail.mit.edu "${SCRIPT}" &
	count=`expr $count + 1`
done

while [ $count -gt 0 ]; do
	wait $pids
	count=`expr $count - 1`
done
date --rfc-3339=ns
