#!/bin/bash
USERNAME=rhardin
HOSTS="$1"
count=0

for HOSTNAME in ${HOSTS}; do
	SCRIPT="env SCHEMA_PATH=\"$2\" timeout -k 3m 3m ./rundb -nid${count} > results.out"
	ssh -n -o BatchMode=yes -o StrictHostKeyChecking=no -l ${USERNAME} ${HOSTNAME}.csail.mit.edu "${SCRIPT}" &
	count=`expr $count + 1`
done

while [ $count -gt 0 ]; do
	wait $pids
	count=`expr $count - 1`
done
