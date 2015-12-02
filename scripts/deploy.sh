#!/bin/bash
USERNAME=rhardin
HOSTS="$1"
NODE_CNT="$3"
count=0

for HOSTNAME in ${HOSTS}; do
  if [ $count -ge $NODE_CNT ]; then
	  SCRIPT="env SCHEMA_PATH=\"$2\" timeout -k 5m 5m ./runcl -nid${count} > results.out 2>&1"
    echo "${HOSTNAME}: runcl"
  else
	  SCRIPT="env SCHEMA_PATH=\"$2\" timeout -k 5m 5m ./rundb -nid${count} > results.out"
    echo "${HOSTNAME}: rundb"
  fi
	ssh -n -o BatchMode=yes -o StrictHostKeyChecking=no -l ${USERNAME} ${HOSTNAME}.csail.mit.edu "${SCRIPT}" &
	count=`expr $count + 1`
done

while [ $count -gt 0 ]; do
	wait $pids
	count=`expr $count - 1`
done
