#!/bin/bash
USERNAME=root
HOSTS="$1"
NODE_CNT=$3
IDENTITY="/usr0/home/dvanaken/.ssh/id_rsa_vcloud"
count=0

for HOSTNAME in ${HOSTS}; do
	#SCRIPT="env SCHEMA_PATH=\"$2\" timeout -k 10m 10m gdb -batch -ex \"run\" -ex \"bt\" --args ./rundb -nid${count} >> results.out 2>&1 | grep -v ^\"No stack.\"$"
    if [ $count -ge $NODE_CNT ]; then
	    SCRIPT="env SCHEMA_PATH=\"$2\" timeout -k 10m 10m ./runcl -nid${count} >> results.out 2>&1"
        echo "${HOSTNAME}: runcl"
    else
	    SCRIPT="env SCHEMA_PATH=\"$2\" timeout -k 10m 10m ./rundb -nid${count} >> results.out 2>&1"
        echo "${HOSTNAME}: rundb"
    fi
	ssh -i ${IDENTITY} -n -o BatchMode=yes -o StrictHostKeyChecking=no -l ${USERNAME} 172.19.153.${HOSTNAME} "${SCRIPT}" &
	count=`expr $count + 1`
done

while [ $count -gt 0 ]; do
	wait $pids
	count=`expr $count - 1`
done
