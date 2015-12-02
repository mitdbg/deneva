#!/bin/bash
LOCAL_UNAME=benchpress
USERNAME=root
HOSTS="$1"
IDENTITY="/usr0/home/${LOCAL_UNAME}/.ssh/id_rsa_vcloud"
NODE_CNT="$3"
count=0

for HOSTNAME in ${HOSTS}; do
	#SCRIPT="env SCHEMA_PATH=\"$2\" timeout -k 10m 10m gdb -batch -ex \"run\" -ex \"bt\" --args ./rundb -nid${count} >> results.out 2>&1 | grep -v ^\"No stack.\"$"
    if [ $count -ge $NODE_CNT ]; then
	    SCRIPT="env SCHEMA_PATH=\"$2\" timeout -k 8m 8m ./runcl -nid${count} >> results.out 2>&1"
        echo "${HOSTNAME}: runcl ${count}"
    else
	    SCRIPT="env SCHEMA_PATH=\"$2\" timeout -k 8m 8m ./rundb -nid${count} >> results.out 2>&1"
        echo "${HOSTNAME}: rundb ${count}"
    fi
	ssh -i ${IDENTITY} -n -o BatchMode=yes -o StrictHostKeyChecking=no -l ${USERNAME} 172.19.153.${HOSTNAME} "${SCRIPT}" &
	count=`expr $count + 1`
done

while [ $count -gt 0 ]; do
	wait $pids
	count=`expr $count - 1`
done
