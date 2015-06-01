#!/bin/bash
USERNAME=root
LOCAL_UNAME=benchpress
HOSTS="$1"
SCRIPT="$2"
IDENTITY="/usr0/home/${LOCAL_UNAME}/.ssh/id_rsa_vcloud"
count=0
date --rfc-3339=ns
for HOSTNAME in ${HOSTS}; do
	ssh -i ${IDENTITY} -n -o BatchMode=yes -o StrictHostKeyChecking=no -l ${USERNAME} 172.19.153.${HOSTNAME} "${SCRIPT}" &
    #; echo ${HOSTNAME} &
	count=`expr $count + 1`
done

while [ $count -gt 0 ]; do
	wait $pids
	count=`expr $count - 1`
done
date --rfc-3339=ns
