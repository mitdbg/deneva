HOSTS=$(cat /home/benchpress/DBx1000/vcloud_ifconfig.txt)
CMD1="pkill -f 'rundb'"
CMD2="pkill -f 'runcl'"
/home/benchpress/DBx1000/scripts/vcloud_cmd.sh "$HOSTS" "$CMD1" 
/home/benchpress/DBx1000/scripts/vcloud_cmd.sh "$HOSTS" "$CMD2" 
