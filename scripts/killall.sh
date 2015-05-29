HOSTS=$(cat ../vcloud_ifconfig.txt)
CMD1="pkill -f 'rundb'"
CMD2="pkill -f 'runcl'"
./vcloud_cmd.sh "$HOSTS" "$CMD1" 
./vcloud_cmd.sh "$HOSTS" "$CMD2" 
