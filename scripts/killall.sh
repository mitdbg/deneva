HOSTS=$(cat ../vcloud_ifconfig.txt)
CMD="pkill -f 'rundb'"
./vcloud_cmd.sh "$HOSTS" "$CMD" 
