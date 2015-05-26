HOSTS=$(cat ../vcloud_ifconfig.txt)
CMD="pkill -f 'rundb'; pkill -f 'runcl'"
./vcloud_cmd.sh "$HOSTS" "$CMD" 
