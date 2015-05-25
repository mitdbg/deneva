HOSTS=$(cat ../vcloud_ifconfig.txt)
CMD="ntpdate -b clock-1.cs.cmu.edu"
./vcloud_cmd.sh "$HOSTS" "$CMD" 
