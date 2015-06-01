HOSTS="$1"
if [ -z "$HOSTS" ]
then
    HOSTS=$(cat ../vcloud_ifconfig.txt)
fi
#CMD="ntpdate -q clock-1.cs.cmu.edu"
CMD="ntpdate -b clock-1.cs.cmu.edu"
./vcloud_cmd.sh "$HOSTS" "$CMD" 
