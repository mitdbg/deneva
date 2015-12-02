import os, sys, re, math, os.path, math
from plot_helper import progress,progress_diff
from helper import get_prog

PATH=os.getcwd()

#exp = sys.argv[1][39:]
#result_dir = PATH + "/../results/1027_ec2_full_experiments/"

exp = sys.argv[1][13:]
#exp = sys.argv[1][28:]
result_dir = PATH + "/../results/"
#result_dir = PATH + "/../results/sigmod_results/"

print(exp)
ncnt = int(sys.argv[2])
summary = {}
#xval = [30,60,90,120,150]
for n in range(ncnt):
    fname = result_dir + "{}_{}".format(n,exp)
    summary[str(n)] = get_prog(fname)
_name = exp
progress_diff(summary,ncnt,'txn_cnt',name=_name)
progress_diff(summary,ncnt,'time_validate',name=_name)
#progress(summary,ncnt,'cpu_ttl',name=_name)
#progress(summary,ncnt,'mbuf_send_time',name=exp)
#progress(summary,ncnt,'phys_mem_usage',name=_name)
progress_diff(summary,ncnt,'abort_cnt',name=_name)
progress(summary,ncnt,'tot_avg_abort_row_cnt',name=_name)
#progress_diff(summary,ncnt,'time_cleanup',name=_name)
progress_diff(summary,ncnt,'txn_rem_cnt',name=_name)
#progress_diff(summary,ncnt,'occ_check_cnt',name=_name)
#progress_diff(summary,ncnt,'occ_abort_check_cnt',name=_name)
#progress(exp,ncnt,'msg_bytes')
