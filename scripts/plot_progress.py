import os, sys, re, math, os.path, math
from plot_helper import progress,progress_diff
from helper import get_prog

PATH=os.getcwd()

exp = sys.argv[1][13:]
result_dir = PATH + "/../results/"

print(exp)
ncnt = int(sys.argv[2])
summary = {}
#xval = [30,60,90,120,150]
for n in range(ncnt):
    fname = result_dir + "{}_{}".format(n,exp)
    summary[str(n)] = get_prog(fname)
progress_diff(summary,ncnt,'txn_cnt',name=exp)
progress(summary,ncnt,'cpu_ttl',name=exp)
progress(summary,ncnt,'mbuf_send_time',name=exp)
progress(summary,ncnt,'phys_mem_usage',name=exp)
#progress(exp,ncnt,'abrt_cnt')
#progress(exp,ncnt,'msg_bytes')
