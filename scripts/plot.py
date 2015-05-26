import os, sys, re, math, os.path, math
from helper import *
from experiments import experiments as experiments
from experiments import configs
from experiments import nnodes,nmpr,nalgos,nthreads,nwfs,ntifs,nnet_delay,ntxn,nzipf,nwr_perc
from plot_helper import *
import glob

PATH=os.getcwd()

rem = False
for arg in sys.argv:
    if arg == "-rem":
        rem = True
    if arg == "-help" or arg == "-h":
        sys.exit("Usage: {} [-rem]".format(sys.argv[0]))

result_dir = PATH + "/../results/"
#result_dir = PATH + "/../results/results_201503pt2/"
test_dir = ""

# Unpack results from remote machine
if rem:
    tests_ = result_dir + "tests-*.tgz"
    test_tgz = sorted(glob.glob(tests_),key=os.path.getmtime,reverse=True)
    print(test_tgz[0])
    test_dir = test_tgz[0][:-4] + "/"
    cmd = "tar -xf {} -C {}".format(test_tgz[0],result_dir)
    os.system(cmd)


############################################
# Compile results into single dictionary
############################################
summary = {}
for e in experiments[1:]:
    r = {}
    cfgs = get_cfgs(experiments[0],e)
    output_f = get_outfile_name(cfgs)
    for n in range(cfgs["NODE_CNT"]):
        if rem:
            ofile = "{}{}/{}_{}.out".format(test_dir,output_f,n,output_f)
        else:
            ofile = "{}{}_{}*.out".format(result_dir,n,output_f)
        res_list = sorted(glob.glob(ofile),key=os.path.getmtime,reverse=True)
        if res_list:
            print(res_list[0])
            r = get_summary(res_list[0],r)
    summary[output_f] = r
 
############
# Plotting
############

#mpr = [0,1,10,20,30,40,50,60,70,80,90,100]

# Runtime contributions
# Throughput vs. MPR for HStore, many node counts
txn_cnt = ntxn
nmpr.sort()
mpr = nmpr #[0,1,10,20]#,30,40,50]
nodes = nnodes #[1]
threads = nthreads #[1,2,4]
#warehouses = nnodes * [2,4,6,8,10]
algos = nalgos #['HSTORE','NO_WAIT','WAIT_DIE','TIMESTAMP','MVCC','OCC']
tifs = ntifs
net_delay = nnet_delay
whs = nwfs
zipf = nzipf
wr_perc = nwr_perc


#artificial network delay
#for algo,thread,wh,node in itertools.product(algos,threads,whs,nodes):
#    _cfg_fmt = ["CC_ALG","MAX_TXN_PER_PART","THREAD_CNT","NUM_WH","NODE_CNT"]
#    _cfg=[algo,txn_cnt,thread,wh,node]
#    _title="Network Delay {} {} Nodes {} Threads {} Warehouses".format(algo,node,thread,wh)
#    tput(mpr,net_delay,summary,cfg_fmt=_cfg_fmt,cfg=_cfg,xname="MPR",vname="NETWORK_DELAY",title=_title)
#    time_breakdown(mpr,summary,normalized=True,cfg_fmt=_cfg_fmt,cfg=_cfg,xname="MPR",title=_title)
#    time_breakdown_basic(mpr,summary,normalized=True,cfg_fmt=_cfg_fmt,cfg=_cfg,xname="MPR",title=_title)

#exit()

#for algo in algos:
#    _cfg_fmt = ["NODE_CNT","CC_ALG","MAX_TXN_PER_PART","THREAD_CNT"]
#    _cfg=[nodes[0],algo,txn_cnt,threads[0]]
#    _title="{}".format(algo)
#    tput(mpr,warehouses,summary,cfg_fmt=_cfg_fmt,cfg=_cfg,xname="MPR",vname="NUM_WH",title=_title)
for n,t,wh,tif in itertools.product(nodes,threads,whs,tifs):
    #tput_mpr(mpr,nodes,[algo], txn_cnt,summary)
    _cfg_fmt = ["MAX_TXN_PER_PART","WORKLOAD","NODE_CNT","THREAD_CNT","NUM_WH","MAX_TXN_IN_FLIGHT"]
    _cfg=[txn_cnt,'TPCC',n,t,wh,tif]
    _title="{} {} Nodes".format('TPCC',n)
    tput(mpr,algos,summary,cfg_fmt=_cfg_fmt,cfg=_cfg,xname="MPR",vname="CC_ALG",title=_title)

exit()

for m,z,wr,t,wh,tif in itertools.product(mpr,zipf,wr_perc,threads,whs,tifs):
    #tput_mpr(mpr,nodes,[algo], txn_cnt,summary)
    _cfg_fmt = ["MAX_TXN_PER_PART","WORKLOAD","MPR","THREAD_CNT","NUM_WH","MAX_TXN_IN_FLIGHT","ZIPF_THETA","READ_PERC","WRITE_PERC"]
    _cfg=[txn_cnt,'YCSB',m,t,wh,tif,z,1.0-wr,wr]
    _title="{} {} MPR {}% Writes".format('YCSB',m,wr*100)
    tput(nodes,algos,summary,cfg_fmt=_cfg_fmt,cfg=_cfg,xname="NODE_CNT",vname="CC_ALG",title=_title)

exit()
 
for algo,thread in itertools.product(algos,threads):
    #tput_mpr(mpr,nodes,[algo], txn_cnt,summary)
    _cfg_fmt = ["CC_ALG","MAX_TXN_PER_PART","THREAD_CNT","NUM_WH"]
    _cfg=[algo,txn_cnt,thread,64]
    _title="{} {} Threads".format(algo,thread)
    tput(mpr,nodes,summary,cfg_fmt=_cfg_fmt,cfg=_cfg,xname="MPR",vname="NODE_CNT",title=_title)
        

for node,thread,tif in itertools.product(nodes,threads,tifs):
    _cfg_fmt = ["NODE_CNT","MAX_TXN_PER_PART","THREAD_CNT","NUM_WH","MAX_TXN_IN_FLIGHT"]
    _cfg=[node,txn_cnt,thread,64,tif]
    _title="{} Nodes {} Threads {} TiF".format(node,thread,tif)
    tput(mpr,algos,summary,cfg_fmt=_cfg_fmt,cfg=_cfg,xname="MPR",vname="CC_ALG",title=_title)

for node,algo,tif in itertools.product(nodes,algos,tifs):
    _cfg_fmt = ["NODE_CNT","CC_ALG","MAX_TXN_PER_PART","MAX_TXN_IN_FLIGHT","NUM_WH"]
    _cfg=[node,algo,txn_cnt,tif,64]
    _title="{} {} Nodes {} TiF".format(algo,node,tif)
    tput(mpr,threads,summary,cfg_fmt=_cfg_fmt,cfg=_cfg,xname="MPR",vname="THREAD_CNT",title=_title)

for node,algo,thread,tif in itertools.product(nodes,algos,threads,tifs):
    _cfg_fmt = ["NODE_CNT","CC_ALG","MAX_TXN_PER_PART","THREAD_CNT","MAX_TXN_IN_FLIGHT","NUM_WH"]
    _cfg=[node,algo,txn_cnt,thread,tif,64]
    _title="{} {} Nodes {} Threads {} TiF".format(algo,node,thread,tif)
#    time_breakdown(mpr,summary,normalized=False,cfg_fmt=_cfg_fmt,cfg=_cfg,xname="MPR",title=_title)
    time_breakdown(mpr,summary,normalized=True,cfg_fmt=_cfg_fmt,cfg=_cfg,xname="MPR",title=_title)
    time_breakdown_basic(mpr,summary,normalized=True,cfg_fmt=_cfg_fmt,cfg=_cfg,xname="MPR",title=_title)
#    time_breakdown_basic(mpr,summary,normalized=False,cfg_fmt=_cfg_fmt,cfg=_cfg,xname="MPR",title=_title)
#    time_breakdown(mpr,node,algo,txn_cnt,summary,normalized=True)
#    cdf(mpr,summary,cfg_fmt=_cfg_fmt,cfg=_cfg,vname="MPR",title="Aborts " + _title)

#    for m in mpr:
#        _cfg_fmt = ["NODE_CNT","CC_ALG","MAX_TXN_PER_PART","THREAD_CNT","MAX_TXN_IN_FLIGHT","NUM_WH","MPR"]
#        _cfg=[node,algo,txn_cnt,thread,tif,64,m]
#        _title="{} {} Nodes {} Threads {} TiF {} MPR".format(algo,node,thread,tif,m)
#        bar_keys(summary,rank=10,cfg_fmt=_cfg_fmt,cfg=_cfg,title=_title)
#    bar_aborts_mpr(mpr,node,algo,txn_cnt,summary)

