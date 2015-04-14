import os, sys, re, math, os.path, math
from helper import *
from experiments import experiments as experiments
from experiments import configs
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
txn_cnt = 10000
mpr = [0,1,10,20]#,30,40,50]
nodes = [1]
threads = [1,2,4]
#warehouses = [2,4,6,8,10]
algos = ['HSTORE','NO_WAIT','WAIT_DIE','TIMESTAMP','MVCC','OCC']

#for algo in algos:
#    _cfg_fmt = ["NODE_CNT","CC_ALG","MAX_TXN_PER_PART","THREAD_CNT"]
#    _cfg=[nodes[0],algo,txn_cnt,threads[0]]
#    _title="{}".format(algo)
#    tput(mpr,warehouses,summary,cfg_fmt=_cfg_fmt,cfg=_cfg,xname="MPR",vname="NUM_WH",title=_title)

for algo,thread in itertools.product(algos,threads):
    #tput_mpr(mpr,nodes,[algo], txn_cnt,summary)
    tput(mpr,nodes,summary,cfg_fmt=["CC_ALG","MAX_TXN_PER_PART","THREAD_CNT"],cfg=[algo,txn_cnt,1],xname="MPR",vname="NODE_CNT",title="{} {} Threads".format(algo,thread))
        

for node,thread in itertools.product(nodes,threads):
    tput(mpr,algos,summary,cfg_fmt=["NODE_CNT","MAX_TXN_PER_PART","THREAD_CNT","NUM_WH"],cfg=[node,txn_cnt,thread,4],xname="MPR",vname="CC_ALG",title="{} Nodes {} Threads".format(node,thread))

for node,algo in itertools.product(nodes,algos):
    _cfg_fmt = ["NODE_CNT","CC_ALG","MAX_TXN_PER_PART"]
    _cfg=[node,algo,txn_cnt]
    _title="{} {} Nodes".format(algo,node)
    tput(mpr,threads,summary,cfg_fmt=_cfg_fmt,cfg=_cfg,xname="MPR",vname="THREAD_CNT",title=_title)

for node,algo,thread in itertools.product(nodes,algos,threads):
    _cfg_fmt = ["NODE_CNT","CC_ALG","MAX_TXN_PER_PART","THREAD_CNT"]
    _cfg=[node,algo,txn_cnt,thread]
    _title="{} {} Nodes {} Threads".format(algo,node,thread)
    time_breakdown(mpr,summary,normalized=False,cfg_fmt=_cfg_fmt,cfg=_cfg,xname="MPR",title=_title)
    time_breakdown(mpr,summary,normalized=True,cfg_fmt=_cfg_fmt,cfg=_cfg,xname="MPR",title=_title)
#    time_breakdown(mpr,node,algo,txn_cnt,summary,normalized=True)
#    cdf_aborts_mpr(mpr,node,algo,txn_cnt,summary)
#    bar_aborts_mpr(mpr,node,algo,txn_cnt,summary)

