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
for e in experiments:
    r = {}
    cfgs = get_cfgs(e)
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
mpr = [0,1,10,20,30,40,50]
nodes = [2,4]
algos = ['HSTORE','NO_WAIT','WAIT_DIE','TIMESTAMP','MVCC','OCC']
for algo in algos:
    tput_mpr(mpr,nodes,[algo], txn_cnt,summary)
for node in nodes:
    tput_mpr(mpr,[node],algos, txn_cnt,summary)

for node,algo in itertools.product(nodes,algos):
    time_breakdown(mpr,node,algo,txn_cnt,summary,normalized=False)
    time_breakdown(mpr,node,algo,txn_cnt,summary,normalized=True)
    cdf_aborts_mpr(mpr,node,algo,txn_cnt,summary)
    bar_aborts_mpr(mpr,node,algo,txn_cnt,summary)


