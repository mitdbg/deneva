import os, sys, re, math, os.path, math
from helper import get_cfgs 
from experiments import *
from plot_helper import *
import glob
import types

PATH=os.getcwd()

drop = False
rem = False
exp_cnt = 1
for arg in sys.argv:
    if arg == "-rem":
        rem = True
    elif exp_cnt == sys.maxint:
        exp_cnt = int(arg)
    elif arg == "-n":
        exp_cnt = sys.maxint
    elif arg == "-help" or arg == "-h":
        sys.exit("Usage: {} [-rem]".format(sys.argv[0]))

result_dir = PATH + "/../results/"
#result_dir = PATH + "/../results/results_201503pt2/"
test_dir = ""

###########################################
# Get experiments from command line
###########################################
exps = []
for arg in sys.argv[1:]:
    if arg == "-help" or arg == "-h":
        sys.exit("Usage: %s experiments \
                -n Number of experiments to average\
                -d Drop max and min of each individual stat\
                " % sys.argv[0])
    elif exp_cnt == sys.maxint:
        exp_cnt = int(arg)
    elif arg == "-n":
        exp_cnt = sys.maxint
    elif arg == "-d":
        drop = True
    else:
        exps.append(arg)


############################################
# Compile results into single dictionary
############################################

for exp in exps:
    summary = {}
    fmt,experiments = experiment_map[exp]()

    for e in experiments:
        r = {}
        cfgs = get_cfgs(fmt,e)
        output_f = get_outfile_name(cfgs)
        for n in range(cfgs["NODE_CNT"]):
            if rem:
                ofile = "{}{}/{}_{}.out".format(test_dir,output_f,n,output_f)
            else:
                ofile = "{}{}_{}*.out".format(result_dir,n,output_f)
            res_list = sorted(glob.glob(ofile),key=os.path.getmtime,reverse=True)
            if res_list:
                for x in range(exp_cnt):
                    print(res_list[x])
                    r = get_summary(res_list[x],r)
                merge_results(r,exp_cnt,drop)
        summary[output_f] = r
 
    exp_plot = exp + '_plot'
    experiment_map[exp_plot](summary)

exit()
