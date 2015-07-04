import os, sys, re, math, os.path, math
from helper import get_cfgs 
from experiments import *
from plot_helper import *
import latency_stats as ls
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
    summary_client = {}
    fmt,experiments = experiment_map[exp]()

    for e in experiments:
        r = {}
        r2 = {}
        cfgs = get_cfgs(fmt,e)
        output_f = get_outfile_name(cfgs,["*","*"])
        is_network_test = cfgs["NETWORK_TEST"] == "true"
        if is_network_test:
            ofile = "{}0_{}*".format(result_dir,output_f) 
            res_list = sorted(glob.glob(ofile),key=os.path.getmtime,reverse=False)
            timestamps = list(set([t.split("_")[5] for t in res_list]))
            exp_cnt = len(timestamps)
            all_exps = []
            all_nodes = []
            for i in range(0,exp_cnt):
                results = [r for r in res_list if r.endswith(timestamps[i])]
                sub_exp = []
                all_latencies = {}
                nodes = []
                for res in results:
                    s = get_network_stats(res)
                    sub_exp.append(s)
                    for msg_bytes,stat in s.iteritems():
                        md = stat.get_metadata()
                        nodes.append(md['n0'])
                        nodes.append(md['n1'])
                        #num_msg_bytes = md['bytes']
                        if msg_bytes not in all_latencies:
                            all_latencies[msg_bytes] = []
                        all_latencies[msg_bytes].append(stat.get_latencies())
                for k,v in all_latencies.iteritems():
                    metadata = {}
                    metadata['bytes'] = k
                    all_latencies[k] = ls.LatencyStats(v,metadata)
                sub_exp.append(all_latencies)
                all_exps.append(sub_exp)
                all_nodes.append(sorted(list(set(nodes))))
        else:
            for n in range(cfgs["NODE_CNT"]+cfgs["CLIENT_NODE_CNT"]):
                if rem:
                    ofile = "{}{}/{}_{}.out".format(test_dir,output_f,n,output_f)
                else:
                    ofile = "{}{}_{}*.out".format(result_dir,n,output_f)
                res_list = sorted(glob.glob(ofile),key=os.path.getmtime,reverse=True)
                if res_list:
                    for x in range(exp_cnt):
                        if x >= len(res_list):
                            continue
                        print(res_list[x])
                        if n < cfgs["NODE_CNT"]:
                            r = get_summary(res_list[x],r)
                        else:
                            r2 = get_summary(res_list[x],r2)
                    merge_results(r,exp_cnt,drop)
                    merge_results(r2,exp_cnt,drop)
            
            get_lstats(r)
            get_lstats(r2)
            summary[output_f] = r
            summary_client[output_f] = r2
 
    exp_plot = exp + '_plot'
    if is_network_test:
        experiment_map[exp_plot](all_exps,all_nodes,timestamps)
    else:
        experiment_map[exp_plot](summary,summary_client)

exit()
