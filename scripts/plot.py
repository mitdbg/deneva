import os, sys, re, math, os.path, math
from helper import get_cfgs 
from experiments import *
from plot_helper import *
import latency_stats as ls
import glob
import types
import pickle

PATH=os.getcwd()

###########################################
# Get experiments from command line
###########################################
drop = False
store_tmp = True
use_tmp = False
exp_cnt = 1
last_arg = None
plot = True;
clear = False;
exps = []
for arg in sys.argv[1:]:
    if last_arg == "-n":
        exp_cnt = int(arg)
    elif arg == "-clear":
        clear = True
    elif arg == "-s":
        store_tmp = True
    elif arg == "-ns":
        store_tmp = False
    elif arg == "-np":
        plot = False
    elif arg == "-u":
        use_tmp = True
#    elif exp_cnt == sys.maxint:
#        exp_cnt = int(arg)
#if arg == "-n":
#        exp_cnt = sys.maxint
    elif arg == "-d":
        drop = True
    elif arg == "-help" or arg == "-h":
        sys.exit("Usage: {}".format(sys.argv[0]))
    else:
        exps.append(arg)
    last_arg = arg

result_dir = PATH + "/../results/"
#result_dir = PATH + "/../results/results_201503pt2/"
test_dir = ""



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
        timestamp = 0
        cfgs = get_cfgs(fmt,e)
        output_f = get_outfile_name(cfgs,fmt,["*","*"])
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
            if not clear and os.path.isfile("{}s_{}.p".format(result_dir,output_f)) and os.path.isfile("{}c_{}.p".format(result_dir,output_f)):
                if not plot and use_tmp:
                    continue
                with open("{}s_{}.p".format(result_dir,output_f),'r') as f:
                    p = pickle.Unpickler(f)
                    r = p.load()
#r = pickle.load(f)
                with open("{}c_{}.p".format(result_dir,output_f),'r') as f:
                    p = pickle.Unpickler(f)
                    r2 = p.load()
#r2 = pickle.load(f)
            else:
                for n in range(cfgs["NODE_CNT"]+cfgs["CLIENT_NODE_CNT"]):
                    ofile = "{}{}_{}*.out".format(result_dir,n,output_f)
                    res_list = sorted(glob.glob(ofile),key=os.path.getmtime,reverse=True)
#timestamp = re.search("(\d{8}-\d{6})",res_list[0]).group(0)
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
            with open("{}s_{}.p".format(result_dir,output_f),'w') as f:
                p = pickle.Pickler(f)
                p.dump(r)
#pickle.dump(r,f)
            with open("{}c_{}.p".format(result_dir,output_f),'w') as f:
                p = pickle.Pickler(f)
                p.dump(r)
#pickle.dump(r2,f)
            if plot:
                summary[output_f] = r
                summary_client[output_f] = r2
#                print(output_f)
#                print(summary[output_f])

    if plot:
        exp_plot = exp + '_plot'
        if is_network_test:
            experiment_map[exp_plot](all_exps,all_nodes,timestamps)
        else:
            experiment_map[exp_plot](summary,summary_client)

exit()
