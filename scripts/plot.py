import os, sys, re, math, os.path, math
from helper import get_cfgs 
from experiments import *
from plot_helper import *
import latency_stats as ls
import glob
import types
import pickle
import pprint

PATH=os.getcwd()

###########################################
# Get experiments from command line
###########################################
blah = False
drop = False
store_tmp = True
use_tmp = False
exp_cnt = 1
last_arg = None
plot = True;
clear = False;
_timedate = [];
exps = []
for arg in sys.argv[1:]:
    if last_arg == "-n":
        exp_cnt = int(arg)
    elif last_arg == "-tdate":
        _timedate.append(arg)
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
    elif arg == "-n" or arg == "-tdate":
        blah = True
#    elif exp_cnt == sys.maxint:
#        exp_cnt = int(arg)
#if arg == "-n":
#        exp_cnt = sys.maxint
    elif arg == "-d":
        drop = True
    elif arg == "-help" or arg == "-h":
        sys.exit("Usage: {} [-np no plot] [-clear clear all pickle files] [-tdate [date-time]] ".format(sys.argv[0]))
    else:
        exps.append(arg)
    last_arg = arg

result_dir = PATH + "/../results/"
#result_dir = PATH + "/../results/1010_zipf/"
#result_dir = PATH + "/../results/results_201503pt2/"
test_dir = ""



############################################
# Compile results into single dictionary
############################################
summary = {}
summary_client = {}

for exp in exps:
#    summary = {}
#    summary_client = {}
    fmt,experiments = experiment_map[exp]()

    for e in experiments:
        s = {}
        s2 = {}
        timestamp = 0
#        if "HSTORE" in e or "HSTORE_SPEC" in e:
#            nfmt=fmt
#            ne=e
#        else:
#            nfmt=fmt[:-1]
#            ne=e[:-1]
#        cfgs = get_cfgs(nfmt,ne)
        cfgs = get_cfgs(fmt,e)
        output_f = get_outfile_name(cfgs,fmt,["*","*"])
#        output_f = get_outfile_name(cfgs,nfmt,["*","*"])
        is_network_test = cfgs["NETWORK_TEST"] == "true"
        if is_network_test:
            r = {}
            r2 = {}
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
            opened = False
            timedate = []
            if _timedate == []:
                ofile = "{}{}_{}*.out".format(result_dir,0,output_f)
                res_list = sorted(glob.glob(ofile),key=os.path.getmtime,reverse=True)
                if res_list == 0:
                    continue
                for x in range(exp_cnt):
                    if x >= len(res_list):
                        print(output_f)
                        print("Exceeded experiment limit")
                        continue
                    timedate.append(re.search("(\d{8}-\d{6})",res_list[x]).group(0))
            else:
                timedate = _timedate
            p_sfiles = ["{}s_{}_{}.p".format(result_dir,output_f,t) for t in timedate]
            p_cfiles = ["{}c_{}_{}.p".format(result_dir,output_f,t) for t in timedate]
            for p_sfile,p_cfile,time in zip(p_sfiles,p_cfiles,timedate):
                print(p_sfile)
                r = {}
                r2 = {}
                if clear or not os.path.isfile(p_sfile) or not os.path.isfile(p_cfile):
                    for n in range(cfgs["NODE_CNT"]+cfgs["CLIENT_NODE_CNT"]):
                        ofile = "{}{}_{}*{}.out".format(result_dir,n,output_f,time)
                        res_list = sorted(glob.glob(ofile),key=os.path.getmtime,reverse=True)
                        if res_list:
                            assert time == re.search("(\d{8}-\d{6})",res_list[0]).group(0)
                            print(res_list[0])
                            if n < cfgs["NODE_CNT"]:
                                r = get_summary(res_list[0],r)
                            else:
                                r2 = get_summary(res_list[0],r2)
                    get_lstats(r)
                    get_lstats(r2)
                    with open(p_sfile,'w') as f:
                        p = pickle.Pickler(f)
                        p.dump(r)
                    with open(p_cfile,'w') as f:
                        p = pickle.Pickler(f)
                        p.dump(r2)
                else:
                    with open(p_sfile,'r') as f:
                        p = pickle.Unpickler(f)
                        r = p.load()
                        opened = True
                    with open(p_cfile,'r') as f:
                        p = pickle.Unpickler(f)
                        r2 = p.load()
                        opened = True

#                merge_results(r,cfgs["NODE_CNT"],0)
#                merge_results(r2,cfgs["CLIENT_NODE_CNT"],0)
                if s == {}:
                    s = r
                    s2 = r2
                else:
                    merge(s,r)
                    merge(s2,r2)
                    print(s['txn_cnt'])

            if plot:
                s = merge_results(s,exp_cnt,drop,cfgs["NODE_CNT"])
                s2 = merge_results(s2,exp_cnt,drop,cfgs["CLIENT_NODE_CNT"])
                summary[output_f] = s
                pp = pprint.PrettyPrinter()
#                pp.pprint(summary[output_f]['txn_cnt'])
#                pp.pprint(summary[output_f]['thd1'])
#                pp.pprint(summary[output_f]['thd2'])
#                pp.pprint(summary[output_f]['thd3'])
                summary_client[output_f] = s2
#                print(output_f)
#                print(summary[output_f])

    if plot:
        exp_plot = exp + '_plot'
        if is_network_test:
            experiment_map[exp_plot](all_exps,all_nodes,timestamps)
        else:
            experiment_map[exp_plot](summary,summary_client)

exit()
