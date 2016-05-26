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
res_dir = False
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
    elif arg == "-r":
        res_dir = True
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
result_dir = PATH + "/../results/plotting_results_0524/"
#result_dir = PATH + "/../results/ec2_results_0524/"
#result_dir = PATH + "/../results/sigmod_results/"
#result_dir = PATH + "/../results/1108_results/"
#result_dir = PATH + "/../results/1027_ec2_full_experiments/"
#result_dir = PATH + "/../results/1028_ec2_results/"
#result_dir = PATH + "/../results/results_201503pt2/"
test_dir = ""
if res_dir:
    result_dir = PATH + "/../results/"



############################################
# Compile results into single dictionary
############################################
summary = {}
summary_client = {}
pp = pprint.PrettyPrinter()

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
        nnodes = cfgs["NODE_CNT"]
        nclients = cfgs["CLIENT_NODE_CNT"]
        try:
            ntotal = nnodes + nclients
        except TypeError:
            nclients = cfgs[cfgs["CLIENT_NODE_CNT"]]
            ntotal = nnodes + nclients
        cc = cfgs["CC_ALG"]
        if  cc == "CALVIN":
            ntotal += 1

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
#                ofile = "{}{}_{}*.out".format(result_dir,0,output_f)
                ofile = "{}*{}*".format(result_dir,output_f)
                res_list = sorted(glob.glob(ofile),key=os.path.getmtime,reverse=True)
                if res_list == 0:
                    continue
                print(output_f)
                for x in range(exp_cnt):
                    if x >= len(res_list):
                        print("Exceeded experiment limit")
                        continue
                    timedate.append(re.search("(\d{8}-\d{6})",res_list[x]).group(0))
            else:
                timedate = _timedate
            p_sfiles = ["{}s_{}_{}.p".format(result_dir,output_f,t) for t in timedate]
            p_cfiles = ["{}c_{}_{}.p".format(result_dir,output_f,t) for t in timedate]
            for p_sfile,p_cfile,time in zip(p_sfiles,p_cfiles,timedate):
                print(p_sfile)
                p_qfile = "{}q_{}_{}.p".format(result_dir,output_f,time)
                r = {}
                r2 = {}
                r3 = {}
                if clear or not os.path.isfile(p_sfile) or not os.path.isfile(p_cfile):
                    for n in range(ntotal):
                        ofile = "{}{}_{}*{}.out".format(result_dir,n,output_f,time)
                        res_list = sorted(glob.glob(ofile),key=os.path.getmtime,reverse=True)
                        if res_list:
                            assert time == re.search("(\d{8}-\d{6})",res_list[0]).group(0)
                            print(res_list[0])
                            if n < nnodes:
                                r = get_summary(res_list[0],r)
                            elif n >= nnodes and n < nnodes + nclients:
                                print(cc,n)
                                r2 = get_summary(res_list[0],r2)
                            else:
                                r3 = get_summary(res_list[0],r3)
                    get_lstats(r)
                    get_lstats(r2)
                    get_lstats(r3)
                    with open(p_sfile,'w') as f:
                        p = pickle.Pickler(f)
                        p.dump(r)
                    with open(p_cfile,'w') as f:
                        p = pickle.Pickler(f)
                        p.dump(r2)
                    if cc == "CALVIN":
                        with open(p_qfile,'w') as f:
                            p = pickle.Pickler(f)
                            p.dump(r3)
                else:
                    with open(p_sfile,'r') as f:
                        p = pickle.Unpickler(f)
                        r = p.load()
                        opened = True
                    with open(p_cfile,'r') as f:
                        p = pickle.Unpickler(f)
                        r2 = p.load()
                        opened = True
                    if cc == "CALVIN":
                        with open(p_qfile,'r') as f:
                            p = pickle.Unpickler(f)
                            r3 = p.load()
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
#                pp.pprint(s)
#                pp.pprint(s2)

            if plot:
#                print("Pre-Merge:")
#                pp.pprint(s)
#                pp.pprint(s2)
                if nnodes == 64:
                    write_breakdown_file(output_f,s,s2)
                write_breakdown_file(output_f,s,s2)
                s = merge_results(s,exp_cnt,drop,nnodes)
                s2 = merge_results(s2,exp_cnt,drop,nclients)
                summary[output_f] = s
#                pp.pprint(summary[output_f]['txn_cnt'])
#                pp.pprint(summary[output_f]['thd1'])
#                pp.pprint(summary[output_f]['thd2'])
#                pp.pprint(summary[output_f]['thd3'])
                summary_client[output_f] = s2
#                print("Post-Merge:")
#                pp.pprint(s)
#                pp.pprint(s2)
#                print(output_f)
#                print(summary[output_f])

    if plot:
#        pp.pprint(summary)
#        pp.pprint(summary_client)
        exp_plot = exp + '_plot'
        if is_network_test:
            experiment_map[exp_plot](all_exps,all_nodes,timestamps)
        else:
            experiment_map[exp_plot](summary,summary_client)

exit()
