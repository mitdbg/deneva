import os,re,sys,math
from experiments import configs
from experiments import config_names
import glob
import latency_stats as ls

SHORTNAMES = {
    "CLIENT_NODE_CNT" : "CN",
    "CLIENT_THREAD_CNT" : "CT",
    "CLIENT_REM_THREAD_CNT" : "CRT",
    "CLIENT_SEND_THREAD_CNT" : "CST",
    "NODE_CNT" : "N",
    "THREAD_CNT" : "T",
    "REM_THREAD_CNT" : "RT",
    "SEND_THREAD_CNT" : "ST",
    "CC_ALG" : "",
    "WORKLOAD" : "",
    "MAX_TXN_PER_PART" : "TXNS",
    "MAX_TXN_IN_FLIGHT" : "TIF",
    "PART_PER_TXN" : "PPT",
    "READ_PERC" : "RD",
    "WRITE_PERC" : "WR",
    "ZIPF_THETA" : "SKEW",
    "MSG_TIME_LIMIT" : "BT",
    "MSG_SIZE_MAX" : "BS",
}

stat_map = {
'latency': [],
 'txn_sent': [],
 'all_lat': [],
 'msg_sent': [],
 'time_msg_sent': [],
 'msg_rcv': [],
 'txn_cnt': [],
 'time_tport_rcv': [],
 'tport_lat': [],
 'time_tport_send': [],
 'clock_time': [],
 'msg_bytes': [],
 'time_getqry': [],
'cc_hold_time': [],
 'abort_cnt': [],
 'time_abort': [],
 'txn_time_wait': [],
 'time_validate': [],
 'txn_time_copy': [],
 'msg_rcv': [],
 'cc_wait_cnt': [],
 'qry_rprep': [],
 'time_index': [],
 'time_tport_send': [],
 'tport_lat': [],
 'cc_wait_abrt_time': [],
 'latency': [],
 'time_cleanup': [],
 'time_lock_man': [],
 'qq_full': [],
 'cc_hold_abrt_time': [],
 'spec_abort_cnt': [],
 'time_msg_sent': [],
 'qry_rack': [],
 'qq_lat': [],
 'txn_cnt': [],
 'time_man': [],
 'time_rqry': [],
 'qry_rfin': [],
 'aq_full': [],
 'cc_wait_time': [],
 'time_wait': [],
 'msg_bytes': [],
 'clock_time': [],
 'qry_rqry_rsp': [],
 'time_tport_rcv': [],
 'rbk_abort_cnt': [],
 'txn_abort_cnt': [],
 'qry_rinit': [],
 'txn_time_q_work': [],
 'txn_time_man': [],
 'txn_time_ts': [],
 'txn_time_net': [],
 'time_clock_rwait': [],
 'run_time': [],
 'txn_time_clean': [],
 'txn_time_misc': [],
 'qry_rqry': [],
 'cc_wait_abrt_cnt': [],
 'time_qq': [],
 'txn_time_twopc': [],
 'mpq_cnt': [],
 'cflt_cnt': [],
 'time_wait_rem': [],
 'qry_cnt': [],
 'time_wait_lock': [],
 'txn_time_idx': [],
 'txn_time_abrt': [],
 'msg_sent': [],
 'qry_rtxn': [],
 'time_clock_wait': [],
 'time_work': [],
 'time_wait_lock_rem': [],
 'spec_commit_cnt': [],
 'time_ts_alloc': [],
 'txn_time_q_abrt': [],
 'lat_min': [],
 'lat_max': [],
 'lat_mean': [],
 'lat_99ile': [],
 'lat_98ile': [],
 'lat_95ile': [],
 'lat_90ile': [],
 'lat_80ile': [],
 'lat_75ile': [],
 'lat_70ile': [],
 'lat_60ile': [],
 'lat_50ile': [],
 'lat_40ile': [],
 'lat_30ile': [],
 'lat_25ile': [],
 'lat_20ile': [],
 'lat_10ile': [],
 'lat_5ile': []

}

cnts = ["all_abort"]
cflts = ["w_cflt","d_cflt","cnp_cflt","c_cflt","ol_cflt","s_cflt","w_abrt","d_abrt","cnp_abrt","c_abrt","ol_abrt","s_abrt"]
lats = ["all_lat"]

def avg(l):
    if len(l) == 0:
        return 0
    return float(sum(l) / float(len(l)))

def stdev(l):
    c = avg(l)
    ss = sum((x-c)**2 for x in l) / len(l)
    return ss**0.5

def find_in_line(key,line,summary,min_time,low_lim,up_lim):
    if re.search(key,line):
        line = [int(s) for s in line.split() if s.isdigit()]
        tid = line[0]
        if min_time == 0:
            min_time = line[1]
        time = line[1] - min_time
#if tid >= low_lim and tid < up_lim:
        if time >= low_lim and time < up_lim:
            summary[key]["time"].append(time)
            summary[key]["tid"].append(tid)
    return summary,min_time

def get_timeline(sfile,summary={},low_lim=0,up_lim=sys.maxint,min_time=0):
    keys = ["START","ABORT","COMMIT","LOCK","UNLOCK"]
    for k in keys:
        if k not in summary.keys():
            summary[k] = {"time":[],"tid":[]}
    with open(sfile,'r') as f:
        for line in f:
            for k in keys:
                summary,min_time = find_in_line(k,line,summary,min_time,low_lim,up_lim)
    return summary,min_time

def get_prog(sfile):
    summary = {}
    with open(sfile,'r') as f:
        for line in f:
            if re.search("prog",line):
                line = line[7:] #remove '[prog] ' from start of line 
                results = re.split(',',line)
                process_results(summary,results)
    return summary
#return summary['txn_cnt'],[int(x) for x in summary['clock_time']]

def get_summary(sfile,summary={}):
    with open(sfile,'r') as f:
        found = False
        last_line = ""
        for line in f:
            if re.search("prog",line):
                last_line = line
            if re.search("summary",line):
                found = True
                line = line.rstrip('\n')
                line = line[10:] #remove '[summary] ' from start of line 
                results = re.split(',',line)
                process_results(summary,results)
                continue
            if found:
                for c in cnts:
                    if re.search("^[.*"+c+".*]",line):
                        line = line.rstrip('\n')
                        process_cflts(summary,line,c)
                for c in cflts:
                    if re.search("^[.*"+c+".*]",line):
                        line = line.rstrip('\n')
                        process_cflts(summary,line,c)
                for l in lats:
                    if re.search(l,line):
                        line = line.rstrip('\n')
                        process_lats(summary,line,l)
        if not found:
            if re.search("prog",last_line):
                line = last_line.rstrip('\n')
                line = line[7:] #remove '[prog] ' from start of line 
                results = re.split(',',line)
                process_results(summary,results)
    return summary

def get_network_stats(n_file):
    setup = n_file.split("/")[-1].split("_")

    # A few checks
    assert setup[0] == "0" # The corresponding file contains no info
    assert setup[3] == "NETWORK"

    # What to call the participating pair of nodes
    node_names = {}
    node_names['n0']=setup[1]
    node_names['n1']=setup[2]

    with open(n_file,'r') as f:
        lines = f.readlines()

    stats = {}
    for line in lines:
        if line.startswith('0:') or line.startswith('1:'):
            assert line.strip()[-3:] in node_names.values()
        elif line.startswith("Network Bytes:"):
            metadata = {}
            metadata.update(node_names.copy())
            num_msg_bytes=line.split(":")[1].strip()
            metadata["bytes"]=num_msg_bytes
        elif line.startswith('ns:'):
            lat_str = line.split(":")[1].strip()
            latencies = lat_str.split(" ")
            latencies = list(map(int,latencies))
            stats[metadata["bytes"]] = ls.LatencyStats(latencies,metadata)
    return stats

def merge(summary,tmp):
#    for k in summary.keys():
    for k in stat_map.keys():
        try:
            if type(summary[k]) is not list:
                continue
            try:
                for i in range(len(tmp[k])):
                    summary[k].append(tmp[k].pop())
            except KeyError:
                print("KeyError {}".format(k))
        except KeyError:
            try:
                if type(tmp[k]) is list:
                    summary[k] = tmp[k]
            except KeyError:
                continue
            continue


def merge_results(summary,cnt,drop,gap):
#    for k in summary.keys():
    new_summary = {}
    for k in stat_map.keys():
        try:
            if type(summary[k]) is not list:
                continue
            new_summary[k] = []
            for g in range(gap):
                if k == 'all_lat':
                    if len(summary[k]) > 0 and isinstance(summary[k][0],list):
                        l = []
                        for c in range(cnt):
                            print "length of summary ", len(summary[k])
                            try:
                                m=summary[k].pop()
                                print "Length of m ",len(m)
                                l = sorted(l + summary[k].pop())
                                #l = sorted(l + m)
                            except TypeError:
                                print "m=",m
                        new_summary[k]=l
                else:
                    l = []
                    for c in range(cnt):
                        try:
                            l.append(summary[k][(c)*gap+g])
                        except IndexError:
#                            print("IndexError {} {}/{}".format(k,c,cnt))
                            continue
                    if drop:
                        l.remove(max(l))
                        l.remove(min(l))
                    if len(l) == 0:
                        continue
                    new_summary[k].append(avg(l))

        except KeyError:
            continue
    return new_summary
                
def process_results(summary,results):
    for r in results:
        (name,val) = re.split('=',r)
        val = float(val)
        if name not in summary.keys():
            summary[name] = [val]
        else:
            summary[name].append(val)

def process_cnts(summary,line,name):
    
    if name not in summary.keys():
        summary[name] = {}
    name_cnt = name + "_cnt"

    line = re.split(' |] |,',line)
    results = line[2:] 

    if name_cnt not in summary.keys():
        summary[name_cnt] = int(line[1]) 
    else:
        summary[name_cnt] =summary[name_cnt] + int(line[1]) 


    for r in results:
        if r == '': continue
        r = int(r)
        if r not in summary[name].keys():
            summary[name][r] = 1
        else:
            summary[name][r] = summary[name][r] + 1

def process_cflts(summary,line,name):
    
    if name not in summary.keys():
        summary[name] = {}
    name_cnt = name + "_cnt"

    line = re.split(' |] |,',line)
    results = line[2:] 

    if name_cnt not in summary.keys():
        summary[name_cnt] = int(line[1]) 
    else:
        summary[name_cnt] =summary[name_cnt] + int(line[1]) 


    for r in results:
        if r == '': continue
        r = re.split('=',r)
        k = int(r[0])
        c = int(r[1])
        summary[name][k] = c

def process_lats(summary,line,name):
    if name not in summary.keys():
        summary[name] = []
    line = re.split(' |] |,',line)
    results = line[2:-1]
    for r in results:
        try:
            summary[name].append(float(r))
        except:
            pass

def get_lstats(summary):
    try:
        latencies = summary['all_lat']
        summary['all_lat']=ls.LatencyStats(latencies,out_time_unit='ms') 
    except:
        pass

def get_outfile_name(cfgs,fmt,network_hosts=[]):
    output_f = ""
    nettest = False
    if "NETWORK_TEST" in cfgs and cfgs["NETWORK_TEST"] == "true":
        nettest = True
#    assert "NETWORK_TEST" in cfgs
    if cfgs["NETWORK_TEST"] == "true":
        assert len(network_hosts) == 2
        for host in sorted(network_hosts):
            parts = host.split(".")
            if len(parts) == 4:
                h = parts[3]
            else:
                h = host
            output_f += "{}_".format(h)

        output_f += "NETWORK_TEST_"
    else:
        #for key in sorted(cfgs.keys()):
        for key in sorted(set(fmt)):
            nkey = SHORTNAMES[key] if key in SHORTNAMES else key
            if nkey == "":
                output_f += "{}_".format(cfgs[key])
            else:
                if str(cfgs[key]).find("*") >= 0:
                    output_f += "{}-{}_".format(nkey,str(cfgs[key])[:cfgs[key].find("*")])
#                    output_f += "{}-{}_".format(nkey,str(cfgs[key]).replace('*','-t-'))
#                elif str(cfgs[key]).find("/") >= 0:
#                    output_f += "{}-{}_".format(nkey,str(cfgs[key]).replace('/','-d-'))
                else:
                    output_f += "{}-{}_".format(nkey,cfgs[key])
    return output_f

def get_cfgs(fmt,e):
    cfgs = configs
    for f,n in zip(fmt,range(len(fmt))):
        cfgs[f] = e[n]
    # For now, spawn NODE_CNT remote threads to avoid potential deadlock
    #if "REM_THREAD_CNT" not in fmt:
    #    cfgs["REM_THREAD_CNT"] = cfgs["NODE_CNT"] * cfgs["THREAD_CNT"]
    if "PART_CNT" not in fmt:
        cfgs["PART_CNT"] = cfgs["NODE_CNT"]# * cfgs["THREAD_CNT"]
    if "NUM_WH" not in fmt:
        cfgs["NUM_WH"] = cfgs["PART_CNT"]
    return cfgs

def print_keys(result_dir="../results",keys=['txn_cnt']):
    cfgs = sorted(glob.glob(os.path.join(result_dir,"*.cfg")))
    bases=[cfg.split('/')[-1][:-4] for cfg in cfgs]
    missing_files = 0
    missing_results = 0
    
    for base in bases:
        print base
        node_cnt=int(base.split('NODE_CNT-')[-1].split('_')[0])
        keys_to_print= []
        for n in range(node_cnt):
            server_file="{}_{}.out".format(n,base)
            try:
                with open(os.path.join(result_dir,server_file),"r") as f:
                    lines = f.readlines()
            except IOError:
                print "Error opening file: {}".format(server_file)
                missing_files += 1
                continue
            summary_line = [l for l in lines if '[summary]' in l]
            if len(summary_line) == 0:
                prog_line = [p for p in lines if "[prog]" in p]
                if len(prog_line) == 0:
                    res_line = None
                    missing_results += 1
                else:
                    res_line = prog_line[-1][len('[prog]'):].strip()
            elif len(summary_line) == 1:
                res_line=summary_line[0][len('[summary]'):].strip()
            else:
                assert false
            if res_line:
                avail_keys = res_line.split(',')
                keys_to_print=[k for k in avail_keys if k.split('=')[0] in keys]
            print "\tNode {}: ".format(n),", ".join(keys_to_print)
        print '\n'
    print "Total missing files (files not in dir): {}".format(missing_files)
    print "Total missing server results (no [summary] or [prog]): {}".format(missing_results)

