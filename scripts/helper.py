import os,re,sys,math
from experiments import configs
#from experiments import config_names
import glob
import pprint
import latency_stats as ls
import itertools

CONFIG_PARAMS = [
#    "TPORT_TYPE",
#    "TPORT_TYPE_IPC",
#    "TPORT_PORT",
#    "DEBUG_DISTR",
    "CC_ALG",
    "MODE",
    "WORKLOAD",
    "PRIORITY",
    "TWOPL_LITE"
#    "SHMEM_ENV"
    ]

FLAG = {
    "CLIENT_NODE_CNT" : "-cn",
    "CLIENT_THREAD_CNT" : "-ct",
    "CLIENT_REM_THREAD_CNT" : "-ctr",
    "CLIENT_SEND_THREAD_CNT" : "-cts",
    "NODE_CNT" : "-n",
    "PART_CNT" : "-p",
    "THREAD_CNT" : "-t",
    "REM_THREAD_CNT" : "-tr",
    "SEND_THREAD_CNT" : "-ts",
    "MAX_TXN_PER_PART" : "-tpp",
    "MAX_TXN_IN_FLIGHT" : "-tif",
    "PART_PER_TXN" : "-ppt",
    "TUP_WRITE_PERC" : "-w",
    "TXN_WRITE_PERC" : "-tw",
    "ZIPF_THETA" : "-zipf",
    "REQ_PER_QUERY": "-rpq",
    "MPR" : "-mpr",
    "MPIR" : "-mpi",
    "NUM_WH": "-wh",
    "DONE_TIMER": "-done",
    "BATCH_TIMER": "-btmr",
    "PROG_TIMER": "-prog",
    "ABORT_PENALTY": "-abrt",
    "SYNTH_TABLE_SIZE":"-s",
    "LOAD_TXN_FILE":"-i",
    "DATA_PERC":"-dp",
    "ACCESS_PERC":"-ap",
    "PERC_PAYMENT":"-pp",
    "STRICT_PPT":"-sppt",
    "NETWORK_DELAY":"-ndly",
}

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
    "TUP_READ_PERC" : "TRD",
    "TUP_WRITE_PERC" : "TWR",
    "TXN_READ_PERC" : "RD",
    "TXN_WRITE_PERC" : "WR",
    "ZIPF_THETA" : "SKEW",
    "MSG_TIME_LIMIT" : "BT",
    "MSG_SIZE_MAX" : "BS",
    "DATA_PERC":"D",
    "ACCESS_PERC":"A",
    "PRIORITY":"",
    "PERC_PAYMENT":"PP",
    "ABORT_PENALTY":"PENALTY",
    "STRICT_PPT":"SPPT",
    "NETWORK_DELAY":"NDLY",
}

stat_map = {
'latency': [],
 'txn_sent': [],
 'all_lat': [],
 'msg_sent': [],
 'time_msg_sent': [],
 'msg_rcv': [],
 'txn_cnt': [],
 'txn_rem_cnt': [],
 'time_tport_rcv': [],
 'tport_lat': [],
 'time_tport_send': [],
 'clock_time': [],
 'finish_time': [],
 'prof_time_twopc': [],
 'msg_bytes': [],
 'time_getqry': [],
'cc_hold_time': [],
 'access_cnt': [],
 'write_cnt': [],
 'rem_row_cnt': [],
 'abort_from_ts': [],
 'occ_check_cnt': [],
 'occ_abort_check_cnt': [],
 'abort_cnt': [],
 'owned_time': [],
 'owned_time_rd': [],
 'owned_time_wr': [],
 'owned_cnt': [],
 'owned_cnt_rd': [],
 'owned_cnt_wr': [],
 'abort_rem_row_cnt': [],
 'avg_abort_rem_row_cnt': [],
 'abort_row_cnt': [],
 'avg_abort_row_cnt': [],
 'tot_avg_abort_row_cnt': [],
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
 'time_cleanup': [],
 'time_lock_man': [],
 'qq_full': [],
 'cc_hold_abrt_time': [],
 'spec_abort_cnt': [],
 'time_msg_sent': [],
 'qry_rack': [],
 'qq_lat': [],
 'time_man': [],
 'txn_time_begintxn': [],
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
 'lat_5ile': [],
 'cpu_ttl':[],
 'virt_mem_usage':[],
 'phys_mem_usage':[],
 'mbuf_send_time':[],
 'msg_batch_size':[],
 'msg_batch_cnt':[],
 'avg_msg_batch':[],
 'avg_msg_batch_bytes':[],
'prof_cc_rel_abort':[],
'prof_cc_rel_commit':[],

 'seq_batch_cnt':[],
 'seq_txn_cnt':[],
 'time_seq_batch':[],
 'time_seq_batch':[],
 'time_seq_prep':[],
 'time_seq_ack':[],

'sthd_prof_1a':[],
'sthd_prof_2':[],
'sthd_prof_3':[],
'sthd_prof_4':[],
'sthd_prof_5a':[],
'sthd_prof_1b':[],
'sthd_prof_5b':[],
'rthd_prof_1':[],
'rthd_prof_2':[],
'mvcc1':[],
'mvcc2':[],
'mvcc3':[],
'mvcc4':[],
'mvcc5':[],
'mvcc6':[],
'mvcc7':[],
'mvcc8':[],
'mvcc9':[],
'occ_val1':[],
'occ_val2a':[],
'occ_val2':[],
'occ_val3':[],
'occ_val4':[],
'occ_val5':[],
'thd1':[],
'thd2':[],
'thd3':[],
'thd_sum':[],
'thd1a':[],
'thd1b':[],
'thd1c':[],
'thd1d':[],
'thd2_loc':[],
'thd2_rem':[],
'rfin0':[],
'rfin1':[],
'rfin2':[],
'rprep0':[],
'rprep1':[],
'rprep2':[],
'rqry_rsp0':[],
'rqry_rsp1':[],
'rqry0':[],
'rqry1':[],
'rqry2':[],
'rack0':[],
'rack1':[],
'rack2a':[],
'rack2':[],
'rack3':[],
'rack4':[],
'rtxn1a':[],
'rtxn1b':[],
'rtxn2':[],
'rtxn3':[],
'rtxn4':[],
'ycsb1':[],
'row1':[],
'row2':[],
'row3':[],
'cc0':[],
'cc1':[],
'cc2':[],
'cc3':[],
'wq1':[],
'wq2':[],
'wq3':[],
'wq4':[],
'txn1':[],
'txn2':[],
'txn_table_add':[],
'txn_table_get':[],
'txn_table_mints1':[],
'txn_table_mints2':[],
'txn_table0a':[],
'txn_table1a':[],
'txn_table0b':[],
'txn_table1b':[],
'txn_table2a':[],
'txn_table2':[],
'type0':[],
'type1':[],
'type2':[],
'type3':[],
'type4':[],
'type5':[],
'type6':[],
'type7':[],
'type8':[],
'type9':[],
'type10':[],
'type11':[],
'type12':[],
'type13':[],
'type14':[],
'type15':[],
'part_cnt1':[],
'part_cnt2':[],
'part_cnt3':[],
'part_cnt4':[],
'part_cnt5':[],
'part_cnt6':[],
'part_cnt7':[],
'part_cnt8':[],
'part_cnt9':[],
'part_cnt10':[],
'part_cnt11':[],
'part_cnt12':[],
'part_cnt13':[],
'part_cnt14':[],
'part_cnt15':[],
'part_cnt16':[],

'txn_table_cnt':[],
'txn_table_cflt':[],
'txn_table_cflt_size':[],
'wq_cnt':[],
'new_wq_cnt':[],
'aq_cnt':[],
'wq_enqueue':[],
'new_wq_enqueue':[],
'aq_enqueue':[],
'wq_dequeue':[],
'new_wq_dequeue':[],
'aq_dequeue':[],
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

#def get_timeline(sfile,summary={},low_lim=0,up_lim=sys.maxint,min_time=0):
#    keys = ["START","ABORT","COMMIT","LOCK","UNLOCK"]
#    for k in keys:
#        if k not in summary.keys():
#            summary[k] = {"time":[],"tid":[]}
#    with open(sfile,'r') as f:
#        for line in f:
#            for k in keys:
#                summary,min_time = find_in_line(k,line,summary,min_time,low_lim,up_lim)
#    return summary,min_time

def plot_prep(nexp,nfmt,x_name,v_name,extras={},constants={}):
    x_vals = []
    v_vals = []
    exp = [list(e) for e in nexp]
    fmt = list(nfmt)
    for x in constants.keys():
        for e in exp[:]:
            if e[fmt.index(x)] != constants[x]:
                exp.remove(e)
    for x in extras.keys():
        if x not in fmt: 
            del extras[x]
            continue
        for e in exp:
            del e[fmt.index(x)]
        fmt.remove(x)
    lst = {}
    tmp_fmt = list(fmt)
    tmp_fmt.remove(x_name)
    for e in exp:
        x_vals.append(e[fmt.index(x_name)])
        x = e[fmt.index(x_name)]
        del e[fmt.index(x_name)]
        if v_name != '':
            v_vals.append(e[tmp_fmt.index(v_name)])
            v = e[tmp_fmt.index(v_name)]
            del e[tmp_fmt.index(v_name)]
        else:
            v = 0
        lst[(x,v)] = e
    fmt.remove(x_name)
    if v_name != '':
        fmt.remove(v_name)
#    for e in exp:
#        x_vals.append(e[fmt.index(x_name)])
#        del e[fmt.index(x_name)]
#    fmt.remove(x_name)
#    if v_name != '':
#        for e in exp:
#            v_vals.append(e[fmt.index(v_name)])
#            del e[fmt.index(v_name)]
#        fmt.remove(v_name)
    x_vals = list(set(x_vals))
    x_vals.sort()
    v_vals = list(set(v_vals))
    v_vals.sort()
    exp.sort()
    exp = list(k for k,_ in itertools.groupby(exp))
#    assert(len(exp)==1)
    return x_vals,v_vals,fmt,exp[0],lst

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
#    pp = pprint.PrettyPrinter()
#    pp.pprint(summary['txn_cnt'])
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
                summary[k] = summary[k] + tmp[k]
#                for i in range(len(tmp[k])):
#                    summary[k].append(tmp[k].pop())
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
#                            print "length of summary ", len(summary[k])
                            try:
                                m=summary[k].pop()
#                                print "Length of m ",len(m)
                                l = sorted(l + summary[k].pop())
                                #l = sorted(l + m)
                            except TypeError:
                                print("m={}".format(m))
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

#                    pp = pprint.PrettyPrinter()
#                    if k is 'txn_cnt':
#                        pp.pprint(l)
#                        pp.pprint(new_summary[k])

        except KeyError:
            continue
    return new_summary
                
def process_results(summary,results):
    for r in results:
        try:
            (name,val) = re.split('=',r)
        except ValueError:
            continue
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

def get_args(fmt,exp):
    cfgs = get_cfgs(fmt,exp)
    args = ""
    for key in fmt:
        if key not in FLAG or key in CONFIG_PARAMS: continue
        flag = FLAG[key]
        args += "{}{} ".format(flag,cfgs[key])
    for c in configs.keys():
        if c in fmt: continue
        key,indirect = get_config_root(c)
        if not indirect or c not in FLAG or c in CONFIG_PARAMS: continue
        flag = FLAG[c]
        args += "{}{} ".format(flag,cfgs[key])
            
    return args

def get_config_root(c):
    indirect = False
    while c in configs and configs[c] in configs:
        c = configs[c]
        indirect = True
    return c,indirect
 
def get_execfile_name(cfgs,fmt,network_hosts=[]):
    output_f = ""
#for key in sorted(cfgs.keys()):
    for key in CONFIG_PARAMS:
        output_f += "{}_".format(cfgs[key])
    return output_f


def get_outfile_name(cfgs,fmt,network_hosts=[]):
    output_f = ""
    nettest = False
    if "NETWORK_TEST" in cfgs and cfgs["NETWORK_TEST"] == "true":
        nettest = True
#    assert "NETWORK_TEST" in cfgs
    print(network_hosts)
    if cfgs["NETWORK_TEST"] == "true":
#assert len(network_hosts) == 2
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
    cfgs = dict(configs)
    for f,n in zip(fmt,range(len(fmt))):
        cfgs[f] = e[n]
    # For now, spawn NODE_CNT remote threads to avoid potential deadlock
    #if "REM_THREAD_CNT" not in fmt:
    #    cfgs["REM_THREAD_CNT"] = cfgs["NODE_CNT"] * cfgs["THREAD_CNT"]
#    if "PART_CNT" not in fmt:
#        cfgs["PART_CNT"] = cfgs["NODE_CNT"]# * cfgs["THREAD_CNT"]
#    if "NUM_WH" not in fmt:
#        cfgs["NUM_WH"] = cfgs["PART_CNT"]
    return cfgs

def print_keys(result_dir="../results",keys=['txn_cnt']):
    cfgs = sorted(glob.glob(os.path.join(result_dir,"*.cfg")))
    bases=[cfg.split('/')[-1][:-4] for cfg in cfgs]
    missing_files = 0
    missing_results = 0
    
    for base in bases:
#        print base
        node_cnt=int(base.split('NODE_CNT-')[-1].split('_')[0])
        keys_to_print= []
        for n in range(node_cnt):
            server_file="{}_{}.out".format(n,base)
            try:
                with open(os.path.join(result_dir,server_file),"r") as f:
                    lines = f.readlines()
            except IOError:
#                print "Error opening file: {}".format(server_file)
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
            print("\tNode {}: ".format(n),", ".join(keys_to_print))
        print('\n')
    print("Total missing files (files not in dir): {}".format(missing_files))
    print("Total missing server results (no [summary] or [prog]): {}".format(missing_results))

def get_summary_stats(stats,summary,summary_cl,summary_sq,x,v,cc):
    print(summary_cl['txn_cnt'])
    nthd = 6
    s = summary_cl
    tot_txn_cnt = sum(s['txn_cnt'])
    avg_txn_cnt = avg(s['txn_cnt'])
    tot_time = sum(s['clock_time'])
    avg_time = avg(s['clock_time'])
    sk = {}
    sk['sys_txn_cnt'] = tot_txn_cnt
    sk['avg_txn_rem_cnt'] = avg(summary['txn_rem_cnt'])
    sk['avg_txn_cnt'] = avg_txn_cnt
    sk['time'] = avg_time
    sk['sys_tput'] = tot_txn_cnt / avg_time
    sk['per_node_tput'] = avg_txn_cnt / avg_time
    sk['thd1'] = avg(summary['thd1'])
    sk['row1'] = avg(summary['row1'])
    sk['row2'] = avg(summary['row2'])
    sk['row3'] = avg(summary['row3'])
    sk['cflt_cnt'] = avg(summary['cflt_cnt'])
    try:
        sk['txn_time_begintxn'] = avg(summary['txn_time_begintxn']) / nthd
        sk['time_validate'] = avg(summary['time_validate']) / nthd
        sk['prof_time_twopc'] = avg(summary['prof_time_twopc']) / nthd
        sk['time_msg_sent'] = avg(summary['time_msg_sent']) / nthd
        sk['time_index'] = avg(summary['time_index']) / nthd
        sk['prof_cc_rel_abort'] = avg(summary['prof_cc_rel_abort']) / nthd
        sk['prof_cc_rel_commit'] = avg(summary['prof_cc_rel_commit']) / nthd
# time_abort contains some row3
        sk['time_abort'] = avg(summary['time_abort']) / nthd
#    sk['time_abort'] = avg(summary['time_abort']) / nthd
#    sk['time_ccman'] = avg(summary['row1']) + avg(summary['row2']) + avg(summary['txn_time_begintxn']) + avg(summary['time_validate'])
        sk['time_ccman'] = sk['row1'] + sk['row2'] + sk['prof_cc_rel_commit']+ sk['txn_time_begintxn'] + sk['time_validate']
#    sk['time_ccman'] = avg(summary['row1']) + avg(summary['row2']) +avg(summary['row3']) + avg(summary['txn_time_begintxn']) + avg(summary['time_validate'])
#    sk['time_ccman'] = avg(summary['time_man']) + avg(summary['txn_time_begintxn']) + avg(summary['time_validate'])
        sk['time_twopc'] = sk['prof_time_twopc'] + sk['time_msg_sent'] 
#    sk['time_twopc'] = avg(summary['prof_time_twopc']) + avg(summary['time_msg_sent']) 
#    sk['time_twopc'] = avg(summary['type9']) + avg(summary['type12']) +avg(summary['type5']) + avg(summary['time_msg_sent']) - avg(summary['time_validate']) - avg(summary['row3'])
        sk['time_work'] = sk['time'] - sk['thd1'] - (sk['time_index'] + sk['time_abort'] + sk['time_ccman'] + sk['time_twopc'])
    except KeyError:
        sk['time_index'] = 0
        sk['time_abort'] = 0
        sk['time_ccman'] = 0
        sk['time_twopc'] = 0
        sk['time_work'] = 0
#    sk['time_work'] = avg(summary['clock_time']) - avg(summary['thd1']) - (sk['time_index'] + sk['time_abort'] + sk['time_ccman'] + sk['time_twopc'])
#    sk['time_work'] = avg(summary['type10']) + avg(summary['type8'])
    total = sk['time_index'] + sk['time_abort'] + sk['time_ccman'] + sk['time_twopc'] + sk['time_work'] 
    if total == 0:
        total = 1
    sk['perc_index'] = sk['time_index'] / total * 100
    sk['perc_abort'] = sk['time_abort'] / total * 100
    sk['perc_ccman'] = sk['time_ccman'] / total * 100
    sk['perc_twopc'] = sk['time_twopc'] / total * 100
    sk['perc_work'] = sk['time_work'] / total * 100
    total = sum(summary['thd2'])
    if total == 0:
        total = 1
    sk['rqry'] =  sum(summary['type4']) / total
    sk['rfin'] =  sum(summary['type5']) / total
    sk['rqry_rsp'] =  sum(summary['type8']) / total
    sk['rack'] =  sum(summary['type9']) / total
    sk['rtxn'] =  sum(summary['type10']) / total
    sk['rinit'] =  sum(summary['type11']) / total
    sk['rprep'] =  sum(summary['type12']) / total
    sk['tot_abort_cnt'] = sum(summary['abort_cnt'])
    sk['abort_cnt'] = avg(summary['abort_cnt'])
    sk['txn_abort_cnt'] = avg(summary['txn_abort_cnt'])
    try:
        sk['avg_abort_cnt'] = avg(summary['abort_cnt']) / avg(summary['txn_abort_cnt'])
    except ZeroDivisionError:
        sk['avg_abort_cnt'] = 0
    try:
        sk['abort_rate'] = avg(summary['abort_cnt']) / avg_txn_cnt
    except ZeroDivisionError:
        sk['abort_rate'] = 0
    sk['abort_row_cnt'] = avg(summary['tot_avg_abort_row_cnt'])
    try:
        sk['write_perc'] = sum(summary['write_cnt']) / sum(summary['access_cnt'])
    except ZeroDivisionError:
        sk['write_perc'] = 0
    total = avg(summary['thd1']) + avg(summary['thd2']) + avg(summary['thd3'])
    sk['stage1'] = avg(summary['thd1']) / total * 100
    sk['stage2'] = avg(summary['thd2']) / total * 100
    sk['stage3'] = avg(summary['thd3']) / total * 100
    sk['latency'] = avg(summary['latency'])
    sk['memory'] = avg(summary['phys_mem_usage']) / 1000000
    sk['wq_cnt'] = avg(summary['wq_cnt'])
    sk['new_wq_cnt'] = avg(summary['new_wq_cnt'])
    sk['aq_cnt'] = avg(summary['aq_cnt'])
    sk['wq_enqueue'] = avg(summary['wq_enqueue'])
    sk['wq_dequeue'] = avg(summary['wq_dequeue'])
    sk['new_wq_enqueue'] = avg(summary['new_wq_enqueue'])
    sk['new_wq_dequeue'] = avg(summary['new_wq_dequeue'])
    sk['aq_enqueue'] = avg(summary['aq_enqueue'])
    sk['aq_dequeue'] = avg(summary['aq_dequeue'])
    sk['txn_table_cnt'] = avg(summary['txn_table_cnt'])
    sk['txn_table_add'] = avg(summary['txn_table_add'])
    sk['txn_table_get'] = avg(summary['txn_table_get'])
    sk['mpq_cnt'] = avg(summary['mpq_cnt'])
    try:
        sk['owned_time'] = avg(summary['owned_time'])
        sk['owned_time_rd'] = avg(summary['owned_time_rd'])
        sk['owned_time_wr'] = avg(summary['owned_time_wr'])
        sk['owned_cnt'] = avg(summary['owned_cnt'])
        sk['owned_cnt_rd'] = avg(summary['owned_cnt_rd'])
        sk['owned_cnt_wr'] = avg(summary['owned_cnt_wr'])
    except KeyError:
        sk['owned_time'] = 0
        sk['owned_time_rd'] = 0
        sk['owned_time_wr'] = 0
        sk['owned_cnt'] = 0
        sk['owned_cnt_rd'] = 0
        sk['owned_cnt_wr'] = 0
    for i in range(10):
        try:
            sk['part'+str(i)] = avg(summary['part_cnt'+str(i)])
        except KeyError:
            sk['part'+str(i)] = 0 
    try:
        sk['batch_cnt'] = avg(summary['seq_batch_cnt'])
        if sk['batch_cnt'] == 0:
            sk['txn_per_batch'] = 0
            sk['batch_intv'] = 0
        else:
            sk['txn_per_batch'] = avg(summary['seq_txn_cnt'])/sk['batch_cnt']
            sk['batch_intv'] = avg(summary['time_seq_batch'])/sk['batch_cnt']
        sk['seq_prep'] = avg(summary['time_seq_prep'])
        sk['seq_ack'] = avg(summary['time_seq_ack'])
    except KeyError:
        sk['batch_cnt'] = 0
        sk['batch_intv'] = 0
        sk['txn_per_batch'] = 0
        sk['seq_prep'] = 0
        sk['seq_ack'] = 0
    print(sk['batch_intv'])
    sk['mvcc1'] = avg(summary['mvcc1'])
    sk['mvcc2'] = avg(summary['mvcc2'])
    sk['mvcc3'] = avg(summary['mvcc3'])
    sk['mvcc4'] = avg(summary['mvcc4'])
    sk['mvcc5'] = avg(summary['mvcc5'])
    sk['mvcc6'] = avg(summary['mvcc6'])
    sk['mvcc7'] = avg(summary['mvcc7'])
    sk['mvcc8'] = avg(summary['mvcc8'])


    if v == '':
        key = (x)
    else:
        key = (x,v)
    stats[key] = sk
    return stats
   
def write_summary_file(fname,stats,x_vals,v_vals):

    ps =  [
    'sys_txn_cnt',
    'avg_txn_cnt',
    'avg_txn_rem_cnt',
    'mpq_cnt',
    'time',
    'sys_tput',
    'per_node_tput',
    'thd1',
    'time_index',
    'time_abort',
    'time_ccman',
    'time_twopc',
    'time_work',
    'perc_index',
    'perc_abort',
    'perc_ccman',
    'perc_twopc',
    'perc_work',
    'rqry',
    'rfin',
    'rqry_rsp',
    'rack',
    'rtxn',
    'rinit',
    'rprep',
    'cflt_cnt',
    'tot_abort_cnt',
    'abort_cnt',
    'txn_abort_cnt',
    'avg_abort_cnt',
    'abort_rate',
    'abort_row_cnt',
    'write_perc',
    'stage1',
    'stage2',
    'stage3',
    'txn_table_add',
    'txn_table_get',
    'latency',
    'memory',
    'txn_table_cnt',
    'wq_cnt',
    'new_wq_cnt',
    'aq_cnt',
    'wq_enqueue',
    'wq_dequeue',
    'new_wq_enqueue',
    'new_wq_dequeue',
    'aq_enqueue',
    'aq_dequeue',
    'owned_time',
    'owned_time_rd',
    'owned_time_wr',
    'owned_cnt',
    'owned_cnt_rd',
    'owned_cnt_wr',
    'batch_cnt',
    'batch_intv',
    'txn_per_batch',
    'seq_prep',
    'seq_ack',
    'part1',
    'part2',
    'part3',
    'part4',
    'part5',
    'part6',
    'part7',
    'part8',
    'part9',
    'mvcc1',
    'mvcc2',
    'mvcc3',
    'mvcc4',
    'mvcc5',
    'mvcc6',
    'mvcc7',
    'mvcc8',
    ]
#    ps = [
#    'time',
#    'thd1',
#    'row1',
#    'row2',
#    'row3',
#    'txn_time_begintxn',
#    'time_validate',
#    'prof_time_twopc',
#    'time_msg_sent',
#    'prof_cc_rel_abort',
#    'prof_cc_rel_commit',
#    'time_index',
#    'time_abort',
#    'time_ccman',
#    'time_twopc',
#    'time_work',
#    ]
    with open('../figs/' + fname+'.csv','w') as f:
        if v_vals == []:
            f.write(', ' + ', '.join(x_vals) +'\n')
            for p in ps:
                s = p + ', '
                for x in x_vals:
                    k = (x)
                    s += str(stats[k][p]) + ', '
                f.write(s+'\n')
        else:
            for x in x_vals:
                f.write(str(x) + ', ' + ', '.join([str(v) for v in v_vals]) +'\n')
                for p in ps:
                    s = p + ', '
                    for v in v_vals:
                        k = (x,v)
                        try:
                            s += '{0:0.2f}'.format(stats[k][p]) + ', '
                        except KeyError:
                            print("helper keyerror {} {}".format(p,k))
                            s += '--, '
                    f.write(s+'\n')
                f.write('\n')
            for v in v_vals:
                f.write(str(v) + ', ' + ', '.join([str(x) for x in x_vals]) +'\n')
                for p in ps:
                    s = p + ', '
                    for x in x_vals:
                        k = (x,v)
                        try:
                            s += '{0:0.2f}'.format(stats[k][p]) + ', '
                        except KeyError:
                            s += '--, '
                    f.write(s+'\n')
                f.write('\n')
 
