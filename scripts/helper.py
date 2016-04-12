import os,re,sys,math
from experiments import configs
from collections import OrderedDict
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
    "TWOPL_LITE",
    "LOGGING"
#    "SHMEM_ENV"
    ]

FLAG = {
    "CLIENT_NODE_CNT" : "-cn",
    "CLIENT_THREAD_CNT" : "-ct",
    "CLIENT_REM_THREAD_CNT" : "-ctr",
    "CLIENT_SEND_THREAD_CNT" : "-cts",
    "REPLICA_CNT":"-rn",
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
    "REPLICA_CNT":"RN",
}

stat_map = OrderedDict([
   ('total_runtime', []),

  # Execution
  ('txn_cnt', []),
  ('remote_txn_cnt', []),
  ('local_txn_cnt', []),
  ('txn_commit_cnt', []),
  ('txn_abort_cnt', []),
  ('txn_run_time', []),
  ('multi_part_txn_cnt', []),
  ('multi_part_txn_run_time', []),
  ('single_part_txn_cnt', []),
  ('single_part_txn_run_time', []),

  # Client
  ('txn_sent_cnt', []),
  ('cl_send_intv', []),

  # Breakdown
  ('ts_alloc_time', []),
  ('abort_time', []),
  ('txn_manager_time', []),
  ('txn_index_time', []),
  ('txn_validate_time', []),
  ('txn_cleanup_time', []),

  # Work queue
  ('work_queue_wait_time', []),
  ('work_queue_cnt', []),
  ('work_queue_new_cnt', []),
  ('work_queue_new_wait_time', []),
  ('work_queue_old_cnt', []),
  ('work_queue_old_wait_time', []),
  ('work_queue_enqueue_time', []),
  ('work_queue_dequeue_time', []),
  ('work_queue_conflict_cnt', []),

  # Abort queue
  ('abort_queue_enqueue_cnt', []),
  ('abort_queue_dequeue_cnt', []),
  ('abort_queue_enqueue_time', []),
  ('abort_queue_dequeue_time', []),
  ('abort_queue_penalty', []),
  ('abort_queue_penalty_extra', []),

  # Worker thread
  ('worker_process_time', []),
  ('worker_process_cnt', []),
  ('worker_process_cnt_by_type', []),
  ('worker_process_time_by_type', []),

  # IO
  ('msg_queue_delay_time', []),
  ('msg_queue_cnt', []),
  ('msg_send_time', []),
  ('msg_recv_time', []),
  ('msg_batch_cnt', []),
  ('msg_batch_size_msgs', []),
  ('msg_batch_size_bytes', []),
  ('msg_send_cnt', []),
  ('msg_recv_cnt', []),
  ('msg_unpack_time', []),
  ('mbuf_send_intv_time', []),

  # Concurrency control), general
  ('cc_conflict_cnt', []),
  ('txn_wait_cnt', []),
  ('txn_conflict_cnt', []),

  # 2PL
  ('twopl_already_owned_cnt', []),
  ('twopl_owned_cnt', []),
  ('twopl_sh_owned_cnt', []),
  ('twopl_ex_owned_cnt', []),
  ('twopl_owned_time', []),
  ('twopl_sh_owned_time', []),
  ('twopl_ex_owned_time', []),
  ('twopl_diff_time', []),

  # OCC
  ('occ_validate_time', []),
  ('occ_cs_wait_time', []),
  ('occ_cs_time', []),
  ('occ_hist_validate_time', []),
  ('occ_act_validate_time', []),
  ('occ_hist_validate_fail_time', []),
  ('occ_act_validate_fail_time', []),
  ('occ_check_cnt', []),
  ('occ_abort_check_cnt', []),
  ('occ_ts_abort_cnt', []),
  ('occ_finish_time', []),

  # Logging
  ('log_write_cnt', []),
  ('log_write_time', []),
  ('log_flush_cnt', []),
  ('log_flush_time', []),
  ('log_process_time', []),

  # Transaction Table
  ('txn_table_new_cnt', []),
  ('txn_table_get_cnt', []),
  ('txn_table_release_cnt', []),
  ('txn_table_cflt_cnt', []),
  ('txn_table_cflt_size', []),
  ('txn_table_get_time', []),
  ('txn_table_release_time', []),
])

stat_map2 = {
  'total_runtime': [],

  # Execution
  'txn_cnt': [],
  'remote_txn_cnt': [],
  'local_txn_cnt': [],
  'txn_commit_cnt': [],
  'txn_abort_cnt': [],
  'txn_run_time': [],
  'multi_part_txn_cnt': [],
  'multi_part_txn_run_time': [],
  'single_part_txn_cnt': [],
  'single_part_txn_run_time': [],

  # Client
  'txn_sent_cnt': [],
  'cl_send_intv': [],

  # Breakdown
  'ts_alloc_time': [],
  'abort_time': [],
  'txn_manager_time': [],
  'txn_index_time': [],
  'txn_validate_time': [],
  'txn_cleanup_time': [],

  # Work queue
  'work_queue_wait_time': [],
  'work_queue_cnt': [],
  'work_queue_new_cnt': [],
  'work_queue_new_wait_time': [],
  'work_queue_old_cnt': [],
  'work_queue_old_wait_time': [],
  'work_queue_enqueue_time': [],
  'work_queue_dequeue_time': [],
  'work_queue_conflict_cnt': [],

  # Abort queue
  'abort_queue_enqueue_time': [],
  'abort_queue_dequeue_time': [],

  # Worker thread
  'worker_process_time': [],
  'worker_process_cnt': [],
  'worker_process_cnt_by_type': [],
  'worker_process_time_by_type': [],

  # IO
  'msg_queue_delay_time': [],
  'msg_queue_cnt': [],
  'msg_send_time': [],
  'msg_recv_time': [],
  'msg_batch_cnt': [],
  'msg_batch_size_msgs': [],
  'msg_batch_size_bytes': [],
  'msg_send_cnt': [],
  'msg_recv_cnt': [],
  'msg_unpack_time': [],
  'mbuf_send_intv_time': [],

  # Concurrency control, general
  'cc_conflict_cnt': [],
  'txn_wait_cnt': [],
  'txn_conflict_cnt': [],

  # 2PL
  'twopl_already_owned_cnt': [],
  'twopl_owned_cnt': [],
  'twopl_sh_owned_cnt': [],
  'twopl_ex_owned_cnt': [],
  'twopl_owned_time': [],
  'twopl_sh_owned_time': [],
  'twopl_ex_owned_time': [],
  'twopl_diff_time': [],

  # OCC
  'occ_validate_time': [],
  'occ_cs_wait_time': [],
  'occ_cs_time': [],
  'occ_hist_validate_time': [],
  'occ_act_validate_time': [],
  'occ_hist_validate_fail_time': [],
  'occ_act_validate_fail_time': [],
  'occ_check_cnt': [],
  'occ_abort_check_cnt': [],
  'occ_ts_abort_cnt': [],
  'occ_finish_time': [],

  # Logging
  'log_write_cnt': [],
  'log_write_time': [],
  'log_flush_cnt': [],
  'log_flush_time': [],
  'log_process_time': [],

  # Transaction Table
  'txn_table_new_cnt': [],
  'txn_table_get_cnt': [],
  'txn_table_release_cnt': [],
  'txn_table_cflt_cnt': [],
  'txn_table_cflt_size': [],
  'txn_table_get_time': [],
  'txn_table_release_time': [],


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
    prog = []
    with open(sfile,'r') as f:
        found = False
        last_line = ""
        for line in f:
            if re.search("prog",line):
                line = line.rstrip('\n')
                line = line[7:] #remove '[prog] ' from start of line 
                results = re.split(',',line)
                prog_tmp = {}
                process_results(prog_tmp,results)
                prog.append(prog_tmp)
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
                results = re.split(',',line)
                process_results(summary,results)
#    pp = pprint.PrettyPrinter()
#    pp.pprint(summary['txn_cnt'])
    summary["progress"] = prog
    print("Added progress: " + str(len(prog)))
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
    merge_helper(summary,tmp)
    if "progress" not in summary:
        summary["progress"] = []
        for p in range(len(tmp["progress"])):
            summary["progress"].append({})
    for p in range(len(tmp["progress"])):
        merge_helper(summary["progress"][p],tmp["progress"][p])

def merge_helper(summary,tmp):
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
    new_summary = merge_results_helper(summary,cnt,drop,gap)
    new_summary["progress"] = []
    for p in range(len(summary["progress"])):
        new_summary["progress"].append( merge_results_helper(summary["progress"][p],cnt,drop,gap))
    return new_summary

def merge_results_helper(summary,cnt,drop,gap):
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

def get_summary_stats(stats,summary,summary_cl,x,v,cc):
    sk = OrderedDict() 
    for k in stat_map.keys():
        try:
            sk[k] = avg(summary[k])
        except KeyError:
            sk[k] = 0
    if "progress" in summary:
        for p in range(len(summary["progress"])):
            for k in stat_map.keys():
                try:
                    sk[(p,k)] = avg(summary["progress"][p][k])
                except KeyError:
                    sk[(p,k)] = 0
    else:
         print("No progress")
    if v == '':
        key = (x)
    else:
        key = (x,v)
    stats[key] = sk
    return stats
   
def write_summary_file(fname,stats,x_vals,v_vals):

    with open('../figs/' + fname+'.csv','w') as f:
        if v_vals == []:
            f.write(', ' + ', '.join(x_vals) +'\n')
            for p in stat_map.keys():
                s = p + ', '
                for x in x_vals:
                    k = (x)
                    s += str(stats[k][p]) + ', '
                f.write(s+'\n')
        else:
            for x in x_vals:
                f.write(str(x) + ', ' + ', '.join([str(v) for v in v_vals]) +'\n')
                for p in stat_map.keys():
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
                for p in stat_map.keys():
                    s = p + ', '
                    for x in x_vals:
                        k = (x,v)
                        try:
                            s += '{0:0.2f}'.format(stats[k][p]) + ', '
                        except KeyError:
                            s += '--, '
                    f.write(s+'\n')
                f.write('\n')
        for x,v in itertools.product(x_vals,v_vals):
            f.write(str(x) + "," + str(v) + "\n")
            for p in stat_map.keys():
                s = p + ', '
                for prog in range(7):
                    k1 = (x,v)
                    k2 = (prog,p)
                    try:
                        s += '{0:0.2f}'.format(stats[k1][k2]) + ', '
                    except KeyError:
                        s += '--, '
                f.write(s+'\n')
            f.write('\n')
 
