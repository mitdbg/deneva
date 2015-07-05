import os,re,sys,math
from experiments import configs
from experiments import config_names
import glob
import latency_stats as ls

cnts = ["all_abort"]
cflts = ["w_cflt","d_cflt","cnp_cflt","c_cflt","ol_cflt","s_cflt","w_abrt","d_abrt","cnp_abrt","c_abrt","ol_abrt","s_abrt"]
lats = ["all_lat"]

def avg(l):
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
                line = line[9:] #remove '[prog 0] ' from start of line 
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

def merge_results(summary,cnt,drop):
    for k in summary.keys():
        if type(summary[k]) is not list:
            continue
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
                summary[k]=l
        else:
            l = []
            for c in range(cnt):
                try:
                    l.append(summary[k].pop())
                except IndexError:
                    print("IndexError {} {}/{}".format(k,c,cnt))
                    continue
            if drop:
                l.remove(max(l))
                l.remove(min(l))
            summary[k].append(avg(l))

        if k == "txn_cnt" or k == "clock_time":
            print("{}: {} {}".format(k,avg(l),stdev(l)))
            
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
            print "Passing...",r
            pass
    print "process_lats len: ",len(summary[name])
    #results = sorted([float(r) for r in results])
    #summary[name] = sorted(results+summary[name])

def get_lstats(summary):
    try:
        latencies = summary['all_lat']
        summary['all_lat']=ls.LatencyStats(latencies,out_time_unit='s') 
    except:
        pass

def get_outfile_name(cfgs,network_hosts=[]):
    output_f = ""
    assert "NETWORK_TEST" in cfgs
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
        for key in sorted(config_names):
            output_f += "{}-{}_".format(key,cfgs[key])
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

