import os,re,sys,math
from experiments import configs
from experiments import config_names

cnts = ["all_abort"]
cflts = ["w_cflt","d_cflt","cnp_cflt","c_cflt","ol_cflt","s_cflt","w_abrt","d_abrt","cnp_abrt","c_abrt","ol_abrt","s_abrt"]

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
            if found:
                for c in cnts:
                    if re.search(c,line):
                        line = line.rstrip('\n')
                        process_cflts(summary,line,c)
                for c in cflts:
                    if re.search(c,line):
                        line = line.rstrip('\n')
                        process_cflts(summary,line,c)
        if not found:
            if re.search("prog",last_line):
                line = last_line.rstrip('\n')
                line = line[9:] #remove '[prog 0] ' from start of line 
                results = re.split(',',line)
                process_results(summary,results)

    return summary

def merge_results(summary,cnt,drop):
    
    for k in summary.keys():
        if type(summary[k]) is not list:
            continue
        l = []
        for c in range(cnt):
            l.append(summary[k].pop())
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


def get_outfile_name(cfgs):
    output_f = ""
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

