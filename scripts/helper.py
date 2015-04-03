import os,re,sys,math
from experiments import configs
from experiments import config_names

def avg(l):
    return float(sum(l) / float(len(l)))

def get_summary(sfile,summary={}):
    with open(sfile,'r') as f:
        for line in f:
            if re.search("summary",line):
                line = line.rstrip('\n')
                line = line[10:] #remove '[summary] ' from start of line 
                results = re.split(',',line)
                process_results(summary,results)
            if re.search("all_abort_cnt",line):
                line = line.rstrip('\n')
                line = line[22:] #remove '[all_abort_cnt thd=0] ' from start of line 
                results = re.split(',',line)
                process_abort_cnts(summary,results)
    return summary

def process_results(summary,results):
	for r in results:
		(name,val) = re.split('=',r)
		val = float(val)
		if name not in summary.keys():
		    summary[name] = [val]
		else:
		    summary[name].append(val)

def process_abort_cnts(summary,results):
    
    name = 'all_abort_cnts'
    if name not in summary.keys():
        summary[name] = {}
    for r in results:
        if r == '': continue
        r = int(r)
        if r not in summary[name].keys():
            summary[name][r] = 1
        else:
            summary[name][r] = summary[name][r] + 1

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
    if "REM_THREAD_CNT" not in fmt:
        cfgs["REM_THREAD_CNT"] = cfgs["NODE_CNT"] * cfgs["THREAD_CNT"]
    if "PART_CNT" not in fmt:
        cfgs["PART_CNT"] = cfgs["NODE_CNT"] * cfgs["THREAD_CNT"]
    if "NUM_WH" not in fmt:
        cfgs["NUM_WH"] = cfgs["PART_CNT"]
    return cfgs

def avg(l):
    return float(sum(l) / float(len(l)))
