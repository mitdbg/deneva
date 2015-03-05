import os,re,sys,math
from experiments import configs

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
    for key in sorted(cfgs.keys()):
        output_f += "{}-{}_".format(key,cfgs[key])
    return output_f

def get_cfgs(l):
    cfgs = configs
    cfgs["NODE_CNT"],cfgs["MAX_TXN_PER_PART"],cfgs["WORKLOAD"],cfgs["CC_ALG"],cfgs["MPR"] = l
    return cfgs

def avg(l):
    return float(sum(l) / float(len(l)))
