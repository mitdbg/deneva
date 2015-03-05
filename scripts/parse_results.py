#!/usr/bin/python

import os,re,sys,datetime,itertools,math
import glob
import numpy as np
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as pyplot


PATH=os.getcwd()
result_dir = PATH + "/results/"

summary = {}

def avg(l):
    return float(sum(l) / float(len(l)))

def get_summary(sfile):
    results = []
    with open(sfile,'r') as f:
        for line in f:
            found = re.search("summary",line)
            line = line.rstrip('\n')
            if found:
                line = line[10:] #remove '[summary] ' from start of line 
                results = re.split(',',line)
                process_results(results)
    return results

def process_results(results):
	for r in results:
		(name,val) = re.split('=',r)
		val = float(val)
		if name not in summary.keys():
		    summary[name] = [val]
		else:
		    summary[name].append(val)


if len(sys.argv) < 2:
	sys.exit("Usage: %s [output-file-name]" % sys.argv[0])

for arg in sys.argv[1:]:
    if not os.path.exists(arg):
        sys.exit("ERROR: File %s does not exist" % arg)
    get_summary(arg)

names = summary.keys()

names.sort()

print summary
print names

s_avg = {}
time_breakdown_total = 0

for n in names:
    s_avg[n] = avg(summary[n])
    if re.search('^time_',n):
        time_breakdown_total += s_avg[n]
        print "{0}: {1}".format(n,s_avg[n] / s_avg['run_time'])

print "% Runtime measured: {0}".format(time_breakdown_total / s_avg['run_time'])
print "Compute time / txn: {0}".format( (s_avg['run_time'] - time_breakdown_total) / s_avg['txn_cnt'])
print "Per-thread throughput: {0}".format(s_avg['txn_cnt'] / s_avg['run_time'])
print "Throughput w/o waiting: {0}".format(s_avg['txn_cnt'] / (s_avg['run_time'] - s_avg['time_wait_lock'] - s_avg['time_wait_rem']))
print "% Remote measured: {0}".format(( (0.000033 * s_avg['msg_sent'])+ s_avg['rtime_unpack'] + s_avg['rtime_proc']) / (s_avg['time_wait_lock'] + s_avg['time_wait_rem']))
print s_avg
