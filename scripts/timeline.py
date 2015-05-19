import os, sys, re, math, os.path, math
from helper import *
from experiments import experiments as experiments
from experiments import configs
from experiments import nnodes,nmpr,nalgos,nthreads,nwfs,ntifs,nnet_delay,ntxn
from plot_helper import *
from draw import *
import glob

PATH=os.getcwd()
result_dir = PATH + "/../results/"

llim = 50 
ulim = 100
for e in experiments[1:]:
    r = {}
    cfgs = get_cfgs(experiments[0],e)
    output_f = get_outfile_name(cfgs)
    for n in range(cfgs["NODE_CNT"]):
        ofile = "{}{}_{}*.out".format(result_dir,n,output_f)
        res_list = sorted(glob.glob(ofile),key=os.path.getmtime,reverse=True)
        if res_list:
            print(res_list[0])
            r = get_timeline(res_list[0],r,low_lim=llim,up_lim=ulim)
    tids = []
    times = []
    types = ["START","ABORT","LOCK","UNLOCK","COMMIT"]
    for i in types:
        tids.append(r[i]["tid"])
        times.append(r[i]["time"])
 
    draw_scatter("scatter_{}".format(output_f),tids,times,title=output_f,linenames=types);
