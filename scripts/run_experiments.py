#!/usr/bin/python

import os,sys,datetime,re
import shlex
import subprocess
from experiments import *
from helper import *

now = datetime.datetime.now()
strnow=now.strftime("%Y%m%d-%H%M%S")

os.chdir('..')

PATH=os.getcwd()
result_dir = PATH + "/results/"
test_dir = PATH + "/tests-" + strnow
test_dir_name = "tests-" + strnow

cfgs = configs

execute = True

for arg in sys.argv:
    if arg == "-help" or arg == "-h":
        sys.exit("Usage: %s [-exec/-e/-noexec/-ne]\n \
                -exec/-e: compile and execute locally (default)\n \
                -noexec/-ne: compile put in a tarball \
                " % sys.argv[0])
    if arg == "-exec" or arg == "-e":
        execute = True
    if arg == "-noexec" or arg == "-ne":
        execute = False

if not execute:
    cmd = "mkdir " + test_dir
    os.system(cmd)

for e in experiments:
    cfgs["NODE_CNT"],cfgs["MAX_TXN_PER_PART"],cfgs["WORKLOAD"],cfgs["CC_ALG"],cfgs["MPR"] = e
    output_f = get_outfile_name(cfgs)
    output_dir = output_f + "/"
    output_f = output_f + strnow 
    print output_f
    
    f = open("config.h",'r');
    lines = f.readlines()
    f.close()
    with open("config.h",'w') as f_cfg:
        for line in lines:
            found_cfg = False
            for c in cfgs:
                found_cfg = re.search("#define "+c + "\t",line) or re.search("#define "+c + " ",line);
                if found_cfg:
                    f_cfg.write("#define " + c + " " + str(cfgs[c]) + "\n")
                    break
            if not found_cfg: f_cfg.write(line)

    cmd = "make clean; make -j"
    os.system(cmd)

    if execute:
        cmd = "cp config.h {}{}.cfg".format(result_dir,output_f)
        os.system(cmd)

        nnodes = cfgs["NODE_CNT"]
        pids = []
        for n in range(nnodes):
            cmd = "./rundb -nid{}".format(n)
            print(cmd)
            cmd = shlex.split(cmd)
            ofile_n = "{}{}_{}.out".format(result_dir,n,output_f)
            ofile = open(ofile_n,'w')
            #cmd = "./rundb -nid{} >> {}{}_{}.out &".format(n,result_dir,n,output_f)
            p = subprocess.Popen(cmd,stdout=ofile)
            pids.insert(0,p)
        for n in range(nnodes):
            pids[n].wait()
    else:
        cmd = "mkdir {}/{}".format(test_dir,output_dir)
        os.system(cmd)
        cmd = "cp rundb {}/{}".format(test_dir,output_dir)
        os.system(cmd)
        cmd = "cp config.h {}/{}".format(test_dir,output_dir)
        os.system(cmd)

if not execute:
    cmd = "tar -czvf tests.tgz {}".format(test_dir_name,test_dir_name)
    os.system(cmd)

