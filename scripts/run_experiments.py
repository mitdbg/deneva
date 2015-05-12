#!/usr/bin/python

import os,sys,datetime,re
import shlex
import subprocess
from experiments import *
from helper import *

uname = "rhardin"

now = datetime.datetime.now()
strnow=now.strftime("%Y%m%d-%H%M%S")

# ISTC Machines ranked by clock skew
machines_=[
#GOOD
"istc8",
"istc6",
"istc1", 
"istc3", 
"istc4", 
#OK
"istc7",
"istc9",
"istc10", 
"istc13", 
#BAD
"istc11"
"istc12", 
"istc2",
"istc5",
]

os.chdir('..')

PATH=os.getcwd()
result_dir = PATH + "/results/"
test_dir = PATH + "/tests-" + strnow
test_dir_name = "tests-" + strnow

cfgs = configs

execute = True
remote = False

for arg in sys.argv:
    if arg == "-help" or arg == "-h":
        sys.exit("Usage: %s [-exec/-e/-noexec/-ne] [-remote/-rem]\n \
                -exec/-e: compile and execute locally (default)\n \
                -noexec/-ne: compile put in a tarball \
                " % sys.argv[0])
    if arg == "-exec" or arg == "-e":
        execute = True
    if arg == "-noexec" or arg == "-ne":
        execute = False
    if arg == "-remote" or arg == "-rem":
        remote = True

#if not execute:
#    cmd = "mkdir " + test_dir
#    os.system(cmd)
fmt = experiments[0]

for e in experiments[1:]:
    cfgs = get_cfgs(fmt,e)
    if remote:
        cfgs["TPORT_TYPE"],cfgs["TPORT_TYPE_IPC"],cfgs["TPORT_PORT"]="\"tcp\"","false",7000

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
        #result_dir_ = result_dir + output_f + "/"
        cmd = "mkdir -p {}".format(result_dir)
        os.system(cmd)
        cmd = "cp config.h {}{}.cfg".format(result_dir,output_f)
        os.system(cmd)

        if remote:
            machines = sorted(machines_[:cfgs["NODE_CNT"]])
            # create ifconfig file
            # TODO: ensure that machine order and node order is the same for ifconfig
            f = open("istc_ifconfig.txt",'r');
            lines = f.readlines()
            f.close()
            with open("ifconfig.txt",'w') as f_ifcfg:
                for line in lines:
                    line = line.rstrip('\n')
                    line = re.split(' ',line)
                    if line[0] in machines:
                        f_ifcfg.write(line[1] + "\n")

            files = ["rundb","ifconfig.txt","./benchmarks/TPCC_short_schema.txt"]
            for m,f in itertools.product(machines,files):
                cmd = 'scp {}/{} {}.csail.mit.edu:/home/{}/'.format(PATH,f,m,uname)
                print(cmd)
                os.system(cmd)

            print("Deploying: {}".format(output_f))
            cmd = './scripts/deploy.sh \'{}\' /home/{}/'.format(' '.join(machines),uname)
            print(cmd)
            os.system(cmd)

            for m,n in zip(machines,range(len(machines))):
                cmd = 'scp {}.csail.mit.edu:/home/{}/results.out {}{}_{}.out'.format(m,uname,result_dir,n,output_f)
                print(cmd)
                os.system(cmd)


        else:
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
    #else:
    #    cmd = "mkdir {}/{}".format(test_dir,output_dir)
    #    os.system(cmd)
    #    cmd = "cp rundb {}/{}".format(test_dir,output_dir)
    #    os.system(cmd)
    #    cmd = "cp config.h {}/{}".format(test_dir,output_dir)
    #    os.system(cmd)

#if not execute:
#    cmd = "tar -czvf tests.tgz {}".format(test_dir_name,test_dir_name)
#    os.system(cmd)

