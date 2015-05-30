#!/usr/bin/python

import os,sys,datetime,re
import shlex
import subprocess
from experiments import *
from helper import *
from run_config import *
import glob

now = datetime.datetime.now()
strnow=now.strftime("%Y%m%d-%H%M%S")

os.chdir('..')

PATH=os.getcwd()
result_dir = PATH + "/results/"
test_dir = PATH + "/tests-" + strnow
test_dir_name = "tests-" + strnow

cfgs = configs

execute = True
remote = False
cluster = None


exps=[]

arg_cluster = False
for arg in sys.argv[1:]:
    if arg == "-help" or arg == "-h":
        sys.exit("Usage: %s [-exec/-e/-noexec/-ne] [-c cluster] experiments\n \
                -exec/-e: compile and execute locally (default)\n \
                -noexec/-ne: compile first target only \
                -c: run remote on cluster; possible values: istc, vcloud\n \
                " % sys.argv[0])
    if arg == "-exec" or arg == "-e":
        execute = True
    elif arg == "-noexec" or arg == "-ne":
        execute = False
    elif arg == "-c":
        remote = True
        arg_cluster = True
    elif arg_cluster:
        cluster = arg
        arg_cluster = False
    else:
        exps.append(arg)

for exp in exps:
    fmt,experiments = experiment_map[exp]()

    for e in experiments:
        cfgs = get_cfgs(fmt,e)
        if remote:
            cfgs["TPORT_TYPE"],cfgs["TPORT_TYPE_IPC"],cfgs["TPORT_PORT"]="\"tcp\"","false",7000

        output_f = get_outfile_name(cfgs)
        output_dir = output_f + "/"
        output_f = output_f + strnow 
        print output_f

        # Check whether experiment has been already been run in this batch
        if len(glob.glob('{}*{}.out'.format(result_dir,output_f))) > 0:
            print "Experiment exists in results folder... skipping"
            continue
        
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
        if not execute:
            exit()

        if execute:
            cmd = "mkdir -p {}".format(result_dir)
            os.system(cmd)
            cmd = "cp config.h {}{}.cfg".format(result_dir,output_f)
            os.system(cmd)

            if remote:
                if cluster == 'istc':
                    machines_ = istc_machines
                    uname = istc_uname
                    cfg_fname = "istc_ifconfig.txt"
                elif cluster == 'vcloud':
                    machines_ = vcloud_machines
                    uname = vcloud_uname
                    cfg_fname = "vcloud_ifconfig.txt"
                else:
                    assert(False)
                machines = sorted(machines_[:(cfgs["NODE_CNT"] + cfgs["CLIENT_NODE_CNT"])])
                # TODO: ensure that machine order and node order is the same for ifconfig
                f = open(cfg_fname,'r');
                lines = f.readlines()
                f.close()
                with open("ifconfig.txt",'w') as f_ifcfg:
                    for line in lines:
                        line = line.rstrip('\n')
                        if cluster == 'istc':
                            line = re.split(' ',line)
                            if line[0] in machines:
                                f_ifcfg.write(line[1] + "\n")
                        elif cluster == 'vcloud':
                            if line in machines:
                                f_ifcfg.write("172.19.153." + line + "\n")

                if cfgs["WORKLOAD"] == "TPCC":
                    files = ["rundb","ifconfig.txt","./benchmarks/TPCC_short_schema.txt"]
                elif cfgs["WORKLOAD"] == "YCSB":
                    files = ["rundb","ifconfig.txt","./benchmarks/YCSB_schema.txt"]
                for m,f in itertools.product(machines,files):
                    if cluster == 'istc':
                        cmd = 'scp {}/{} {}.csail.mit.edu:/home/{}/'.format(PATH,f,m,uname)
                    elif cluster == 'vcloud':
                        cmd = 'scp -i {} {}/{} root@172.19.153.{}:/{}/'.format(identity,PATH,f,m,uname)
                    print(cmd)
                    os.system(cmd)

                print("Deploying: {}".format(output_f))
                if cluster == 'istc':
                    cmd = './scripts/deploy.sh \'{}\' /home/{}/ {}'.format(' '.join(machines),uname),cfgs["NODE_CNT"]
                elif cluster == 'vcloud':
                    cmd = './scripts/vcloud_deploy.sh \'{}\' /{}/ {}'.format(' '.join(machines),uname,cfgs["NODE_CNT"])
                print(cmd)
                os.system(cmd)

                for m,n in zip(machines,range(len(machines))):
                    if cluster == 'istc':
                        cmd = 'scp {}.csail.mit.edu:/home/{}/results.out {}{}_{}.out'.format(m,uname,result_dir,n,output_f)
                        print(cmd)
                        os.system(cmd)
                    elif cluster == 'vcloud':
                        cmd = 'scp -i {} root@172.19.153.{}:/{}/results.out {}{}_{}.out'.format(identity,m,uname,result_dir,n,output_f)
                        print(cmd)
                        os.system(cmd)
                        cmd = 'ssh -i {} root@172.19.153.{} \"rm /{}/results.out\"'.format(identity,m,uname)
                        print(cmd)
                        os.system(cmd)


            else:
                nnodes = cfgs["NODE_CNT"]
                nclnodes = cfgs["CLIENT_NODE_CNT"]
                pids = []
                print("Deploying: {}".format(output_f))
                for n in range(nnodes+nclnodes):
                    if n < nnodes:
                        cmd = "./rundb -nid{}".format(n)
                    else:
                        cmd = "./runcl -nid{}".format(n)
                    print(cmd)
                    cmd = shlex.split(cmd)
                    ofile_n = "{}{}_{}.out".format(result_dir,n,output_f)
                    ofile = open(ofile_n,'w')
                    p = subprocess.Popen(cmd,stdout=ofile,stderr=ofile)
                    pids.insert(0,p)
                for n in range(nnodes + nclnodes):
                    pids[n].wait()

