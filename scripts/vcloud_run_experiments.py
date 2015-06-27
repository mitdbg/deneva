#!/usr/bin/python

import os,sys,datetime,re
import shlex
import subprocess
import glob
from experiments import *
from helper import *

uname = "root"
local_uname = "benchpress"
identity = "/usr0/home/" + local_uname + "/.ssh/id_rsa_vcloud"

now = datetime.datetime.now()
strnow=now.strftime("%Y%m%d-%H%M%S")

machines_ = [
 "100",
 "101",
 "102",
 "103",
 "104",
 "105",
 "106",
 "107",
 "108",
 "109",
 "110",
 "111",
# "112",
 "113",
 "119",
 "136",
 "137",
 "138",
 "25",
 "80",
 "87",
 "89",
 "90",
 "91",
 "92",
 "93",
 "94",
 "95",
 "96",
 "97",
 "98",
 "99"
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
    if len(glob.glob('{}*{}*.out'.format(result_dir,get_outfile_name(cfgs)))) > 0:
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

    if execute:
        #result_dir_ = result_dir + output_f + "/"
        cmd = "mkdir -p {}".format(result_dir)
        os.system(cmd)
        cmd = "cp config.h {}{}.cfg".format(result_dir,output_f)
        os.system(cmd)

        if remote:
            machines = sorted(machines_[:(cfgs["NODE_CNT"] + cfgs["CLIENT_NODE_CNT"])])
            # create ifconfig file
            # TODO: ensure that machine order and node order is the same for ifconfig
            f = open("vcloud_ifconfig.txt",'r');
            lines = f.readlines()
            f.close()
            with open("ifconfig.txt",'w') as f_ifcfg:
                for line in lines:
                    line = line.rstrip('\n')
                    #line = re.split(' ',line)
                    if line in machines:
                        f_ifcfg.write("172.19.153." + line + "\n")

            if cfgs["WORKLOAD"] == "TPCC":
                files = ["rundb","runcl","ifconfig.txt","./benchmarks/TPCC_short_schema.txt"]
            elif cfgs["WORKLOAD"] == "YCSB":
                files = ["rundb","runcl","ifconfig.txt","./benchmarks/YCSB_schema.txt"]
            for m,f in itertools.product(machines,files):
                cmd = 'scp -i {} {}/{} root@172.19.153.{}:/{}/'.format(identity,PATH,f,m,uname)
                print(cmd)
                os.system(cmd)

            print("Deploying: {}".format(output_f))
            cmd = './scripts/vcloud_deploy.sh \'{}\' /{}/ {}'.format(' '.join(machines),uname,cfgs["NODE_CNT"])
            print(cmd)
            os.system(cmd)

            for m,n in zip(machines,range(len(machines))):
                cmd = 'scp -i {} root@172.19.153.{}:/{}/results.out {}{}_{}.out'.format(identity,m,uname,result_dir,n,output_f)
                print(cmd)
                os.system(cmd)
                cmd = 'ssh -i {} root@172.19.153.{} \"rm /{}/results.out\"'.format(identity,m,uname)
                print(cmd)
                os.system(cmd)


        else:
            nnodes = cfgs["NODE_CNT"]
            clnodes = cfgs["CLIENT_NODE_CNT"]
            pids = []
            print("Deploying: {}".format(output_f))
            for n in range(nnodes + clnodes):
                #for n in range(nnodes):
                if n < nnodes:
                    cmd = "./rundb -nid{}".format(n)
                else:
                    cmd = "./runcl -nid{}".format(n)
                print(cmd)
                cmd = shlex.split(cmd)
                ofile_n = "{}{}_{}.out".format(result_dir,n,output_f)
                ofile = open(ofile_n,'w')
                #cmd = "./rundb -nid{} >> {}{}_{}.out &".format(n,result_dir,n,output_f)
                p = subprocess.Popen(cmd,stdout=ofile,stderr=ofile)
                pids.insert(0,p)
            for n in range(nnodes + clnodes):
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

