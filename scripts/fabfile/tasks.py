#!/usr/bin/python

from fabric.api import task,run,local,put,get,execute,settings
from fabric.decorators import *
from fabric.context_managers import shell_env
from fabric.exceptions import *
import traceback
import os,sys,datetime,re
import itertools
import glob
import shlex
import subprocess

sys.path.append('..')

from environment import *
from experiments import *
from helper import get_cfgs,get_outfile_name


now=datetime.datetime.now()
strnow=now.strftime("%Y%m%d-%H%M%S")

os.chdir('../..')

max_time_per_exp = 60 * 6   # in seconds

cfgs = configs

execute_exps = True
skip = False
cc_alg = ""

set_env()

@task
@hosts('localhost')
def using_vcloud():
    set_env_vcloud()

@task
@hosts('localhost')
def using_istc():
    set_env_istc()

@task
@hosts('localhost')
def using_local():
    set_env_local()

## Basic usage:
##      fab using_vcloud run_exps:experiment_1
##      fab using_local  run_exps:experiment_1
##      fab using_istc   run_exps:experiment_1
@task
@hosts('localhost')
def run_exps(exps,skip_completed='False',exec_exps='True'):
    global skip, execute_exps 
    skip = skip_completed == 'True'
    execute_exps = exec_exps == 'True'
    execute(run_exp,exps)


## Basic usage:
##      fab using_vcloud network_test
##      fab using_istc   network_test:4
@task
@hosts(['localhost'])
def network_test(num_nodes=16,exps="network_experiment",skip_completed='False',exec_exps='True'):
    global skip, execute_exps, max_time_per_exp 
    skip = skip_completed == 'True'
    execute_exps = exec_exps == 'True'
    max_time_per_exp = 30
    num_nodes = int(num_nodes)
    if num_nodes < 2 or len(env.hosts) < num_nodes:
        raise("Not enough hosts in ifconfig!")
    exp_hosts=env.hosts[0:num_nodes]
    pairs = list(itertools.combinations(exp_hosts,2))
    for pair in pairs:
        set_hosts(list(pair))
        execute(run_exp,exps,network_test=True)


@task
@hosts('localhost')
def delete_local_results():
    local("rm -f results/*");

@task
@parallel
def copy_files(schema):
    executable_files = ["rundb","runcl"]
    if cc_alg == "CALVIN":
        executable_files.append("runsq")
    files = ["ifconfig.txt"]
    files.append(schema)
    succeeded = True
    try:
        for f in (files + executable_files):
            put(f,env.rem_homedir)
        for f in executable_files:
            run("chmod +x {}/{}".format(env.rem_homedir,f))
    except NetworkError as ne:
        print "ERROR: Host: {}".format(env.host)
        traceback.print_exc()
        succeeded = False
    except SystemExit as e:
        print "ERROR: Host: {}".format(env.host)
        traceback.print_exc()
        succeeded = False
    return succeeded

@task
@parallel
def sync_clocks():
    run("ntpdate -b clock-1.cs.cmu.edu")

@task
@hosts('localhost')
def compile():
    local("make clean; make -j")

@task
@hosts('localhost')
def setup_and_exec(cluster,function):
    if cluster == 'istc':
        set_env_istc()
    else:
        set_env_vcloud()
    execute(function)

@task
@parallel
def killall():
    with settings(warn_only=True):
        run("pkill -f rundb")
        run("pkill -f runcl")
        run("pkill -f runsq")

@task
@parallel
def deploy(schema_path):
    nid = env.hosts.index(env.host)
    succeeded = True
    with shell_env(SCHEMA_PATH=schema_path):
        with settings(command_timeout=max_time_per_exp):
            if env.host in env.roledefs["servers"]:
                cmd = "./rundb -nid{} >> results.out 2>&1".format(nid)  
            elif env.host in env.roledefs["clients"]:
                cmd = "./runcl -nid{} >> results.out 2>&1".format(nid)
            elif "sequencer" in env.roledefs and env.host in env.roledefs["sequencer"]:
                cmd = "./runsq -nid{} >> results.out 2>&1".format(nid)
            else:
                print env.roledefs
                raise Exception("Host does not belong to any role")

            print(env.host + ": " + str(nid))
            try:
                run(cmd)
            except CommandTimeout:
                pass
            except NetworkError as ne:
                print "ERROR: Host: {}".format(env.host)
                traceback.print_exc()
                succeeded = False
            except SystemExit as e:
                print "ERROR: Host: {}".format(env.host)
                traceback.print_exc()
                succeeded = False
    return succeeded

@task
@parallel
def get_results(output_basename):
    succeeded = True
    nid = env.hosts.index(env.host)
    rem_path=os.path.join(env.rem_homedir,"results.out")
    loc_path=os.path.join(env.result_dir,"{}_{}".format(nid,output_basename))
    try:
        get(remote_path=rem_path, local_path=loc_path)
        run("rm -f results.out")
    except NetworkError as ne:
        print "ERROR: Host: {}".format(env.host)
        traceback.print_exc()
        succeeded = False
    except SystemExit as e:
        print "ERROR: Host: {}".format(env.host)
        traceback.print_exc()
        succeeded = False
    return succeeded

@task
@hosts('localhost')
def write_config(cfgs):
    dbx_cfg = os.path.join(env.local_path,"config.h")
    f = open(dbx_cfg,'r');
    lines = f.readlines()
    f.close()
    with open(dbx_cfg,'w') as f_cfg:
        for line in lines:
            found_cfg = False
            for c in cfgs:
                found_cfg = re.search("#define "+c + "\t",line) or re.search("#define "+c + " ",line);
                if found_cfg:
                    f_cfg.write("#define " + c + " " + str(cfgs[c]) + "\n")
                    break
            if not found_cfg: f_cfg.write(line)

@task
@hosts('localhost')
def write_ifconfig(machines):
    with open("ifconfig.txt",'w') as f:
        for server in env.roledefs['servers']:
            f.write(server + "\n")
        for client in env.roledefs['clients']:
            f.write(client + "\n")
        if "sequencer" in env.roledefs:
            assert cc_alg == "CALVIN"
            f.write(env.roledefs['sequencer'][0] + "\n")
            
@task
@hosts('localhost')
def assign_roles(server_cnt,client_cnt):
    assert(len(env.hosts) >= server_cnt+client_cnt)
    servers=env.hosts[0:server_cnt]
    clients=env.hosts[server_cnt:server_cnt+client_cnt]
    env.roledefs={}
    env.roledefs['clients']=clients
    env.roledefs['servers']=servers

    if cc_alg == 'CALVIN':
        sequencer = env.hosts[server_cnt+client_cnt:server_cnt+client_cnt+1]
        env.roledefs['sequencer']=sequencer

@task
@hosts(['localhost'])
def run_exp(expss,network_test=False):
    # TODO: fix this
    exps = []
    exps.append(expss)
    for exp in exps:
        fmt,experiments = experiment_map[exp]()
        
        for e in experiments:
            cfgs = get_cfgs(fmt,e)
            if env.remote:
                cfgs["TPORT_TYPE"],cfgs["TPORT_TYPE_IPC"],cfgs["TPORT_PORT"]="\"tcp\"","false",7000
            output_f = get_outfile_name(cfgs,env.hosts) 

            # Check whether experiment has been already been run in this batch
            if skip:
                if len(glob.glob('{}*{}*.out'.format(env.result_dir,output_f))) > 0:
                    print "Experiment exists in results folder... skipping"
                    continue

            output_dir = output_f + "/"
            output_f = output_f + strnow 

            write_config(cfgs)
            global cc_alg
            cc_alg = cfgs["CC_ALG"]
            execute(compile)
            if execute_exps:
                cmd = "mkdir -p {}".format(env.result_dir)
                local(cmd)
                cmd = "cp config.h {}{}.cfg".format(env.result_dir,output_f)
                local(cmd)

                nnodes = cfgs["NODE_CNT"]
                nclnodes = cfgs["CLIENT_NODE_CNT"]
                ntotal = nnodes + nclnodes
                if cc_alg == 'CALVIN':
                    ntotal += 1

                if env.remote:
                    completed = False
                    attempts = 0
                    while not completed and attempts < 3:
                        if not network_test:
                            set_hosts()
                            
                            # Find and skip bad hosts
                            ping_results = execute(ping)
                            for host in ping_results:
                                if ping_results[host] != 0:
                                    env.hosts.remove(host)
                                    print "Skipping non-responsive host {}".format(host)

                        machines = env.hosts[:ntotal]
                        execute(assign_roles,nnodes,nclnodes)
                        set_hosts(machines)

                        write_ifconfig(machines)

                        if cfgs["WORKLOAD"] == "TPCC":
                            schema = "benchmarks/TPCC_short_schema.txt"
                        elif cfgs["WORKLOAD"] == "YCSB":
                            schema = "benchmarks/YCSB_schema.txt"
                        # NOTE: copy_files will fail if any (possibly) stray processes
                        # are still running one of the executables. Setting the 'kill'
                        # flag in environment.py to true to kill these processes. This
                        # is useful for running real experiments but dangerous when both
                        # of us are debugging...
                        res = execute(copy_files,schema)
                        if not succeeded(res):
                            if env.kill:
                                execute(killall)
                            attempts += 1
                            continue
                        
                        if env.cluster != 'istc':
                            # Sync clocks before each experiment
                            print("Syncing Clocks...")
                            execute(sync_clocks)
                        print("Deploying: {}".format(output_f))
                        schema_path = "{}/".format(env.rem_homedir)
                        res = execute(deploy,schema_path)
                        if not succeeded(res):
                            attempts += 1
                            continue
                        res = execute(get_results,output_f)
                        if not succeeded(res):
                            attempts += 1
                            continue
                        completed = True

                else:
                    pids = []
                    print("Deploying: {}".format(output_f))
                    for n in range(ntotal):
                        if n < nnodes:
                            cmd = "./rundb -nid{}".format(n)
                        elif n < nnodes+nclnodes:
                            cmd = "./runcl -nid{}".format(n)
                        elif n == nnodes+nclnodes:
                            assert(cc_alg == 'CALVIN')
                            cmd = "./runsq -nid{}".format(n)
                        else:
                            assert(false)
                        print(cmd)
                        cmd = shlex.split(cmd)
                        ofile_n = "{}{}_{}.out".format(env.result_dir,n,output_f)
                        ofile = open(ofile_n,'w')
                        p = subprocess.Popen(cmd,stdout=ofile,stderr=ofile)
                        pids.insert(0,p)
                    for n in range(ntotal):
                        pids[n].wait()

def succeeded(outcomes):
    for host,outcome in outcomes.iteritems():
        if not outcome:
            return False
    return True

@task
@parallel
def ping():
    with settings(warn_only=True):
        res=local("ping -w5 -c2 {}".format(env.host),capture=True)

    assert res != None
    return res.return_code

