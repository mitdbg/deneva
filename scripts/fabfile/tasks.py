#!/usr/bin/python

from fabric.api import task,run,local,put,get,execute,settings
from fabric.decorators import *
from fabric.context_managers import shell_env
from fabric.exceptions import *
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

PATH=os.getcwd()
result_dir = PATH + "/results/"
test_dir = PATH + "/tests-" + strnow
test_dir_name = "tests-" + strnow
max_time_per_exp = 60 * 6	# in seconds

cfgs = configs

vcloud_uname="root"
istc_uname="dvanaken"

execute_exps = True
remote = False
cluster = None
skip = False
uname = None
cfg_fname = ""
rem_homedir = ""
cc_alg = ""

set_env()

## Basic usage:
##		fab run_exps_vcloud:experiment_1
@task
@hosts('localhost')
def run_exps_vcloud(exps,skip_completed=False,exec_exps=True):
	set_env_vcloud()
	set_hosts_vcloud()
	global remote, cluster, skip, machines, uname, cfg_fname, rem_homedir, execute_exps 
	remote = True
	cluster = 'vcloud'
	skip = skip_completed
	uname = vcloud_uname
	cfg_fname = "vcloud_ifconfig.txt"
	rem_homedir = "/" + uname
	execute_exps = exec_exps
	execute(run_exp,exps)

## Basic usage:
##		fab run_exps_istc:experiment_1
@task
@hosts('localhost')
def run_exps_istc(exps,skip_completed=False,exec_exps=True):
	set_env_istc()
	set_hosts_istc()
	global remote, cluster, skip, machines, uname, cfg_fname, rem_homedir, execute_exps 
	remote = True
	cluster = 'istc'
	skip = skip_completed
	uname = istc_uname
	cfg_fname = "istc_ifconfig.txt"
	rem_homedir = "/home/" + uname
	execute_exps = exec_exps
	execute(run_exp,exps)

## Basic usage:
##		fab run_exps_local:experiment_1
@task
@hosts('localhost')
def run_exps_local(exps,skip_completed=False,exec_exps=True):
	global skip, execute_exps
	skip = skip_completed
	execute_exps = exec_exps
	execute(run_exp,exps)

@task
@hosts('localhost')
def delete_local_results():
	local("rm -f results/*");

@task
@parallel
def copy_files(schema,rem_homedir):
	executable_files = ["rundb","runcl"]
	if cc_alg == "CALVIN":
		executable_files.append("runsq")
	files = ["ifconfig.txt"]
	files.append(schema)
	for f in (files + executable_files):
		put(f,rem_homedir)
	for f in executable_files:
		run("chmod +x {}/{}".format(rem_homedir,f))

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
		set_hosts_istc()
	else:
		set_env_istc()
		set_hosts_istc()
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
	with shell_env(SCHEMA_PATH=schema_path):
		with settings(command_timeout=max_time_per_exp):
			if env.host in env.roledefs["servers"]:
				cmd = "./rundb -nid{} >> results.out 2>&1".format(nid)	
			elif env.host in env.roledefs["clients"]:
				cmd = "./runcl -nid{} >> results.out 2>&1".format(nid)
			elif env.host in env.roledefs["sequencer"]:
				cmd = "./runsq -nid{} >> results.out 2>&1".format(nid)

			print(env.host + ": " + str(nid))
			try:
				run(cmd)
			except CommandTimeout:
				pass

@task
@parallel
def get_results(output_basename):
	nid = env.hosts.index(env.host)
	rem_path='{}/results.out'.format(rem_homedir)
	print rem_path
	loc_path="{}{}_{}.out".format(result_dir,nid,output_basename)
	with settings(warn_only=True):
		get(remote_path=rem_path, local_path=loc_path)
		run("rm -f results.out")

def write_config(cfgs):
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

def write_ifconfig(machines):
	assert(cfg_fname != "")
	f = open(cfg_fname,'r');
	lines = f.readlines()
	f.close()
	with open("ifconfig.txt",'w') as f_ifcfg:
		for line in lines:
			line = line.rstrip('\n')
			if cluster == 'istc':
				line = re.split(' ',line)
				machine = "{}.csail.mit.edu".format(line[0])
				if machine in machines:
					f_ifcfg.write(line[1] + "\n")
			elif cluster == 'vcloud':
				if line in machines:
					f_ifcfg.write(line + "\n")


def assign_roles(server_cnt,client_cnt):
	assert(len(env.hosts) >= server_cnt+client_cnt)
	servers = env.hosts[0:server_cnt]
	clients = env.hosts[server_cnt:server_cnt+client_cnt]
	env.roledefs={}
	env.roledefs['clients']=clients
	env.roledefs['servers']=servers

	if cc_alg == 'CALVIN':
		sequencer = env.hosts[server_cnt+client_cnt:server_cnt+client_cnt+1]
		env.roledefs['sequencer']=sequencer
	

@task
@hosts('localhost')
def reset_hosts(new_hosts):
	if new_hosts:
		env.hosts = new_hosts;
	else:
		if cluster == 'vcloud':
			set_hosts_vcloud()
		else:
			set_hosts_istc()

@task
@hosts('localhost')
def run_exp(expss):
	# TODO: fix this
	exps = []
	exps.append(expss)
	for exp in exps:
		fmt,experiments = experiment_map[exp]()

		for e in experiments:
			cfgs = get_cfgs(fmt,e)
			if remote:
				cfgs["TPORT_TYPE"],cfgs["TPORT_TYPE_IPC"],cfgs["TPORT_PORT"]="\"tcp\"","false",7000

			output_f = get_outfile_name(cfgs)

			# Check whether experiment has been already been run in this batch
			if skip:
				if len(glob.glob('{}*{}*.out'.format(result_dir,output_f))) > 0:
					print "Experiment exists in results folder... skipping"
					continue

			output_dir = output_f + "/"
			output_f = output_f + strnow 

			write_config(cfgs)
			global cc_alg
			cc_alg = cfgs["CC_ALG"]
			execute(compile)
			if not execute_exps:
				exit()

			if execute_exps:
				cmd = "mkdir -p {}".format(result_dir)
				local(cmd)
				cmd = "cp config.h {}{}.cfg".format(result_dir,output_f)
				local(cmd)

				nnodes = cfgs["NODE_CNT"]
				nclnodes = cfgs["CLIENT_NODE_CNT"]
				ntotal = nnodes + nclnodes
				if cc_alg == 'CALVIN':
					ntotal += 1

				if remote:
					execute(reset_hosts,None)
					machines = sorted(env.hosts[:(ntotal)])
					execute(assign_roles,nnodes,nclnodes)
					execute(reset_hosts,machines)

					write_ifconfig(machines)

					if cfgs["WORKLOAD"] == "TPCC":
						schema = "benchmarks/TPCC_short_schema.txt"
					elif cfgs["WORKLOAD"] == "YCSB":
						schema = "benchmarks/YCSB_schema.txt"
					execute(copy_files,schema,rem_homedir)

					if cluster == 'vcloud':
						# Sync clocks before each experiment
						print("Syncing Clocks...")
						execute(sync_clocks)
					print("Deploying: {}".format(output_f))
					schema_path = "{}/".format(rem_homedir)
					execute(deploy,schema_path)
					execute(get_results,output_f)

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
						ofile_n = "{}{}_{}.out".format(result_dir,n,output_f)
						ofile = open(ofile_n,'w')
						p = subprocess.Popen(cmd,stdout=ofile,stderr=ofile)
						pids.insert(0,p)
					for n in range(ntotal):
						pids[n].wait()


	
