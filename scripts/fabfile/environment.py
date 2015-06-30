import sys,os,os.path
from fabric.api import env

user="dvanaken"

def set_env():
    env.disable_known_hosts = True
    env.connection_attempts = 2
    env.timeout = 5
    env.remote_interrupt = True
    env.abort_on_prompts = True
    env.skip_bad_hosts = False
    env.colorize_errors = True
    env.kill = False
    env.batch_mode = True 
    env.local_path = os.getcwd()
    env.result_dir = os.path.join(env.local_path,"results/")

def set_env_vcloud():
    env.user = "root"
    env.uname = "root"
    env.cluster = "vcloud"
    env.remote = True
    env.key_filename = "~/.ssh/id_rsa_vcloud"
    env.ifconfig = os.path.join(env.local_path,"vcloud_ifconfig.txt")
    env.rem_homedir = os.path.join("/",env.uname)
    set_hosts()

def set_env_istc():
    env.user = user
    env.uname = user
    env.cluster = "istc"
    env.remote = True
    env.ifconfig = os.path.join(env.local_path,"istc_ifconfig.txt")
    env.rem_homedir = os.path.join("/home/",env.uname)
    set_hosts()

def set_env_local():
    env.remote = False

def set_hosts(hosts=[]):
    if len(hosts) == 0:
        env.hosts = []
        with open(env.ifconfig,"r") as f:
            lines = f.readlines()
        for line in lines:
            if not line.startswith('#'):
                env.hosts.append(line.split(" ")[0].rstrip())
    else:
        env.hosts = hosts

