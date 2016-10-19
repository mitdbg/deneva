import sys,os,os.path
from fabric.api import env,output

user="rhardin"

def set_env():
    env.disable_known_hosts = True
    env.connection_attempts = 2
    env.timeout = 5
    env.remote_interrupt = True
    env.abort_on_prompts = True
    env.skip_bad_hosts = False
    env.colorize_errors = True
    env.kill = True
    env.dry_run = False
    env.batch_mode = True 
    env.local_path = os.getcwd()
    env.result_dir = os.path.join(env.local_path,"results/")
    env.same_node = False
    env.cram = False
    env.overlap = False
    env.shmem = True

    # These control the Fabric output
    output.status = False
    output.aborts = True
    output.warnings = True
    output.running = True
    output.stdout = True
    output.stderr = True
    output.exceptions = True
    output.debug = False
    output.user = True

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

def set_env_ec2():
    env.user = "ubuntu"
    env.uname = "ubuntu"
#    env.user = "ec2-user"
#    env.uname = "ec2-user"
    env.cluster = "ec2"
    env.remote = True
    env.key_filename = ["~/.ssh/devenv-key.pem","~/.ssh/devenv-key-wc.pem"]
    env.ifconfig = os.path.join(env.local_path,"ec2_ifconfig.txt")
    env.rem_homedir = os.path.join("/home/",env.uname)
    env.shmem = False
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

