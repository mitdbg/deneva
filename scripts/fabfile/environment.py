from fabric.api import env

def set_env():
	env.disable_known_hosts = True
	env.connection_attempts = 3
	env.abort_on_prompts = True
	env.skip_bad_hosts = False
	env.colorize_errors = True

def set_env_vcloud():
	env.user = "root"
	env.key_filename = "/usr0/home/dvanaken/.ssh/id_rsa_vcloud"

def set_env_istc():
	env.user = "dvanaken"

def set_hosts_vcloud():
	env.hosts = [
		"172.19.153.100",
		"172.19.153.101",
		"172.19.153.102",
		"172.19.153.103",
		"172.19.153.104",
		"172.19.153.105",
		"172.19.153.106",
		"172.19.153.107",
		"172.19.153.108",
		"172.19.153.109",
		"172.19.153.110",
		"172.19.153.111",
		"172.19.153.112",
		"172.19.153.113",
		"172.19.153.119",
		"172.19.153.136",
		"172.19.153.137",
		"172.19.153.138",
		"172.19.153.139",
		"172.19.153.140",
		"172.19.153.141",
		"172.19.153.142",
		"172.19.153.143",
		"172.19.153.144",
		"172.19.153.25",
		"172.19.153.80",
		"172.19.153.87",
		"172.19.153.89",
		"172.19.153.90",
		"172.19.153.91",
		"172.19.153.92",
		"172.19.153.93",
		"172.19.153.94",
		"172.19.153.95",
		"172.19.153.96",
		"172.19.153.97",
		"172.19.153.98",
		"172.19.153.99"
	]

def set_hosts_istc():
	env.hosts = [
		#GOOD
		"istc1.csail.mit.edu", 
		"istc3.csail.mit.edu", 
		#"istc4", 
		"istc6.csail.mit.edu",
		"istc8.csail.mit.edu",
		#OK
		"istc7.csail.mit.edu",
		"istc9.csail.mit.edu",
		"istc10.csail.mit.edu", 
		"istc13.csail.mit.edu", 
		#BAD
		"istc11.csail.mit.edu",
		"istc12.csail.mit.edu", 
		"istc2.csail.mit.edu",
		"istc5.csail.mit.edu",
	]
