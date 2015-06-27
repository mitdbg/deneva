#!/usr/bin/python

#Configuration file for run_experiments.py

istc_uname = 'rhardin'

# ISTC Machines ranked by clock skew
istc_machines=[

#GOOD
"istc1", 
"istc3", 
#"istc4", 
"istc6",
"istc8",
#OK
"istc7",
"istc9",
"istc10", 
"istc13", 
#BAD
"istc11",
"istc12", 
"istc2",
"istc5",
]

vcloud_uname = 'root'
#identity = "/usr0/home/dvanaken/.ssh/id_rsa_vcloud"
identity = "/usr0/home/benchpress/.ssh/id_rsa_vcloud"
vcloud_machines = [
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
# "90",
 "91",
 "92",
 "93",
 "94",
 "95",
 "96",
 "97",
 "98",
 "99",
 "139",
 "140",
 "141",
 "142",
 "143",
 "144",
]


