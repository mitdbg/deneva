import itertools
# Experiments to run and analyze
# Go to end of file to fill in experiments 

# Format: [#Nodes,#Txns,Workload,CC_ALG,MPR]

simple = [
#[2,1000,'TPCC','HSTORE',1],
[2,1000,'TPCC','OCC',20]
]

experiments_100K = [
    [n,100000,'TPCC','HSTORE',m] for n,m in itertools.product([2,4,8,16],[1]+range(0,101,10))
]


experiments_10K_wait_die = [
    [n,10000,'TPCC',cc,m] for n,m,cc in itertools.product([2,4],[1]+range(0,51,10),['WAIT_DIE'])
]
experiments_10K_no_wait = [
    [n,10000,'TPCC',cc,m] for n,m,cc in itertools.product([2,4],[1]+range(0,51,10),['NO_WAIT'])
]
experiments_10K_2pl = experiments_10K_no_wait + experiments_10K_wait_die

experiments_10K_hstore = [
    [n,10000,'TPCC',cc,m] for n,m,cc in itertools.product([2,4],[1]+range(0,51,10),['HSTORE'])
]

experiments_10K_tso = [
    [n,10000,'TPCC',cc,m] for n,m,cc in itertools.product([2,4],[1]+range(0,51,10),['TIMESTAMP'])
]

experiments_10K_mvcc = [
    [n,10000,'TPCC',cc,m] for n,m,cc in itertools.product([2,4],[1]+range(0,51,10),['MVCC'])
]

experiments_10K_occ = [
    [n,10000,'TPCC',cc,m] for n,m,cc in itertools.product([2,4],[1]+range(0,51,10),['OCC'])
]


experiments_10K_all = experiments_10K_2pl + experiments_10K_tso + experiments_10K_hstore + experiments_10K_mvcc + experiments_10K_occ

experiments_1K = [
    [n,1000,'TPCC',cc,m] for n,m,cc in itertools.product([2,4,8],[1]+range(0,51,10),['HSTORE','NO_WAIT','WAIT_DIE'])
]

experiments_1K_2pl = [
    [n,1000,'TPCC',cc,m] for n,m,cc in itertools.product([2,4,8],[1]+range(0,51,10),['NO_WAIT','WAIT_DIE'])
]

experiments_1K_hstore = [
    [n,1000,'TPCC',cc,m] for n,m,cc in itertools.product([2,4,8],[1]+range(0,51,10),['HSTORE'])
]

experiments_1K_tso = [
    [n,1000,'TPCC',cc,m] for n,m,cc in itertools.product([2],[30],['TIMESTAMP'])
]

experiments_n2 = [
    [2,10000,'TPCC','HSTORE',m] for m in [1] + range(0,101,10)
]
experiments_n4 = [
    [4,10000,'TPCC','HSTORE',m] for m in [1] + range(0,101,10)
]

# Configs used in output file names
config_names=["NODE_CNT","MAX_TXN_PER_PART","WORKLOAD","CC_ALG","MPR"]

# Default values for variable configurations
configs = {
    "NODE_CNT" : 2,
    "MAX_TXN_PER_PART" : 100,
    "WORKLOAD" : "TPCC",
    "CC_ALG" : "HSTORE",
    "MPR" : 0,
    "TPORT_TYPE":"\"ipc\"",
    "TPORT_TYPE_IPC":"true",
    "TPORT_PORT":"\"_.ipc\"",
    "REM_THREAD_CNT": 1
}

##################
# FIXME
#################
experiments = experiments_10K_all
