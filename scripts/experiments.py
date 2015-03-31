import itertools
# Experiments to run and analyze
# Go to end of file to fill in experiments 

# Format: [#Nodes,#Txns,Workload,CC_ALG,MPR]
fmt1 = [["NODE_CNT","MAX_TXN_PER_PART","WORKLOAD","CC_ALG","MPR"]]
fmt2 = [["NODE_CNT","MAX_TXN_PER_PART","WORKLOAD","CC_ALG","MPR","THREAD_CNT"]]

simple = [
#[2,1000,'TPCC','HSTORE',10]
[2,10000,'TPCC','MVCC',20,4],
[2,10000,'TPCC','MVCC',30,4],
[2,10000,'TPCC','MVCC',40,4],
#[2,10000,'TPCC','TIMESTAMP',20,4],
#[2,10000,'TPCC','TIMESTAMP',30,4],
#[2,10000,'TPCC','TIMESTAMP',40,4]
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

experiments_10K_wait_die_mt = [
    [n,10000,'TPCC',cc,m,t] for n,m,cc,t in itertools.product([2],[1]+range(0,51,10),['WAIT_DIE'],[1,2,4])
]
experiments_10K_no_wait_mt = [
    [n,10000,'TPCC',cc,m,t] for n,m,cc,t in itertools.product([2],[1]+range(0,51,10),['NO_WAIT'],[1,2,4])
]
experiments_10K_2pl_mt = experiments_10K_no_wait_mt + experiments_10K_wait_die_mt

experiments_10K_hstore_mt = [
    [n,10000,'TPCC',cc,m,t] for n,m,cc,t in itertools.product([2],[1]+range(0,51,10),['HSTORE'],[1,2,4])
]

experiments_10K_tso_mt = [
    [n,10000,'TPCC',cc,m,t] for n,m,cc,t in itertools.product([2],[1]+range(0,51,10),['TIMESTAMP'],[1,2,4])
]

experiments_10K_mvcc_mt = [
    [n,10000,'TPCC',cc,m,t] for n,m,cc,t in itertools.product([2],[1]+range(0,51,10),['MVCC'],[1,2,4])
]

experiments_10K_occ_mt = [
    [n,10000,'TPCC',cc,m,t] for n,m,cc,t in itertools.product([2],[1]+range(0,51,10),['OCC'],[1,2,4])
]

experiments_10K_all_mt = experiments_10K_2pl_mt + experiments_10K_tso_mt + experiments_10K_hstore_mt + experiments_10K_mvcc_mt + experiments_10K_occ_mt

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
#config_names=["NODE_CNT","MAX_TXN_PER_PART","WORKLOAD","CC_ALG","MPR"]#,"THREAD_CNT","REM_THREAD_CNT","PART_CNT"]

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
    "REM_THREAD_CNT": 2,
    "THREAD_CNT": 1,
    "PART_CNT": 2 
}

##################
# FIXME
#################
experiments = fmt2 + simple
config_names = fmt2[0]
