import itertools
# Experiments to run and analyze
# Go to end of file to fill in experiments 

# Format: [#Nodes,#Txns,Workload,CC_ALG,MPR]

experiments_100K = [
    [n,100000,'TPCC','HSTORE',m] for n,m in itertools.product([2,4,8,16],[1]+range(0,101,10))
]

experiments_10K = [
    [n,10000,'TPCC','HSTORE',m] for n,m in itertools.product([2,4,8,16],[1]+range(0,101,10))
]

experiments_n2 = [
    [2,10000,'TPCC','HSTORE',m] for m in [1] + range(0,101,10)
]
experiments_n4 = [
    [4,10000,'TPCC','HSTORE',m] for m in [1] + range(0,101,10)
]

# Default values for variable configurations
configs = {
    "NODE_CNT" : 2,
    "MAX_TXN_PER_PART" : 100,
    "WORKLOAD" : "TPCC",
    "CC_ALG" : "HSTORE",
    "MPR" : 0
}

##################
# FIXME
#################
experiments = experiments_10K
