import itertools
# Experiments to run and analyze
# Go to end of file to fill in experiments 

# Format: [#Nodes,#Txns,Workload,CC_ALG,MPR]
fmt1 = [["NODE_CNT","MAX_TXN_PER_PART","WORKLOAD","CC_ALG","MPR"]]
fmt2 = [["NODE_CNT","MAX_TXN_PER_PART","WORKLOAD","CC_ALG","MPR","THREAD_CNT"]]
fmt3 = [["NODE_CNT","MAX_TXN_PER_PART","WORKLOAD","CC_ALG","MPR","THREAD_CNT","REM_THREAD_CNT"]]
fmt4 = [["NODE_CNT","MAX_TXN_PER_PART","WORKLOAD","CC_ALG","MPR","THREAD_CNT","NUM_WH"]]
fmt5 = [["NODE_CNT","MAX_TXN_PER_PART","WORKLOAD","CC_ALG","MPR","THREAD_CNT","NUM_WH","MAX_TXN_IN_FLIGHT"]]
fmt6 = [["NODE_CNT","MAX_TXN_PER_PART","WORKLOAD","CC_ALG","MPR","THREAD_CNT","NUM_WH","MAX_TXN_IN_FLIGHT","NETWORK_DELAY"]]



#nnodes=[1]
nnodes=[4,9,13]
#nmpr= [1,5,10]
nmpr=[1] + range(0,11,5)
#nalgos=['TIMESTAMP']
nalgos=['NO_WAIT','WAIT_DIE','TIMESTAMP','OCC','MVCC','HSTORE']
nthreads=[1]
#nthreads=[1,2]
nwfs=[64]
ntifs=[1,4]
#ntifs=[1,2,4,8]
#nnet_delay=['0UL','50000UL']
#nnet_delay=['0UL','50000UL','100000UL','500000UL']
ntxn=1000000
nnet_delay=['0UL','50000UL','100000UL','500000UL','1000000UL','5000000UL']

simple = [

[4,1000,'TPCC','HSTORE',5,1,64,1],


#[2,10000,'TPCC','NO_WAIT',30,2,8,16]
]

experiments_nd = [
    [n,ntxn,'TPCC',cc,m,t,wf,tif,nd] for n,m,cc,t,wf,tif,nd in itertools.product(nnodes,nmpr,nalgos,nthreads,nwfs,ntifs,nnet_delay)
]

experiments = [
    [n,ntxn,'TPCC',cc,m,t,wf,tif] for n,m,cc,t,wf,tif in itertools.product(nnodes,nmpr,nalgos,nthreads,nwfs,ntifs)
]

experiments_100K = [
    [n,100000,'TPCC','HSTORE',m] for n,m in itertools.product([2,4,8,16],[1]+range(0,101,10))
]

experiments_10K_1node = [
    [n,10000,'TPCC',cc,m,t,wh] for n,m,cc,t,wh in itertools.product([1],[1]+range(0,21,10),['HSTORE','NO_WAIT','WAIT_DIE','TIMESTAMP','MVCC','OCC'],[1,2,4],[4])
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
    [n,10000,'TPCC',cc,m,t,wf*n,tif] for n,m,cc,t,wf,tif in itertools.product([1,2,4],[1]+range(0,51,10),['WAIT_DIE'],[1,2],[1,2,4],[1,2,4,8,32,64])
]
experiments_10K_no_wait_mt = [
    [n,10000,'TPCC',cc,m,t,wf*n,tif] for n,m,cc,t,wf,tif in itertools.product([1,2,4],[1]+range(0,51,10),['NO_WAIT'],[1,2],[1,2,4],[1,2,4,8,32,64])
]
experiments_10K_2pl_mt = experiments_10K_no_wait_mt + experiments_10K_wait_die_mt

experiments_10K_hstore_mt = [
    [n,10000,'TPCC',cc,m,t] for n,m,cc,t in itertools.product([2,4],[1]+range(0,51,10),['HSTORE'],[1,2,4])
]

experiments_10K_tso_mt = [
    [n,10000,'TPCC',cc,m,t,wf*n,tif] for n,m,cc,t,wf,tif in itertools.product([1,2,4],[1]+range(0,51,10),['TIMESTAMP'],[1,2],[1,2,4],[1,2,4,8,32,64])
]

experiments_10K_mvcc_mt = [
    [n,10000,'TPCC',cc,m,t,wf*n,tif] for n,m,cc,t,wf,tif in itertools.product([1,2,4],[1]+range(0,51,10),['MVCC'],[1,2],[1,2,4],[1,2,4,8,32,64])
]

experiments_10K_occ_mt = [
    [n,10000,'TPCC',cc,m,t] for n,m,cc,t in itertools.product([2,4],[1]+range(0,51,10),['OCC'],[1,2,4])
]

experiments_10K_all_mt = experiments_10K_2pl_mt + experiments_10K_tso_mt + experiments_10K_mvcc_mt #+ experiments_10K_hstore_mt + experiments_10K_occ_mt

experiments_10K_wh = [
    [n,10000,'TPCC',cc,m,t,n*t*wh] for n,m,cc,t,wh in itertools.product([2],[1]+range(0,51,10),['HSTORE','NO_WAIT','WAIT_DIE','TIMESTAMP','MVCC','OCC'],[1],[1,2,3,4,5])
]

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
    "REM_THREAD_CNT": 1,
    "THREAD_CNT": 1,
    "PART_CNT": 2,
    "NUM_WH": 2,
    "MAX_TXN_IN_FLIGHT": 1,
    "NETWORK_DELAY": '0UL'
}

##################
# FIXME
#################
experiments = fmt5 + simple #experiments
config_names = fmt5[0]
