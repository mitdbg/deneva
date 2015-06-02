import itertools
# Experiments to run and analyze
# Go to end of file to fill in experiments 

# Format: [#Nodes,#Txns,Workload,CC_ALG,MPR]
fmt_tpcc = [["NODE_CNT","MAX_TXN_PER_PART","WORKLOAD","CC_ALG","MPR","THREAD_CNT","NUM_WH","MAX_TXN_IN_FLIGHT"]]
fmt_nd = [["NODE_CNT","MAX_TXN_PER_PART","WORKLOAD","CC_ALG","MPR","THREAD_CNT","NUM_WH","MAX_TXN_IN_FLIGHT","NETWORK_DELAY"]]
fmt_ycsb = [["NODE_CNT","MAX_TXN_PER_PART","WORKLOAD","CC_ALG","MPR","THREAD_CNT","MAX_TXN_IN_FLIGHT","ZIPF_THETA","READ_PERC","WRITE_PERC"]]


#nnodes=[1,2,4,8,16,32]
nnodes=[2,4,8]
nmpr=[0,0.001,0.01,0.1]
#nmpr= range(0,6,1)
#nmpr=[1] + range(0,11,5)
nalgos=['MVCC','OCC']
#nalgos=['NO_WAIT','WAIT_DIE','TIMESTAMP','OCC','MVCC','HSTORE','HSTORE_SPEC']
nthreads=[1]
#nthreads=[1,2]
nwfs=[64]
ntifs=[8]
nzipf=[0.6]
nwr_perc=[0.5]
#ntifs=[1,4,8,16,32]
ntxn=1000000
nnet_delay=['100000UL']
#nnet_delay=['0UL','50000UL','100000UL','500000UL']
#nnet_delay=['0UL','50000UL','100000UL','500000UL','1000000UL','5000000UL']

simple = [

[2,10000,'YCSB','OCC',1.0,1,1,0.6,0.5,0.5],
[2,10000,'YCSB','MVCC',1.0,1,1,0.6,0.5,0.5],


#[2,10000,'TPCC','NO_WAIT',30,2,8,16]
]

experiments_nd = [
    [n,ntxn,'TPCC',cc,m,t,wf,tif,nd] for n,m,cc,t,wf,tif,nd in itertools.product(nnodes,nmpr,nalgos,nthreads,nwfs,ntifs,nnet_delay)
]

experiments = [
    [n,ntxn,'TPCC',cc,m,t,wf,tif] for n,m,cc,t,wf,tif in itertools.product(nnodes,nmpr,nalgos,nthreads,nwfs,ntifs)
]

experiments_ycsb = [
    [n,ntxn,'YCSB',cc,m,t,tif,z,1.0-wp,wp] for n,m,cc,t,tif,z,wp in itertools.product(nnodes,nmpr,nalgos,nthreads,ntifs,nzipf,nwr_perc)
]

def test():
    fmt = fmt_ycsb
    exp = [[2,10000,'YCSB','OCC',1.0,1,1,0.6,0.5,0.5],
    [2,10000,'YCSB','MVCC',1.0,1,1,0.6,0.5,0.5],
    ]
    return fmt[0],exp

# Performance: throughput vs. node count
# Vary: Node count, % writes
def experiment_1():
    fmt = fmt_ycsb
    nnodes = [1,2,4,8,16,32]
    nmpr=[0.01]
    nalgos=['TIMESTAMP','MVCC','HSTORE']
    #nalgos=['NO_WAIT','WAIT_DIE','TIMESTAMP','OCC','MVCC','HSTORE','HSTORE_SPEC']
    nthreads=[1]
    ntifs=[8]
    nzipf=[0.6]
    nwr_perc=[0.0,0.25,0.5]
    ntxn=1000000
    exp = [[n,ntxn,'YCSB',cc,m,t,tif,z,1.0-wp,wp] for n,m,cc,t,tif,z,wp in itertools.product(nnodes,nmpr,nalgos,nthreads,ntifs,nzipf,nwr_perc)]
    return fmt[0],exp

def experiment_1_plot(summary):
    from plot_helper import tput,abort_rate
    fmt = fmt_ycsb
    nnodes = [1,2,4,8,16,32]
    nmpr=[0.01]
    nalgos=['NO_WAIT','WAIT_DIE','TIMESTAMP','OCC','MVCC','HSTORE','HSTORE_SPEC']
    nthreads=[1]
    ntifs=[8]
    nzipf=[0.6]
    nwr_perc=[0.0,0.25,0.5]
    ntxn=1000000
    # x-axis: nodes; one plot for each wr %
    for wr in nwr_perc:
        _cfg_fmt = ["MPR","MAX_TXN_PER_PART","WORKLOAD","THREAD_CNT","MAX_TXN_IN_FLIGHT","ZIPF_THETA","READ_PERC","WRITE_PERC"]
        _cfg=[nmpr[0],ntxn,'YCSB',nthreads[0],ntifs[0],nzipf[0],1.0-wr,wr]
        _title="{} {}% Writes {} MPR".format('YCSB',wr*100,nmpr[0])
        tput(nnodes,nalgos,summary,cfg_fmt=_cfg_fmt,cfg=_cfg,xname="NODE_CNT",vname="CC_ALG",title=_title)
        abort_rate(nnodes,nalgos,summary,cfg_fmt=_cfg_fmt,cfg=_cfg,xname="NODE_CNT",vname="CC_ALG",title=_title)

# Performance: throughput vs. node count
# Vary: Node count, Contention
def experiment_2():
    fmt = fmt_ycsb
    nnodes = [1,2,4,8,16,32]
    nmpr=[0.01]
    nalgos=['TIMESTAMP','MVCC','HSTORE']
    #nalgos=['NO_WAIT','WAIT_DIE','TIMESTAMP','OCC','MVCC','HSTORE','HSTORE_SPEC']
    nthreads=[1]
    ntifs=[8]
    nzipf=[0.0,0.2,0.4,0.6,0.8]
    nwr_perc=[0.5]
    ntxn=1000000
    exp = [[n,ntxn,'YCSB',cc,m,t,tif,z,1.0-wp,wp] for n,m,cc,t,tif,z,wp in itertools.product(nnodes,nmpr,nalgos,nthreads,ntifs,nzipf,nwr_perc)]
    return fmt[0],exp

def experiment_2_plot(summary):
    from plot_helper import tput,abort_rate
    fmt = fmt_ycsb
    nnodes = [1,2,4,8,16,32]
    nmpr=[0.01]
    nalgos=['NO_WAIT','WAIT_DIE','TIMESTAMP','OCC','MVCC','HSTORE','HSTORE_SPEC']
    nthreads=[1]
    ntifs=[8]
    nzipf=[0.0,0.2,0.4,0.6,0.8]
    nwr_perc=[0.5]
    ntxn=1000000
    # x-axis: nodes; one plot for each wr %
    for z in nzipf:
        _cfg_fmt = ["MPR","MAX_TXN_PER_PART","WORKLOAD","THREAD_CNT","MAX_TXN_IN_FLIGHT","ZIPF_THETA","READ_PERC","WRITE_PERC"]
        _cfg=[nmpr[0],ntxn,'YCSB',nthreads[0],ntifs[0],z,1.0-nwr_perc[0],nwr_perc[0]]
        _title="{} {}% Writes {} MPR {} zipf".format('YCSB',nwr_perc[0]*100,nmpr[0],z)
        tput(nnodes,nalgos,summary,cfg_fmt=_cfg_fmt,cfg=_cfg,xname="NODE_CNT",vname="CC_ALG",title=_title)
        abort_rate(nnodes,nalgos,summary,cfg_fmt=_cfg_fmt,cfg=_cfg,xname="NODE_CNT",vname="CC_ALG",title=_title)


# Performance: throughput vs. node count
# Vary: Node count, % writes, mpr
def experiment_3():
    fmt = fmt_ycsb
    nnodes = [1,2,4,8,16,32]
    nmpr=[0,0.1,1]
    nalgos=['NO_WAIT','WAIT_DIE','TIMESTAMP','OCC','MVCC','HSTORE','HSTORE_SPEC']
    nthreads=[1]
    ntifs=[8]
    nzipf=[0.6]
    nwr_perc=[0.0,0.25,0.5]
    ntxn=1000000
    exp = [[n,ntxn,'YCSB',cc,m,t,tif,z,1.0-wp,wp] for n,m,cc,t,tif,z,wp in itertools.product(nnodes,nmpr,nalgos,nthreads,ntifs,nzipf,nwr_perc)]
    return fmt[0],exp

# Performance: throughput vs. node count
# Vary: Node count, txn in flight
def experiment_4():
    fmt = fmt_ycsb
    nnodes = [1,2,4,8,16,32]
    nmpr=[0.1]
    nalgos=['NO_WAIT','WAIT_DIE','TIMESTAMP','OCC','MVCC','HSTORE','HSTORE_SPEC']
    nthreads=[1]
    ntifs=[50,100,500,1000]
    nzipf=[0.6]
    nwr_perc=[0.5]
    ntxn=1000000
    exp = [[n,ntxn,'YCSB',cc,m,t,tif,z,1.0-wp,wp] for n,m,cc,t,tif,z,wp in itertools.product(nnodes,nmpr,nalgos,nthreads,ntifs,nzipf,nwr_perc)]
    return fmt[0],exp


experiment_map = {
    'test': test,
    'experiment_1': experiment_1,
    'experiment_2': experiment_2,
    'experiment_3': experiment_3,
    'experiment_4': experiment_4,
    'experiment_1_plot': experiment_1_plot,
}


# Default values for variable configurations
configs = {
    "NODE_CNT" : 2,
    "CLIENT_NODE_CNT" : 1,
    "CLIENT_THREAD_CNT" : 1,
    "CLIENT_REM_THREAD_CNT" : 1,
    "MAX_TXN_PER_PART" : 100,
    "WORKLOAD" : "TPCC",
    "CC_ALG" : "HSTORE",
    "MPR" : 0.0,
    "TPORT_TYPE":"\"ipc\"",
    "TPORT_TYPE_IPC":"true",
    "TPORT_PORT":"\"_.ipc\"",
    "REM_THREAD_CNT": 1,
    "THREAD_CNT": 1,
    "PART_CNT": 2,
    "NUM_WH": 2,
    "MAX_TXN_IN_FLIGHT": 1,
    "NETWORK_DELAY": '0UL',
#YCSB
    "READ_PERC":0.5,
    "WRITE_PERC":0.5,
    "ZIPF_THETA":0.6,
}

##################
# FIXME
#################
experiments = fmt_ycsb + experiments_ycsb
config_names = fmt_ycsb[0]
