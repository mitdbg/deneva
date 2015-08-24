import itertools
import math
# Experiments to run and analyze
# Go to end of file to fill in experiments 

# Format: [#Nodes,#Txns,Workload,CC_ALG,MPR]
fmt_tpcc = [["NODE_CNT","MAX_TXN_PER_PART","WORKLOAD","CC_ALG","MPR","THREAD_CNT","NUM_WH","MAX_TXN_IN_FLIGHT"]]
fmt_nd = [["NODE_CNT","MAX_TXN_PER_PART","WORKLOAD","CC_ALG","MPR","THREAD_CNT","NUM_WH","MAX_TXN_IN_FLIGHT","NETWORK_DELAY"]]
fmt_ycsb = [["CLIENT_NODE_CNT","NODE_CNT","MAX_TXN_PER_PART","WORKLOAD","CC_ALG","MPR","CLIENT_THREAD_CNT","THREAD_CNT","MAX_TXN_IN_FLIGHT","ZIPF_THETA","READ_PERC","WRITE_PERC","PART_PER_TXN","PART_CNT"]]
fmt_ycsb_plot = [["CLIENT_NODE_CNT","NODE_CNT","MAX_TXN_PER_PART","WORKLOAD","CC_ALG","MPR","CLIENT_THREAD_CNT","THREAD_CNT","MAX_TXN_IN_FLIGHT","ZIPF_THETA","READ_PERC","WRITE_PERC","PART_PER_TXN"]]
fmt_nt = [["NODE_CNT","CLIENT_NODE_CNT","NETWORK_TEST"]]


def test():
    fmt = fmt_ycsb
    nnodes = [4]
#    nnodes = [1,2,4,8]
    nmpr=[1,25]
#    nmpr=[25,50,75]
#    nmpr=[1,5,10,25,50,75,100]
    nalgos=['OCC']
#    nalgos=['TIMESTAMP','WAIT_DIE','MVCC','OCC','NO_WAIT']
#    nalgos=['WAIT_DIE','NO_WAIT','OCC','MVCC','TIMESTAMP','HSTORE','VLL']
    #nalgos=['WAIT_DIE','NO_WAIT','OCC','MVCC','HSTORE','HSTORE_SPEC','VLL','TIMESTAMP']
    nthreads=[2]
    ncthreads=[8]
    ntifs=[1000]
    nzipf=[0.6]
#    nzipf=[0.0]
    nwr_perc=[0.5]
#    nwr_perc=[0.0,0.05,0.2,0.5]
#    nwr_perc=[0.0,0.5]
    ntxn=[2000000]
    nparts = [2]
    exp = [[int(math.ceil(n)) if n > 2 else 1,n,txn,'YCSB',cc,m,ct,t,tif,z,1.0-wp,wp,p if p <= n else 1,n if cc!='HSTORE' and cc!='HSTORE_SPEC' else t*n] for n,ct,t,tif,z,wp,m,cc,p,txn in itertools.product(nnodes,ncthreads,nthreads,ntifs,nzipf,nwr_perc,nmpr,nalgos,nparts,ntxn)]
    return fmt[0],exp

def test_plot(summary,summary_client):
    from plot_helper import tput
    fmt,exp = test()
    fmt = ["CLIENT_NODE_CNT","MAX_TXN_PER_PART","WORKLOAD","MPR","CLIENT_THREAD_CNT","THREAD_CNT","MAX_TXN_IN_FLIGHT","ZIPF_THETA","READ_PERC","WRITE_PERC","PART_PER_TXN"]
    x_name = "NODE_CNT"
    v_name = "CC_ALG"
    x_vals = [1,2,4,8]
    nmpr=[1,5,10,25,50,75,100]
    nwr_perc=[0.5]
    nalgos=['TIMESTAMP','WAIT_DIE','MVCC','OCC','NO_WAIT']
    v_vals=nalgos
    for mpr,wr in itertools.product(nmpr,nwr_perc):
        c = [1,2000000,"YCSB",mpr,8,2,1000,0.6,1.0-wr,wr,2]
        title = "YCSB System Throughput {}% Writes, {}% Multi-part rate".format(wr*100,mpr);
        tput(x_vals,v_vals,summary,cfg_fmt=fmt,cfg=c,xname=x_name,vname=v_name,title=title)

    fmt = ["CLIENT_NODE_CNT","MAX_TXN_PER_PART","WORKLOAD","CC_ALG","CLIENT_THREAD_CNT","THREAD_CNT","MAX_TXN_IN_FLIGHT","ZIPF_THETA","READ_PERC","WRITE_PERC","PART_PER_TXN"]
    v_vals = nmpr
    v_name = "MPR"
    for a in nalgos: 
        wr = 0.5
        c = [1,2000000,"YCSB",a,8,2,1000,0.6,1.0-wr,wr,2]
        title = "YCSB System Throughput {}% Writes, {}".format(wr*100,a);
        tput(x_vals,v_vals,summary,cfg_fmt=fmt,cfg=c,xname=x_name,vname=v_name,title=title)

# Performance: throughput vs. node count
# Vary: Node count, % writes
def experiment_1():
    fmt = fmt_ycsb
    nnodes = [2]
    nmpr=[0,1,5,10,25]#,50,75,100]
    nalgos=['HSTORE_SPEC']
    #nalgos=['WAIT_DIE','NO_WAIT','OCC','MVCC','HSTORE','HSTORE_SPEC','VLL','TIMESTAMP']
    nthreads=[1]
    ncthreads=[4]
    ntifs=[1000]
    nzipf=[0.0]
    nwr_perc=[0.5]
    ntxn=2000000
    nparts = [2]
    exp = [[int(math.ceil(n/2)) if n > 1 else 1,n,ntxn,'YCSB',cc,m,ct,t,tif,z,1.0-wp,wp,p if p <= n else 1] for n,ct,t,tif,z,wp,m,cc,p in itertools.product(nnodes,ncthreads,nthreads,ntifs,nzipf,nwr_perc,nmpr,nalgos,nparts)]
    return fmt[0],exp

def experiment_1_plot(summary,summary_client):
    from plot_helper import tput_plotter
    fmt,exp = experiment_1()
    x_name = "NODE_CNT"
    v_name = "CC_ALG"
    title = "Experiment_1"
    tput_plotter(title,x_name,v_name,fmt,exp,summary,summary_client);



def partition_sweep():
    fmt = fmt_ycsb
    nnodes = [1,2,4,8]
    nmpr=[1]
    nalgos=['WAIT_DIE','NO_WAIT','OCC','MVCC','TIMESTAMP','HSTORE','HSTORE_SPEC']
    #nalgos=['WAIT_DIE','NO_WAIT','OCC','MVCC','HSTORE','HSTORE_SPEC','VLL','TIMESTAMP']
    nthreads=[2]
    ncthreads=[4]
    ntifs=[1000]
    nzipf=[0.6]
    nwr_perc=[0.5]
    ntxn=2000000
    exp = []
    for node in nnodes:
        nparts = range(2,node,2)
        tmp = [[int(math.ceil(n/2)) if n > 1 else 1,n,ntxn,'YCSB',cc,m,ct,t if cc!="HSTORE" and cc!= "HSTORE_SPEC" else 1,tif,z,1.0-wp,wp,p if p <= n else 1] for n,ct,t,tif,z,wp,m,cc,p in itertools.product(nnodes,ncthreads,nthreads,ntifs,nzipf,nwr_perc,nmpr,nalgos,nparts)]
        exp = exp + tmp
    return fmt[0],exp

def partition_sweep_plot(summary,summary_client):
    from plot_helper import tput_plotter
    fmt,exp = partition_sweep()
    x_name = "NODE_CNT"
    v_name = "CC_ALG"
    x_idx = fmt.index(x_name)
    v_idx = fmt.index(v_name)
    _cfg_fmt = fmt.remove(x_name).remove(v_name)
    _cfg = exp
    x_vals = []
    v_vals = []
    for p in _cfg:
        x_vals.append(p[x_idx])
        del p[x_idx]
        v_vals.append(p[v_idx])
        del p[v_idx]
    x_vals = sorted(list(set(x_vals)))
    v_vals = sorted(list(set(v_vals)))
    tput(x_vals,v_vals,summary,cfg_fmt=_cfg_fmt,cfg=_cfg,xname=x_name,vname=v_name,title=_title)

def node_sweep():
    fmt = fmt_ycsb
#    nnodes = [8]
    nnodes = [16,8,4,2,1]
#    nmpr=[5]
    nmpr=[0,1,5]
#    nalgos=['VLL']
    nalgos=['WAIT_DIE','NO_WAIT','OCC','MVCC','TIMESTAMP','HSTORE','VLL']
    #nalgos=['WAIT_DIE','NO_WAIT','OCC','MVCC','HSTORE','HSTORE_SPEC','VLL','TIMESTAMP']
    nthreads=[2]
    ncthreads=[4]
    ntifs=[1000]
    nzipf=[0.0]
#    nwr_perc=[0.5]
    nwr_perc=[0.0,0.5]
    ntxn=[3000000]
    nparts = [2]
    exp = [[int(math.ceil(n/2)) if n > 1 else 1,n,txn,'YCSB',cc,m,ct,t,tif,z,1.0-wp,wp,p if p <= n else 1,n if cc!='HSTORE' and cc!='HSTORE_SPEC' else t*n] for n,ct,t,tif,z,wp,m,cc,p,txn in itertools.product(nnodes,ncthreads,nthreads,ntifs,nzipf,nwr_perc,nmpr,nalgos,nparts,ntxn)]
    return fmt[0],exp

def node_sweep_plot(summary,summary_client):
    from plot_helper import tput_plotter
    fmt,exp = node_sweep()
    x_name = "NODE_CNT"
    v_name = "CC_ALG"
    title = "Scalability"
    tput_plotter(title,x_name,v_name,fmt,exp,summary,summary_client);

def node_sweep_plot2(summary,summary_client):
    from plot_helper import tput
    fmt,exp = node_sweep()
    fmt = ["CLIENT_NODE_CNT","MAX_TXN_PER_PART","WORKLOAD","MPR","CLIENT_THREAD_CNT","THREAD_CNT","MAX_TXN_IN_FLIGHT","ZIPF_THETA","READ_PERC","WRITE_PERC","PART_PER_TXN"]
    x_name = "NODE_CNT"
    v_name = "CC_ALG"
    x_vals = [1,2,4,8,16]
    v_vals = ['WAIT_DIE','NO_WAIT','MVCC','TIMESTAMP','HSTORE','VLL']
    for mpr,wr in itertools.product([0,1,5],[0.0,0.5]):
        c = [1,3000000,"YCSB",mpr,4,2,1000,0.0,1.0-wr,wr,2]
        title = "YCSB System Throughput {}% Writes, {}% Multi-part rate".format(wr*100,mpr);
        tput(x_vals,v_vals,summary,cfg_fmt=fmt,cfg=c,xname=x_name,vname=v_name,title=title)

def mpr_sweep():
    fmt = fmt_ycsb
    nnodes = [1,2,4,8,16]
    #nmpr=[0,1,5,10,20,30,40,50,60,70,80,90,100]
    nmpr=[0,1,5,10,25,50,75,100]
    nalgos=['WAIT_DIE','NO_WAIT','OCC','MVCC','TIMESTAMP','VLL']
    #nalgos=['WAIT_DIE','NO_WAIT','OCC','MVCC','HSTORE','HSTORE_SPEC','VLL','TIMESTAMP']
    nthreads=[2]
    ncthreads=[4]
    ntifs=[1000]
    nzipf=[0.0]
    nwr_perc=[0.0,0.5]
    ntxn=3000000
    nparts = [2]
    exp = [[int(math.ceil(n/2)) if n > 1 else 1,n,ntxn,'YCSB',cc,m,ct,t if cc!="HSTORE" and cc!= "HSTORE_SPEC" else 1,tif,z,1.0-wp,wp,p if p <= n else 1] for n,ct,t,tif,z,wp,m,cc,p in itertools.product(nnodes,ncthreads,nthreads,ntifs,nzipf,nwr_perc,nmpr,nalgos,nparts)]
    return fmt[0],exp

def mpr_sweep_plot(summary,summary_client):
    from plot_helper import tput_plotter
    fmt,exp = mpr_sweep()
    x_name = "MPR"
    v_name = "CC_ALG"
    title = "MPR Sweep"
    tput_plotter(title,x_name,v_name,fmt,exp,summary,summary_client);
    fmt,exp = mpr_sweep()
    x_name = "NODE_CNT"
    v_name = "CC_ALG"
    title = "Node Sweep"
    tput_plotter(title,x_name,v_name,fmt,exp,summary,summary_client);

def tif_sweep():
    fmt = fmt_ycsb
    nnodes = [1,2,4,8]
    nmpr=[1]
    nalgos=['WAIT_DIE','NO_WAIT','OCC','MVCC','TIMESTAMP','HSTORE','HSTORE_SPEC']
    #nalgos=['WAIT_DIE','NO_WAIT','OCC','MVCC','HSTORE','HSTORE_SPEC','VLL','TIMESTAMP']
    nthreads=[2]
    ncthreads=[4]
    ntifs=[1,2,10,35,100,1000]
    nzipf=[0.6]
    nwr_perc=[0.5]
    ntxn=3000000
    nparts = [2]
    exp = [[int(math.ceil(n/2)) if n > 1 else 1,n,ntxn,'YCSB',cc,m,ct,t if cc!="HSTORE" and cc!= "HSTORE_SPEC" else 1,tif,z,1.0-wp,wp,p if p <= n else 1] for n,ct,t,tif,z,wp,m,cc,p in itertools.product(nnodes,ncthreads,nthreads,ntifs,nzipf,nwr_perc,nmpr,nalgos,nparts)]
    return fmt[0],exp

def write_ratio_sweep():
    fmt = fmt_ycsb
    nnodes = [1,2,4,8]
    nmpr=[1]
    nalgos=['WAIT_DIE','NO_WAIT','OCC','MVCC','TIMESTAMP','HSTORE','HSTORE_SPEC']
    #nalgos=['WAIT_DIE','NO_WAIT','OCC','MVCC','HSTORE','HSTORE_SPEC','VLL','TIMESTAMP']
    nthreads=[2]
    ncthreads=[4]
    ntifs=[1000]
    nzipf=[0.6]
    nwr_perc=[0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0]
    ntxn=3000000
    nparts = [2]
    exp = [[int(math.ceil(n/2)) if n > 1 else 1,n,ntxn,'YCSB',cc,m,ct,t if cc!="HSTORE" and cc!= "HSTORE_SPEC" else 1,tif,z,1.0-wp,wp,p if p <= n else 1] for n,ct,t,tif,z,wp,m,cc,p in itertools.product(nnodes,ncthreads,nthreads,ntifs,nzipf,nwr_perc,nmpr,nalgos,nparts)]
    return fmt[0],exp

def skew_sweep():
    fmt = fmt_ycsb
    nnodes = [1,2,4,8]
    nmpr=[1]
    nalgos=['WAIT_DIE','NO_WAIT','OCC','MVCC','TIMESTAMP','HSTORE','HSTORE_SPEC']
    #nalgos=['WAIT_DIE','NO_WAIT','OCC','MVCC','HSTORE','HSTORE_SPEC','VLL','TIMESTAMP']
    nthreads=[2]
    ncthreads=[4]
    ntifs=[1000]
    nzipf=[0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0]
    nwr_perc=[0.5]
    ntxn=3000000
    nparts = [2]
    exp = [[int(math.ceil(n/2)) if n > 1 else 1,n,ntxn,'YCSB',cc,m,ct,t if cc!="HSTORE" and cc!= "HSTORE_SPEC" else 1,tif,z,1.0-wp,wp,p if p <= n else 1] for n,ct,t,tif,z,wp,m,cc,p in itertools.product(nnodes,ncthreads,nthreads,ntifs,nzipf,nwr_perc,nmpr,nalgos,nparts)]
    return fmt[0],exp

#Should do this one on an ISTC machine
def network_sweep():
    fmt = [fmt_ycsb[0] + ["NETWORK_DELAY"]]
    nnodes = [1,2,4,8]
    nmpr=[1]
    nalgos=['WAIT_DIE','NO_WAIT','OCC','MVCC','TIMESTAMP','HSTORE','HSTORE_SPEC']
    #nalgos=['WAIT_DIE','NO_WAIT','OCC','MVCC','HSTORE','HSTORE_SPEC','VLL','TIMESTAMP']
    nthreads=[2]
    ncthreads=[4]
    ntifs=[1000]
    nzipf=[0.6]
    nwr_perc=[0.5]
    nparts=[2]
    network_delay = ["0UL","10000UL","100000UL","1000000UL","10000000UL","100000000UL"]
    ntxn=3000000
    exp = [[int(math.ceil(n/2)) if n > 1 else 1,n,ntxn,'YCSB',cc,m,ct,t if cc!="HSTORE" and cc!= "HSTORE_SPEC" else 1,tif,z,1.0-wp,wp,p if p <= n else 1,nd] for n,ct,t,tif,z,wp,m,cc,p,nd in itertools.product(nnodes,ncthreads,nthreads,ntifs,nzipf,nwr_perc,nmpr,nalgos,nparts,network_delay)]
    return fmt[0],exp

def tpcc_sweep():
    fmt = fmt_tpcc
    nnodes = [1,2,4,8,16]
    nmpr=[1]
    nalgos=['WAIT_DIE']
    #nalgos=['WAIT_DIE','NO_WAIT','OCC','MVCC','HSTORE','HSTORE_SPEC','VLL','TIMESTAMP']
    nthreads=[1]
    ncthreads=[4]
    ntifs=[1000]
    nzipf=[0.6]
    nwr_perc=[0.5]
    ntxn=3000000
    exp = [[int(math.ceil(n/2)) if n > 1 else 1,n,ntxn,'YCSB',cc,m,ct,t,tif,z,1.0-wp,wp] for n,ct,t,tif,z,wp,m,cc in itertools.product(nnodes,ncthreads,nthreads,ntifs,nzipf,nwr_perc,nmpr,nalgos)]
    return fmt[0],exp



def network_experiment():
    fmt = fmt_nt
    nnodes = [1]
    cnodes = [1]
    ntest = ["true"]
    exp = [nnodes,cnodes,ntest]
    exp = [[n,c,t] for n,c,t in itertools.product(nnodes,cnodes,ntest)]
    return fmt[0],exp

def network_experiment_plot(all_exps,all_nodes,timestamps):
    from plot_helper import lat_node_tbls,lat_tbl
    fmt = fmt_nt
    nnodes = [1]
    cnodes = [1]
    ntest = ["true"]
    rexp = [nnodes,cnodes,ntest]
    rexp = [[n,c,t] for n,c,t in itertools.product(nnodes,cnodes,ntest)]
    for i,exp in enumerate(all_exps):
        lat_node_tbls(exp[:-1],all_nodes[i],exp[0].keys(),timestamps[i])
        lat_tbl(exp[-1],exp[-1].keys(),timestamps[i])

def abort_penalty_sweep():
    fmt = [fmt_ycsb[0] + ["ABORT_PENALTY"]]
    global config_names
    config_names = fmt[0]
    nnodes = [1,2,4]#,8,16]
    nmpr=[1]
    nalgos=['WAIT_DIE']
    #nalgos=['WAIT_DIE','NO_WAIT','OCC','MVCC','HSTORE','HSTORE_SPEC','VLL','TIMESTAMP']
    nthreads=[1,2,4]
    #nthreads=[4]
    ncthreads=[4]
    ntifs=[1000]
    nzipf=[0.6]
    nwr_perc=[0.5]
    nparts=[2]
    abt_pen = ["1000000UL","5000000UL","1000000000UL","2000000000UL","4000000000UL","8000000000UL"]
    ntxn=2000000
    exp = [[int(math.ceil(n/2)) if n > 1 else 1,n,ntxn,'YCSB',cc,m,ct,t,tif,z,1.0-wp,wp,p if p <= n else 1,ap] for n,ct,t,tif,z,wp,m,cc,p,ap in itertools.product(nnodes,ncthreads,nthreads,ntifs,nzipf,nwr_perc,nmpr,nalgos,nparts,abt_pen)]
    return fmt[0],exp

def abort_penalty_sweep_plot(summary,summary_client):
    from plot_helper import tput_plotter
    fmt,exp = abort_penalty_sweep()
    x_name = "ABORT_PENALTY"
    v_name = "CC_ALG"
    title = "Abort Penalty Sweep"
    tput_plotter(title,x_name,v_name,fmt,exp,summary,summary_client);


experiment_map = {
    'test': test,
    'test_plot': test_plot,
    'experiment_1': experiment_1,
    'experiment_1_plot': experiment_1_plot,
    'partition_sweep': partition_sweep,
    'node_sweep': node_sweep,
    'mpr_sweep': mpr_sweep,
    'tif_sweep': tif_sweep,
    'write_ratio_sweep': write_ratio_sweep,
    'skew_sweep': skew_sweep,
    'network_sweep': network_sweep,
    'tpcc_sweep': tpcc_sweep,
    'network_experiment' : network_experiment,
    'abort_penalty_sweep': abort_penalty_sweep,
    'network_experiment_plot' : network_experiment_plot,
    'partition_sweep_plot': partition_sweep_plot,
    'mpr_sweep_plot': mpr_sweep_plot,
    'node_sweep_plot': node_sweep_plot2,
#    'node_sweep_plot': node_sweep_plot,
    'abort_penalty_sweep_plot': abort_penalty_sweep_plot,
}


# Default values for variable configurations
configs = {
    "NODE_CNT" : 2,
    "CLIENT_NODE_CNT" : 1,
    "CLIENT_THREAD_CNT" : 2,
    "CLIENT_REM_THREAD_CNT" : 1,
    "MAX_TXN_PER_PART" : 100,
    "WORKLOAD" : "YCSB",
    "CC_ALG" : "NO_WAIT",
    "MPR" : 0.0,
    "TPORT_TYPE":"\"ipc\"",
    "TPORT_TYPE_IPC":"true",
    "TPORT_PORT":"\"_.ipc\"",
    "REM_THREAD_CNT": 1,
    "THREAD_CNT": 1,
    "PART_CNT": "NODE_CNT", #2,
    "NUM_WH": 2,
    "MAX_TXN_IN_FLIGHT": 1,
    "NETWORK_DELAY": '0UL',
    "DONE_TIMER": "3 * 60 * BILLION // 3 minutes",
    "PROG_TIMER" : "30 * BILLION // in s",
    "NETWORK_TEST" : "false",
    "ABORT_PENALTY": "1 * 1000000UL   // in ns.",
    "PRT_LAT_DISTR": "false", #"true",
#YCSB
    "INIT_PARALLELISM" : 4, 
    "READ_PERC":0.5,
    "WRITE_PERC":0.5,
    "ZIPF_THETA":0.6,
    "PART_PER_TXN": 1,
    "SYNTH_TABLE_SIZE":"2097152",
    "LOAD_TXN_FILE":"false"
}

config_names = fmt_ycsb[0]
