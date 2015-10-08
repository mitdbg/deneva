import itertools
import math
# Experiments to run and analyze
# Go to end of file to fill in experiments 

# Format: [#Nodes,#Txns,Workload,CC_ALG,MPR]
fmt_tpcc = [["NODE_CNT","MAX_TXN_PER_PART","WORKLOAD","CC_ALG","MPR","THREAD_CNT","NUM_WH","MAX_TXN_IN_FLIGHT"]]
fmt_nd = [["NODE_CNT","MAX_TXN_PER_PART","WORKLOAD","CC_ALG","MPR","THREAD_CNT","NUM_WH","MAX_TXN_IN_FLIGHT","NETWORK_DELAY"]]
fmt_ycsb = [["CLIENT_NODE_CNT","NODE_CNT","MAX_TXN_PER_PART","WORKLOAD","CC_ALG","MPR","CLIENT_THREAD_CNT","CLIENT_REM_THREAD_CNT","CLIENT_SEND_THREAD_CNT","THREAD_CNT","REM_THREAD_CNT","SEND_THREAD_CNT","MAX_TXN_IN_FLIGHT","ZIPF_THETA","READ_PERC","WRITE_PERC","PART_PER_TXN","PART_CNT","MSG_TIME_LIMIT","MSG_SIZE_MAX","MODE"]]
fmt_nt = [["NODE_CNT","CLIENT_NODE_CNT","NETWORK_TEST"]]
fmt_title=["NODE_CNT","MPR","ZIPF_THETA","WRITE_PERC","CC_ALG","MAX_TXN_IN_FLIGHT","MODE"]

def test():
    fmt = fmt_ycsb
#    nnodes = [4]
    nnodes = [1,2,4,8]
    nmpr=[100]
    nalgos=['NO_WAIT']
#    nalgos=['NO_WAIT','WAIT_DIE']
    nthreads=[2]
    nrthreads=[1]
    nsthreads=[1]
    ncthreads=[2]
    ncrthreads=[1]
    ncsthreads=[4]
    ntifs=[1500]
#    ntifs=[1500,5000,7500]
    nzipf=[0.0]
#    nwr_perc=[0.0]
    nwr_perc=[0.0,1.0]
    ntxn=[1000000]
#    ntxn=[5000000]
    nbtime=[1000] # in ns
    nbsize=[4096]
    nparts = [8]
    nmodes = ["NORMAL_MODE","NOCC_MODE","QRY_ONLY_MODE"]#,"SETUP_MODE","SIMPLE_MODE"]
#    nmodes = ["NOCC_MODE"]
    exp = [[n,n,txn,'YCSB',cc,m,ct,crt,cst,t,rt,st,tif,z,1.0-wp,wp,p if p <= n else n,n if cc!='HSTORE' and cc!='HSTORE_SPEC' else t*n,str(mt) + '*1000UL',bsize,mode] for n,ct,crt,cst,t,rt,st,tif,z,wp,m,cc,p,txn,mt,bsize,mode in itertools.product(nnodes,ncthreads,ncrthreads,ncsthreads,nthreads,nrthreads,nsthreads,ntifs,nzipf,nwr_perc,nmpr,nalgos,nparts,ntxn,nbtime,nbsize,nmodes)]
    return fmt[0],exp

def tputvlat_setup(summary,summary_cl,nfmt,nexp,x_name,v_name):
    from plot_helper import tput_v_lat
    x_vals = []
    v_vals = []
    exp = [list(e) for e in nexp]
    fmt = list(nfmt)
    print(x_name)
    print(v_name)
    for e in exp:
        x_vals.append(e[fmt.index(x_name)])
        del e[fmt.index(x_name)]
    fmt.remove(x_name)
    for e in exp:
        v_vals.append(e[fmt.index(v_name)])
        del e[fmt.index(v_name)]
    fmt.remove(v_name)
    x_vals = list(set(x_vals))
    x_vals.sort()
    v_vals = list(set(v_vals))
    v_vals.sort()
    exp.sort()
    exp = list(k for k,_ in itertools.groupby(exp))
    for e in exp:
        title = "Tput vs Lat "
        for t in fmt_title:
            if t not in fmt: continue
            title+="{} {}, ".format(t,e[fmt.index(t)])
        tput_v_lat(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=e,xname=x_name,vname=v_name,title=title)


def tput_setup(summary,summary_cl,nfmt,nexp,x_name,v_name
        ,extras={'PART_CNT':'NODE_CNT','PART_PER_TXN':'NODE_CNT','CLIENT_NODE_CNT':'NODE_CNT'}
        ):
    from plot_helper import tput
    x_vals = []
    v_vals = []
    exp = [list(e) for e in nexp]
    fmt = list(nfmt)
    print(x_name)
    print(v_name)
    for e in exp:
        x_vals.append(e[fmt.index(x_name)])
        del e[fmt.index(x_name)]
    fmt.remove(x_name)
    for e in exp:
        v_vals.append(e[fmt.index(v_name)])
        del e[fmt.index(v_name)]
    fmt.remove(v_name)
    for x in extras.keys():
        for e in exp:
            del e[fmt.index(x)]
        fmt.remove(x)
    x_vals = list(set(x_vals))
    x_vals.sort()
    v_vals = list(set(v_vals))
    v_vals.sort()
    exp.sort()
    exp = list(k for k,_ in itertools.groupby(exp))
    for e in exp:
        title = "System Tput "
        for t in fmt_title:
            if t not in fmt: continue
            title+="{} {}, ".format(t,e[fmt.index(t)])
        tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=e,xname=x_name,vname=v_name,title=title,extras=extras)

def line_setup(summary,summary_cl,nfmt,nexp,x_name,v_name,key
        ,extras={'PART_CNT':'NODE_CNT','PART_PER_TXN':'NODE_CNT','CLIENT_NODE_CNT':'NODE_CNT'}
        ):
    from plot_helper import line_general
    x_vals = []
    v_vals = []
    exp = [list(e) for e in nexp]
    fmt = list(nfmt)
    print(x_name)
    print(v_name)
    for e in exp:
        x_vals.append(e[fmt.index(x_name)])
        del e[fmt.index(x_name)]
    fmt.remove(x_name)
    for e in exp:
        v_vals.append(e[fmt.index(v_name)])
        del e[fmt.index(v_name)]
    fmt.remove(v_name)
    x_vals = list(set(x_vals))
    x_vals.sort()
    v_vals = list(set(v_vals))
    v_vals.sort()
    exp.sort()
    exp = list(k for k,_ in itertools.groupby(exp))
    for e in exp:
        title = key 
        for t in fmt_title:
            if t not in fmt: continue
            title+="{} {}, ".format(t,e[fmt.index(t)])
        line_general(x_vals,v_vals,summary,summary_cl,key,cfg_fmt=fmt,cfg=e,xname=x_name,vname=v_name,title=title,extras=extras)
        
def stacks_setup(summary,nfmt,nexp,x_name,keys,key_names=[],norm=False
        ,extras={'PART_CNT':'NODE_CNT','PART_PER_TXN':'NODE_CNT','CLIENT_NODE_CNT':'NODE_CNT'}
        ):
    from plot_helper import stacks_general
    x_vals = []
    exp = [list(e) for e in nexp]
    fmt = list(nfmt)
    print(x_name)
    print(keys)
    for e in exp:
        x_vals.append(e[fmt.index(x_name)])
        del e[fmt.index(x_name)]
    fmt.remove(x_name)
    x_vals = list(set(x_vals))
    x_vals.sort()
    exp.sort()
    exp = list(k for k,_ in itertools.groupby(exp))
    for e in exp:
        title = "Stacks "
        for t in fmt_title:
            if t not in fmt: continue
            title+="{} {}, ".format(t,e[fmt.index(t)])
        stacks_general(x_vals,summary,keys,xname=x_name,key_names=list(key_names),title=title,cfg_fmt=fmt,cfg=e,extras=extras)


def ft_mode_plot(summary,summary_client):
    nfmt,nexp = ft_mode()
    x_name = "MAX_TXN_IN_FLIGHT"
    v_name = "CC_ALG"
    tput_setup(summary,nfmt,nexp,x_name,v_name)

def test_plot(summary,summary_client):
    nfmt,nexp = test()
#    tput_setup(summary,summary_client,nfmt,nexp,x_name="MAX_TXN_IN_FLIGHT",v_name="MODE")
#    tputvlat_setup(summary,summary_client,nfmt,nexp,x_name="NODE_CNT",v_name="MODE")
#    tput_setup(summary,summary_client,nfmt,nexp,x_name="NODE_CNT",v_name="MODE")

#    stacks_setup(summary,nfmt,nexp,x_name="MAX_TXN_IN_FLIGHT",keys=['thd1','thd2','thd3'],norm=False)
#    stacks_setup(summary,nfmt,nexp,x_name="MAX_TXN_IN_FLIGHT",keys=['txn_table_add','txn_table_get','txn_table0a','txn_table1a','txn_table0b','txn_table1b','txn_table2a','txn_table2'],norm=False)
#    stacks_setup(summary,nfmt,nexp,x_name="MAX_TXN_IN_FLIGHT"            ,keys=['type1','type2','type3','type4','type5','type6','type7','type8','type9','type10','type11','type12','type13','type14']            ,norm=False)
#    stacks_setup(summary,nfmt,nexp,x_name="MAX_TXN_IN_FLIGHT",keys=['thd1a','thd1b','thd1c','thd1d'],norm=False)
#    stacks_setup(summary,nfmt,nexp,x_name="MAX_TXN_IN_FLIGHT",keys=['rtxn1a','rtxn1b','rtxn2','rtxn3','rtxn4'],norm=False)

#    stacks_setup(summary,nfmt,nexp,x_name="MODE",keys=['thd1','thd2','thd3'],norm=False,key_names=['Getting work','Execution','Wrap-up'])
#    stacks_setup(summary,nfmt,nexp,x_name="MODE",keys=['txn_table_add','txn_table_get','txn_table0a','txn_table1a','txn_table0b','txn_table1b','txn_table2a'],norm=False)
#    stacks_setup(summary,nfmt,nexp,x_name="MODE"
#            ,keys=['type1','type2','type3','type4','type5','type6','type7','type8','type9','type10','type11','type12','type13','type14']
#            ,norm=False)
#    stacks_setup(summary,nfmt,nexp,x_name="MODE",keys=['thd1a','thd1b','thd1c','thd1d'],norm=False)
#    stacks_setup(summary,nfmt,nexp,x_name="MODE",keys=['rtxn1a','rtxn1b','rtxn2','rtxn3','rtxn4'],norm=False)
#    stacks_setup(summary,nfmt,nexp,x_name="MODE",keys=['row1','row2','row3'],norm=False)

#    line_setup(summary,summary_client,nfmt,nexp,x_name="MAX_TXN_IN_FLIGHT",v_name="MODE",key='cpu_ttl')

    stacks_setup(summary,nfmt,nexp,x_name="NODE_CNT",keys=['thd1','thd2','thd3'],norm=False,key_names=['Prep work','Execution','Wrap-up'])
#    stacks_setup(summary,nfmt,nexp,x_name="NODE_CNT",keys=['txn_table_add','txn_table_get','txn_table0a','txn_table1a','txn_table0b','txn_table1b','txn_table2a'],norm=False)

    stacks_setup(summary,nfmt,nexp,x_name="NODE_CNT"
            ,keys=['type4','type5','type8','type9','type10','type12']
            ,key_names=['REM QRY','FIN','QRY RSP','ACK','Exec','PREP']
            ,norm=False)



#    stacks_setup(summary,nfmt,nexp,x_name="NODE_CNT"
#            ,keys=['type1','type2','type3','type4','type5','type6','type7','type8','type9','type10','type11','type12','type13','type14']
#            ,key_names=['DONE','LOCK','UNLOCK','Rem QRY','FIN','LOCK RSP','UNLOCK RSP','QRY RSP','ACK','Exec','INIT','PREP','PASS','CLIENT']
#            ,norm=False)
#    stacks_setup(summary,nfmt,nexp,x_name="NODE_CNT",keys=['thd1a','thd1b','thd1c','thd1d'],norm=False)
#    stacks_setup(summary,nfmt,nexp,x_name="NODE_CNT",keys=['rtxn1a','rtxn1b','rtxn2','rtxn3','rtxn4'],norm=False)
#    stacks_setup(summary,nfmt,nexp,x_name="NODE_CNT",keys=['row1','row2','row3'],norm=False)
#    line_setup(summary,summary_client,nfmt,nexp,x_name="MAX_TXN_IN_FLIGHT",v_name="MODE",key='mbuf_send_time')
#    line_setup(summary,summary_client,nfmt,nexp,x_name="MAX_TXN_IN_FLIGHT",v_name="MODE",key='avg_msg_batch')
#    line_setup(summary,summary_client,nfmt,nexp,x_name="MAX_TXN_IN_FLIGHT",v_name="MODE",key='txn_cnt')
#    line_setup(summary,summary_client,nfmt,nexp,x_name="MAX_TXN_IN_FLIGHT",v_name="MODE",key='time_tport_send')
#    line_setup(summary,summary_client,nfmt,nexp,x_name="MAX_TXN_IN_FLIGHT",v_name="MODE",key='time_tport_rcv')
#    line_setup(summary,summary_client,nfmt,nexp,x_name="MAX_TXN_IN_FLIGHT",v_name="MODE",key='time_tport_rcv')

def network_sweep():
    fmt = [fmt_ycsb[0] + ["NETWORK_DELAY"]]
    nnodes = [1,2,4,8,16]
    nmpr=[100]
    nalgos=['WAIT_DIE']
    #nalgos=['WAIT_DIE','NO_WAIT','OCC','MVCC','HSTORE','HSTORE_SPEC','VLL','TIMESTAMP']
    nthreads=[3]
    nrthreads=[1]
    ncthreads=[3]
    ncrthreads=[1]
    ntifs=[1300]
    nzipf=[0.8]
    nwr_perc=[0.5]
    nparts=[16]
    network_delay = ["0UL","10000UL","100000UL","1000000UL","10000000UL","100000000UL"]
    ntxn=[3000000]
    exp = [[int(math.ceil(n)) if n > 1 else 1,n,txn,'YCSB',cc,m,ct,crt,t,rt,tif,z,1.0-wp,wp,p if p <= n else n,n if cc!='HSTORE' and cc!='HSTORE_SPEC' else t*n,nd] for n,ct,crt,t,rt,tif,z,wp,m,cc,p,txn,nd in itertools.product(nnodes,ncthreads,ncrthreads,nthreads,nrthreads,ntifs,nzipf,nwr_perc,nmpr,nalgos,nparts,ntxn,network_delay)]
    exp.sort()
    exp = list(k for k,_ in itertools.groupby(exp))
    return fmt[0],exp

def network_sweep_plot(summary,summary_client):
    from plot_helper import tput
    fmt,exp = node_sweep()
    fmt = ["CC_ALG","CLIENT_NODE_CNT","MAX_TXN_PER_PART","WORKLOAD","MPR","CLIENT_THREAD_CNT","CLIENT_REM_THREAD_CNT","THREAD_CNT","REM_THREAD_CNT","MAX_TXN_IN_FLIGHT","ZIPF_THETA","READ_PERC","WRITE_PERC","PART_PER_TXN"]
    nmpr=[100]
    nnodes = [1,2,4,8,16]
    network_delay = ["0UL","10000UL","100000UL","1000000UL","10000000UL","100000000UL"]
    nalgos=['WAIT_DIE']
    nthreads=3
    nrthreads=1
    ncthreads=3
    ncrthreads=1
    ntifs=1300
    nzipf=[0.8]
    nwr_perc=[0.5]
    nparts = 16 
 
    v_vals = network_delay
    v_name = "NETWORK_DELAY"
    x_vals = nnodes
    x_name = "NODE_CNT"
    for mpr,wr,zipf,alg in itertools.product(nmpr,nwr_perc,nzipf,nalgos):
        c = [alg,1,3000000,"YCSB",mpr,ncthreads,ncrthreads,nthreads,nrthreads,ntifs,zipf,1.0-wr,wr,nparts]
        assert(len(c) == len(fmt))
        title = "YCSB System Throughput {} {}% Writes {} Skew".format(alg,wr*100,zipf);
        tput(x_vals,v_vals,summary,cfg_fmt=fmt,cfg=c,xname=x_name,vname=v_name,title=title)


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

def ft_mode():
    fmt,exp = test()
    fmt = fmt+["MODE_FT"]
    exp = [f+["true"] for f in exp]
    return fmt,exp

experiment_map = {
    'test': test,
    'ft_mode': ft_mode,
    'ft_mode_plot': ft_mode_plot,
    'test_plot': test_plot,
    'network_sweep': network_sweep,
    'network_sweep_plot': network_sweep_plot,
    'tpcc_sweep': tpcc_sweep,
    'network_experiment' : network_experiment,
    'network_experiment_plot' : network_experiment_plot,
}


# Default values for variable configurations
configs = {
    "NODE_CNT" : 2,
    "CLIENT_NODE_CNT" : 1,
    "CLIENT_THREAD_CNT" : 2,
    "CLIENT_REM_THREAD_CNT" : 1,
    "MAX_TXN_PER_PART" : 100,
    "WORKLOAD" : "YCSB",
    "REQ_PER_QUERY": 10, #16
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
    "DONE_TIMER": "1 * 100 * BILLION // 3 minutes",
    "PROG_TIMER" : "10 * BILLION // in s",
    "NETWORK_TEST" : "false",
    "ABORT_PENALTY": "1 * 1000000UL   // in ns.",
    "MSG_TIME_LIMIT": "10 * 1000000UL",
    "MSG_SIZE_MAX": 4096,
#    "PRT_LAT_DISTR": "true",
#YCSB
    "INIT_PARALLELISM" : 4, 
    "MAX_PART_PER_TXN":2,#16,
    "READ_PERC":0.5,
    "WRITE_PERC":0.5,
    "ZIPF_THETA":0.6,
    "PART_PER_TXN": 1,
    "SYNTH_TABLE_SIZE":"2097152",
    "LOAD_TXN_FILE":"false",
    "DEBUG_DISTR":"false",
    "MODE":"NORMAL",
    "SHMEM_ENV":"false",
}

config_names = fmt_ycsb[0]
