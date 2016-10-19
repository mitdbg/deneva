import itertools
import math
from paper_plots import *
# Experiments to run and analyze
# Go to end of file to fill in experiments 
SHORTNAMES = {
    "CLIENT_NODE_CNT" : "CN",
    "CLIENT_THREAD_CNT" : "CT",
    "CLIENT_REM_THREAD_CNT" : "CRT",
    "CLIENT_SEND_THREAD_CNT" : "CST",
    "NODE_CNT" : "N",
    "THREAD_CNT" : "T",
    "REM_THREAD_CNT" : "RT",
    "SEND_THREAD_CNT" : "ST",
    "CC_ALG" : "",
    "WORKLOAD" : "",
    "MAX_TXN_PER_PART" : "TXNS",
    "MAX_TXN_IN_FLIGHT" : "TIF",
    "PART_PER_TXN" : "PPT",
    "TUP_READ_PERC" : "TRD",
    "TUP_WRITE_PERC" : "TWR",
    "TXN_READ_PERC" : "RD",
    "TXN_WRITE_PERC" : "WR",
    "ZIPF_THETA" : "SKEW",
    "MSG_TIME_LIMIT" : "BT",
    "MSG_SIZE_MAX" : "BS",
    "DATA_PERC":"D",
    "ACCESS_PERC":"A",
    "PERC_PAYMENT":"PP",
    "MPR":"MPR",
    "REQ_PER_QUERY": "RPQ",
    "MODE":"",
    "PRIORITY":"",
    "ABORT_PENALTY":"PENALTY",
    "STRICT_PPT":"SPPT",
    "NETWORK_DELAY":"NDLY",
    "NETWORK_DELAY_TEST":"NDT",
    "REPLICA_CNT":"RN",
    "SYNTH_TABLE_SIZE":"TBL",
    "ISOLATION_LEVEL":"LVL",
    "YCSB_ABORT_MODE":"ABRTMODE",
    "NUM_WH":"WH",
}

#config_names=["CLIENT_NODE_CNT","NODE_CNT","MAX_TXN_PER_PART","WORKLOAD","CC_ALG","MPR","CLIENT_THREAD_CNT","CLIENT_REM_THREAD_CNT","CLIENT_SEND_THREAD_CNT","THREAD_CNT","REM_THREAD_CNT","SEND_THREAD_CNT","MAX_TXN_IN_FLIGHT","ZIPF_THETA","TXN_WRITE_PERC","TUP_WRITE_PERC","PART_PER_TXN","PART_CNT","MSG_TIME_LIMIT","MSG_SIZE_MAX","MODE","DATA_PERC","ACCESS_PERC","REQ_PER_QUERY"]
fmt_title=["NODE_CNT","CC_ALG","ACCESS_PERC","TXN_WRITE_PERC","PERC_PAYMENT","MPR","MODE","MAX_TXN_IN_FLIGHT","SEND_THREAD_CNT","REM_THREAD_CNT","THREAD_CNT","TXN_WRITE_PERC","TUP_WRITE_PERC","ZIPF_THETA","NUM_WH"]

##############################
# VLDB PAPER PLOTS
##############################

def pps_scaling():
    wl = 'PPS'
    nnodes = [1,2,4,8,16,32,64]
    nalgos=['NO_WAIT','WAIT_DIE','MVCC','MAAT','CALVIN','TIMESTAMP']
    load = [10000]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","MAX_TXN_IN_FLIGHT"]
    exp = [[wl,n,cc,tif] for tif,n,cc in itertools.product(load,nnodes,nalgos)]
    return fmt,exp


def ycsb_scaling():
    wl = 'YCSB'
    nnodes = [1,2,4,8,16,32,64]
#    nnodes = [16]
    algos=['NO_WAIT','WAIT_DIE','MVCC','MAAT','CALVIN','TIMESTAMP']
    base_table_size=2097152*8
    txn_write_perc = [0.5]
    tup_write_perc = [0.5]
    load = [10000]
    tcnt = [4]
    skew = [0.6,0.7]
    skew = [0.6]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr] for thr,txn_wr_perc,tup_wr_perc,sk,ld,n,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,skew,load,nnodes,algos)]
#    txn_write_perc = [0.0]
#    skew = [0.0]
#    exp = exp + [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr] for thr,txn_wr_perc,tup_wr_perc,sk,ld,n,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,skew,load,nnodes,algos)]
    return fmt,exp

def ecwc():
    wl = 'YCSB'
    nnodes = [2]
    algos=['NO_WAIT','WAIT_DIE','MVCC','CALVIN','TIMESTAMP','MAAT']
    base_table_size=2097152*8
    txn_write_perc = [0.5]
    tup_write_perc = [0.5]
    load = [10000]
    tcnt = [4]
    skew = [0.6]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr] for thr,txn_wr_perc,tup_wr_perc,sk,ld,n,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,skew,load,nnodes,algos)]
    return fmt,exp


def ycsb_scaling_abort():
    wl = 'YCSB'
    nnodes = [1,2,4,8,16,32,64]
    algos=['NO_WAIT','WAIT_DIE','MVCC','MAAT','CALVIN','TIMESTAMP']
    base_table_size=2097152*8
    txn_write_perc = [0.5]
    tup_write_perc = [0.5]
    load = [10000,12000]
    load = [10000]
    tcnt = [4]
    skew = [0.6,0.7]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","YCSB_ABORT_MODE"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr,'true'] for thr,txn_wr_perc,tup_wr_perc,sk,ld,n,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,skew,load,nnodes,algos)]
    return fmt,exp


def ycsb_skew():
    wl = 'YCSB'
    nnodes = [16]
    algos=['NO_WAIT','WAIT_DIE','MVCC','MAAT','CALVIN','TIMESTAMP']
    base_table_size=2097152*8
    txn_write_perc = [0.5]
    tup_write_perc = [0.5]
    load = [10000]
    tcnt = [4]
    skew = [0.0,0.25,0.5,0.55,0.6,0.65,0.7,0.75,0.8,0.9]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos)]
    return fmt,exp

def ycsb_writes():
    wl = 'YCSB'
    nnodes = [16]
    algos=['NO_WAIT','WAIT_DIE','MVCC','MAAT','CALVIN','TIMESTAMP']
    base_table_size=2097152*8
    txn_write_perc = [0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0]
    tup_write_perc = [0.5]
    load = [10000]
    tcnt = [4]
    skew = [0.6]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos)]
    return fmt,exp



def isolation_levels():
    wl = 'YCSB'
    nnodes = [1,2,4,8,16,32,64]
    algos=['NO_WAIT']
    levels=["READ_UNCOMMITTED","READ_COMMITTED","SERIALIZABLE","NOLOCK"]
    base_table_size=2097152*8
    load = [10000]
    txn_write_perc = [0.5]
    tup_write_perc = [0.5]
    skew = [0.6,0.7]
    skew = [0.6]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","ISOLATION_LEVEL","MAX_TXN_IN_FLIGHT","ZIPF_THETA"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,level,ld,sk] for txn_wr_perc,tup_wr_perc,algo,sk,ld,n,level in itertools.product(txn_write_perc,tup_write_perc,algos,skew,load,nnodes,levels)]
    return fmt,exp

def ycsb_partitions():
    wl = 'YCSB'
    nnodes = [16]
    algos=['NO_WAIT','WAIT_DIE','MVCC','MAAT','CALVIN','TIMESTAMP']
    load = [10000,12000]
    load = [10000]
    nparts = [1,2,4,6,8,10,12,14,16]
    base_table_size=2097152*8
    txn_write_perc = [0.5]
    tup_write_perc = [0.5]
    tcnt = [4]
    skew = [0.6]
    rpq =  16
    fmt = ["WORKLOAD","REQ_PER_QUERY","PART_PER_TXN","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","STRICT_PPT"]
    exp = [[wl,rpq,p,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr,1] for thr,txn_wr_perc,tup_wr_perc,algo,sk,ld,n,p in itertools.product(tcnt,txn_write_perc,tup_write_perc,algos,skew,load,nnodes,nparts)]
    return fmt,exp

def ycsb_partitions_distr():
    wl = 'YCSB'
    nnodes = [16]
    algos=['NO_WAIT','WAIT_DIE','MVCC','MAAT','CALVIN','TIMESTAMP']
    load = [10000]
    nparts = [2,4,6,8,10,12,14,16]
    base_table_size=2097152*8
    txn_write_perc = [0.5]
    tup_write_perc = [0.5]
    tcnt = [4]
    skew = [0.6]
    rpq =  16
    fmt = ["WORKLOAD","REQ_PER_QUERY","PART_PER_TXN","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","STRICT_PPT"]
    exp = [[wl,rpq,p,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr,1] for thr,txn_wr_perc,tup_wr_perc,algo,sk,ld,n,p in itertools.product(tcnt,txn_write_perc,tup_write_perc,algos,skew,load,nnodes,nparts)]
    return fmt,exp


def tpcc_scaling():
    wl = 'TPCC'
    nnodes = [1,2,4,8,16,32,64]
    nalgos=['NO_WAIT','WAIT_DIE','MAAT','MVCC','TIMESTAMP','CALVIN']
    npercpay=[0.0,1.0]
    wh=128
    load = [10000]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","MAX_TXN_IN_FLIGHT"]
    exp = [[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
    wh=4
    exp = exp+[[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
    return fmt,exp

def tpcc_scaling1():
    wl = 'TPCC'
    nnodes = [1,2,4,8,16,32,64]
    nalgos=['NO_WAIT','WAIT_DIE','MAAT','MVCC','TIMESTAMP','CALVIN']
    npercpay=[0.0,1.0]
    wh=128
    load = [10000]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","MAX_TXN_IN_FLIGHT"]
    exp = [[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
    return fmt,exp


def tpcc_scaling2():
    wl = 'TPCC'
    nnodes = [1,2,4,8,16,32,64]
    nalgos=['NO_WAIT','WAIT_DIE','MAAT','MVCC','TIMESTAMP','CALVIN']
    npercpay=[0.0,1.0]
    wh=4
    load = [10000]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH","MAX_TXN_IN_FLIGHT"]
    exp = [[wl,n,cc,pp,wh*n,tif] for tif,pp,n,cc in itertools.product(load,npercpay,nnodes,nalgos)]
    return fmt,exp

def tpcc_scaling_whset():
    wl = 'TPCC'
    nnodes = [1,2,4,8,16,32,64]
    nalgos=['NO_WAIT','WAIT_DIE','MAAT','MVCC','TIMESTAMP','CALVIN']
    npercpay=[0.0,0.5,1.0]
    wh=128
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH"]
    exp = [[wl,n,cc,pp,wh] for pp,n,cc in itertools.product(npercpay,nnodes,nalgos)]
    wh=256
    exp = exp + [[wl,n,cc,pp,wh] for pp,n,cc in itertools.product(npercpay,nnodes,nalgos)]
    return fmt,exp

def ycsb_skew_abort_writes():
    wl = 'YCSB'
    nnodes = [16]
    algos=['NO_WAIT','WAIT_DIE','MVCC','MAAT','CALVIN','TIMESTAMP']
    base_table_size=2097152*8
    txn_write_perc = [0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0]
    tup_write_perc = [0.5]
    load = [10000]
    tcnt = [4]
    skew = [0.6]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","YCSB_ABORT_MODE"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr,'true'] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos)]
    return fmt,exp

def ycsb_skew_abort():
    wl = 'YCSB'
    nnodes = [16]
    algos=['NO_WAIT','WAIT_DIE','MVCC','MAAT','CALVIN','TIMESTAMP']
    base_table_size=2097152*8
    txn_write_perc = [0.5]
    tup_write_perc = [0.5]
    load = [10000]
    tcnt = [4]
    skew = [0.0,0.25,0.5,0.55,0.6,0.65,0.7,0.75,0.8,0.825,0.85,0.875,0.9]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","YCSB_ABORT_MODE"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr,'true'] for thr,txn_wr_perc,tup_wr_perc,ld,n,sk,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,load,nnodes,skew,algos)]
    return fmt,exp


def ycsb_partitions_abort():
    wl = 'YCSB'
    nnodes = [16]
    algos=['NO_WAIT','WAIT_DIE','MVCC','MAAT','CALVIN','TIMESTAMP']
    load = [10000]
    nparts = [1,2,4,6,8,10,12,14,16]
    base_table_size=2097152*8
    txn_write_perc = [0.5]
    tup_write_perc = [0.5]
    tcnt = [4]
    skew = [0.6]
    rpq =  16
    fmt = ["WORKLOAD","REQ_PER_QUERY","PART_PER_TXN","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","STRICT_PPT","YCSB_ABORT_MODE"]
    exp = [[wl,rpq,p,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr,1,'true'] for thr,txn_wr_perc,tup_wr_perc,algo,sk,ld,n,p in itertools.product(tcnt,txn_write_perc,tup_write_perc,algos,skew,load,nnodes,nparts)]
    return fmt,exp



##############################
# END VLDB PAPER PLOTS
##############################

def ycsb_scaling_plot(summary,summary_client):
    from helper import plot_prep
    from plot_helper import tput,time_breakdown
    nfmt,nexp = ycsb_scaling()
    x_name="NODE_CNT"
    v_name="CC_ALG"
#    tput_setup(summary,summary_client,nfmt,nexp,x_name,v_name)
    skew = [0.0,0.6,0.7]
    txn_write_perc = [0.0,0.5]
    load = [10000,12000,15000]
    for sk,wr,tif in itertools.product(skew,txn_write_perc,load):
        try:
            const={"ZIPF_THETA":sk,"TXN_WRITE_PERC":wr,"MAX_TXN_IN_FLIGHT":tif}
            title = ""
            for c in const.keys():
                title += "{} {},".format(SHORTNAMES[c],const[c])
            x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants=const)
            tput(x_vals,v_vals,summary,summary_client,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,xlab="Server Count",new_cfgs=lst,ylimit=140,logscalex=False,title=title)
        except IndexError:
            continue

def ycsb_skew_plot(summary,summary_client):
    from helper import plot_prep
    from plot_helper import tput,time_breakdown
    nfmt,nexp = ycsb_skew()
    x_name="ZIPF_THETA"
    v_name="CC_ALG"
#    tput_setup(summary,summary_client,nfmt,nexp,x_name,v_name)
    txn_write_perc = [0.5,1.0]
    for wr in txn_write_perc:#itertools.product(skew,txn_write_perc):
        const={"TXN_WRITE_PERC":wr}
        title = ""
        for c in const.keys():
            title += "{} {},".format(SHORTNAMES[c],const[c])
        x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants=const)
        tput(x_vals,v_vals,summary,summary_client,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,xlab="Server Count",new_cfgs=lst,ylimit=140,logscalex=False,title=title)

def ycsb_writes_plot(summary,summary_client):
    from helper import plot_prep
    from plot_helper import tput,time_breakdown
    nfmt,nexp = ycsb_writes()
    x_name="TXN_WRITE_PERC"
    v_name="CC_ALG"
#    tput_setup(summary,summary_client,nfmt,nexp,x_name,v_name)
    skew = [0.6,0.7]
    for sk in skew:
        const={"ZIPF_THETA":sk}
        title = ""
        for c in const.keys():
            title += "{} {},".format(SHORTNAMES[c],const[c])
        x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants=const)
        tput(x_vals,v_vals,summary,summary_client,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,xlab="Server Count",new_cfgs=lst,ylimit=140,logscalex=False,title=title)


def ycsb_partitions_plot(summary,summary_client):
    from helper import plot_prep
    from plot_helper import tput,time_breakdown
    nfmt,nexp = ycsb_partitions()
    x_name="PART_PER_TXN"
    v_name="CC_ALG"
#    tput_setup(summary,summary_client,nfmt,nexp,x_name,v_name)
    txn_write_perc = [0.0,0.5,1.0]
    skew = [0.0,0.6,0.7]
    for sk,wr in itertools.product(skew,txn_write_perc):
        try:
            const={"TXN_WRITE_PERC":wr,"ZIPF_THETA":sk}
            title = ""
            for c in const.keys():
                title += "{} {},".format(SHORTNAMES[c],const[c])
            x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants=const)
            tput(x_vals,v_vals,summary,summary_client,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,xlab="Server Count",new_cfgs=lst,ylimit=140,logscalex=False,title=title)
        except IndexError:
            continue

def tpcc_scaling_plot(summary,summary_client):
    from helper import plot_prep
    from plot_helper import tput,time_breakdown
    nfmt,nexp = tpcc_scaling()
    x_name="NODE_CNT"
    v_name="CC_ALG"
#    tput_setup(summary,summary_client,nfmt,nexp,x_name,v_name)
    warehouses = [128]
    npercpay=[0.0,0.5,1.0]
    for pp in npercpay:
        const={"PERC_PAYMENT":pp}
        title = ""
        for c in const.keys():
            title += "{} {},".format(SHORTNAMES[c],const[c])
        x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants=const)
        tput(x_vals,v_vals,summary,summary_client,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,xlab="Server Count",new_cfgs=lst,ylimit=140,logscalex=False,title=title)

def tpcc_scaling2_plot(summary,summary_client):
    from helper import plot_prep
    from plot_helper import tput,time_breakdown
    nfmt,nexp = tpcc_scaling2()
    x_name="NODE_CNT"
    v_name="CC_ALG"
#    tput_setup(summary,summary_client,nfmt,nexp,x_name,v_name)
    warehouses = [128]
    npercpay=[0.0,0.5,1.0]
    for pp in npercpay:
        const={"PERC_PAYMENT":pp}
        title = ""
        for c in const.keys():
            title += "{} {},".format(SHORTNAMES[c],const[c])
        x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants=const)
        tput(x_vals,v_vals,summary,summary_client,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,xlab="Server Count",new_cfgs=lst,ylimit=140,logscalex=False,title=title)


def tpcc_scaling_whset_plot(summary,summary_client):
    from helper import plot_prep
    from plot_helper import tput,time_breakdown
    nfmt,nexp = tpcc_scaling_whset()
    x_name="NODE_CNT"
    v_name="CC_ALG"
#    tput_setup(summary,summary_client,nfmt,nexp,x_name,v_name)
    warehouses = [128,256]
    npercpay=[0.0,0.5,1.0]
    for wh,pp in itertools.product(warehouses,npercpay):
        const={"NUM_WH":wh,"PERC_PAYMENT":pp}
        title = ""
        for c in const.keys():
            title += "{} {},".format(SHORTNAMES[c],const[c])
        x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants=const)
        tput(x_vals,v_vals,summary,summary_client,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,xlab="Server Count",new_cfgs=lst,ylimit=140,logscalex=False,title=title)


#        for x in x_vals:
#            const[x_name] = x
#            title2 = title + "{} {},".format(SHORTNAMES[x_name],const[x_name])
#            v_vals,tmp,fmt,exp,lst = plot_prep(nexp,nfmt,v_name,'',constants=const)
#            time_breakdown(v_vals,summary,cfg_fmt=fmt,cfg=list(exp),xname=v_name,new_cfgs=lst,title=title2)
#    breakdown_setup(summary,nfmt,nexp,x_name)



def ycsb_load():
    wl = 'YCSB'
    algos=['NO_WAIT']
    algos=['NO_WAIT','WAIT_DIE','MVCC','MAAT']
    algos=['NO_WAIT','WAIT_DIE','MAAT']
    load = [10000]
    base_table_size=2097152*8
    txn_write_perc = [0.5]
    tup_write_perc = [0.5]
    nnodes = [8]
    tcnt = [4]
    skew = [0.3,0.6,0.7]
#    recv = [2,4]
    rpq =  8 
#    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","REM_THREAD_CNT"]
#    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,rcv] for rcv,txn_wr_perc,tup_wr_perc,algo,sk,ld,n in itertools.product(recv,txn_write_perc,tup_write_perc,algos,skew,load,nnodes)]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","YCSB_ABORT_MODE"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr,'false'] for thr,txn_wr_perc,tup_wr_perc,algo,sk,ld,n in itertools.product(tcnt,txn_write_perc,tup_write_perc,algos,skew,load,nnodes)]
    return fmt,exp


def ycsb_load_plot(summary,summary_client):
    from helper import plot_prep
    from plot_helper import tput
    nfmt,nexp = ycsb_load()
    v_name="NODE_CNT"
    x_name="MAX_TXN_IN_FLIGHT"
#    tput_setup(summary,summary_client,nfmt,nexp,x_name,v_name)
    txn_write_perc = [0.01,0.1,0.2,0.5,1.0]
    txn_write_perc = [0.0,0.5]
    skew = [0.0,0.3,0.6,0.9]
    skew = [0.6]
    tcnt = [4]
    for wr,sk,thr in itertools.product(txn_write_perc,skew,tcnt):
        const={"TXN_WRITE_PERC":wr,"ZIPF_THETA":sk,"THREAD_CNT":thr}
        title = ""
        for c in const.keys():
            title += "{} {},".format(SHORTNAMES[c],const[c])
        x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants=const)
        tput(x_vals,v_vals,summary,summary_client,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,xlab="Open Client Connections",new_cfgs=lst,ylimit=0.14,logscalex=False,title=title)


def isolation_levels_plot(summary,summary_client):
    from helper import plot_prep
    from plot_helper import tput
    nfmt,nexp = isolation_levels()
    x_name="NODE_CNT"
    v_name="ISOLATION_LEVEL"
#    tput_setup(summary,summary_client,nfmt,nexp,x_name,v_name)
    txn_write_perc = [0.1,0.2,0.5,1.0]
    txn_write_perc = [0.5,1.0]
    skew = [0.0,0.3,0.6]
    skew = [0.6,0.7]
    for wr,sk in itertools.product(txn_write_perc,skew):
        const={"TXN_WRITE_PERC":wr,"ZIPF_THETA":sk}
        title = ""
        for c in const.keys():
            title += "{} {},".format(SHORTNAMES[c],const[c])
        x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants=const)
        tput(x_vals,v_vals,summary,summary_client,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,xlab="Server Count",new_cfgs=lst,logscalex=False,title=title,name='')


##############################
##############################

def replica_test():
    wl = 'YCSB'
    nnodes = [1,2]
    nrepl = [0,1]
    nwr = [0.2]
    nalgos=['NO_WAIT']
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","TUP_WRITE_PERC","MAX_TXN_IN_FLIGHT","REPLICA_CNT","LOGGING"]
    exp = [[wl,n,cc,wr,1000,repl,"true"] for n,cc,wr,repl in itertools.product(nnodes,nalgos,nwr,nrepl)]
    exp += [[wl,n,cc,wr,1000,0,"false"] for n,cc,wr in itertools.product(nnodes,nalgos,nwr)]
    return fmt,exp


def malviya():
    wl = 'YCSB'
    nnodes = [1]
    nrepl = [0,1]
    nwr = [0.2]
    nalgos=['NO_WAIT']
    ntif = [5000,10000,25000,50000,100000,150000,200000,250000,300000,350000]
    nload=[]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PART_PER_TXN","TUP_WRITE_PERC","MAX_TXN_IN_FLIGHT","REPLICA_CNT","MAX_TXN_IN_FLIGHT","LOGGING"]
    exp = [[wl,n,cc,2,wr,1000,repl,tif,"true"] for n,cc,wr,repl,tif in itertools.product(nnodes,nalgos,nwr,nrepl,ntif)]
    exp += [[wl,n,cc,2,wr,1000,0,tif,"false"] for n,cc,wr,tif in itertools.product(nnodes,nalgos,nwr,ntif)]
    return fmt,exp

def malviya_plot(summary,summary_client):
    nfmt,nexp = malviya()
    x_name = "MAX_TXN_IN_FLIGHT"
    v_name = "REPLICA_CNT"
    tput_setup(summary,summary_client,nfmt,nexp,x_name,v_name)
    v_name = "LOGGING"
    tput_setup(summary,summary_client,nfmt,nexp,x_name,v_name)

def test():
    wl = 'YCSB'
    nnodes = [1,2,4]
    algos=['CALVIN']
    base_table_size=2097152*8
#    txn_write_perc = [0.5,1.0]
    txn_write_perc = [0.5]
    tup_write_perc = [0.5]
    load = [10000]
    tcnt = [4]
    skew = [0.6]
    stimer = [5000000]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","SEQ_BATCH_TIMER"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr,timer] for timer,thr,txn_wr_perc,tup_wr_perc,sk,ld,n,algo in itertools.product(stimer,tcnt,txn_write_perc,tup_write_perc,skew,load,nnodes,algos)]
    return fmt,exp

def test2():
    nnodes = [1,2,4]
    nwr = [0.2]
    nalgos=['NO_WAIT','WAIT_DIE','MAAT','OCC']
#    nalgos=['MVCC','TIMESTAMP','CALVIN']
#    nnodes = [2]
#    nalgos=['OCC']

    wl = 'TPCC'
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","TUP_WRITE_PERC","MAX_TXN_IN_FLIGHT","NUM_WH","PERC_PAYMENT"]
    exp = [[wl,n,cc,wr,10000,128,1.0] for n,cc,wr in itertools.product(nnodes,nalgos,nwr)]
    return fmt,exp


def test_plot(summary,summary_client):
    from helper import plot_prep
    from plot_helper import tput,time_breakdown
    nfmt,nexp = test()
    x_name = "NODE_CNT"
    v_name = "CC_ALG"
    title = ""
    const={"TXN_WRITE_PERC":0.5}
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants=const)
    tput(x_vals,v_vals,summary,summary_client,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,xlab="Server Count",new_cfgs=lst,logscalex=True,legend=True)
#    v_name = "CC_ALG"
#    const={"TXN_WRITE_PERC":0.0,"ZIPF_THETA":0.0}
#    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants=const)
#    tput(x_vals,v_vals,summary,summary_client,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,xlab="Server Count",new_cfgs=lst,logscalex=True,legend=True)
#    tput_setup(summary,summary_client,nfmt,nexp,x_name,v_name)
#    txn_write_perc = [0.5] # TXN_WRITE_PERC
#    skew = [0.6]
#    nmodes=['false']
#    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants=const)
#    tput(x_vals,v_vals,summary,summary_client,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,xlab="Server Count",new_cfgs=lst,ylimit=140,logscalex=False,title=title)
#    for sk,mode in itertools.product(skew,nmodes):
#    for sk,wr,mode in itertools.product(skew,txn_write_perc,modes):
#        try:
#            const={"ZIPF_THETA":sk,"YCSB_ABORT_MODE":mode,"TXN_WRITE_PERC":wr}
#            title = ""
#            for c in const.keys():
#                try:
#                    title += "{} {},".format(SHORTNAMES[c],const[c])
#                except KeyError:
#                    title += "{} {},".format(c,const[c])
#            x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants=const)
#            tput(x_vals,v_vals,summary,summary_client,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,xlab="Server Count",new_cfgs=lst,logscalex=False,title=title,legend=True)
#        except IndexError:
#            continue

def test2_plot(summary,summary_client):
    nfmt,nexp = test2()
    x_name = "NODE_CNT"
    v_name = "CC_ALG"
    tput_setup(summary,summary_client,nfmt,nexp,x_name,v_name,title="TPCC")

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
            title+="{} {}, ".format(SHORTNAMES[t],e[fmt.index(t)])
        tput_v_lat(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=e,xname=x_name,vname=v_name,title=title)


def tput_setup(summary,summary_cl,nfmt,nexp,x_name,v_name
        ,extras={}
        ,title=""
#        ,extras={'PART_CNT':'NODE_CNT','CLIENT_NODE_CNT':'NODE_CNT','PART_PER_TXN':'NODE_CNT'}
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
        if x not in fmt: 
            del extras[x]
            continue
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
#        if "PART_PER_TXN" in fmt and e[fmt.index("PART_PER_TXN")] == 1 and ("NODE_CNT" == x_name or "NODE_CNT" == v_name):
#            continue
#title = "System Tput "
        _title = ""
        if title == "":
            for t in fmt_title:
                if t not in fmt: continue
                _title+="{} {} ".format(SHORTNAMES[t],e[fmt.index(t)])
        else:
            _title = title
        tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(e),xname=x_name,vname=v_name,title=_title,extras=extras)

def line_rate_setup(summary,summary_cl,nfmt,nexp,x_name,v_name,key
        ,extras={}
        ):
    from plot_helper import line_rate
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
        if x not in fmt: continue
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
        title = key 
        for t in fmt_title:
            if t not in fmt: continue
            title+="{} {}, ".format(t,e[fmt.index(t)])
        line_rate(x_vals,v_vals,summary,summary_cl,key,cfg_fmt=fmt,cfg=list(e),xname=x_name,vname=v_name,title=title,extras=extras)
     
def line_setup(summary,summary_cl,nfmt,nexp,x_name,v_name,key
        ,extras={}
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
    for x in extras.keys():
        if x not in fmt: continue
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
        title = key 
        for t in fmt_title:
            if t not in fmt: continue
            title+="{} {}, ".format(t,e[fmt.index(t)])
        line_general(x_vals,v_vals,summary,summary_cl,key,cfg_fmt=fmt,cfg=list(e),xname=x_name,vname=v_name,title=title,extras=extras)
        
def stacks_setup(summary,nfmt,nexp,x_name,keys,key_names=[],norm=False
        ,extras={}
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
    for x in extras.keys():
        if x not in fmt: continue
        for e in exp:
            del e[fmt.index(x)]
        fmt.remove(x)
    x_vals = list(set(x_vals))
    x_vals.sort()
    exp.sort()
    exp = list(k for k,_ in itertools.groupby(exp))
    for e in exp:
        title = "Stacks "
        for t in fmt_title:
            if t not in fmt: continue
            title+="{} {}, ".format(t,e[fmt.index(t)])
        stacks_general(x_vals,summary,list(keys),xname=x_name,key_names=list(key_names),title=title,cfg_fmt=fmt,cfg=list(e),extras=extras)

def breakdown_setup(summary,nfmt,nexp,x_name,key_names=[],norm=False
        ,extras={}
        ):
    from plot_helper import time_breakdown
    x_vals = []
    exp = [list(e) for e in nexp]
    fmt = list(nfmt)
    print('Breakdown Setup: {}'.format(x_name))
    for e in exp:
        x_vals.append(e[fmt.index(x_name)])
        del e[fmt.index(x_name)]
    fmt.remove(x_name)
    for x in extras.keys():
        if x not in fmt: continue
        for e in exp:
            del e[fmt.index(x)]
        fmt.remove(x)
    x_vals = list(set(x_vals))
    x_vals.sort()
    exp.sort()
    exp = list(k for k,_ in itertools.groupby(exp))
    for e in exp:
        title = "Breakdown "
        for t in fmt_title:
            if t not in fmt: continue
            title+="{} {}, ".format(t,e[fmt.index(t)])
        time_breakdown(x_vals,summary,xname=x_name,title=title,cfg_fmt=fmt,cfg=list(e),extras=extras,normalized=norm)

def network_sweep_abort():
    wl = 'YCSB'
    nalgos=['NO_WAIT','WAIT_DIE','MVCC','MAAT','TIMESTAMP','CALVIN']
    algos=['CALVIN']
# Network delay in ms
    ndelay=[0,0.05,0.1,0.25,0.5,0.75,1,1.75,2.5,5,7.5,10,17.5,25,50,75,100,175,250,500,750,1000]
    ndelay = [int(n*1000000) for n in ndelay]
    nnodes = [2,8]
    nnodes = [2]
    txn_write_perc = [0.5]
    tup_write_perc = [0.5]
    load = [10000]
    tcnt = [4]
    skew = [0.6]
    base_table_size=2097152*8
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","NETWORK_DELAY_TEST","NETWORK_DELAY","SET_AFFINITY","YCSB_ABORT_MODE"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr,"true",d,"false","true"] for thr,txn_wr_perc,tup_wr_perc,sk,ld,n,d,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,skew,load,nnodes,ndelay,nalgos)]
    return fmt,exp


def network_sweep():
    wl = 'YCSB'
    nalgos=['NO_WAIT','WAIT_DIE','MVCC','MAAT','TIMESTAMP','CALVIN']
    algos=['CALVIN']
# Network delay in ms
    ndelay=[0,0.05,0.1,0.25,0.5,0.75,1,1.75,2.5,5,7.5,10,17.5,25,50]#,75,100]
    ndelay=[0,0.05,0.1,0.25,0.5,1,1.75,2.5,5,7.5,10,17.5,25,50]#,75,100]
#    ndelay=[175,250,500,750,1000]
    ndelay = [int(n*1000000) for n in ndelay]
    nnodes = [2,8]
    nnodes = [2]
    txn_write_perc = [0.5]
    tup_write_perc = [0.5]
    load = [10000]
    tcnt = [4]
    skew = [0.6]
    base_table_size=2097152*8
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","SYNTH_TABLE_SIZE","TUP_WRITE_PERC","TXN_WRITE_PERC","MAX_TXN_IN_FLIGHT","ZIPF_THETA","THREAD_CNT","NETWORK_DELAY_TEST","NETWORK_DELAY","SET_AFFINITY"]
    exp = [[wl,n,algo,base_table_size*n,tup_wr_perc,txn_wr_perc,ld,sk,thr,"true",d,"false"] for thr,txn_wr_perc,tup_wr_perc,sk,ld,n,d,algo in itertools.product(tcnt,txn_write_perc,tup_write_perc,skew,load,nnodes,ndelay,nalgos)]
    return fmt,exp

def network_sweep_plot(summary,summary_client):
    from helper import plot_prep
    from plot_helper import tput
    nfmt,nexp = network_sweep()
    x_name="NETWORK_DELAY"
    v_name="CC_ALG"
#    for sk,wr in itertools.product(skew,txn_write_perc):
    nnodes = [2,8]
    skew = [0.0,0.6]
    for sk,nodes in itertools.product(skew,nnodes):
        const={"NODE_CNT":nodes,"ZIPF_THETA":sk}
        title = ""
        for c in const.keys():
            title += "{} {},".format(SHORTNAMES[c],const[c])
        x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants=const)
        tput(x_vals,v_vals,summary,summary_client,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,xlab="Server Count",new_cfgs=lst,logscalex=False,title=title)

def network_experiment():
    fmt_nt = ["NODE_CNT","CLIENT_NODE_CNT","NETWORK_TEST"]
    nnodes = [1]
    cnodes = [1]
    ntest = ["true"]
    exp = [nnodes,cnodes,ntest]
    exp = [[n,c,t] for n,c,t in itertools.product(nnodes,cnodes,ntest)]
    return fmt_nt,exp

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


def network_test():
    wl = 'YCSB'
    nnodes = [1]
    nalgos=['NO_WAIT']
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","NETWORK_TEST","SET_AFFINITY"]
    exp = [[wl,n,cc,'true','false'] for n,cc in itertools.product(nnodes,nalgos)]
    return fmt,exp


experiment_map = {
    'network_test':network_test,
    'pps_scaling': pps_scaling,
    'ycsb_scaling': ycsb_scaling,
    'ycsb_scaling_plot': ycsb_scaling_plot,
    'ycsb_scaling_abort': ycsb_scaling_abort,
    'ppr_ycsb_scaling_abort': ycsb_scaling_abort,
    'ppr_ycsb_scaling_abort_plot': ppr_ycsb_scaling_abort_plot,
    'ycsb_writes': ycsb_writes,
    'ycsb_writes_plot': ycsb_writes_plot,
    'ycsb_skew': ycsb_skew,
    'ycsb_skew_plot': ycsb_skew_plot,
    'isolation_levels': isolation_levels,
    'isolation_levels_plot': isolation_levels_plot,
    'ycsb_partitions': ycsb_partitions,
    'ycsb_partitions_plot': ycsb_partitions_plot,
    'ycsb_partitions_abort': ycsb_partitions_abort,
    'ppr_ycsb_partitions_abort': ycsb_partitions_abort,
    'ppr_ycsb_partitions_abort_plot': ppr_ycsb_partitions_abort_plot,
    'ycsb_load': ycsb_load,
    'tpcc_scaling': tpcc_scaling,
    'tpcc_scaling_plot': tpcc_scaling_plot,
    'tpcc_scaling2': tpcc_scaling2,
    'tpcc_scaling2_plot': tpcc_scaling2_plot,
    'tpcc_scaling_whset': tpcc_scaling_whset,
    'tpcc_scaling_whset_plot': tpcc_scaling_whset_plot,
    'ycsb_skew_abort': ycsb_skew_abort,
    'test': test,
    'test_plot': test_plot,
    'test2': test2,
    'test2_plot': test2_plot,
    'network_sweep_abort': network_sweep_abort,
    'network_sweep': network_sweep,
    'network_sweep_plot': network_sweep_plot,
    'ppr_network_sweep': network_sweep,
    'ppr_network_sweep_plot': ppr_network_plot,
    'ppr_network_sweep_abort': network_sweep_abort,
    'ppr_network_sweep_abort_plot': ppr_network_abort_plot,
    'ppr_pps_scaling': pps_scaling,
    'ppr_pps_scaling_plot': ppr_pps_scaling_plot,
    'ppr_ycsb_scaling': ycsb_scaling,
    'ppr_ycsb_scaling_plot': ppr_ycsb_scaling_plot,
    'ecwc': ecwc,
    'ppr_ecwc': ecwc,
    'ppr_ecwc_plot': ppr_ecwc_plot,
    'ppr_ycsb_skew': ycsb_skew,
    'ppr_ycsb_skew_plot': ppr_ycsb_skew_plot,
    'ppr_ycsb_skew_abort': ycsb_skew_abort,
    'ppr_ycsb_skew_abort_plot': ppr_ycsb_skew_abort_plot,
    'ppr_ycsb_writes': ycsb_writes,
    'ppr_ycsb_writes_plot': ppr_ycsb_writes_plot,
    'ppr_ycsb_partitions': ycsb_partitions,
    'ppr_ycsb_partitions_plot': ppr_ycsb_partitions_plot,
    'ppr_isolation_levels': isolation_levels,
    'ppr_isolation_levels_plot': ppr_isolation_levels_plot,
    'ppr_tpcc_scaling': tpcc_scaling,
    'ppr_tpcc_scaling_plot': ppr_tpcc_scaling_plot,
}


# Default values for variable configurations
configs = {
    "NODE_CNT" : 16,
    "THREAD_CNT": 4,
    "REPLICA_CNT": 0,
    "REPLICA_TYPE": "AP",
    "REM_THREAD_CNT": "THREAD_CNT",
    "SEND_THREAD_CNT": "THREAD_CNT",
    "CLIENT_NODE_CNT" : "NODE_CNT",
    "CLIENT_THREAD_CNT" : 4,
    "CLIENT_REM_THREAD_CNT" : 2,
    "CLIENT_SEND_THREAD_CNT" : 2,
    "MAX_TXN_PER_PART" : 500000,
#    "MAX_TXN_PER_PART" : 1,
    "WORKLOAD" : "YCSB",
    "CC_ALG" : "WAIT_DIE",
    "MPR" : 1.0,
    "TPORT_TYPE":"IPC",
    "TPORT_PORT":"17000",#"63200",
    "PART_CNT": "NODE_CNT", #2,
    "PART_PER_TXN": "PART_CNT",
    "MAX_TXN_IN_FLIGHT": 100,
    "NETWORK_DELAY": '0UL',
    "NETWORK_DELAY_TEST": 'false',
    "DONE_TIMER": "1 * 60 * BILLION // ~1 minutes",
#    "DONE_TIMER": "1 * 10 * BILLION // ~1 minutes",
    "WARMUP_TIMER": "1 * 60 * BILLION // ~1 minutes",
#    "WARMUP_TIMER": "0 * 60 * BILLION // ~1 minutes",
    "SEQ_BATCH_TIMER": "5 * 1 * MILLION // ~5ms -- same as CALVIN paper",
    "BATCH_TIMER" : "0",#"10000000",
    "PROG_TIMER" : "10 * BILLION // in s",
    "NETWORK_TEST" : "false",
    "ABORT_PENALTY": "10 * 1000000UL   // in ns.",
    "ABORT_PENALTY_MAX": "5 * 100 * 1000000UL   // in ns.",
    "MSG_TIME_LIMIT": "0",#"10 * 1000000UL",
    "MSG_SIZE_MAX": 4096,
#    "PRT_LAT_DISTR": "true",
    "TXN_WRITE_PERC":0.0,
    "PRIORITY":"PRIORITY_ACTIVE",
    "TWOPL_LITE":"false",
#YCSB
    "INIT_PARALLELISM" : 8, 
    "TUP_WRITE_PERC":0.0,
    "ZIPF_THETA":0.3,
    "ACCESS_PERC":0.03,
    "DATA_PERC": 100,
    "REQ_PER_QUERY": 10, #16
    "SYNTH_TABLE_SIZE":"65536",
#    "SYNTH_TABLE_SIZE":"2097152*8",
#TPCC
    "NUM_WH": 'PART_CNT',
    "PERC_PAYMENT":0.0,
    "DEBUG_DISTR":"false",
#    "DEBUG_DISTR":"true",
#    "DEBUG_ALLOC":"true",
    "DEBUG_ALLOC":"false",
    "DEBUG_RACE":"false",
    "MODE":"NORMAL_MODE",
    "SHMEM_ENV":"false",
    "STRICT_PPT":0,
    "SET_AFFINITY":"true",
    "LOGGING":"false",
#    "SIM_FULL_ROW":"false",
    "SERVER_GENERATE_QUERIES":"false",
    "SKEW_METHOD":"ZIPF",
    "ENVIRONMENT_EC2":"false",
    "YCSB_ABORT_MODE":"false",
    "LOAD_METHOD": "LOAD_MAX", #"LOAD_RATE","LOAD_MAX"
    "ISOLATION_LEVEL":"SERIALIZABLE"
}

