import itertools
import math
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
}
# Format: [#Nodes,#Txns,Workload,CC_ALG,MPR]
fmt_tpcc = [["CLIENT_NODE_CNT","NODE_CNT","MAX_TXN_PER_PART","WORKLOAD","CC_ALG","MPR","MPIR","CLIENT_THREAD_CNT","CLIENT_REM_THREAD_CNT","CLIENT_SEND_THREAD_CNT","THREAD_CNT","REM_THREAD_CNT","SEND_THREAD_CNT","MAX_TXN_IN_FLIGHT","NUM_WH","PERC_PAYMENT","PART_PER_TXN","PART_CNT","MSG_TIME_LIMIT","MSG_SIZE_MAX","MODE"]]
fmt_nd = [["NODE_CNT","MAX_TXN_PER_PART","WORKLOAD","CC_ALG","MPR","THREAD_CNT","NUM_WH","MAX_TXN_IN_FLIGHT","NETWORK_DELAY"]]
#fmt_ycsb = [["CLIENT_NODE_CNT","NODE_CNT","MAX_TXN_PER_PART","WORKLOAD","CC_ALG","MPR","CLIENT_THREAD_CNT","CLIENT_REM_THREAD_CNT","CLIENT_SEND_THREAD_CNT","THREAD_CNT","REM_THREAD_CNT","SEND_THREAD_CNT","MAX_TXN_IN_FLIGHT","ZIPF_THETA","READ_PERC","WRITE_PERC","PART_PER_TXN","PART_CNT","MSG_TIME_LIMIT","MSG_SIZE_MAX","MODE"]]
fmt_ycsb = [["CLIENT_NODE_CNT","NODE_CNT","MAX_TXN_PER_PART","WORKLOAD","CC_ALG","MPR","CLIENT_THREAD_CNT","CLIENT_REM_THREAD_CNT","CLIENT_SEND_THREAD_CNT","THREAD_CNT","REM_THREAD_CNT","SEND_THREAD_CNT","MAX_TXN_IN_FLIGHT","ZIPF_THETA","TXN_WRITE_PERC","TUP_WRITE_PERC","PART_PER_TXN","PART_CNT","MSG_TIME_LIMIT","MSG_SIZE_MAX","MODE","DATA_PERC","ACCESS_PERC","REQ_PER_QUERY"]]
fmt_nt = [["NODE_CNT","CLIENT_NODE_CNT","NETWORK_TEST"]]
#fmt_title=["NODE_CNT","ZIPF_THETA","WRITE_PERC","CC_ALG","MAX_TXN_IN_FLIGHT","MODE"]
#fmt_title=["NODE_CNT","WRITE_PERC","CC_ALG","MAX_TXN_IN_FLIGHT","MODE","DATA_PERC","ACCESS_PERC"]
fmt_title=["NODE_CNT","CC_ALG","ACCESS_PERC","TXN_WRITE_PERC","PERC_PAYMENT","MPR","MODE","MAX_TXN_IN_FLIGHT"]

def test():
    wl = 'YCSB'
#    wl = 'TPCC'
    nnodes = [1]
#    nnodes = [1,2,4,8,16]
    nmpr=[1.0]
    nmpir=[0.01]
#    nalgos=['NO_WAIT','WAIT_DIE','OCC','MVCC','TIMESTAMP']
    nalgos=['WAIT_DIE']
    nthreads=[6]
    nrthreads=[1]
    nsthreads=[1]
    ncthreads=[2]
    ncrthreads=[1]
    ncsthreads=[4]
    ntxn=[1000000]
    nbtime=[1000] # in ns
    nbsize=[4096]
    nparts = [64]
#    nmodes = ["NORMAL_MODE","NOCC_MODE","QRY_ONLY_MODE"]#,"SETUP_MODE","SIMPLE_MODE"]
#    nmodes = ["NORMAL_MODE","NOCC_MODE"]
    nmodes = ["NORMAL_MODE"]
#    nmodes = ["NOCC_MODE"]
    ntifs=[50000]
#    ntifs=[1000,2500,5000,10000,20000,30000,50000,70000]
#    ntifs=[250,500,1000,2000,4000,8000,12000,20000,30000,50000]#,75000,100000]
#TPCC
    npercpay=[0.0]
    nwh=[128]
#YCSB
    nzipf=[0.0]
    d_perc=[100]
    a_perc=[0.03]
    ntwr_perc=[0.5]
    nwr_perc=[0.5]
    nrpq=[10]
    if wl == "TPCC":
        fmt = fmt_tpcc
        exp = [[n,n,txn,wl,cc,m,mi,ct,crt,cst,t,rt,st,tif,n if wh<n else wh,pp,p if p <= n else n,n if cc!='HSTORE' and cc!='HSTORE_SPEC' else t*n,str(mt) + '*1000UL',bsize,mode] for n,ct,crt,cst,t,rt,st,tif,m,mi,cc,p,txn,mt,bsize,mode,pp,wh in itertools.product(nnodes,ncthreads,ncrthreads,ncsthreads,nthreads,nrthreads,nsthreads,ntifs,nmpr,nmpir,nalgos,nparts,ntxn,nbtime,nbsize,nmodes,npercpay,nwh)]
    elif wl == "YCSB":
        fmt = fmt_ycsb
        exp = [[n,n,txn,wl,cc,m,ct,crt,cst,t,rt,st,tif,z,twp,wp,p if p <= n else n,n if cc!='HSTORE' and cc!='HSTORE_SPEC' else t*n,str(mt) + '*1000UL',bsize,mode,dp,ap,rpq] for n,ct,crt,cst,t,rt,st,tif,z,twp,wp,m,cc,p,txn,mt,bsize,mode,dp,ap,rpq in itertools.product(nnodes,ncthreads,ncrthreads,ncsthreads,nthreads,nrthreads,nsthreads,ntifs,nzipf,ntwr_perc,nwr_perc,nmpr,nalgos,nparts,ntxn,nbtime,nbsize,nmodes,d_perc,a_perc,nrpq)]
    exp.sort()
    exp = list(k for k,_ in itertools.groupby(exp))
    return fmt[0],exp

def tpcc_priorities():
    wl = 'TPCC'
    nnodes = [1,2,4,8,16,32,64]
    nprio = ["PRIORITY_HOME","PRIORITY_FCFS","PRIORITY_ACTIVE"]
    fmt = ["WORKLOAD","NODE_CNT","PRIORITY"]
    exp = [[wl,n,pp] for n,pp in itertools.product(nnodes,nprio)]
    return fmt,exp

def tpcc_modes():
    wl = 'TPCC'
    nnodes = [1,2,4,8,16,32,64]
    nmodes = ["NORMAL_MODE","NOCC_MODE","QRY_ONLY_MODE"]
    fmt = ["WORKLOAD","NODE_CNT","MODE"]
    exp = [[wl,n,m] for n,m in itertools.product(nnodes,nmodes)]
    return fmt,exp

# 7x5x2x2 = 140
def tpcc_scaling():
    wl = 'TPCC'
    nnodes = [1,2,4,8,16,32,64]
    nalgos=['NO_WAIT','WAIT_DIE','OCC','MVCC','TIMESTAMP','CALVIN']
#    nalgos=['OCC']
    npercpay=[1.0,0.0]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT"]
    exp = [[wl,n,cc,pp] for pp,n,cc in itertools.product(npercpay,nnodes,nalgos)]
    return fmt,exp

def tpcc_scaling_whset():
    wl = 'TPCC'
    nnodes = [1,2,4,8,16,32,64]
    nalgos=['NO_WAIT','WAIT_DIE','OCC','MVCC','TIMESTAMP','CALVIN']
    npercpay=[1.0,0.0]
    wh=128
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PERC_PAYMENT","NUM_WH"]
    exp = [[wl,n,cc,pp,wh] for pp,n,cc in itertools.product(npercpay,nnodes,nalgos)]
    return fmt,exp

def inflight_study():
    wl = 'YCSB'
    nnodes = [2]
    nalgos=['NO_WAIT','MVCC']
    ntif=[250]#,1000,5000,10000,15000,20000,25000,30000,35000,40000]
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","MAX_TXN_IN_FLIGHT"]
    exp = [[wl,n,cc,tif] for n,cc,tif in itertools.product(nnodes,nalgos,ntif)]
    return fmt,exp


# 7x5x2x2 = 140
def ycsb_scaling():
    wl = 'YCSB'
    nnodes = [1,2,4,8,16,32,64]
    nalgos=['NO_WAIT','WAIT_DIE','OCC','MVCC','TIMESTAMP','CALVIN']
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG"]
    exp = [[wl,n,cc] for n,cc in itertools.product(nnodes,nalgos)]
    return fmt,exp

def ycsb_scaling_2():
    wl = 'YCSB'
    nnodes = [1,2,4,8,16,32,64]
    nalgos=['NO_WAIT','WAIT_DIE','OCC','MVCC','TIMESTAMP','CALVIN']
    fmt = ["WORKLOAD","NODE_CNT","CC_ALG","PART_PER_TXN"]
    exp = [[wl,n,cc,2] for n,cc in itertools.product(nnodes,nalgos)]
    return fmt,exp


# 2x5x9x2 = 180
def ycsb_parts():
    wl = 'YCSB'
    nalgos=['NO_WAIT','WAIT_DIE','OCC','MVCC','TIMESTAMP','CALVIN']
    nparts = [2,4,6,8,10,12,14,16]
    rpq =  16
    fmt = ["WORKLOAD","REQ_PER_QUERY","PART_PER_TXN","CC_ALG","STRICT_PPT"]
    exp = [[wl,rpq,p,cc,1] for p,cc in itertools.product(nparts,nalgos)]
    return fmt,exp

# 2x5x2x7 = 140
def ycsb_contention_2():
    wl = 'YCSB'
    nalgos=['NO_WAIT','WAIT_DIE','OCC','MVCC','TIMESTAMP','CALVIN']
#    nalgos=['OCC']
    nparts = [2]
    a_perc=[0.0,0.01,0.02,0.03,0.05,0.06,0.07]
    fmt = ["WORKLOAD","ACCESS_PERC","CC_ALG","PART_PER_TXN"]
    exp = [[wl,a,cc,p] for a,cc,p in itertools.product(a_perc,nalgos,nparts)]
    return fmt,exp

def ycsb_contention_N():
    wl = 'YCSB'
    nalgos=['NO_WAIT','WAIT_DIE','OCC','MVCC','TIMESTAMP','CALVIN']
    a_perc=[0.0,0.01,0.02,0.03,0.05,0.06,0.07]
    fmt = ["WORKLOAD","ACCESS_PERC","CC_ALG"]
    exp = [[wl,a,cc] for a,cc in itertools.product(a_perc,nalgos)]
    return fmt,exp


def tputvlat_setup(summary,summary_cl,summary_seq,nfmt,nexp,x_name,v_name):
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


def tput_setup(summary,summary_cl,summary_seq,nfmt,nexp,x_name,v_name
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
        if "PART_PER_TXN" in fmt and e[fmt.index("PART_PER_TXN")] == 1 and ("NODE_CNT" == x_name or "NODE_CNT" == v_name):
            continue
#title = "System Tput "
        if title == "":
            for t in fmt_title:
                if t not in fmt: continue
                title+="{} {} ".format(SHORTNAMES[t],e[fmt.index(t)])
        tput(x_vals,v_vals,summary,summary_cl,summary_seq,cfg_fmt=fmt,cfg=list(e),xname=x_name,vname=v_name,title=title,extras=extras)

def line_rate_setup(summary,summary_cl,summary_seq,nfmt,nexp,x_name,v_name,key
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
     
def line_setup(summary,summary_cl,summary_seq,nfmt,nexp,x_name,v_name,key
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
        
def stacks_setup(summary,summary_seq,nfmt,nexp,x_name,keys,key_names=[],norm=False
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

def breakdown_setup(summary,summary_seq,nfmt,nexp,x_name,key_names=[],norm=False
        ,extras={}
        ):
    from plot_helper import time_breakdown
    x_vals = []
    exp = [list(e) for e in nexp]
    fmt = list(nfmt)
    print(x_name)
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


def ft_mode_plot(summary,summary_client,summary_seq):
    nfmt,nexp = ft_mode()
    x_name = "MAX_TXN_IN_FLIGHT"
    v_name = "CC_ALG"
    tput_setup(summary,summary_seq,nfmt,nexp,x_name,v_name)

def test_plot(summary,summary_client,summary_seq):
    nfmt,nexp = test()
#    tput_setup(summary,summary_client,nfmt,nexp,x_name="MAX_TXN_IN_FLIGHT",v_name="MODE")
    tput_setup(summary,summary_client,summary_seq,nfmt,nexp,x_name="NODE_CNT",v_name="MODE",extras={'PART_CNT':'NODE_CNT','CLIENT_NODE_CNT':'NODE_CNT','PART_PER_TXN':'NODE_CNT','NUM_WH':128,'PERC_PAYMENT':0.0})
#    tput_setup(summary,summary_client,nfmt,nexp,x_name="NODE_CNT",v_name="ZIPF_THETA")
#    tput_setup(summary,summary_client,nfmt,nexp,x_name="NODE_CNT",v_name="CC_ALG")
#    tput_setup(summary,summary_client,nfmt,nexp,x_name="NODE_CNT",v_name="MPR")
#    tput_setup(summary,summary_client,nfmt,nexp,x_name="NODE_CNT",v_name="MAX_TXN_IN_FLIGHT")
#    tput_setup(summary,summary_client,nfmt,nexp,x_name="MAX_TXN_IN_FLIGHT",v_name="NODE_CNT")

#    breakdown_setup(summary,nfmt,nexp,x_name="NODE_CNT",norm=True)
#    tput_setup(summary,summary_client,nfmt,nexp,x_name="WRITE_PERC",v_name="ZIPF_THETA")
#    tput_setup(summary,summary_client,nfmt,nexp,x_name="WRITE_PERC",v_name="ACCESS_PERC")
#    tput_setup(summary,summary_client,nfmt,nexp,x_name="NODE_CNT",v_name="WRITE_PERC")
#    tput_setup(summary,summary_client,nfmt,nexp,x_name="ACCESS_PERC",v_name="WRITE_PERC")

#    line_rate_setup(summary,summary_client,nfmt,nexp,x_name="MAX_TXN_IN_FLIGHT",v_name="CC_ALG",key='abort_cnt')
#    line_setup(summary,summary_client,nfmt,nexp,x_name="MAX_TXN_IN_FLIGHT",v_name="NODE_CNT",key='thd1')
#    line_setup(summary,summary_client,nfmt,nexp,x_name="MAX_TXN_IN_FLIGHT",v_name="NODE_CNT",key='txn_cnt')
#    line_rate_setup(summary,summary_client,nfmt,nexp,x_name="MAX_TXN_IN_FLIGHT",v_name="CC_ALG",key='abort_cnt')
#    line_setup(summary,summary_client,nfmt,nexp,x_name="MAX_TXN_IN_FLIGHT",v_name="CC_ALG",key='tot_avg_abort_row_cnt')
#    line_setup(summary,summary_client,nfmt,nexp,x_name="MAX_TXN_IN_FLIGHT",v_name="CC_ALG",key='time_validate')
#    line_setup(summary,summary_client,nfmt,nexp,x_name="NODE_CNT",v_name="ACCESS_PERC",key='cpu_ttl')
#    line_setup(summary,summary_client,nfmt,nexp,x_name="NODE_CNT",v_name="NODE_CNT",key='abort_cnt')
#    line_setup(summary,summary_client,nfmt,nexp,x_name="NODE_CNT",v_name="CC_ALG",key='tot_avg_abort_row_cnt')
#    line_rate_setup(summary,summary_client,nfmt,nexp,x_name="NODE_CNT",v_name="ACCESS_PERC",key='abort_cnt')
#    line_setup(summary,summary_client,nfmt,nexp,x_name="NODE_CNT",v_name="ACCESS_PERC",key='avg_abort_row_cnt')
#    line_rate_setup(summary,summary_client,nfmt,nexp,x_name="NODE_CNT",v_name="CC_ALG",key='mpq_cnt')
#    line_rate_setup(summary,summary_client,nfmt,nexp,x_name="NODE_CNT",v_name="CC_ALG",key='abort_cnt')
#    line_setup(summary,summary_client,nfmt,nexp,x_name="NODE_CNT",v_name="CC_ALG",key='avg_abort_row_cnt')
#    line_rate_setup(summary,summary_client,nfmt,nexp,x_name="NODE_CNT",v_name="NODE_CNT",key='abort_cnt')
#    line_setup(summary,summary_client,nfmt,nexp,x_name="NODE_CNT",v_name="NODE_CNT",key='latency')
#    line_setup(summary,summary_client,nfmt,nexp,x_name="NODE_CNT",v_name="ACCESS_PERC",key='cflt_cnt')
#    line_setup(summary,summary_client,nfmt,nexp,x_name="NODE_CNT",v_name="ACCESS_PERC",key='busy_cnt')
#    line_setup(summary,summary_client,nfmt,nexp,x_name="NODE_CNT",v_name="ACCESS_PERC",key='txn_cnt')
#    line_setup(summary,summary_client,nfmt,nexp,x_name="NODE_CNT",v_name="ACCESS_PERC",key='virt_mem_usage')
#    line_setup(summary,summary_client,nfmt,nexp,x_name="NODE_CNT",v_name="ACCESS_PERC",key='time_abort')
#    line_setup(summary,summary_client,nfmt,nexp,x_name="NODE_CNT",v_name="ACCESS_PERC",key='abort_from_ts')
#    line_setup(summary,summary_client,nfmt,nexp,x_name="NODE_CNT",v_name="ACCESS_PERC",key='time_validate')
#    tputvlat_setup(summary,summary_client,nfmt,nexp,x_name="NODE_CNT",v_name="MODE")

#    stacks_setup(summary,nfmt,nexp,x_name="MAX_TXN_IN_FLIGHT",keys=['thd1','thd2','thd3'],norm=False)
#    stacks_setup(summary,nfmt,nexp,x_name="MAX_TXN_IN_FLIGHT",keys=['txn_table_add','txn_table_get','txn_table0a','txn_table1a','txn_table0b','txn_table1b','txn_table2a','txn_table2'],norm=False)
#    stacks_setup(summary,nfmt,nexp,x_name="MAX_TXN_IN_FLIGHT"            ,keys=['type1','type2','type3','type4','type5','type6','type7','type8','type9','type10','type11','type12','type13','type14']            ,norm=False)
#    stacks_setup(summary,nfmt,nexp,x_name="MAX_TXN_IN_FLIGHT",keys=['thd1a','thd1b','thd1c','thd1d'],norm=False)
#    stacks_setup(summary,nfmt,nexp,x_name="MAX_TXN_IN_FLIGHT",keys=['rtxn1a','rtxn1b','rtxn2','rtxn3','rtxn4'],norm=False)

#    stacks_setup(summary,nfmt,nexp,x_name="NODE_CNT",keys=['thd1','thd2','thd3'],norm=False,key_names=['Getting work','Execution','Wrap-up'])
#    stacks_setup(summary,nfmt,nexp,x_name="NODE_CNT",keys=['txn_table_add','txn_table_get','txn_table_mints1','txn_table_mints2','txn_table0a','txn_table1a','txn_table0b','txn_table1b','txn_table2a'],norm=False)
#    stacks_setup(summary,nfmt,nexp,x_name="NODE_CNT",keys=['type1','type2','type3','type4','type5','type6','type7','type8','type9','type10','type11','type12','type13','type14'],key_names=['DONE','LOCK','UNLOCK','Rem QRY','FIN','LOCK RSP','UNLOCK RSP','QRY RSP','ACK','Exec','INIT','PREP','PASS','CLIENT'],norm=False)
#    stacks_setup(summary,nfmt,nexp,x_name="NODE_CNT",keys=['occ_val1','occ_val2a','occ_val2','occ_val3','occ_val4','occ_val5'],norm=False)
 #   stacks_setup(summary,nfmt,nexp,x_name="NODE_CNT",keys=['mvcc1','mvcc2','mvcc3','mvcc4','mvcc5','mvcc6','mvcc7','mvcc8'],norm=False)
#    stacks_setup(summary,nfmt,nexp,x_name="NODE_CNT",keys=['thd1','thd2','thd3'],norm=False,key_names=['Getting work','Execution','Wrap-up'])
#    stacks_setup(summary,nfmt,nexp,x_name="NODE_CNT",keys=['txn_table_add','txn_table_get','txn_table_mints1','txn_table_mints2','txn_table0a','txn_table1a','txn_table0b','txn_table1b','txn_table2a'],norm=False)
#    stacks_setup(summary,nfmt,nexp,x_name="NODE_CNT",keys=['type1','type2','type3','type4','type5','type6','type7','type8','type9','type10','type11','type12','type13','type14'],key_names=['DONE','LOCK','UNLOCK','Rem QRY','FIN','LOCK RSP','UNLOCK RSP','QRY RSP','ACK','Exec','INIT','PREP','PASS','CLIENT'],norm=False)
#    stacks_setup(summary,nfmt,nexp,x_name="NODE_CNT",keys=['occ_val1','occ_val2a','occ_val2','occ_val3','occ_val4','occ_val5'],norm=False)
#    stacks_setup(summary,nfmt,nexp,x_name="NODE_CNT",keys=['mvcc1','mvcc2','mvcc3','mvcc4','mvcc5','mvcc6','mvcc7','mvcc8'],norm=False)
#    stacks_setup(summary,nfmt,nexp,x_name="WRITE_PERC",keys=['thd1a','thd1c','thd1d'],key_names=['Prog stats','Spinning','Abort queue'],norm=False)
#    stacks_setup(summary,nfmt,nexp,x_name="MODE",keys=['rtxn1a','rtxn1b','rtxn2','rtxn3','rtxn4'],norm=False)
#    stacks_setup(summary,nfmt,nexp,x_name="MODE",keys=['row1','row2','row3'],norm=False)

#    line_setup(summary,summary_client,nfmt,nexp,x_name="MAX_TXN_IN_FLIGHT",v_name="MODE",key='cpu_ttl')

#    stacks_setup(summary,nfmt,nexp,x_name="NODE_CNT",keys=['thd1','thd2','thd3'],norm=False,key_names=['Prep work','Execution','Wrap-up'])
#    stacks_setup(summary,nfmt,nexp,x_name="NODE_CNT",keys=['txn_table_add','txn_table_get','txn_table0a','txn_table1a','txn_table0b','txn_table1b','txn_table2a'],norm=False)

#    stacks_setup(summary,nfmt,nexp,x_name="NODE_CNT"
#            ,keys=['type4','type5','type8','type9','type10','type12']
#            ,key_names=['REM QRY','FIN','QRY RSP','ACK','Exec','PREP']
#            ,norm=False)

#    stacks_setup(summary,nfmt,nexp,x_name="NODE_CNT"
#            ,keys=['type1','type2','type3','type4','type5','type6','type7','type8','type9','type10','type11','type12','type13','type14']
#            ,key_names=['DONE','LOCK','UNLOCK','Rem QRY','FIN','LOCK RSP','UNLOCK RSP','QRY RSP','ACK','Exec','INIT','PREP','PASS','CLIENT']
#            ,norm=False)
#    stacks_setup(summary,nfmt,nexp,x_name="CC_ALG",keys=['thd1a','thd1b','thd1c','thd1d'],norm=False)
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
    nrpq=[10]
    nparts=[16]
    network_delay = ["0UL","10000UL","100000UL","1000000UL","10000000UL","100000000UL"]
    ntxn=[3000000]
    exp = [[int(math.ceil(n)) if n > 1 else 1,n,txn,'YCSB',cc,m,ct,crt,t,rt,tif,z,1.0-wp,wp,p if p <= n else n,n if cc!='HSTORE' and cc!='HSTORE_SPEC' else t*n,nd] for n,ct,crt,t,rt,tif,z,wp,m,cc,p,txn,nd in itertools.product(nnodes,ncthreads,ncrthreads,nthreads,nrthreads,ntifs,nzipf,nwr_perc,nmpr,nalgos,nparts,ntxn,network_delay)]
    exp.sort()
    exp = list(k for k,_ in itertools.groupby(exp))
    return fmt[0],exp

def network_sweep_plot(summary,summary_client,summary_seq):
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

def network_experiment():
    fmt = fmt_nt
    nnodes = [1]
    cnodes = [1]
    ntest = ["true"]
    exp = [nnodes,cnodes,ntest]
    exp = [[n,c,t] for n,c,t in itertools.product(nnodes,cnodes,ntest)]
    return fmt[0],exp

def network_experiment_plot(all_exps,all_nodes,timestamps,summary_seq):
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

def tpcc_scaling_whset_plot(summary,summary_client,summary_seq):
    nfmt,nexp = tpcc_scaling_whset()
    tput_setup(summary,summary_client,summary_seq,nfmt,nexp,x_name="NODE_CNT",v_name="CC_ALG",extras={'PART_CNT':'NODE_CNT','CLIENT_NODE_CNT':'NODE_CNT','PART_PER_TXN':'NODE_CNT','NUM_WH':128,'PERC_PAYMENT':1.0},title="TPCC System Throughput, 128 warehouses, Payment only")
    tput_setup(summary,summary_client,summary_seq,nfmt,nexp,x_name="NODE_CNT",v_name="CC_ALG",extras={'PART_CNT':'NODE_CNT','CLIENT_NODE_CNT':'NODE_CNT','PART_PER_TXN':'NODE_CNT','NUM_WH':128,'PERC_PAYMENT':0.0},title="TPCC System Throughput, 128 warehouses, New order only")

def tpcc_scaling_plot(summary,summary_client,summary_seq):
    nfmt,nexp = tpcc_scaling()
    tput_setup(summary,summary_client,summary_seq,nfmt,nexp,x_name="NODE_CNT",v_name="CC_ALG",extras={'PART_CNT':'NODE_CNT','CLIENT_NODE_CNT':'NODE_CNT','PART_PER_TXN':'NODE_CNT','NUM_WH':'NODE_CNT','PERC_PAYMENT':1.0},title="TPCC System Throughput, N warehouses, Payment only")
    tput_setup(summary,summary_client,summary_seq,nfmt,nexp,x_name="NODE_CNT",v_name="CC_ALG",extras={'PART_CNT':'NODE_CNT','CLIENT_NODE_CNT':'NODE_CNT','PART_PER_TXN':'NODE_CNT','NUM_WH':'NODE_CNT','PERC_PAYMENT':0.0},title="TPCC System Throughput, N warehouses, New order only")
#    stacks_setup(summary,nfmt,nexp,x_name="NODE_CNT",keys=['thd1','thd2','thd3'],norm=False,key_names=['Getting work','Execution','Wrap-up'],extras={'PART_CNT':'NODE_CNT','CLIENT_NODE_CNT':'NODE_CNT','NUM_WH':128,'PART_PER_TXN':'NODE_CNT','PERC_PAYMENT':0.0})
#    stacks_setup(summary,nfmt,nexp,x_name="NODE_CNT",keys=['txn_table_add','txn_table_get','txn_table0a','txn_table1a','txn_table0b','txn_table1b','txn_table2a','txn_table2'],norm=False,extras={'PART_CNT':'NODE_CNT','CLIENT_NODE_CNT':'NODE_CNT','PART_PER_TXN':'NODE_CNT','NUM_WH':128})
#    stacks_setup(summary,nfmt,nexp,x_name="NODE_CNT"            ,keys=['type1','type2','type3','type4','type5','type6','type7','type8','type9','type10','type11','type12','type13','type14']            ,norm=False,extras={'PART_CNT':'NODE_CNT','CLIENT_NODE_CNT':'NODE_CNT','PART_PER_TXN':'NODE_CNT','NUM_WH':128})
#    stacks_setup(summary,nfmt,nexp,x_name="NODE_CNT"            ,keys=['part_cnt1','part_cnt2','part_cnt3','part_cnt4','part_cnt5','part_cnt6','part_cnt7','part_cnt8','part_cnt9','part_cnt10']            ,norm=False,extras={'PART_CNT':'NODE_CNT','CLIENT_NODE_CNT':'NODE_CNT','PART_PER_TXN':'NODE_CNT','NUM_WH':128})
#    stacks_setup(summary,nfmt,nexp,x_name="NODE_CNT",keys=['thd1a','thd1b','thd1c','thd1d'],norm=False,extras={'PART_CNT':'NODE_CNT','CLIENT_NODE_CNT':'NODE_CNT','PART_PER_TXN':'NODE_CNT','NUM_WH':128})
#    stacks_setup(summary,nfmt,nexp,x_name="NODE_CNT",keys=['rtxn1a','rtxn1b','rtxn2','rtxn3','rtxn4'],norm=False,extras={'PART_CNT':'NODE_CNT','CLIENT_NODE_CNT':'NODE_CNT','PART_PER_TXN':'NODE_CNT','NUM_WH':128})
#    line_rate_setup(summary,summary_client,nfmt,nexp,x_name="NODE_CNT",v_name="CC_ALG",key='thd1',extras={'PART_CNT':'NODE_CNT','CLIENT_NODE_CNT':'NODE_CNT','PART_PER_TXN':'NODE_CNT','NUM_WH':128})
#    line_rate_setup(summary,summary_client,nfmt,nexp,x_name="NODE_CNT",v_name="CC_ALG",key='abort_cnt',extras={'PART_CNT':'NODE_CNT','CLIENT_NODE_CNT':'NODE_CNT','PART_PER_TXN':'NODE_CNT','NUM_WH':128})
#    line_setup(summary,summary_client,nfmt,nexp,x_name="NODE_CNT",v_name="CC_ALG",key='tot_avg_abort_row_cnt',extras={'PART_CNT':'NODE_CNT','CLIENT_NODE_CNT':'NODE_CNT','PART_PER_TXN':'NODE_CNT','NUM_WH':128})
#    line_setup(summary,summary_client,nfmt,nexp,x_name="NODE_CNT",v_name="CC_ALG",key='txn_cnt',extras={'PART_CNT':'NODE_CNT','CLIENT_NODE_CNT':'NODE_CNT','PART_PER_TXN':'NODE_CNT','NUM_WH':128})

def ycsb_scaling_2_plot(summary,summary_client,summary_seq):
    nfmt,nexp = ycsb_scaling_2()
    tput_setup(summary,summary_client,summary_seq,nfmt,nexp,x_name="NODE_CNT",v_name="CC_ALG",title='YCSB System Throughput, default configs, 2 parts/txn')

def ycsb_scaling_plot(summary,summary_client,summary_seq):
    nfmt,nexp = ycsb_scaling()
    tput_setup(summary,summary_client,summary_seq,nfmt,nexp,x_name="NODE_CNT",v_name="CC_ALG",title='YCSB System Throughput, default configs, 10 parts/txn')
#    tput_setup(summary,summary_client,nfmt,nexp,x_name="NODE_CNT",v_name="CC_ALG",title='YCSB System Throughput, default configs, 2 parts/txn')
#    stacks_setup(summary,nfmt,nexp,x_name="NODE_CNT",keys=['thd1','thd2','thd3'],norm=False,key_names=['Getting work','Execution','Wrap-up'])
#    stacks_setup(summary,nfmt,nexp,x_name="NODE_CNT",keys=['txn_table_add','txn_table_get','txn_table0a','txn_table1a','txn_table0b','txn_table1b','txn_table2a','txn_table2'],norm=False)
#    stacks_setup(summary,nfmt,nexp,x_name="NODE_CNT"            ,keys=['type1','type2','type3','type4','type5','type6','type7','type8','type9','type10','type11','type12','type13','type14']            ,norm=False)
#    stacks_setup(summary,nfmt,nexp,x_name="NODE_CNT"            ,keys=['part_cnt1','part_cnt2','part_cnt3','part_cnt4','part_cnt5','part_cnt6','part_cnt7','part_cnt8','part_cnt9','part_cnt10']            ,norm=False)
 #   stacks_setup(summary,nfmt,nexp,x_name="NODE_CNT",keys=['thd1a','thd1b','thd1c','thd1d'],norm=False)
  #  stacks_setup(summary,nfmt,nexp,x_name="NODE_CNT",keys=['rtxn1a','rtxn1b','rtxn2','rtxn3','rtxn4'],norm=False)
   # line_rate_setup(summary,summary_client,nfmt,nexp,x_name="NODE_CNT",v_name="CC_ALG",key='abort_cnt')
   # line_setup(summary,summary_client,nfmt,nexp,x_name="NODE_CNT",v_name="CC_ALG",key='tot_avg_abort_row_cnt')
   # line_setup(summary,summary_client,nfmt,nexp,x_name="NODE_CNT",v_name="CC_ALG",key='txn_cnt')



def ycsb_contention_N_plot(summary,summary_client,summary_seq):
    nfmt,nexp = ycsb_contention_N()
    tput_setup(summary,summary_client,summary_seq,nfmt,nexp,x_name="ACCESS_PERC",v_name="CC_ALG",extras={'PART_CNT':'NODE_CNT','CLIENT_NODE_CNT':'NODE_CNT','PART_PER_TXN':'NODE_CNT'},title='YCSB Contention 16 Nodes N parts/txn') 
def ycsb_contention_2_plot(summary,summary_client,summary_seq):
    nfmt,nexp = ycsb_contention_2()
    tput_setup(summary,summary_client,summary_seq,nfmt,nexp,x_name="ACCESS_PERC",v_name="CC_ALG",extras={'PART_CNT':'NODE_CNT','CLIENT_NODE_CNT':'NODE_CNT'},title='YCSB Contention 16 Nodes 2 parts/txn') 

def ycsb_parts_plot(summary,summary_client,summary_seq):
    nfmt,nexp = ycsb_parts()

    tput_setup(summary,summary_client,summary_seq,nfmt,nexp,x_name="PART_PER_TXN",v_name="CC_ALG",title='YCSB Partition Sweep',extras={'PART_CNT':'NODE_CNT','CLIENT_NODE_CNT':'NODE_CNT'})
#    stacks_setup(summary,nfmt,nexp,x_name="PART_PER_TXN",keys=['thd1','thd2','thd3'],norm=False,key_names=['Getting work','Execution','Wrap-up'],extras={'PART_CNT':'NODE_CNT','CLIENT_NODE_CNT':'NODE_CNT'})
#    stacks_setup(summary,nfmt,nexp,x_name="PART_PER_TXN",keys=['txn_table_add','txn_table_get','txn_table0a','txn_table1a','txn_table0b','txn_table1b','txn_table2a','txn_table2'],norm=False,extras={'PART_CNT':'NODE_CNT','CLIENT_NODE_CNT':'NODE_CNT'})
#    stacks_setup(summary,nfmt,nexp,x_name="PART_PER_TXN"            ,keys=['type1','type2','type3','type4','type5','type6','type7','type8','type9','type10','type11','type12','type13','type14']            ,norm=False,extras={'PART_CNT':'NODE_CNT','CLIENT_NODE_CNT':'NODE_CNT'})
#    stacks_setup(summary,nfmt,nexp,x_name="PART_PER_TXN",keys=['thd1a','thd1b','thd1c','thd1d'],norm=False,extras={'PART_CNT':'NODE_CNT','CLIENT_NODE_CNT':'NODE_CNT'})
#    stacks_setup(summary,nfmt,nexp,x_name="PART_PER_TXN",keys=['rtxn1a','rtxn1b','rtxn2','rtxn3','rtxn4'],norm=False,extras={'PART_CNT':'NODE_CNT','CLIENT_NODE_CNT':'NODE_CNT'})
#    line_rate_setup(summary,summary_client,nfmt,nexp,x_name="PART_PER_TXN",v_name="CC_ALG",key='abort_cnt',extras={'PART_CNT':'NODE_CNT','CLIENT_NODE_CNT':'NODE_CNT'})
#    line_setup(summary,summary_client,nfmt,nexp,x_name="PART_PER_TXN",v_name="CC_ALG",key='tot_avg_abort_row_cnt',extras={'PART_CNT':'NODE_CNT','CLIENT_NODE_CNT':'NODE_CNT'})





experiment_map = {
    'test': test,
    'inflight_study': inflight_study,
    'ycsb_scaling': ycsb_scaling,
    'ycsb_scaling_2': ycsb_scaling_2,
    'tpcc_scaling': tpcc_scaling,
    'tpcc_priorities': tpcc_priorities,
    'tpcc_modes': tpcc_modes,
    'tpcc_scaling_whset': tpcc_scaling_whset,
    'ycsb_parts': ycsb_parts,
    'ycsb_contention_2': ycsb_contention_2,
    'ycsb_contention_N': ycsb_contention_N,
    'ycsb_scaling_plot': ycsb_scaling_plot,
    'ycsb_scaling_2_plot': ycsb_scaling_2_plot,
    'tpcc_scaling_plot': tpcc_scaling_plot,
    'tpcc_scaling_whset_plot': tpcc_scaling_whset_plot,
    'ycsb_parts_plot': ycsb_parts_plot,
    'ycsb_contention_2_plot': ycsb_contention_2_plot,
    'ycsb_contention_N_plot': ycsb_contention_N_plot,
    'ft_mode': ft_mode,
    'ft_mode_plot': ft_mode_plot,
    'test_plot': test_plot,
    'network_sweep': network_sweep,
    'network_sweep_plot': network_sweep_plot,
    'network_experiment' : network_experiment,
    'network_experiment_plot' : network_experiment_plot,
}


# Default values for variable configurations
configs = {
    "NODE_CNT" : 16,
    "THREAD_CNT": 6,
    "REM_THREAD_CNT": 1,
    "SEND_THREAD_CNT": 1,
    "CLIENT_NODE_CNT" : "NODE_CNT",
    "CLIENT_THREAD_CNT" : 2,
    "CLIENT_REM_THREAD_CNT" : 1,
    "CLIENT_SEND_THREAD_CNT" : 4,
    "MAX_TXN_PER_PART" : 1000000,
    "WORKLOAD" : "YCSB",
    "CC_ALG" : "WAIT_DIE",
    "MPR" : 1.0,
    "TPORT_TYPE":"\"ipc\"",
    "TPORT_TYPE_IPC":"true",
    "TPORT_PORT":"\"_.ipc\"",
    "PART_CNT": "NODE_CNT", #2,
    "PART_PER_TXN": "NODE_CNT",
    "MAX_TXN_IN_FLIGHT": 50000,
    "NETWORK_DELAY": '0UL',
    "DONE_TIMER": "1 * 60 * BILLION // ~2 minutes",
    "PROG_TIMER" : "10 * BILLION // in s",
    "NETWORK_TEST" : "false",
    "ABORT_PENALTY": "1 * 1000000UL   // in ns.",
    "ABORT_PENALTY_MAX": "100 * 1000000UL   // in ns.",
    "MSG_TIME_LIMIT": "10 * 1000000UL",
    "MSG_SIZE_MAX": 4096,
#    "PRT_LAT_DISTR": "true",
    "TXN_WRITE_PERC":0.5,
    "PRIORITY":"PRIORITY_ACTIVE",
#YCSB
    "INIT_PARALLELISM" : 8, 
    "TUP_WRITE_PERC":0.5,
    "ZIPF_THETA":0.6,
    "ACCESS_PERC":0.03,
    "DATA_PERC": 100,
    "REQ_PER_QUERY": 10, #16
    "SYNTH_TABLE_SIZE":"2097152*8",
#TPCC
    "NUM_WH": 'PART_CNT',
    "PERC_PAYMENT":0.0,
    "DEBUG_DISTR":"false",
    "DEBUG_ALLOC":"false",
    "MODE":"NORMAL_MODE",
    "SHMEM_ENV":"false",
    "STRICT_PPT":0,
}

config_names = fmt_ycsb[0]
