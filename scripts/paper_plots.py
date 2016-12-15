from experiments import *
import pprint

def plot_all():
    return 0

def ppr_ycsb_scaling_plot(summary,summary_cl):
    from experiments import ycsb_scaling
    from helper import plot_prep
    from plot_helper import tput,time_breakdown,latency,abort_rate,latency_breakdown,time_breakdown_line
    nfmt,nexp = ycsb_scaling()
    x_name = "NODE_CNT"
    v_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"TXN_WRITE_PERC":0.0,"ZIPF_THETA":0.0})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_scaling_readonly",xlab="Server Count",new_cfgs=lst,logscalex=True)
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"TXN_WRITE_PERC":0.0,"ZIPF_THETA":0.0})
    latency(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="latency_ycsb_scaling_readonly",xlab="Server Count",new_cfgs=lst,logscalex=True)
    nfmt,nexp = ycsb_scaling()
    x_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,'',constants={"NODE_CNT":16,"TXN_WRITE_PERC":0.0,"ZIPF_THETA":0.0})
    time_breakdown(x_vals,summary,xname=x_name,title='',name='breakdown_ycsb_scaling_readonly',cfg_fmt=fmt,cfg=list(exp),normalized=True,new_cfgs=lst)

    x_name = "NODE_CNT"
    v_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"TXN_WRITE_PERC":0.5,"ZIPF_THETA":0.6})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_scaling_med",xlab="Server Count",new_cfgs=lst,logscalex=True)
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"TXN_WRITE_PERC":0.5,"ZIPF_THETA":0.6})
    latency(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="latency_ycsb_scaling_med",xlab="Server Count",new_cfgs=lst,logscalex=True)
    nfmt,nexp = ycsb_scaling()
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"TXN_WRITE_PERC":0.5,"ZIPF_THETA":0.6})
    time_breakdown_line(x_vals,v_vals,summary,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="time_break_line_ycsb_scaling_med",xlab="Server Count",new_cfgs=lst,logscalex=True)
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"TXN_WRITE_PERC":0.5,"ZIPF_THETA":0.6})
    abort_rate(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="aborts_ycsb_scaling_med",xlab="Server Count",new_cfgs=lst,logscalex=True)
    nfmt,nexp = ycsb_scaling()
    x_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,'',constants={"NODE_CNT":16,"TXN_WRITE_PERC":0.5,"ZIPF_THETA":0.6})
    time_breakdown(x_vals,summary,xname=x_name,title='',name='breakdown_ycsb_scaling_med',cfg_fmt=fmt,cfg=list(exp),normalized=True,new_cfgs=lst)
    nfmt,nexp = ycsb_scaling()
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,'',constants={"NODE_CNT":16,"TXN_WRITE_PERC":0.5,"ZIPF_THETA":0.6})
    latency_breakdown(x_vals,summary,xname=x_name,title='',name='latency_breakdown_ycsb_scaling_med',cfg_fmt=fmt,cfg=list(exp),normalized=False,new_cfgs=lst)


    x_name = "NODE_CNT"
    v_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"TXN_WRITE_PERC":0.5,"ZIPF_THETA":0.7})
    time_breakdown_line(x_vals,v_vals,summary,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="time_break_line_ycsb_scaling_high",xlab="Server Count",new_cfgs=lst,logscalex=True)
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_scaling_high",xlab="Server Count",new_cfgs=lst,logscalex=True)
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"TXN_WRITE_PERC":0.5,"ZIPF_THETA":0.7})
    latency(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="latency_ycsb_scaling_high",xlab="Server Count",new_cfgs=lst,logscalex=True)
    abort_rate(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="aborts_ycsb_scaling_high",xlab="Server Count",new_cfgs=lst,logscalex=True)
    nfmt,nexp = ycsb_scaling()
    x_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,'',constants={"NODE_CNT":16,"TXN_WRITE_PERC":0.5,"ZIPF_THETA":0.7})
    time_breakdown(x_vals,summary,xname=x_name,title='',name='breakdown_ycsb_scaling_high',cfg_fmt=fmt,cfg=list(exp),normalized=True,new_cfgs=lst)

def ppr_pps_scaling_plot(summary,summary_cl):
    from experiments import pps_scaling
    from helper import plot_prep
    from plot_helper import tput,time_breakdown,latency,abort_rate,latency_breakdown
    nfmt,nexp = pps_scaling()
    x_name = "NODE_CNT"
    v_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_pps_scaling",xlab="Server Count",new_cfgs=lst,logscalex=True)
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={})
    latency(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="latency_pps_scaling",xlab="Server Count",new_cfgs=lst,logscalex=True)
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={})
    abort_rate(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="aborts_pps_scaling",xlab="Server Count",new_cfgs=lst,logscalex=True)
    x_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,'',constants={"NODE_CNT":16})
    latency_breakdown(x_vals,summary,xname=x_name,title='',name='latency_breakdown_pps_scaling',cfg_fmt=fmt,cfg=list(exp),normalized=False,new_cfgs=lst)

def ppr_ecwc_plot(summary,summary_cl):
    from experiments import ecwc
    from helper import plot_prep
    from plot_helper import tput,time_breakdown,latency,abort_rate,latency_breakdown,tput_stack
    nfmt,nexp = ecwc()
    x_name = "CC_ALG"
    v_name = ""
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"TXN_WRITE_PERC":0.5,"ZIPF_THETA":0.6})
    tput_stack(x_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,title="",name="tput_ecwc",new_cfgs=lst)

def ppr_ycsb_scaling_abort_plot(summary,summary_cl):
    from experiments import ycsb_scaling_abort
    from helper import plot_prep
    from plot_helper import tput,time_breakdown
    nfmt,nexp = ycsb_scaling_abort()
    x_name = "NODE_CNT"
    v_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"TXN_WRITE_PERC":0.5,"ZIPF_THETA":0.6,"MAX_TXN_IN_FLIGHT":10000})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_scaling_abort_med",xlab="Server Count",new_cfgs=lst,logscalex=True)
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"TXN_WRITE_PERC":0.5,"ZIPF_THETA":0.7,"MAX_TXN_IN_FLIGHT":10000})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_scaling_abort_high",xlab="Server Count",new_cfgs=lst,logscalex=True)


def ppr_tpcc_scaling_plot(summary,summary_cl):
    from experiments import tpcc_scaling,tpcc_scaling1,tpcc_scaling2
    from helper import plot_prep
    from plot_helper import tput
    x_name = "NODE_CNT"
    v_name = "CC_ALG"
#    extras = {'PART_CNT':'NODE_CNT','CLIENT_NODE_CNT':'NODE_CNT','PART_PER_TXN':'NODE_CNT','NUM_WH':128,'PERC_PAYMENT':0.0}

    nfmt,nexp = tpcc_scaling2()
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"PERC_PAYMENT":0.0})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_tpcc_neworder_4",xlab="Server Count",logscalex=True,new_cfgs=lst)
    nfmt,nexp = tpcc_scaling2()
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"PERC_PAYMENT":1.0})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_tpcc_payment_4",xlab="Server Count",logscalex=True,new_cfgs=lst)

    nfmt,nexp = tpcc_scaling()
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"PERC_PAYMENT":0.0})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_tpcc_neworder_10",xlab="Server Count",logscalex=True,new_cfgs=lst)
    nfmt,nexp = tpcc_scaling()
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"PERC_PAYMENT":1.0})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_tpcc_payment_10",xlab="Server Count",logscalex=True,new_cfgs=lst)



    nfmt,nexp = tpcc_scaling1()
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"PERC_PAYMENT":0.0})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_tpcc_neworder",xlab="Server Count",logscalex=True,new_cfgs=lst)
    nfmt,nexp = tpcc_scaling1()
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"PERC_PAYMENT":1.0})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_tpcc_payment",xlab="Server Count",logscalex=True,new_cfgs=lst)

def ppr_ycsb_partitions_plot(summary,summary_cl):
    from experiments import ycsb_partitions,ycsb_partitions_distr
    from helper import plot_prep
    from plot_helper import tput
    nfmt,nexp = ycsb_partitions()
    x_name = "PART_PER_TXN"
    v_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"MAX_TXN_IN_FLIGHT":10000})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_partitions",xlab="Partitions Accessed",new_cfgs=lst)
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"MAX_TXN_IN_FLIGHT":12000})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_partitions_12k",xlab="Partitions Accessed",new_cfgs=lst)

    nfmt,nexp = ycsb_partitions_distr()
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"MAX_TXN_IN_FLIGHT":10000})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_partitions_distr",xlab="Partitions Accessed",new_cfgs=lst)

def ppr_ycsb_partitions_abort_plot(summary,summary_cl):
    from experiments import ycsb_partitions_abort
    from helper import plot_prep
    from plot_helper import tput
    nfmt,nexp = ycsb_partitions_abort()
    x_name = "PART_PER_TXN"
    v_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"MAX_TXN_IN_FLIGHT":10000})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_partitions_abort",xlab="Partitions Accessed",new_cfgs=lst)
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"MAX_TXN_IN_FLIGHT":12000})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_partitions_12k",xlab="Partitions Accessed",new_cfgs=lst)



def ppr_ycsb_writes_plot(summary,summary_cl):
    from experiments import ycsb_writes   
    from helper import plot_prep
    from plot_helper import tput
    nfmt,nexp = ycsb_writes()
    x_name = "TXN_WRITE_PERC"
    v_name = "CC_ALG"

    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"NODE_CNT":16,"ZIPF_THETA":0.6,"MAX_TXN_IN_FLIGHT":10000})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_writes_16",xlab="% of Update Transactions",new_cfgs=lst)
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"NODE_CNT":16,"ZIPF_THETA":0.6,"MAX_TXN_IN_FLIGHT":10000})
    time_breakdown(x_vals,summary,xname=x_name,title='',name='breakdown_ycsb_writes',cfg_fmt=fmt,cfg=list(exp),normalized=True,new_cfgs=lst)
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"NODE_CNT":16,"ZIPF_THETA":0.6,"MAX_TXN_IN_FLIGHT":12000})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_writes_16_12k",xlab="% of Update Transactions",new_cfgs=lst)

def ppr_ycsb_skew_abort_plot(summary,summary_cl):
    from experiments import ycsb_skew_abort   
    from helper import plot_prep
    from plot_helper import tput
    nfmt,nexp = ycsb_skew_abort()
    x_name = "ZIPF_THETA"
    v_name = "CC_ALG"

    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"NODE_CNT":16})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_skew_abort_16",xlab="Skew Factor (Theta)",new_cfgs=lst)

    x_name = "NODE_CNT"
    nfmt,nexp = ycsb_skew_abort()
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"TXN_WRITE_PERC":0.5,"ZIPF_THETA":0.6})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_scaling_abort",xlab="Server Count",new_cfgs=lst,logscalex=True)



def ppr_ycsb_skew_plot(summary,summary_cl):
    from experiments import ycsb_skew   
    from helper import plot_prep
    from plot_helper import tput
    nfmt,nexp = ycsb_skew()
    x_name = "ZIPF_THETA"
    v_name = "CC_ALG"

    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"NODE_CNT":2})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_skew_2",xlab="Zipf Theta",new_cfgs=lst,ylimit=120)

    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"NODE_CNT":4})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_skew_4",xlab="Zipf Theta",new_cfgs=lst,ylimit=120)

    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"NODE_CNT":8})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_skew_8",xlab="Zipf Theta",new_cfgs=lst,ylimit=120)

    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"NODE_CNT":16})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_skew_16",xlab="Skew Factor (Theta)",new_cfgs=lst)

def ppr_isolation_levels_plot(summary,summary_cl):
    from experiments import isolation_levels 
    from helper import plot_prep
    from plot_helper import tput
    nfmt,nexp = isolation_levels()
    x_name = "NODE_CNT"
    v_name = "ISOLATION_LEVEL"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={'ZIPF_THETA':0.6})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_gold",xlab="Server Count",logscalex=True,new_cfgs=lst,legend=True)

def ppr_network_plot(summary,summary_cl):
    from experiments import network_sweep
    from helper import plot_prep
    from plot_helper import tput
    nfmt,nexp = network_sweep()
    v_name = "CC_ALG"
    x_name = "NETWORK_DELAY"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"TXN_WRITE_PERC":0.5,"ZIPF_THETA":0.6,"NODE_CNT":2})
    x_vals = [float(v)/1000 for v in x_vals]
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_network",xlab="Network Latency (ms)",new_cfgs=lst,logscalex=True)
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"TXN_WRITE_PERC":0.5,"ZIPF_THETA":0.6,"NODE_CNT":8})
    tput(x_vals,v_vals,summary,summary_cl,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_network_8",xlab="Network Latency (ms)",new_cfgs=lst,logscalex=True)


