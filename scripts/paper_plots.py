from experiments import *
import pprint

def plot_all():
    return 0

def ppr_ycsb_scaling_optimal_load_plot(summary,summary_cl,summary_seq):
    from experiments import ycsb_writes_optimal_load
    from helper import plot_prep
    from plot_helper import tput,time_breakdown
    nfmt,nexp = ycsb_writes_optimal_load()
    x_name = "NODE_CNT"
    v_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"TUP_WRITE_PERC":0.0})
    tput(x_vals,v_vals,summary,summary_cl,summary_seq,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_scaling_readonly",xlab="Server Count",new_cfgs=lst,ylimit=0.14,logscalex=True)
    nfmt,nexp = ycsb_writes_optimal_load()
    x_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,'',constants={'NODE_CNT':16,"TUP_WRITE_PERC":0.0})
    time_breakdown(x_vals,summary,xname=x_name,title='',name='breakdown_ycsb_scaling_readonly',cfg_fmt=fmt,cfg=list(exp),normalized=True,new_cfgs=lst)

    x_name = "NODE_CNT"
    v_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"TUP_WRITE_PERC":0.1})
    tput(x_vals,v_vals,summary,summary_cl,summary_seq,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_scaling_low",xlab="Server Count",new_cfgs=lst,ylimit=0.14,logscalex=True)
    nfmt,nexp = ycsb_writes_optimal_load()
    x_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,'',constants={'NODE_CNT':16,"TUP_WRITE_PERC":0.1})
    time_breakdown(x_vals,summary,xname=x_name,title='',name='breakdown_ycsb_scaling_low',cfg_fmt=fmt,cfg=list(exp),normalized=True,new_cfgs=lst)


    x_name = "NODE_CNT"
    v_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"TUP_WRITE_PERC":0.2})
    tput(x_vals,v_vals,summary,summary_cl,summary_seq,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_scaling_med",xlab="Server Count",new_cfgs=lst,ylimit=0.14,logscalex=True)
    nfmt,nexp = ycsb_writes_optimal_load()
    x_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,'',constants={'NODE_CNT':16,"TUP_WRITE_PERC":0.2})
    time_breakdown(x_vals,summary,xname=x_name,title='',name='breakdown_ycsb_scaling_med',cfg_fmt=fmt,cfg=list(exp),normalized=True,new_cfgs=lst)

    x_name = "NODE_CNT"
    v_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"TUP_WRITE_PERC":0.5})
    tput(x_vals,v_vals,summary,summary_cl,summary_seq,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_scaling_high",xlab="Server Count",new_cfgs=lst,ylimit=0.14,logscalex=True)
    nfmt,nexp = ycsb_writes_optimal_load()
    x_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,'',constants={'NODE_CNT':16,"TUP_WRITE_PERC":0.5})
    time_breakdown(x_vals,summary,xname=x_name,title='',name='breakdown_ycsb_scaling_high',cfg_fmt=fmt,cfg=list(exp),normalized=True,new_cfgs=lst)


def ppr_ycsb_scaling_plot(summary,summary_cl,summary_seq):
    from experiments import ycsb_writes
    from helper import plot_prep
    from plot_helper import tput,time_breakdown
    nfmt,nexp = ycsb_writes()
    x_name = "NODE_CNT"
    v_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"TUP_WRITE_PERC":0.0})
    tput(x_vals,v_vals,summary,summary_cl,summary_seq,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_scaling_readonly",xlab="Server Count",new_cfgs=lst,ylimit=0.14,logscalex=True)
    nfmt,nexp = ycsb_writes()
    x_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,'',constants={'NODE_CNT':16,"TUP_WRITE_PERC":0.0})
    time_breakdown(x_vals,summary,xname=x_name,title='',name='breakdown_ycsb_scaling_readonly',cfg_fmt=fmt,cfg=list(exp),normalized=True,new_cfgs=lst)

    x_name = "NODE_CNT"
    v_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"TUP_WRITE_PERC":0.1})
    tput(x_vals,v_vals,summary,summary_cl,summary_seq,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_scaling_low",xlab="Server Count",new_cfgs=lst,ylimit=0.14,logscalex=True)
    nfmt,nexp = ycsb_writes()
    x_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,'',constants={'NODE_CNT':16,"TUP_WRITE_PERC":0.1})
    time_breakdown(x_vals,summary,xname=x_name,title='',name='breakdown_ycsb_scaling_low',cfg_fmt=fmt,cfg=list(exp),normalized=True,new_cfgs=lst)


    x_name = "NODE_CNT"
    v_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"TUP_WRITE_PERC":0.2})
    tput(x_vals,v_vals,summary,summary_cl,summary_seq,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_scaling_med",xlab="Server Count",new_cfgs=lst,ylimit=0.14,logscalex=True)
    nfmt,nexp = ycsb_writes()
    x_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,'',constants={'NODE_CNT':16,"TUP_WRITE_PERC":0.2})
    time_breakdown(x_vals,summary,xname=x_name,title='',name='breakdown_ycsb_scaling_med',cfg_fmt=fmt,cfg=list(exp),normalized=True,new_cfgs=lst)

    x_name = "NODE_CNT"
    v_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"TUP_WRITE_PERC":0.5})
    tput(x_vals,v_vals,summary,summary_cl,summary_seq,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_scaling_high",xlab="Server Count",new_cfgs=lst,ylimit=0.14,logscalex=True)
    nfmt,nexp = ycsb_writes()
    x_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,'',constants={'NODE_CNT':16,"TUP_WRITE_PERC":0.5})
    time_breakdown(x_vals,summary,xname=x_name,title='',name='breakdown_ycsb_scaling_high',cfg_fmt=fmt,cfg=list(exp),normalized=True,new_cfgs=lst)

def ppr_tpcc_plot(summary,summary_cl,summary_seq):
    from experiments import tpcc_scaling_whset
    from helper import plot_prep
    from plot_helper import tput
    nfmt,nexp = tpcc_scaling_whset()
    x_name = "NODE_CNT"
    v_name = "CC_ALG"
    extras = {'PART_CNT':'NODE_CNT','CLIENT_NODE_CNT':'NODE_CNT','PART_PER_TXN':'NODE_CNT','NUM_WH':128,'PERC_PAYMENT':1.0}
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,extras=extras)
    tput(x_vals,v_vals,summary,summary_cl,summary_seq,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_tpcc_pay",xlab="Server Count",extras=extras,logscalex=True)
    nfmt,nexp = tpcc_scaling_whset()
    x_name = "NODE_CNT"
    v_name = "CC_ALG"
    extras = {'PART_CNT':'NODE_CNT','CLIENT_NODE_CNT':'NODE_CNT','PART_PER_TXN':'NODE_CNT','NUM_WH':128,'PERC_PAYMENT':0.0}
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,extras=extras)
    tput(x_vals,v_vals,summary,summary_cl,summary_seq,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_tpcc_neworder",xlab="Server Count",extras=extras,logscalex=True)
    nfmt,nexp = tpcc_scaling_whset()
    x_name = "NODE_CNT"
    v_name = "CC_ALG"
    extras = {'PART_CNT':'NODE_CNT','CLIENT_NODE_CNT':'NODE_CNT','PART_PER_TXN':'NODE_CNT','NUM_WH':128,'PERC_PAYMENT':0.5}
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,extras=extras)
    tput(x_vals,v_vals,summary,summary_cl,summary_seq,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_tpcc_mix",xlab="Server Count",extras=extras,logscalex=True)


def ppr_tpcc_pay_plot(summary,summary_cl,summary_seq):
    from experiments import tpcc_scaling_whset
    from helper import plot_prep
    from plot_helper import tput
    nfmt,nexp = tpcc_scaling_whset()
    x_name = "NODE_CNT"
    v_name = "CC_ALG"
    extras = {'PART_CNT':'NODE_CNT','CLIENT_NODE_CNT':'NODE_CNT','PART_PER_TXN':'NODE_CNT','NUM_WH':128,'PERC_PAYMENT':1.0}
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,extras=extras)
    tput(x_vals,v_vals,summary,summary_cl,summary_seq,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_tpcc_pay",xlab="Server Count",extras=extras)
    nfmt,nexp = tpcc_scaling_whset()
    x_name = "NODE_CNT"
    v_name = "CC_ALG"
    extras = {'PART_CNT':'NODE_CNT','CLIENT_NODE_CNT':'NODE_CNT','PART_PER_TXN':'NODE_CNT','NUM_WH':128,'PERC_PAYMENT':0.0}
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,extras=extras)
    tput(x_vals,v_vals,summary,summary_cl,summary_seq,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_tpcc_neworder",xlab="Server Count",extras=extras)
    nfmt,nexp = tpcc_scaling_whset()
    x_name = "NODE_CNT"
    v_name = "CC_ALG"
    extras = {'PART_CNT':'NODE_CNT','CLIENT_NODE_CNT':'NODE_CNT','PART_PER_TXN':'NODE_CNT','NUM_WH':128,'PERC_PAYMENT':0.5}
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,extras=extras)
    tput(x_vals,v_vals,summary,summary_cl,summary_seq,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_tpcc_mix",xlab="Server Count",extras=extras)



def ppr_tpcc_neworder_plot(summary,summary_cl,summary_seq):
    from experiments import tpcc_scaling_whset
    from helper import plot_prep
    from plot_helper import tput
    nfmt,nexp = tpcc_scaling_whset()
    x_name = "NODE_CNT"
    v_name = "CC_ALG"
    extras = {'PART_CNT':'NODE_CNT','CLIENT_NODE_CNT':'NODE_CNT','PART_PER_TXN':'NODE_CNT','NUM_WH':128,'PERC_PAYMENT':0.0}
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,extras=extras)
    tput(x_vals,v_vals,summary,summary_cl,summary_seq,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_tpcc_neworder",xlab="Server Count",extras=extras)

def ppr_ycsb_parts_plot(summary,summary_cl,summary_seq):
    from experiments import ycsb_parts
    from helper import plot_prep
    from plot_helper import tput
    nfmt,nexp = ycsb_parts()
    x_name = "PART_PER_TXN"
    v_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name)
    pp = pprint.PrettyPrinter()
    pp.pprint(lst)
    tput(x_vals,v_vals,summary,summary_cl,summary_seq,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_parts",xlab="Partitions Accessed",new_cfgs=lst)

def ppr_ycsb_contention_plot(summary,summary_cl,summary_seq):
    from experiments import ycsb_contention_2_nodesweep
    from helper import plot_prep
    from plot_helper import tput
    nfmt,nexp = ycsb_contention_2_nodesweep()
    x_name = "ACCESS_PERC"
    v_name = "CC_ALG"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"NODE_CNT":1})
    tput(x_vals,v_vals,summary,summary_cl,summary_seq,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_contention_1",xlab="Hot Data Access %",new_cfgs=lst,ylimit=0.7)

    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"NODE_CNT":2})
    tput(x_vals,v_vals,summary,summary_cl,summary_seq,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_contention_2",xlab="Hot Data Access %",new_cfgs=lst,ylimit=0.7)

    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"NODE_CNT":4})
    tput(x_vals,v_vals,summary,summary_cl,summary_seq,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_contention_4",xlab="Hot Data Access %",new_cfgs=lst,ylimit=0.7)

    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"NODE_CNT":8})
    tput(x_vals,v_vals,summary,summary_cl,summary_seq,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_contention_8",xlab="Hot Data Access %",new_cfgs=lst,ylimit=0.7)

    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"NODE_CNT":16})
    tput(x_vals,v_vals,summary,summary_cl,summary_seq,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_contention_16",xlab="Hot Data Access %",new_cfgs=lst,ylimit=0.7)

    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name,constants={"NODE_CNT":32})
    tput(x_vals,v_vals,summary,summary_cl,summary_seq,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_contention_32",xlab="Hot Data Access %",new_cfgs=lst,ylimit=0.7)

def ppr_ycsb_gold_plot(summary,summary_cl,summary_seq):
    from experiments import ycsb_gold
    from helper import plot_prep
    from plot_helper import tput
    nfmt,nexp = ycsb_gold()
    x_name = "NODE_CNT"
    v_name = "MODE"
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name)
    tput(x_vals,v_vals,summary,summary_cl,summary_seq,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_gold",xlab="Server Count",logscalex=True)

def ppr_ycsb_readonly_plot(summary,summary_cl,summary_seq):
    from experiments import ycsb_readonly
    from helper import plot_prep
    from plot_helper import tput
    nfmt,nexp = ycsb_readonly()
    x_name = "NODE_CNT"
    v_name = "CC_ALG"
    extras = {'CLIENT_NODE_CNT':'NODE_CNT','PART_PER_TXN':'NODE_CNT','TUP_WRITE_PERC':0}
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name)
    tput(x_vals,v_vals,summary,summary_cl,summary_seq,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_readonly",xlab="Server Count")

def ppr_ycsb_medwrite_plot(summary,summary_cl,summary_seq):
    from experiments import ycsb_medwrite
    from helper import plot_prep
    from plot_helper import tput
    nfmt,nexp = ycsb_medwrite()
    x_name = "NODE_CNT"
    v_name = "CC_ALG"
    extras = {'CLIENT_NODE_CNT':'NODE_CNT','PART_PER_TXN':'NODE_CNT','TUP_WRITE_PERC':0.2}
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name)
    tput(x_vals,v_vals,summary,summary_cl,summary_seq,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_medwrite",xlab="Server Count")

def ppr_ycsb_load_ro_plot(summary,summary_cl,summary_seq):
    from experiments import ycsb_load_ro
    from helper import plot_prep
    from plot_helper import tput
    nfmt,nexp = ycsb_load_ro()
    x_name = "MAX_TXN_IN_FLIGHT"
    v_name = "CC_ALG"
    extras = {'CLIENT_NODE_CNT':'NODE_CNT','PART_PER_TXN':'NODE_CNT'}
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name)
    tput(x_vals,v_vals,summary,summary_cl,summary_seq,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_load_ro",xlab="Load (txns/server)")


def ppr_ycsb_load_plot(summary,summary_cl,summary_seq):
    from experiments import ycsb_load_small
    from helper import plot_prep
    from plot_helper import tput
    nfmt,nexp = ycsb_load_small()
    x_name = "MAX_TXN_IN_FLIGHT"
    v_name = "CC_ALG"
    extras = {'CLIENT_NODE_CNT':'NODE_CNT','PART_PER_TXN':'NODE_CNT'}
    x_vals,v_vals,fmt,exp,lst = plot_prep(nexp,nfmt,x_name,v_name)
    tput(x_vals,v_vals,summary,summary_cl,summary_seq,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_ycsb_load",xlab="Load (txns/server)")

def ppr_network_plot(summary,summary_cl,summary_seq):
    from experiments import network_sweep
    from helper import plot_prep
    from plot_helper import tput
    nfmt,nexp = network_sweep()
    v_name = "CC_ALG"
    x_name = "NETWORK_DELAY"
    extras = {'CLIENT_NODE_CNT':'NODE_CNT','PART_CNT':'NODE_CNT'}
    x_vals,v_vals,fmt,exp = plot_prep(nexp,nfmt,x_name,v_name)
#    x_vals = [float(v)/1000 for v in x_vals]
    tput(x_vals,v_vals,summary,summary_cl,summary_seq,cfg_fmt=fmt,cfg=list(exp),xname=x_name,vname=v_name,title="",name="tput_network",xlab="Network Latency (ms)")


