import os, sys, re, math, os.path
from helper import *
from draw import *
#from experiments import experiments as experiments
#from experiments import configs

PATH=os.getcwd()

def tput(xval,vval,summary,
        cfg_fmt=[],
        cfg=[],
        xname="MPR",
        vname="CC_ALG",
        title=""
        ):
    tpt = {}
    name = 'tput_{}_{}'.format(xname.lower(),title.replace(" ","_").lower())
    _title = 'Per Node Throughput {}'.format(title)

    for v in vval:
        tpt[v] = [0] * len(xval)

        for x,xi in zip(xval,range(len(xval))):
            cfgs = get_cfgs(cfg_fmt + [xname] + [vname], cfg + [x] + [v] )
            cfgs = get_outfile_name(cfgs)
            if cfgs not in summary.keys(): break
            try:
                avg_run_time = avg(summary[cfgs]['run_time'])
                avg_txn_cnt = avg(summary[cfgs]['txn_cnt'])
            except KeyError:
                print("KeyError: {} {} {}".format(v,x,cfg))
                tpt[v][xi] = 0
                continue
            tpt[v][xi] = (avg_txn_cnt/avg_run_time)

    draw_line(name,tpt,xval,ylab='Throughput (Txn/sec)',xlab='Multi-Partition Rate',title=_title,bbox=[0.5,0.95]) 



# Plots Throughput vs. MPR 
# mpr: list of MPR values to plot along the x-axis
# nodes: list of node counts; if more than 1 node count is
#   provided, each node is plotted as a separate line, and
#   first CC algo is chosen by default
# algos: list of CC algos to plot
# max_txn: MAX_TXN_PER_PART
# summary: dictionary loaded with results
def tput_mpr(mpr,nodes,algos,max_txn,summary):
    tpt = {}
    xs = []
    node,algo = None,None
    name = 'tput_mpr'
    _title = 'Per Node Throughput'
    if len(nodes) > 1:
        xs = nodes
        algo = algos[0]
        name = 'tput_mpr_' + algo
        _title = 'Per Node Throughput ' + algo
    else:
        xs = algos
        node = nodes[0]
        name = 'tput_mpr_n' + str(node)
        _title = 'Per Node Throughput ' + str(node) + ' Nodes'

    for x in xs:
        tpt[x] = [0] * len(mpr)

        for i in range(len(mpr)):
            m = mpr[i]
            if algo == None:
                cfgs = get_cfgs([node,max_txn,'TPCC',x,m])
            if node == None:
                cfgs = get_cfgs([x,max_txn,'TPCC',algo,m])
            cfgs = get_outfile_name(cfgs)
            if cfgs not in summary.keys(): break
            try:
                avg_run_time = avg(summary[cfgs]['run_time'])
                avg_txn_cnt = avg(summary[cfgs]['txn_cnt'])
            except KeyError:
                print("KeyError: {} {} {} {}".format(algo,node,max_txn,m))
                tpt[x][i] = 0
                continue
            tpt[x][i] = (avg_txn_cnt/avg_run_time)

    draw_line(name,tpt,mpr,ylab='Throughput (Txn/sec)',xlab='Multi-Partition Rate',title=_title,bbox=[0.5,0.95]) 

# Plots Transport latency vs. MPR 
# mpr: list of MPR values to plot along the x-axis
# nodes: list of node counts; if more than 1 node count is
#   provided, each node is plotted as a separate line, and
#   first CC algo is chosen by default
# algos: list of CC algos to plot
# max_txn: MAX_TXN_PER_PART
# summary: dictionary loaded with results
def tportlat_mpr(mpr,nodes,algos,max_txn,summary):
    tport_lat = {}
    xs = []
    node,algo = None,None
    name = 'tportlat_mpr'
    if len(nodes) > 1:
        xs = nodes
        algo = algos[0]
        name = 'tportlat_mpr_' + algo
    else:
        xs = algos
        node = nodes[0]
        name = 'tportlat_mpr_n' + str(node)

    for x in xs:
        if algo == None: algo = x
        if node == None: node = x
        tport_lat[x] = [0] * len(mpr)

        for i in range(len(mpr)):
            m = mpr[i]
            cfgs = get_cfgs([node,max_txn,'TPCC',algo,m])
            cfgs = get_outfile_name(cfgs)
            if cfgs not in summary.keys(): break
            avg_tport_lat = avg(summary[cfgs]['tport_lat'])
            tport_lat[x][i] = avg_tport_lat

    draw_line(name,tport_lat,mpr,ylab='Latency (s)',xlab='Multi-Partition Rate',title='Average Message Latency',bbox=[0.5,0.95]) 

# Stack graph of time breakdowns
# mpr: list of MPR values to plot along the x-axis
# nodes: node count to plot
# algos: CC algo to plot
# max_txn: MAX_TXN_PER_PART
# summary: dictionary loaded with results
# normalized: if true, normalize the results
def time_breakdown(xval,summary,
        normalized=False,
        xname="MPR",
        cfg_fmt=[],
        cfg=[],
        title=''
        ):
    stack_names = ['Useful Work','Abort','Timestamp','Index','Lock Wait','Remote Wait','Manager']
    _title = ''
    _ymax=1.0
    if normalized:
        _title = 'Time Breakdown Normalized {}'.format(title)
    else:
        _title = 'Time Breakdown {}'.format(title)
    name = '{}'.format(_title.replace(" ","_").lower())

    run_time = [0] * len(xval)
    time_man = [0] * len(xval)
    time_wait_rem = [0] * len(xval)
    time_wait_lock = [0] * len(xval)
    time_index = [0] * len(xval)
    time_ts_alloc = [0] * len(xval)
    time_abort = [0] * len(xval)
    time_work = [0] * len(xval)

    for x,i in zip(xval,range(len(xval))):
        _cfgs = get_cfgs(cfg_fmt + [xname],cfg + [x])
        cfgs = get_outfile_name(_cfgs)
        if cfgs not in summary.keys(): break
        try:
            if normalized:
                run_time[i] = avg(summary[cfgs]['run_time'])
            else:
                run_time[i] = 1.0
            time_abort[i] = avg(summary[cfgs]['time_abort']) / run_time[i]
            time_ts_alloc[i] = avg(summary[cfgs]['time_ts_alloc']) / run_time[i]
            time_index[i] = avg(summary[cfgs]['time_index']) / run_time[i]
            if _cfgs["CC_ALG"] == "HSTORE":
                time_wait_lock[i] = avg(summary[cfgs]['time_wait_lock']) / run_time[i]
            else:
                time_wait_lock[i] = avg(summary[cfgs]['time_wait']) / run_time[i]
            time_wait_rem[i] = avg(summary[cfgs]['time_wait_rem']) / run_time[i]
            time_man[i] = (avg(summary[cfgs]['time_lock_man']) - avg(summary[cfgs]['time_wait_lock'])) / run_time[i]

            if normalized:
                assert(sum([time_man[i],time_wait_rem[i],time_wait_lock[i],time_index[i],time_ts_alloc[i],time_abort[i]]) < 1.0)
                time_work[i] = 1.0 - sum([time_man[i],time_wait_rem[i],time_wait_lock[i],time_index[i],time_ts_alloc[i],time_abort[i]])
            else:
                assert(sum([time_man[i],time_wait_rem[i],time_wait_lock[i],time_index[i],time_ts_alloc[i],time_abort[i]]) < avg(summary[cfgs]['run_time']))
                time_work[i] = avg(summary[cfgs]['run_time']) - sum([time_man[i],time_wait_rem[i],time_wait_lock[i],time_index[i],time_ts_alloc[i],time_abort[i]])
            _ymax = max(_ymax, sum([time_work[i],time_man[i],time_wait_rem[i],time_wait_lock[i],time_index[i],time_ts_alloc[i],time_abort[i]])) 
        except KeyError:
            print("KeyError: {} {}".format(x,cfg))
            run_time[i] = 1.0
            time_abort[i] = 0.0
            time_ts_alloc[i] = 0.0
            time_index[i] = 0.0
            time_wait_lock[i] = 0.0
            time_wait_rem[i] = 0.0
            time_man[i] = 0.0
            time_work[i] = 0.0
    data = [time_man,time_wait_rem,time_wait_lock,time_index,time_ts_alloc,time_abort,time_work]

    draw_stack(data,xval,stack_names,figname=name,title=_title,ymax=_ymax)

# Cumulative density function of total number of aborts per transaction
# mpr: list of MPR values to plot along the x-axis
# nodes: node count to plot
# algos: CC algo to plot
# max_txn: MAX_TXN_PER_PART
# summary: dictionary loaded with results
def cdf_aborts_mpr(mpr,node,algo,max_txn,summary):

    name = 'cdf_aborts_mpr_n{}_{}'.format(node,algo)
    _title = 'CDF of Aborts {} {} Nodes'.format(algo,node)
    ys_mpr = {}
    # find max abort
    max_abort = 0
    for i in range(len(mpr)):
        m = mpr[i]
        cfgs = get_cfgs([node,max_txn,'TPCC',algo,m])
        cfgs = get_outfile_name(cfgs)
        if cfgs not in summary.keys(): break
        try:
            if len(summary[cfgs]['all_abort_cnts']) == 0: continue
            max_abort = max(max_abort, max(summary[cfgs]['all_abort_cnts'].keys()))
        except KeyError:
            print("KeyError: {} {} {} {}".format(algo,node,max_txn,m))
            max_abort = max_abort

    xs_mpr = range(max_abort + 1)
    for i in range(len(mpr)):
        m = mpr[i]
        ys_mpr[m] = [0] * (max_abort + 1)
        cfgs = get_cfgs([node,max_txn,'TPCC',algo,m])
        cfgs = get_outfile_name(cfgs)
        if cfgs not in summary.keys(): break
        y = 0
        for x in xs_mpr:
            try:
                if x in summary[cfgs]['all_abort_cnts'].keys():
                    ys_mpr[m][x] = y + (summary[cfgs]['all_abort_cnts'][x] / sum(summary[cfgs]['txn_abort_cnt']))
                    y = ys_mpr[m][x]
                else:
                    ys_mpr[m][x] = y
            except KeyError:
                print("KeyError: {} {} {} {}".format(algo,node,max_txn,m))
                ys_mpr[m][x] = y


    draw_line(name,ys_mpr,xs_mpr,ylab='% Transactions',xlab='# Aborts',title=_title,bbox=[0.8,0.6],ylimit=1.0) 


# Bar graph of total number of aborts per transaction
# mpr: list of MPR values to plot along the x-axis
# nodes: node count to plot
# algos: CC algo to plot
# max_txn: MAX_TXN_PER_PART
# summary: dictionary loaded with results
def bar_aborts_mpr(mpr,node,algo,max_txn,summary):

    name = 'bar_aborts_mpr_n{}_{}'.format(node,algo)
    _title = 'Abort Counts {} {} Nodes'.format(algo,node)
    ys_mpr = {}
    # find max abort
    max_abort = 0
    for i in range(len(mpr)):
        m = mpr[i]
        cfgs = get_cfgs([node,max_txn,'TPCC',algo,m])
        cfgs = get_outfile_name(cfgs)
        if cfgs not in summary.keys(): break
        try:
            if len(summary[cfgs]['all_abort_cnts']) == 0: continue
            max_abort = max(max_abort, max(summary[cfgs]['all_abort_cnts'].keys()))
        except KeyError:
            continue

    xs_mpr = range(max_abort + 1)
    for i in range(len(mpr)):
        m = mpr[i]
        ys_mpr[m] = [0] * (max_abort + 1)
        cfgs = get_cfgs([node,max_txn,'TPCC',algo,m])
        cfgs = get_outfile_name(cfgs)
        if cfgs not in summary.keys(): break
        for x in xs_mpr:
            try:
                if x in summary[cfgs]['all_abort_cnts'].keys():
                    ys_mpr[m][x] = summary[cfgs]['all_abort_cnts'][x]
            except KeyError:
                print("KeyError: {} {} {} {}".format(algo,node,max_txn,m))
                ys_mpr[m][x] = 0

    draw_bars(ys_mpr,xs_mpr,ylab='# Transactions',xlab='# Aborts',title=_title,figname=name) 


# Plots Average of a result vs. MPR 
# mpr: list of MPR values to plot along the x-axis
# nodes: list of node counts; if more than 1 node count is
#   provided, each node is plotted as a separate line, and
#   first CC algo is chosen by default
# algos: list of CC algos to plot
# max_txn: MAX_TXN_PER_PART
# summary: dictionary loaded with results
# value: result to average and plot
def plot_avg(mpr,nodes,algos,max_txn,summary,value='run_time'):
    avgs = {}
    xs = []
    node,algo = None,None
    name = 'avg_' + value + "_"
    if len(nodes) > 1:
        xs = nodes
        algo = algos[0]
        name = name + algo
    else:
        xs = algos
        node = nodes[0]
        name = name + "n" + str(node)

    for x in xs:
        avgs[x] = [0] * len(mpr)

        for i in range(len(mpr)):
            m = mpr[i]
            if algo == None:
                cfgs = get_cfgs([node,max_txn,'TPCC',x,m])
            if node == None:
                cfgs = get_cfgs([x,max_txn,'TPCC',algo,m])
            cfgs = get_outfile_name(cfgs)
            if cfgs not in summary.keys(): break
            try:
                avg_ = avg(summary[cfgs][value])
            except KeyError:
                print("KeyError: {} {} {} {}".format(algo,node,max_txn,m))
                avgs[x][i] = 0
                continue
            avgs[x][i] = avg_

    draw_line(name,avgs,mpr,ylab='average ' + value,xlab='Multi-Partition Rate',title='Per Node Throughput',bbox=[0.5,0.95]) 


