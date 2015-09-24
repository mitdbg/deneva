import os, sys, re, math, os.path
import numpy as np
import operator
from helper import get_cfgs 
from draw import *
import types
import latency_stats as ls
#from experiments import experiments as experiments
#from experiments import configs

plot_cnt = 0

PATH=os.getcwd()

def tput_plotter(title,x_name,v_name,fmt,exp,summary,summary_client):
    _cfg_fmt = list(fmt)
    x_idx = _cfg_fmt.index(x_name)
    _cfg_fmt.remove(x_name)
    v_idx = _cfg_fmt.index(v_name)
    _cfg_fmt.remove(v_name)
    p_idx = _cfg_fmt.index("PART_CNT")
    _cfg_fmt.remove("PART_CNT")
    _cfg = list(exp)
    x_vals = []
    v_vals = []
    for p in _cfg:
        x_vals.append(p[x_idx])
        del p[x_idx]
        v_vals.append(p[v_idx])
        del p[v_idx]
        del p[p_idx]
    x_vals = sorted(list(set(x_vals)))
    v_vals = sorted(list(set(v_vals)))
#    if x_name == "NODE_CNT" or v_name == "NODE_CNT":
#        cl_idx = _cfg_fmt.index("CLIENT_NODE_CNT")
#        _cfg_fmt.remove("CLIENT_NODE_CNT")
#        for p in _cfg:
#            del p[cl_idx]
        
    cfg_list = []
#    print(_cfg)
#    print(_cfg_fmt)
    for f in _cfg_fmt:
        f_idx = _cfg_fmt.index(f)
        f_vals = []
        for p in _cfg:
            f_vals.append(p[f_idx])
        f_vals = sorted(list(set(f_vals)))
        if f == "THREAD_CNT": 
            f_vals = [2] 
        if f == "PART_PER_TXN": 
            f_vals = [2] 
        if f == "CLIENT_NODE_CNT": 
            f_vals = [1] 
        cfg_list.append(f_vals)

    for c in itertools.product(*cfg_list):
        c = list(c)
        if c[_cfg_fmt.index("READ_PERC")] + c[_cfg_fmt.index("WRITE_PERC")] != 1.0:
            continue
        title2 = ""
        if x_name != "NODE_CNT" and v_name != "NODE_CNT":
            cl_idx = _cfg_fmt.index("CLIENT_NODE_CNT")
            n_idx = _cfg_fmt.index("NODE_CNT")
            c[cl_idx] = c[n_idx] / 2 if c[n_idx] > 1 else 1
        print c
        title2 = get_outfile_name(get_cfgs(_cfg_fmt,c),_cfg_fmt,["*","*"])
#        for f in sorted(_cfg_fmt):
#            f_idx = _cfg_fmt.index(f)
#            title2 = title2 + "_%s-%s" %(f,c[f_idx])
        title2 = title + title2
            
        tput(x_vals,v_vals,summary,cfg_fmt=_cfg_fmt,cfg=c,xname=x_name,vname=v_name,title=title2)
        abort_rate(x_vals,v_vals,summary,cfg_fmt=_cfg_fmt,cfg=c,xname=x_name,vname=v_name,title=title2)
        lat(x_vals,v_vals,summary_client,cfg_fmt=_cfg_fmt,cfg=c,xname=x_name,vname=v_name,title=title2,stat_types=["99th"])
#        lat(x_vals,v_vals,summary_client,cfg_fmt=_cfg_fmt,cfg=c,xname=x_name,vname=v_name,title=title2,stat_types=["99th","90th","mean","max"])
#        for v in v_vals:
#            time_breakdown(x_vals,summary,normalized=True,xname=x_name,cfg_fmt=_cfg_fmt+[v_name],cfg=c+[v],title=title2+v_name)

def tput2(xval,vval,summary,
        cfg={},
        xname="MPR",
        vname="CC_ALG",
        title=""
        ):
    global plot_cnt
    tpt = {}
    name = 'tput_plot{}'.format(plot_cnt)
    plot_cnt += 1
    _title = title

    print "X-axis: " + str(xval)
    print "Var: " + str(vval)
    if xname == "ABORT_PENALTY":
        _xval = [(float(x.replace("UL","")))/1000000000 for x in xval]
        sort_idxs = sorted(range(len(_xval)),key=lambda x:_xval[x])
        xval = [xval[i] for i in sort_idxs]
        _xval = sorted(_xval)
        _xlab = xname + " (Sec)"
    else:
        _xval = xval
        _xlab = xname
    for v in vval:
        if vname == "NETWORK_DELAY":
            _v = (float(v.replace("UL","")))/1000000
        else:
            _v = v
        tpt[_v] = [0] * len(xval)

        for x,xi in zip(xval,range(len(xval))):
            my_cfg = cfg
            my_cfg[xname] = x
            my_cfg[vname] = v
            print my_cfg

            cfgs = get_cfgs(my_cfg_fmt, my_cfg)
            cfgs = get_outfile_name(cfgs,my_cfg_fmt)
            if cfgs not in summary.keys(): 
                print("Not in summary: {}".format(cfgs))
                continue 
            try:
                tot_run_time = sum(summary[cfgs]['clock_time'])
                tot_txn_cnt = sum(summary[cfgs]['txn_cnt'])
                avg_run_time = avg(summary[cfgs]['clock_time'])
                avg_txn_cnt = avg(summary[cfgs]['txn_cnt'])
            except KeyError:
                print("KeyError: {} {} {} -- {}".format(v,x,cfg,cfgs))
                tpt[_v][xi] = 0
                continue
            # System Throughput: total txn count / average of all node's run time
            # Per Node Throughput: avg txn count / average of all node's run time
            # Per txn latency: total of all node's run time / total txn count
            tpt[_v][xi] = (tot_txn_cnt/avg_run_time)
            #tpt[v][xi] = (avg_txn_cnt/avg_run_time)

    bbox = [0.4,0.9]
    print("Created plot {}".format(name))
    draw_line(name,tpt,_xval,ylab='Throughput (Txn/sec)',xlab=_xlab,title=_title,bbox=bbox,ncol=2) 

def gput(xval,vval,summary,
        cfg_fmt=[],
        cfg=[],
        xname="MPR",
        vname="CC_ALG",
        title=""
        ):
    global plot_cnt
    tpt = {}
    name = 'gput_plot{}'.format(plot_cnt)
    plot_cnt += 1
    _title = title

    print "X-axis: " + str(xval)
    print "Var: " + str(vval)
    if xname == "ABORT_PENALTY":
        _xval = [(float(x.replace("UL","")))/1000000000 for x in xval]
        sort_idxs = sorted(range(len(_xval)),key=lambda x:_xval[x])
        xval = [xval[i] for i in sort_idxs]
        _xval = sorted(_xval)
        _xlab = xname + " (Sec)"
    else:
        _xval = xval
        _xlab = xname
    for v in vval:
        if vname == "NETWORK_DELAY":
            _v = (float(v.replace("UL","")))/1000000
        else:
            _v = v
        tpt[_v] = [0] * len(xval)

        for x,xi in zip(xval,range(len(xval))):
            my_cfg_fmt = cfg_fmt + [xname] + [vname]
            my_cfg = cfg + [x] + [v]
            n_cnt = my_cfg[my_cfg_fmt.index("NODE_CNT")]
            n_clt = my_cfg[my_cfg_fmt.index("CLIENT_NODE_CNT")]
            my_cfg[my_cfg_fmt.index("CLIENT_NODE_CNT")] = int(math.ceil(n_cnt/2)) if n_cnt > 1 else 1
            n_ppt = my_cfg[my_cfg_fmt.index("PART_PER_TXN")]
            my_cfg[my_cfg_fmt.index("PART_PER_TXN")] = n_ppt if n_ppt <= n_cnt else n_cnt
            n_thd =  my_cfg[my_cfg_fmt.index("THREAD_CNT")]
            n_alg =  my_cfg[my_cfg_fmt.index("CC_ALG")]
            if n_alg == "HSTORE" or n_alg == "HSTORE_SPEC":
                my_cfg_fmt = my_cfg_fmt + ["PART_CNT"]
                my_cfg = my_cfg + [n_thd*n_cnt]
            else:
                my_cfg_fmt = my_cfg_fmt + ["PART_CNT"]

            cfgs = get_cfgs(my_cfg_fmt, my_cfg)
            cfgs = get_outfile_name(cfgs,my_cfg_fmt)
            if cfgs not in summary.keys(): 
                print("Not in summary: {}".format(cfgs))
                continue 
            try:
                tot_run_time = sum(summary[cfgs]['clock_time'])
                tot_txn_cnt = sum(summary[cfgs]['txn_cnt']) + sum(summary[cfgs]['abort_cnt'])
                avg_run_time = avg(summary[cfgs]['clock_time'])
                avg_txn_cnt = avg(summary[cfgs]['txn_cnt'])
            except KeyError:
                print("KeyError: {} {} {} -- {}".format(v,x,cfg,cfgs))
                tpt[_v][xi] = 0
                continue
            # System Throughput: total txn count / average of all node's run time
            # Per Node Throughput: avg txn count / average of all node's run time
            # Per txn latency: total of all node's run time / total txn count
            tpt[_v][xi] = (tot_txn_cnt/avg_run_time)
            #tpt[v][xi] = (avg_txn_cnt/avg_run_time)

    bbox = [0.4,0.9]
    print("Created plot {}".format(name))
    draw_line(name,tpt,_xval,ylab='Goodput (Txn/sec)',xlab=_xlab,title=_title,bbox=bbox,ncol=2) 

def tput_v_lat(xval,vval,summary,
        cfg_fmt=[],
        cfg=[],
        xname="MPR",
        vname="CC_ALG",
        title=""
        ):
    global plot_cnt
    tpt = {}
    name = 'tputvlat_plot{}'.format(plot_cnt)
    plot_cnt += 1
    _title = title

    print "X-axis: " + str(xval)
    print "Var: " + str(vval)
    if xname == "ABORT_PENALTY":
        _xval = [(float(x.replace("UL","")))/1000000000 for x in xval]
        sort_idxs = sorted(range(len(_xval)),key=lambda x:_xval[x])
        xval = [xval[i] for i in sort_idxs]
        _xval = sorted(_xval)
        _xlab = xname + " (Sec)"
    else:
        _xval = xval
        _xlab = xname
    for v in vval:
        if vname == "NETWORK_DELAY":
            _v = (float(v.replace("UL","")))/1000000
        else:
            _v = v
        tpt[_v] = [0] * len(xval)

        for x,xi in zip(xval,range(len(xval))):
            my_cfg_fmt = cfg_fmt + [xname] + [vname]
            my_cfg = cfg + [x] + [v]
            n_cnt = my_cfg[my_cfg_fmt.index("NODE_CNT")]
            n_clt = my_cfg[my_cfg_fmt.index("CLIENT_NODE_CNT")]
            my_cfg[my_cfg_fmt.index("CLIENT_NODE_CNT")] = int(math.ceil(n_cnt)) if n_cnt > 1 else 1
            n_ppt = my_cfg[my_cfg_fmt.index("PART_PER_TXN")]
            my_cfg[my_cfg_fmt.index("PART_PER_TXN")] = n_ppt if n_ppt <= n_cnt else n_cnt
            n_thd =  my_cfg[my_cfg_fmt.index("THREAD_CNT")]
            n_alg =  my_cfg[my_cfg_fmt.index("CC_ALG")]
            if n_alg == "HSTORE" or n_alg == "HSTORE_SPEC":
#                my_cfg[my_cfg_fmt.index("PART_CNT")] = n_thd*n_cnt
                my_cfg_fmt = my_cfg_fmt + ["PART_CNT"]
                my_cfg = my_cfg + [n_thd*n_cnt]
            else:
#                my_cfg[my_cfg_fmt.index("PART_CNT")] = n_cnt
                my_cfg_fmt = my_cfg_fmt + ["PART_CNT"]
                my_cfg = my_cfg + [n_cnt]
#                my_cfg.pop(my_cfg_fmt.index("PART_CNT"))
#                my_cfg_fmt.remove("PART_CNT")
#            if "CLIENT_NODE_CNT" not in my_cfg_fmt:
#                my_cfg_fmt = my_cfg_fmt + ["CLIENT_NODE_CNT"]
#                my_cfg = my_cfg + [int(math.ceil(n_cnt/2)) if n_cnt > 1 else 1]

            cfgs = get_cfgs(my_cfg_fmt, my_cfg)
            cfgs = get_outfile_name(cfgs,my_cfg_fmt)
            if cfgs not in summary.keys(): 
                print("Not in summary: {}".format(cfgs))
                continue 
            try:
                tot_run_time = sum(summary[cfgs]['clock_time'])
                tot_txn_cnt = sum(summary[cfgs]['txn_cnt'])
                avg_run_time = avg(summary[cfgs]['clock_time'])
                avg_txn_cnt = avg(summary[cfgs]['txn_cnt'])
            except KeyError:
                print("KeyError: {} {} {} -- {}".format(v,x,cfg,cfgs))
                tpt[_v][xi] = 0
                continue
            # System Throughput: total txn count / average of all node's run time
            # Per Node Throughput: avg txn count / average of all node's run time
            # Per txn latency: total of all node's run time / total txn count
            tpt[_v][xi] = (tot_txn_cnt/avg_run_time)
            #tpt[v][xi] = (avg_txn_cnt/avg_run_time)

    bbox = [0.7,0.9]
    print("Created plot {}".format(name))
    draw_line(name,tpt,_xval,ylab='Throughput (Txn/sec)',xlab=_xlab,title=_title,bbox=bbox,ncol=2) 


def tput(xval,vval,summary,
        cfg_fmt=[],
        cfg=[],
        xname="MPR",
        vname="CC_ALG",
        title=""
        ):
    global plot_cnt
    tpt = {}
    name = 'tput_plot{}'.format(plot_cnt)
    plot_cnt += 1
#    name = 'tput_{}_{}_{}'.format(xname.lower(),vname.lower(),title.replace(" ","_").lower())
#    _title = 'System Throughput {}'.format(title)
    _title = title

    print "X-axis: " + str(xval)
    print "Var: " + str(vval)
    if xname == "ABORT_PENALTY":
        _xval = [(float(x.replace("UL","")))/1000000000 for x in xval]
        sort_idxs = sorted(range(len(_xval)),key=lambda x:_xval[x])
        xval = [xval[i] for i in sort_idxs]
        _xval = sorted(_xval)
        _xlab = xname + " (Sec)"
    else:
        _xval = xval
        _xlab = xname
    for v in vval:
        if vname == "NETWORK_DELAY":
            _v = (float(v.replace("UL","")))/1000000
        else:
            _v = v
        tpt[_v] = [0] * len(xval)

        for x,xi in zip(xval,range(len(xval))):
            my_cfg_fmt = cfg_fmt + [xname] + [vname]
            my_cfg = cfg + [x] + [v]
#            my_cfg_fmt = cfg_fmt
#            my_cfg = cfg
#            my_cfg[my_cfg_fmt.index(xname)] = x
#            my_cfg[my_cfg_fmt.index(vname)] = v
#            print my_cfg_fmt
            n_cnt = my_cfg[my_cfg_fmt.index("NODE_CNT")]
            n_clt = my_cfg[my_cfg_fmt.index("CLIENT_NODE_CNT")]
#            my_cfg[my_cfg_fmt.index("CLIENT_NODE_CNT")] = n_cnt
            my_cfg[my_cfg_fmt.index("CLIENT_NODE_CNT")] = int(math.ceil(n_cnt)) if n_cnt > 1 else 1
            n_ppt = my_cfg[my_cfg_fmt.index("PART_PER_TXN")]
            my_cfg[my_cfg_fmt.index("PART_PER_TXN")] = n_ppt if n_ppt <= n_cnt else n_cnt
            n_thd =  my_cfg[my_cfg_fmt.index("THREAD_CNT")]
            n_alg =  my_cfg[my_cfg_fmt.index("CC_ALG")]
#            my_cfg[my_cfg_fmt.index("THREAD_CNT")] = 1 if n_alg == "HSTORE" or n_alg == "HSTORE_SPEC" else n_thd
            if n_alg == "HSTORE" or n_alg == "HSTORE_SPEC":
#                my_cfg[my_cfg_fmt.index("PART_CNT")] = n_thd*n_cnt
                my_cfg_fmt = my_cfg_fmt + ["PART_CNT"]
                my_cfg = my_cfg + [n_thd*n_cnt]
            else:
#                my_cfg[my_cfg_fmt.index("PART_CNT")] = n_cnt
                my_cfg_fmt = my_cfg_fmt + ["PART_CNT"]
                my_cfg = my_cfg + [n_cnt]
#                my_cfg.pop(my_cfg_fmt.index("PART_CNT"))
#                my_cfg_fmt.remove("PART_CNT")
#            if "CLIENT_NODE_CNT" not in my_cfg_fmt:
#                my_cfg_fmt = my_cfg_fmt + ["CLIENT_NODE_CNT"]
#                my_cfg = my_cfg + [int(math.ceil(n_cnt/2)) if n_cnt > 1 else 1]

            cfgs = get_cfgs(my_cfg_fmt, my_cfg)
            cfgs = get_outfile_name(cfgs,my_cfg_fmt)
            if cfgs not in summary.keys(): 
                print("Not in summary: {}".format(cfgs))
                continue 
            try:
                tot_run_time = sum(summary[cfgs]['clock_time'])
                tot_txn_cnt = sum(summary[cfgs]['txn_cnt'])
                avg_run_time = avg(summary[cfgs]['clock_time'])
                avg_txn_cnt = avg(summary[cfgs]['txn_cnt'])
            except KeyError:
                print("KeyError: {} {} {} -- {}".format(v,x,cfg,cfgs))
                tpt[_v][xi] = 0
                continue
            # System Throughput: total txn count / average of all node's run time
            # Per Node Throughput: avg txn count / average of all node's run time
            # Per txn latency: total of all node's run time / total txn count
            tpt[_v][xi] = (tot_txn_cnt/avg_run_time)
            #tpt[v][xi] = (avg_txn_cnt/avg_run_time)

    bbox = [0.7,0.9]
    print("Created plot {}".format(name))
    draw_line(name,tpt,_xval,ylab='Throughput (Txn/sec)',xlab=_xlab,title=_title,bbox=bbox,ncol=2) 


def lat(xval,vval,summary,
        cfg_fmt=[],
        cfg=[],
        xname="MPR",
        vname="CC_ALG",
        title="",
        stat_types=['99th']
        ):
    for stat in stat_types:
        name = 'lat-{}_{}_{}_{}'.format(stat,xname.lower(),vname.lower(),title.replace(" ","_").lower())
        _title = 'Transaction Latency-{}% {}'.format(stat,title)
        lat = {}

        for v in vval:
            if vname == "NETWORK_DELAY":
                _v = (float(v.replace("UL","")))/1000000
            else:
                _v = v
            if xname == "ABORT_PENALTY":
                _xval = [(float(x.replace("UL","")))/1000000000 for x in xval]
                sort_idxs = sorted(range(len(_xval)),key=lambda x:_xval[x])
                xval = [xval[i] for i in sort_idxs]
                _xval = sorted(_xval)
                _xlab = xname + " (Sec)"
            else:
                _xval = xval
                _xlab = xname
            lat[_v] = [0] * len(xval)

            for x,xi in zip(xval,range(len(xval))):
                my_cfg_fmt = cfg_fmt + [xname] + [vname]
                my_cfg = cfg + [x] + [v]
#            my_cfg_fmt = cfg_fmt
#            my_cfg = cfg
#            my_cfg[my_cfg_fmt.index(xname)] = x
#            my_cfg[my_cfg_fmt.index(vname)] = v
                n_cnt = my_cfg[my_cfg_fmt.index("NODE_CNT")]
                n_clt = my_cfg[my_cfg_fmt.index("CLIENT_NODE_CNT")]
                my_cfg[my_cfg_fmt.index("CLIENT_NODE_CNT")] = int(math.ceil(n_cnt/2)) if n_cnt > 1 else 1
                n_ppt = my_cfg[my_cfg_fmt.index("PART_PER_TXN")]
                my_cfg[my_cfg_fmt.index("PART_PER_TXN")] = n_ppt if n_ppt <= n_cnt else 1
                n_thd =  my_cfg[my_cfg_fmt.index("THREAD_CNT")]
                n_alg =  my_cfg[my_cfg_fmt.index("CC_ALG")]
#            my_cfg[my_cfg_fmt.index("THREAD_CNT")] = 1 if n_alg == "HSTORE" or n_alg == "HSTORE_SPEC" else n_thd
                if n_alg == "HSTORE" or n_alg == "HSTORE_SPEC":
#                my_cfg[my_cfg_fmt.index("PART_CNT")] = n_thd*n_cnt
                    my_cfg_fmt = my_cfg_fmt + ["PART_CNT"]
                    my_cfg = my_cfg + [n_thd*n_cnt]
                else:
#                my_cfg[my_cfg_fmt.index("PART_CNT")] = n_cnt
                    my_cfg_fmt = my_cfg_fmt + ["PART_CNT"]
                    my_cfg = my_cfg + [n_cnt]
#                my_cfg.pop(my_cfg_fmt.index("PART_CNT"))
#                my_cfg_fmt.remove("PART_CNT")
#            if "CLIENT_NODE_CNT" not in my_cfg_fmt:
#                my_cfg_fmt = my_cfg_fmt + ["CLIENT_NODE_CNT"]
#                my_cfg = my_cfg + [int(math.ceil(n_cnt/2)) if n_cnt > 1 else 1]

                cfgs = get_cfgs(my_cfg_fmt, my_cfg)
                cfgs = get_outfile_name(cfgs,my_cfg_fmt)
                if cfgs not in summary.keys(): 
                    print("Not in summary: {}".format(cfgs))
                    break
                try:
                    lat[_v][xi] = ls.exec_fn(summary[cfgs]['all_lat'],stat)
                except KeyError:
                    print("KeyError: {} {} {} -- {}".format(v,x,cfg,cfgs))
                    lat[_v][xi] = 0
                    continue

        bbox = [0.85,0.5]
        if vname == "NETWORK_DELAY":
            bbox = [0.8,0.2]
        print("Created plot {}".format(name))
        draw_line(name,lat,_xval,ylab='Latency-{}% (Sec)'.format(stat),xlab=_xlab,title=_title,bbox=bbox,ncol=2) 


def lat_node_tbls(exp,nodes,msg_bytes,ts,
                title="",
                stats=['99th','95th','90th','50th','mean']):
    nnodes = len(nodes)
    tables = {}
    for i,b in enumerate(msg_bytes):
        tables[b]=np.empty([nnodes,nnodes],dtype=object)
    for i,e in enumerate(exp):
        for k,v in e.iteritems():
            n0 = nodes.index(v.get_metadata()["n0"])
            n1 = nodes.index(v.get_metadata()["n1"])
            tables[k][n0][n1] = v
    for i,b in enumerate(msg_bytes):
        for s in stats:
            _title="{} % Latencies (ms), Message Size of {} bytes".format(s,b)
            name="nlat_{}_{}bytes_{}nodes_{}".format(s,b,nnodes,ts)
            draw_lat_matrix(name,tables[b],lat_type=s,rows=nodes,columns=nodes,title=_title)

def lat_tbl(totals,msg_bytes,ts,
            title="",
            stats=['99th','95th','90th','50th','mean']):
    nrows = len(stats)
    ncols = len(msg_bytes)
    for i,b in enumerate(msg_bytes):
        table=np.empty([nrows,ncols],dtype=object)
    for (i,j),val in np.ndenumerate(table):
        table[i,j] = totals[msg_bytes[j]]
    _title="Latencies Stats(ms), Message Size of {} bytes".format("all")
    name="nlat_{}_{}bytes_{}".format('all-lat',"all-msgs",ts)
    draw_lat_matrix(name,table,lat_types=stats,rows=stats,columns=msg_bytes,title=_title)
    

def abort_rate(xval,vval,summary,
        cfg_fmt=[],
        cfg=[],
        xname="MPR",
        vname="CC_ALG",
        title=""
        ):
    tpt = {}
    name = 'abortrate_{}_{}_{}'.format(xname.lower(),vname.lower(),title.replace(" ","_").lower())
    _title = 'Abort Rate {}'.format(title)

    for v in vval:
        if vname == "NETWORK_DELAY":
            _v = (float(v.replace("UL","")))/1000000
        else:
            _v = v
        tpt[_v] = [0] * len(xval)

        for x,xi in zip(xval,range(len(xval))):
            my_cfg_fmt = cfg_fmt + [xname] + [vname]
            my_cfg = cfg + [x] + [v]
#            my_cfg_fmt = cfg_fmt
#            my_cfg = cfg
#            my_cfg[my_cfg_fmt.index(xname)] = x
#            my_cfg[my_cfg_fmt.index(vname)] = v
            n_cnt = my_cfg[my_cfg_fmt.index("NODE_CNT")]
            n_clt = my_cfg[my_cfg_fmt.index("CLIENT_NODE_CNT")]
            my_cfg[my_cfg_fmt.index("CLIENT_NODE_CNT")] = int(math.ceil(n_cnt/2)) if n_cnt > 1 else 1
            n_ppt = my_cfg[my_cfg_fmt.index("PART_PER_TXN")]
            my_cfg[my_cfg_fmt.index("PART_PER_TXN")] = n_ppt if n_ppt <= n_cnt else 1
            n_thd =  my_cfg[my_cfg_fmt.index("THREAD_CNT")]
            n_alg =  my_cfg[my_cfg_fmt.index("CC_ALG")]
#            my_cfg[my_cfg_fmt.index("THREAD_CNT")] = 1 if n_alg == "HSTORE" or n_alg == "HSTORE_SPEC" else n_thd
            if n_alg == "HSTORE" or n_alg == "HSTORE_SPEC":
#                my_cfg[my_cfg_fmt.index("PART_CNT")] = n_thd*n_cnt
                my_cfg_fmt = my_cfg_fmt + ["PART_CNT"]
                my_cfg = my_cfg + [n_thd*n_cnt]
            else:
#                my_cfg[my_cfg_fmt.index("PART_CNT")] = n_cnt
                my_cfg_fmt = my_cfg_fmt + ["PART_CNT"]
                my_cfg = my_cfg + [n_cnt]
#                my_cfg.pop(my_cfg_fmt.index("PART_CNT"))
#                my_cfg_fmt.remove("PART_CNT")
#            if "CLIENT_NODE_CNT" not in my_cfg_fmt:
#                my_cfg_fmt = my_cfg_fmt + ["CLIENT_NODE_CNT"]
#                my_cfg = my_cfg + [int(math.ceil(n_cnt/2)) if n_cnt > 1 else 1]

            cfgs = get_cfgs(my_cfg_fmt, my_cfg)
            cfgs = get_outfile_name(cfgs,my_cfg_fmt)
            if cfgs not in summary.keys(): 
                print("Not in summary: {}".format(cfgs))
                break
            try:
                tot_txn_cnt = sum(summary[cfgs]['txn_cnt'])
                avg_txn_cnt = avg(summary[cfgs]['txn_cnt'])
                tot_abrt_cnt = sum(summary[cfgs]['abort_cnt'])
                avg_abrt_cnt = avg(summary[cfgs]['abort_cnt'])
            except KeyError:
                print("KeyError: {} {} {} -- {}".format(v,x,my_cfg,cfgs))
                tpt[_v][xi] = 0
                continue
            # System Throughput: total txn count / average of all node's run time
            # Per Node Throughput: avg txn count / average of all node's run time
            # Per txn latency: total of all node's run time / total txn count
#print("Aborts:: {} / {} = {} -- {}".format(tot_abrt_cnt, tot_txn_cnt,(float(tot_abrt_cnt) / float(tot_txn_cnt)),cfgs))
            try:
                tpt[_v][xi] = (float(tot_abrt_cnt) / float(tot_txn_cnt))
            except ZeroDivisionError:
                print("ZeroDivisionError: {} {} {} -- {}".format(v,x,my_cfg,cfgs));
                tpt[_v][xi] = 0
            #tpt[v][xi] = (avg_txn_cnt/avg_run_time)

    bbox = [0.5,0.95]
    if vname == "NETWORK_DELAY":
        bbox = [0.8,0.95]
    print("Created plot {}".format(name))
    draw_line(name,tpt,xval,ylab='Average Aborts / Txn',xlab=xname,title=_title,bbox=bbox,ncol=2) 



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
    stack_names = [
    'Index',
    'Abort',
    'Cleanup',
    'Wait',
    '2PC',
    'Backoff',
    'Queue',
    'Remote',
    'Work'
    ]
#stack_names = ['Useful Work','Abort','Timestamp','Index','Lock Wait','Remote Wait','Manager']
    _title = ''
    _ymax=1.0
    if normalized:
        _title = 'Time Breakdown Normalized {}'.format(title)
    else:
        _title = 'Time Breakdown {}'.format(title)
    name = '{}'.format(_title.replace(" ","_").lower())

    if xname == "ABORT_PENALTY":
        _xval = [(float(x.replace("UL","")))/1000000000 for x in xval]
        sort_idxs = sorted(range(len(_xval)),key=lambda x:_xval[x])
        xval = [xval[i] for i in sort_idxs]
        _xval = sorted(_xval)
        _xlab = xname + " (Sec)"
    else:
        _xval = xval
        _xlab = xname

    time_wait = [0] * len(xval)
    time_index = [0] * len(xval)
    time_abort = [0] * len(xval)
    time_queue = [0] * len(xval)
    time_net = [0] * len(xval)
    time_twopc = [0] * len(xval)
    time_backoff = [0] * len(xval)
    time_work = [0] * len(xval)
    time_cleanup = [0] * len(xval)
    total = [1] * len(xval)

    for x,i in zip(xval,range(len(xval))):
        my_cfg_fmt = cfg_fmt + [xname]
        my_cfg = cfg + [x]
#            my_cfg_fmt = cfg_fmt
#            my_cfg = cfg
#            my_cfg[my_cfg_fmt.index(xname)] = x
#            my_cfg[my_cfg_fmt.index(vname)] = v
        n_cnt = my_cfg[my_cfg_fmt.index("NODE_CNT")]
        n_clt = my_cfg[my_cfg_fmt.index("CLIENT_NODE_CNT")]
        my_cfg[my_cfg_fmt.index("CLIENT_NODE_CNT")] = int(math.ceil(n_cnt/2)) if n_cnt > 1 else 1
        n_ppt = my_cfg[my_cfg_fmt.index("PART_PER_TXN")]
        my_cfg[my_cfg_fmt.index("PART_PER_TXN")] = n_ppt if n_ppt <= n_cnt else 1
        n_thd =  my_cfg[my_cfg_fmt.index("THREAD_CNT")]
        n_alg =  my_cfg[my_cfg_fmt.index("CC_ALG")]
#            my_cfg[my_cfg_fmt.index("THREAD_CNT")] = 1 if n_alg == "HSTORE" or n_alg == "HSTORE_SPEC" else n_thd
        if n_alg == "HSTORE" or n_alg == "HSTORE_SPEC":
#                my_cfg[my_cfg_fmt.index("PART_CNT")] = n_thd*n_cnt
            my_cfg_fmt = my_cfg_fmt + ["PART_CNT"]
            my_cfg = my_cfg + [n_thd*n_cnt]
        else:
#                my_cfg[my_cfg_fmt.index("PART_CNT")] = n_cnt
            my_cfg_fmt = my_cfg_fmt + ["PART_CNT"]
            my_cfg = my_cfg + [n_cnt]
#                my_cfg.pop(my_cfg_fmt.index("PART_CNT"))
#                my_cfg_fmt.remove("PART_CNT")
#            if "CLIENT_NODE_CNT" not in my_cfg_fmt:
#                my_cfg_fmt = my_cfg_fmt + ["CLIENT_NODE_CNT"]
#                my_cfg = my_cfg + [int(math.ceil(n_cnt/2)) if n_cnt > 1 else 1]

        cfgs = get_cfgs(my_cfg_fmt, my_cfg)
        cfgs = get_outfile_name(cfgs,my_cfg_fmt)
        if cfgs not in summary.keys(): break
        try:
            time_index[i] = avg(summary[cfgs]['txn_time_idx'])
            time_abort[i] = avg(summary[cfgs]['txn_time_abrt'])
            time_cleanup[i] = avg(summary[cfgs]['txn_time_clean'])
            time_wait[i] = avg(summary[cfgs]['txn_time_wait'])
            time_twopc[i] = avg(summary[cfgs]['txn_time_twopc'])
            time_backoff[i] = avg(summary[cfgs]['txn_time_q_abrt'])
            time_queue[i] = avg(summary[cfgs]['txn_time_q_work'])
            time_net[i] = avg(summary[cfgs]['txn_time_net'])
            time_work[i] = avg(summary[cfgs]['txn_time_misc']) + avg(summary[cfgs]['txn_time_copy'])
            total[i] = sum(time_index[i] + time_abort[i] + time_cleanup[i] + time_wait[i] + time_twopc[i] + time_backoff[i] + time_queue[i] + time_net[i] + time_work[i] )

        except KeyError:
            print("KeyError: {} {}".format(x,cfg))


    if normalized:
        time_index = [i / j for i,j in zip(time_index,total)]
        time_abort =  [i / j for i,j in zip(time_abort,total)]
        time_cleanup =  [i / j for i,j in zip(time_cleanup,total)]
        time_wait =  [i / j for i,j in zip(time_wait,total)]
        time_twopc =  [i / j for i,j in zip(time_twopc,total)]
        time_backoff =  [i / j for i,j in zip(time_backoff,total)]
        time_queue =  [i / j for i,j in zip(time_queue,total)]
        time_net =  [i / j for i,j in zip(time_net,total)]
        time_work =  [i / j for i,j in zip(time_work,total)]
        _ymax = 1
    else:
        _ymax = max(total)
    data = [time_index,
        time_abort,
        time_cleanup,
        time_wait,
        time_twopc,
        time_backoff, 
        time_queue,
        time_net, 
        time_work]
    print data
    draw_stack(data,_xval,stack_names,figname=name,title=_title,ymax=_ymax)
    print("Created plot {}".format(name))


# Stack graph of time breakdowns
# mpr: list of MPR values to plot along the x-axis
# nodes: node count to plot
# algos: CC algo to plot
# max_txn: MAX_TXN_PER_PART
# summary: dictionary loaded with results
# normalized: if true, normalize the results
def time_breakdown_basic(xval,summary,
        normalized=False,
        xname="MPR",
        cfg_fmt=[],
        cfg=[],
        title=''
        ):
    stack_names = ['Work','Lock Wait','Rem Wait']
    _title = ''
    _ymax=1.0
    if normalized:
        _title = 'Basic Time Breakdown Normalized {}'.format(title)
    else:
        _title = 'Basic Time Breakdown {}'.format(title)
    name = '{}'.format(_title.replace(" ","_").lower())

    run_time = [0] * len(xval)
    time_work = [0] * len(xval)
    time_wait_lock = [0] * len(xval)
    time_wait = [0] * len(xval)

    for x,i in zip(xval,range(len(xval))):
        _cfgs = get_cfgs(cfg_fmt + [xname],cfg + [x])
        cfgs = get_outfile_name(_cfgs)
        if cfgs not in summary.keys(): break
        try:
            if normalized:
                run_time[i] = avg(summary[cfgs]['run_time'])
            else:
                run_time[i] = 1.0
            time_work[i] = avg(summary[cfgs]['time_work']) / run_time[i]
            time_wait_lock[i] = avg(summary[cfgs]['time_wait_lock']) / run_time[i]

            if normalized:
                if sum([time_work[i],time_wait_lock[i]]) > 1.0:
                    print("Sum error: {} + {} = {}; {} {}".format(time_work[i],time_wait_lock[i],sum([time_work[i],time_wait_lock[i]]),x,cfg))
                #assert(sum([time_work[i],time_wait_lock[i]]) <= 1.0)
                time_wait[i] = 1.0 - sum([time_work[i],time_wait_lock[i]])
            else:
                time_wait[i] = avg(summary[cfgs]['run_time']) - sum([time_work[i],time_wait_lock[i]])
            _ymax = max(_ymax, sum([time_wait[i],time_wait_lock[i],time_work[i]]))
        except KeyError:
            print("KeyError: {} {}".format(x,cfg))
            run_time[i] = 1.0
            time_work[i] = 0.0
            time_wait[i] = 0.0
    data = [time_wait,time_wait_lock,time_work]

    draw_stack(data,xval,stack_names,figname=name,title=_title,ymax=_ymax)


# Cumulative density function of total number of aborts per transaction
# mpr: list of MPR values to plot along the x-axis
# nodes: node count to plot
# algos: CC algo to plot
# max_txn: MAX_TXN_PER_PART
# summary: dictionary loaded with results
def cdf(vval,summary,
        cfg_fmt=[],
        cfg=[],
        xname="Abort Count",
        vname="CC_ALG",
        xlabel="all_abort",
        title=""
        ):

    name = 'cdf_{}_{}'.format(vname.lower(),title.replace(" ","_").lower())
    _title = 'CDF {}'.format(title)
    ys = {}
    # find max abort
    max_abort = 0
    for v in vval:
        cfgs = get_cfgs(cfg_fmt + [vname], cfg + [v] )
        cfgs = get_outfile_name(cfgs)
        if cfgs not in summary.keys(): break
        try:
            if len(summary[cfgs][xlabel]) == 0: continue
            max_abort = max(max_abort, max(summary[cfgs][xlabel].keys()))
        except KeyError:
            print("KeyError: {} {}".format(xlabel,cfgs))
            max_abort = max_abort

    xs = range(max_abort + 1)
    for v in vval:
        ys[v] = [0] * (max_abort + 1)
        cfgs = get_cfgs(cfg_fmt + [vname], cfg + [v] )
        cfgs = get_outfile_name(cfgs)
        if cfgs not in summary.keys(): break
        y = 0
        for x in xs:
            try:
                if x in summary[cfgs][xlabel].keys():
                    ys[v][x] = y + (float(summary[cfgs][xlabel][x]) / float(summary[cfgs][xlabel+"_cnt"]))
                    y = ys[v][x]
                else:
                    ys[v][x] = y
            except KeyError:
                print("KeyError: {} {} {}".format(cfgs,xlabel,x))
                ys[v][x] = y


    draw_line(name,ys,xs,ylab='Percent',xlab=xname,title=_title,bbox=[0.8,0.6],ylimit=1.0) 


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

def bar_keys(
        summary,
        rank=0,
        cfg_fmt=[],
        cfg=[],
        title=''
        ):

    _cfgs = get_cfgs(cfg_fmt,cfg)
    cfgs = get_outfile_name(_cfgs)
    if cfgs not in summary.keys(): return

    keys = ["d_cflt","d_abrt","s_cflt","s_abrt"]

    for k in keys:
        _title = 'Bar Keys {} {}'.format(k,title)
        name = '{}'.format(_title.replace(" ","_").lower())

        try:
            xs = [i[0] for i in sorted(summary[cfgs][k].items(),key=operator.itemgetter(1),reverse=True)];
        except KeyError:
            print("KeyError bar keys {} {}".format(k,cfgs))
            continue
        if rank > 0:
            xs = xs[:rank]
        ys = [0] * len(xs)
        for x,n in zip(xs,range(len(xs))):
            try:
                ys[n] = summary[cfgs][k][x]
            except KeyError:
                ys[n] = 0

        draw_bars_single(ys,xs,ylab='# Transactions',xlab='Key',title=_title,figname=name) 


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


