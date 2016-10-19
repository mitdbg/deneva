import os, sys, re, math, os.path
import numpy as np
import operator
from helper import get_cfgs 
from helper import write_summary_file,get_summary_stats
from draw import *
import types
import latency_stats as ls
import pprint
#from experiments import experiments as experiments
#from experiments import configs

plot_cnt = 0
PATH=os.getcwd()

def apply_extras(my_cfg_fmt,my_cfg,extras,xname,vname):
    for e in extras.keys():
        if e in my_cfg_fmt:
            if extras[e] in my_cfg_fmt:
                new_val = my_cfg[my_cfg_fmt.index(extras[e])]
            else:
                new_val = extras[e]
            if e == 'READ_PERC':
                my_cfg[my_cfg_fmt.index(e)] = 1-new_val
            elif e == 'PART_PER_TXN':
                my_cfg[my_cfg_fmt.index(e)] = new_val
#                        my_cfg[my_cfg_fmt.index(e)] = min(my_cfg[my_cfg_fmt.index(extras[e])],8)
            else:
                my_cfg[my_cfg_fmt.index(e)] = new_val
        else:
            if extras[e] in my_cfg_fmt:
                new_val = my_cfg[my_cfg_fmt.index(extras[e])]
            else:
                new_val = extras[e]
            my_cfg_fmt = my_cfg_fmt + [e]
            if e == 'READ_PERC':
                my_cfg = my_cfg + [1.0-new_val]
            elif e == 'PART_PER_TXN':
#                        my_cfg = my_cfg + [min(my_cfg[my_cfg_fmt.index(extras[e])],8)]
                my_cfg = my_cfg + [new_val]
            else:
                my_cfg = my_cfg + [new_val]
    if "PART_PER_TXN" in my_cfg_fmt and "PART_CNT" in my_cfg_fmt and my_cfg[my_cfg_fmt.index("PART_PER_TXN")] > my_cfg[my_cfg_fmt.index("PART_CNT")]:
        my_cfg[my_cfg_fmt.index("PART_PER_TXN")] = my_cfg[my_cfg_fmt.index("PART_CNT")]
    return my_cfg,my_cfg_fmt

def progress_diff(summary,ncnt,stat,name=''):
    print(stat)
    output = {}
    for n in range(ncnt):
        nint = n
        n = str(n)
        clk = [int(x) for x in summary[n]['clock_time']]
        arr = summary[n][stat]
        print(arr)
        output[n] = [arr[i]/clk[i] if i == 0 else (arr[i] - arr[i-1])/(clk[i]-clk[i-1]) for i in range(len(arr))]
        for j in range(nint):
            j = str(j)
            if len(output[j]) < len(output[n]):
                output[n] = output[n][:len(output[j])]
                clk = clk[:len(output[j])]
            if len(output[j]) > len(output[n]):
                output[j] = output[j][:len(output[n])]
        print(output[n])
        xval = [int(c) for c in clk]

#    try:
#        output["total"] = [ sum(output[str(n)][i] for n in range(ncnt)) for i in range(len(output["0"])) ]
#        output["avg"] = [ int(float(sum(output["total"])) / float(ncnt) / float(len(output["total"]))) ] * len(output["total"])
#    except ValueError:
#        output["total"] = [0] * len(output["0"])
#        output["avg"] = [0] * len(output["0"])

    bbox = [0.4,0.9]
    draw_line('prog_'+stat+name,output,xval,ylab=stat+'/sec',xlab="Interval",title="Progress "+stat,bbox=bbox,ncol=2) 


def progress(summary,ncnt,stat,name=''):
    print(stat)
    output = {}
    for n in range(ncnt):
        nint = n
        n = str(n)
        clk = [int(x) for x in summary[n]['clock_time']]
        arr = summary[n][stat]
        print(arr)
        output[n] = [arr[i] for i in range(len(arr))]
#        output[n] = [arr[i]/clk[i] if i == 0 else (arr[i])/(clk[i]-clk[i-1]) for i in range(len(arr))]
        for j in range(nint):
            j = str(j)
            if len(output[j]) < len(output[n]):
                output[n] = output[n][:len(output[j])]
                clk = clk[:len(output[j])]
            if len(output[j]) > len(output[n]):
                output[j] = output[j][:len(output[n])]
        print(output[n])
        xval = [int(c) for c in clk]

#    try:
#        output["total"] = [ sum(output[str(n)][i] for n in range(ncnt)) for i in range(len(output["0"])) ]
#        output["avg"] = [ int(float(sum(output["total"])) / float(ncnt) / float(len(output["total"]))) ] * len(output["total"])
#    except ValueError:
#        output["total"] = [0] * len(output["0"])
#        output["avg"] = [0] * len(output["0"])

    bbox = [0.4,0.9]
    draw_line('prog_'+stat+name,output,xval,ylab=stat+'/sec',xlab="Interval",title="Progress "+stat,bbox=bbox,ncol=2) 

#Use summary from client nodes
def tput_v_lat(xval,vval,summary,summary_cl,
        cfg_fmt=[],
        cfg=[],
        xname="MPR",
        vname="CC_ALG",
        title=""
        ):
    global plot_cnt
    tpt = {}
    _tpt = {}
    name = 'tputvlat_plot{}'.format(plot_cnt)
    plot_cnt += 1
    _title = title

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
        _tpt[_v] = [0] * len(xval)

        for x,xi in zip(xval,range(len(xval))):
            my_cfg_fmt = cfg_fmt + [xname] + [vname]
            my_cfg = cfg + [x] + [v]

            print(my_cfg_fmt)
            print(my_cfg)
            cfgs = get_cfgs(my_cfg_fmt, my_cfg)
            cfgs = get_outfile_name(cfgs,my_cfg_fmt)
            if cfgs not in summary.keys(): 
                print("Not in summary: {}".format(cfgs))
                continue 
            try:
                tot_run_time = sum(summary[cfgs]['clock_time'])
#                tot_lat = sum(summary[cfgs]['latency'])
                tot_lat = sum(summary_cl[cfgs]['latency'])
                avg_run_time = avg(summary[cfgs]['clock_time'])
                avg_txn_lat = avg(summary_cl[cfgs]['latency'])
                tot_txn_cnt = sum(summary_cl[cfgs]['txn_cnt'])
#                avg_txn_lat = avg(summary[cfgs]['latency'])
#                tot_txn_cnt = sum(summary[cfgs]['txn_cnt'])
            except KeyError:
                print("KeyError: {} {} {} -- {}".format(v,x,cfg,cfgs))
                tpt[_v][xi] = 0
                continue
            # System Throughput: total txn count / average of all node's run time
            # Per Node Throughput: avg txn count / average of all node's run time
            # Per txn latency: total of all node's run time / total txn count
            tpt[_v][xi] = avg_txn_lat
            _tpt[_v][xi] = tot_txn_cnt / avg_run_time
            #tpt[v][xi] = (avg_txn_cnt/avg_run_time)

#    bbox = [0.7,0.9]
    bbox = [1.0,0.95]
    print("Created plot {}".format(name))
    draw_line2(name,tpt,_tpt,ylab='Latency (s/txn)',xlab='Throughput (txn/sec)',title=_title,bbox=bbox,ncol=2) 

def line_general(xval,vval,summary,summary_cl,
        key,
        cfg_fmt=[],
        cfg=[],
        xname="MPR",
        vname="CC_ALG",
        title="",
        extras={},
        ylimit=0,
        logscalex=False
        ):
    global plot_cnt
    data = {}
    name = '{}_plot{}'.format(key,plot_cnt)
    plot_cnt += 1
    _title = title

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
        data[_v] = [0] * len(xval)

        for x,xi in zip(xval,range(len(xval))):
            my_cfg_fmt = cfg_fmt + [xname] + [vname]
            my_cfg = cfg + [x] + [v]
            my_cfg,my_cfg_fmt = apply_extras(my_cfg_fmt,my_cfg,extras,xname,vname)

            cfgs = get_cfgs(my_cfg_fmt, my_cfg)
            cfgs = get_outfile_name(cfgs,my_cfg_fmt)
            if cfgs not in summary.keys(): 
                print("Not in summary: {}".format(cfgs))
                continue 
            try:
                tot = avg(summary[cfgs][key])
            except KeyError:
                print("KeyError: {} {} {} -- {}".format(v,x,cfg,cfgs))
                data[_v][xi] = 0
                continue
            data[_v][xi] = tot

#    bbox = [0.7,0.9]
    bbox = [1.0,0.95]
    if xname == 'ACCESS_PERC':
        _xval = [int(x*100) for x in _xval]
    if xname == 'NETWORK_DELAY':
        _xval = [float(x)/1000000 for x in _xval]
        base = 10
    if xname == 'MAX_TXN_IN_FLIGHT':
        _xval = [float(x)/100000 for x in _xval]
#        _xval = [float(x)*nnodes/100000 for x in _xval]

    if logscalex:
        _xlab = _xlab + " (Log Scale)"
    print("Created plot {}".format(name))
    draw_line(name,data,_xval,ylab='',xlab=_xlab,title=_title,bbox=bbox,ncol=2,ltitle=vname,ylimit=ylimit,logscalex=logscalex) 

def line_rate(xval,vval,summary,summary_cl,
        key,
        cfg_fmt=[],
        cfg=[],
        xname="MPR",
        vname="CC_ALG",
        title="",
        extras={}
        ):
    global plot_cnt
    data = {}
    name = '{}_rate_plot{}'.format(key,plot_cnt)
    plot_cnt += 1
    _title = title

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
        data[_v] = [0] * len(xval)

        for x,xi in zip(xval,range(len(xval))):
            my_cfg_fmt = cfg_fmt + [xname] + [vname]
            my_cfg = cfg + [x] + [v]
            my_cfg,my_cfg_fmt = apply_extras(my_cfg_fmt,my_cfg,extras,xname,vname)

            cfgs = get_cfgs(my_cfg_fmt, my_cfg)
            cfgs = get_outfile_name(cfgs,my_cfg_fmt)
            if cfgs not in summary.keys(): 
                print("Not in summary: {}".format(cfgs))
                continue 
            try:
                tcnt = avg(summary[cfgs]['txn_cnt'])
                if tcnt == 0:
                    tcnt = 1
                tot = avg(summary[cfgs][key]) / tcnt
            except KeyError:
                print("KeyError: {} {} {} -- {}".format(v,x,cfg,cfgs))
                data[_v][xi] = 0
                continue
            data[_v][xi] = tot

#    bbox = [0.7,0.9]
    bbox = [1.0,0.95]
    print("Created plot {}".format(name))
    draw_line(name,data,_xval,ylab='',xlab=_xlab,title=_title,bbox=bbox,ncol=2,ltitle=vname) 

def latency(xval,vval,summary,summary_cl,
        cfg_fmt=[],
        cfg=[],
        lst={},
        xname="MPR",
        vname="CC_ALG",
        title="",
        extras={},
        name="",
        xlab="",
        new_cfgs = {},
        ylimit=0,
        logscale=False,
        logscalex=False,
        legend=False,
#        legend=True,
        ):
    global plot_cnt
    ccl50 = {}
    ccl99 = {}
    fscl50 = {}
    fscl99 = {}
    lscl50 = {}
    lscl99 = {}
    sacl50 = {}
    sacl99 = {}
    if name == "":
        name = 'latency_plot{}'.format(plot_cnt)
    plot_cnt += 1
#    name += '_{}'.format(title.replace(" ","_").lower())
#    _title = 'System Throughput {}'.format(title)
    _title = title
    stats={}

#    print "X-axis: " + str(xval)
#    print "Var: " + str(vval)
    if xname == "ABORT_PENALTY":
        _xval = [(float(x.replace("UL","")))/1000000000 for x in xval]
        sort_idxs = sorted(range(len(_xval)),key=lambda x:_xval[x])
        xval = [xval[i] for i in sort_idxs]
        _xval = sorted(_xval)
        _xlab = xname + " (Sec)"
    else:
        _xval = xval
        if xlab == "":
            _xlab = xname
        else:
            _xlab = xlab
    for v in vval:
        if vname == 'MODE':
            mode_nice = {"NOCC_MODE":"No CC","NORMAL_MODE":"Serializable Execution","QRY_ONLY_MODE":"No Concurrency Control"}
            _v = mode_nice[v]
        else:
            _v = v
        ccl50[_v] = [0] * len(xval)
        ccl99[_v] = [0] * len(xval)
        fscl50[_v] = [0] * len(xval)
        fscl99[_v] = [0] * len(xval)
        lscl50[_v] = [0] * len(xval)
        lscl99[_v] = [0] * len(xval)
        sacl50[_v] = [0] * len(xval)
        sacl99[_v] = [0] * len(xval)

        for x,xi in zip(xval,range(len(xval))):
            if new_cfgs != {}:
                my_cfg_fmt = cfg_fmt + [xname] + [vname]
                my_cfg = new_cfgs[(x,v)] + [x] + [v]
                my_cfg,my_cfg_fmt = apply_extras(my_cfg_fmt,my_cfg,extras,xname,vname)
            else:
                my_cfg_fmt = cfg_fmt + [xname] + [vname]
                my_cfg = cfg + [x] + [v]
                my_cfg,my_cfg_fmt = apply_extras(my_cfg_fmt,my_cfg,extras,xname,vname)
            if lst != {}:
                my_cfg_fmt = cfg_fmt + [xname] + [vname]
                my_cfg = lst[(x,v)] + [x] + [v]
                my_cfg,my_cfg_fmt = apply_extras(my_cfg_fmt,my_cfg,extras,xname,vname)
                print("fmt ({},{}): {}".format(x,v,my_cfg_fmt))
                print("lst ({},{}): {}".format(x,v,my_cfg))

            cfgs = get_cfgs(my_cfg_fmt, my_cfg)
            cc = cfgs["CC_ALG"]
            nnodes = cfgs["NODE_CNT"]
            cfgs = get_outfile_name(cfgs,my_cfg_fmt)
            print(cfgs)
            tmp = 0
            if cfgs not in summary.keys(): 
                print("Not in summary: {}".format(cfgs))
                continue 
            tmp = 0
            try:
                s = summary
                s2 = summary_cl
#                ccl50[_v][xi] = min(avg(s2[cfgs]['ccl50']),60)
#                ccl99[_v][xi] = min(avg(s2[cfgs]['ccl99']),60)
                fscl50[_v][xi] = min(avg(s[cfgs]['fscl50']),60)
                fscl99[_v][xi] = min(avg(s[cfgs]['fscl99']),60)
#                lscl50[_v][xi] = min(avg(s[cfgs]['lscl50']),60)
#                lscl99[_v][xi] = min(avg(s[cfgs]['lscl99']),60)
#                sacl50[_v][xi] = min(avg(s[cfgs]['sacl50']),60)
#                sacl99[_v][xi] = min(avg(s[cfgs]['sacl99']),60)
            except KeyError:
                print("KeyError: {}, {} {} -- {}".format(tmp,v,x,cfgs))
                ccl50[_v][xi] = 0
                ccl99[_v][xi] = 0
                fscl50[_v][xi] = 0
                fscl99[_v][xi] = 0
                lscl50[_v][xi] = 0
                lscl99[_v][xi] = 0
                sacl50[_v][xi] = 0
                sacl99[_v][xi] = 0
                continue
            stats = get_summary_stats(stats,summary[cfgs],summary_cl[cfgs],x,v,cc)

    pp = pprint.PrettyPrinter()
    pp.pprint(ccl50)
    pp.pprint(ccl99)
    pp.pprint(fscl50)
    pp.pprint(fscl99)
    pp.pprint(lscl50)
    pp.pprint(lscl99)
    pp.pprint(sacl50)
    pp.pprint(sacl99)
    bbox = [1.0,0.95]
    base = 2
    if xname == 'TXN_WRITE_PERC':
        _xval = [int(x*100) for x in _xval]
    if xname == 'ACCESS_PERC':
        _xval = [int(x*100) for x in _xval]
    if xname == 'NETWORK_DELAY':
        _xval = [float(x)/1000000 for x in _xval]
        base = 10
#    if xname == 'MAX_TXN_IN_FLIGHT':
#        _xval = [float(x)*nnodes/100000 for x in _xval]
    if xname == 'LOAD_PER_SERVER':
        _xval = [float(x)*nnodes/1000 for x in _xval]
#bbox = [0.7,0.9]
    print("Created plot {} {}".format(name,_title))
    if logscalex:
        _xlab = _xlab + " (Log Scale)"
    _ylab= 'Latency (s)'
    if logscale:
        _ylab = 'Latency\n(s, Log Scale)'
    print(_xval)
    
    if "WAIT_DIE" in fscl99.keys():
        fscl99["WAIT_DIE"] = [ 60 if x > 2 else y for x,y in zip(_xval,fscl99["WAIT_DIE"])]
        
    draw_line(name+'ccl50',ccl50,_xval,ylab=_ylab,xlab=_xlab,title=_title,bbox=bbox,ncol=2,ltitle=vname,ylimit=ylimit,logscale=logscale,logscalex=logscalex,legend=legend,base=base)
    draw_line(name+'ccl99',ccl99,_xval,ylab=_ylab,xlab=_xlab,title=_title,bbox=bbox,ncol=2,ltitle=vname,ylimit=ylimit,logscale=logscale,logscalex=logscalex,legend=legend,base=base)
    draw_line(name+'fscl50',fscl50,_xval,ylab=_ylab,xlab=_xlab,title=_title,bbox=bbox,ncol=2,ltitle=vname,ylimit=ylimit,logscale=logscale,logscalex=logscalex,legend=legend,base=base)
    draw_line(name+'fscl99',fscl99,_xval,ylab=_ylab,xlab=_xlab,title=_title,bbox=bbox,ncol=2,ltitle=vname,ylimit=ylimit,logscale=logscale,logscalex=logscalex,legend=legend,base=base)
    draw_line(name+'lscl50',lscl50,_xval,ylab=_ylab,xlab=_xlab,title=_title,bbox=bbox,ncol=2,ltitle=vname,ylimit=ylimit,logscale=logscale,logscalex=logscalex,legend=legend,base=base)
    draw_line(name+'lscl99',lscl99,_xval,ylab=_ylab,xlab=_xlab,title=_title,bbox=bbox,ncol=2,ltitle=vname,ylimit=ylimit,logscale=logscale,logscalex=logscalex,legend=legend,base=base)
    draw_line(name+'sacl50',sacl50,_xval,ylab=_ylab,xlab=_xlab,title=_title,bbox=bbox,ncol=2,ltitle=vname,ylimit=ylimit,logscale=logscale,logscalex=logscalex,legend=legend,base=base)
    draw_line(name+'sacl99',sacl99,_xval,ylab=_ylab,xlab=_xlab,title=_title,bbox=bbox,ncol=2,ltitle=vname,ylimit=ylimit,logscale=logscale,logscalex=logscalex,legend=legend,base=base)


   
def tput(xval,vval,summary,summary_cl,
        cfg_fmt=[],
        cfg=[],
        lst={},
        xname="MPR",
        vname="CC_ALG",
        title="",
        extras={},
        name="",
        xlab="",
        new_cfgs = {},
        ylimit=0,
        logscale=False,
        logscalex=False,
        legend=False,
#        legend=True,
        ):
    global plot_cnt
    tpt = {}
    pntpt = {}
    if name == "":
        name = 'tput_plot{}'.format(plot_cnt)
    plot_cnt += 1
#    name += '_{}'.format(title.replace(" ","_").lower())
#    _title = 'System Throughput {}'.format(title)
    _title = title
    stats={}

#    print "X-axis: " + str(xval)
#    print "Var: " + str(vval)
    if xname == "ABORT_PENALTY":
        _xval = [(float(x.replace("UL","")))/1000000000 for x in xval]
        sort_idxs = sorted(range(len(_xval)),key=lambda x:_xval[x])
        xval = [xval[i] for i in sort_idxs]
        _xval = sorted(_xval)
        _xlab = xname + " (Sec)"
    else:
        _xval = xval
        if xlab == "":
            _xlab = xname
        else:
            _xlab = xlab
    for v in vval:
        if vname == 'MODE':
            mode_nice = {"NOCC_MODE":"No CC","NORMAL_MODE":"Serializable Execution","QRY_ONLY_MODE":"No Concurrency Control"}
            _v = mode_nice[v]
        else:
            _v = v
        tpt[_v] = [0] * len(xval)
        pntpt[_v] = [0] * len(xval)

        for x,xi in zip(xval,range(len(xval))):
            if new_cfgs != {}:
                my_cfg_fmt = cfg_fmt + [xname] + [vname]
                my_cfg = new_cfgs[(x,v)] + [x] + [v]
                my_cfg,my_cfg_fmt = apply_extras(my_cfg_fmt,my_cfg,extras,xname,vname)
            else:
                my_cfg_fmt = cfg_fmt + [xname] + [vname]
                my_cfg = cfg + [x] + [v]
                my_cfg,my_cfg_fmt = apply_extras(my_cfg_fmt,my_cfg,extras,xname,vname)
            if lst != {}:
                my_cfg_fmt = cfg_fmt + [xname] + [vname]
                my_cfg = lst[(x,v)] + [x] + [v]
                my_cfg,my_cfg_fmt = apply_extras(my_cfg_fmt,my_cfg,extras,xname,vname)
                print("fmt ({},{}): {}".format(x,v,my_cfg_fmt))
                print("lst ({},{}): {}".format(x,v,my_cfg))

            cfgs = get_cfgs(my_cfg_fmt, my_cfg)
            cc = cfgs["CC_ALG"]
            nnodes = cfgs["NODE_CNT"]
            cfgs = get_outfile_name(cfgs,my_cfg_fmt)
            print(cfgs)
            tmp = 0
            if cfgs not in summary.keys(): 
                print("Not in summary: {}".format(cfgs))
                continue 
            tmp = 0
            try:
                s = summary_cl
#                s = summary
#                print s[cfgs]
#                print s[cfgs]['total_runtime']
                tot_run_time = sum(s[cfgs]['total_runtime'])
                tmp = 1
                tot_txn_cnt = sum(s[cfgs]['txn_cnt'])
                tot_txn_cnt = sum(s[cfgs]['txn_cnt']) - sum(s[cfgs]['post_warmup_txn_cnt'])
#                tot_txn_cnt = sum(s[cfgs]['txn_cnt']) - sum(s[cfgs]["progress"][5]['txn_cnt'])
                print("{} - {} -> {}".format(s[cfgs]['txn_cnt'],s[cfgs]['post_warmup_txn_cnt'],tot_txn_cnt))
                tmp = 2
                avg_run_time = avg(s[cfgs]['total_runtime'])
                avg_run_time = 60
                tmp = 3
#FIXME
                avg_txn_cnt = avg(s[cfgs]['txn_cnt'])
            except KeyError:
                print("KeyError: {}, {} {} -- {}".format(tmp,v,x,cfgs))
                tpt[_v][xi] = 0
                pntpt[_v][xi] = 0
                continue
            stats = get_summary_stats(stats,summary[cfgs],summary_cl[cfgs],x,v,cc)
            # System Throughput: total txn count / average of all node's run time
            # Per Node Throughput: avg txn count / average of all node's run time
            # Per txn latency: total of all node's run time / total txn count
#FIXME
#            avg_run_time = 60
            tpt[_v][xi] = (tot_txn_cnt/avg_run_time/1000)
            pntpt[_v][xi] = (tot_txn_cnt/avg_run_time/nnodes)
            print("{} {} TTC {} Avg {}".format(_v,xi,tot_txn_cnt,avg_run_time))
            #tpt[v][xi] = (avg_txn_cnt/avg_run_time)

#    if name == "tput_ycsb_gold":
#        tpt["Single Server"] = [tpt["Serializable Execution"][0]]*len(_xval)
    pp = pprint.PrettyPrinter()
    pp.pprint(tpt)
    write_summary_file(name,stats,_xval,vval)
#    bbox = [0.8,0.35]
    bbox = [1.0,0.95]
    base = 2
    if xname == 'TXN_WRITE_PERC':
        _xval = [int(x*100) for x in _xval]
    if xname == 'ACCESS_PERC':
        _xval = [int(x*100) for x in _xval]
    if xname == 'NETWORK_DELAY':
        _xval = [float(x)/1000000 for x in _xval]
        base = 10
#    if xname == 'MAX_TXN_IN_FLIGHT':
#        _xval = [float(x)*nnodes/100000 for x in _xval]
    if xname == 'LOAD_PER_SERVER':
        _xval = [float(x)*nnodes/1000 for x in _xval]
#bbox = [0.7,0.9]
    print("Created plot {} {}".format(name,_title))
    if logscalex:
        _xlab = _xlab + " (Log Scale)"
    _ylab= 'System Throughput\n(Thousand txn/s)'
    if logscale:
        _ylab = 'System Throughput\n(Thousand txn/s, Log Scale)'
    print(_xval)
    
    # FIXME (Dana): MAAT --> OCC quick fix
    print tpt.keys()
    if "MAAT" in tpt.keys():
        occ_tput = tpt["MAAT"]
        del tpt["MAAT"]
        tpt["OCC"] = occ_tput
        
    draw_line(name,tpt,_xval,ylab=_ylab,xlab=_xlab,title=_title,bbox=bbox,ncol=2,ltitle=vname,ylimit=ylimit,logscale=logscale,logscalex=logscalex,legend=legend,base=base)
#    draw_line("pn"+name,pntpt,_xval,ylab='Throughput (Txn/sec)',xlab=_xlab,title="Per Node "+_title,bbox=bbox,ncol=2,ltitle=vname) 

def speedup(xval,vval,summary,summary_cl,
        cfg_fmt=[],
        cfg=[],
        lst={},
        xname="MPR",
        vname="CC_ALG",
        title="",
        extras={},
        name="",
        xlab="",
        new_cfgs = {},
        ylimit=0,
        logscale=False,
        logscalex=False,
#        legend=False,
        legend=True,
        ):
    global plot_cnt
    speedup = {}
    tpt = {}
    tpt_sn = {}
    if name == "":
        name = 'tput_plot{}'.format(plot_cnt)
    plot_cnt += 1
#    name += '_{}'.format(title.replace(" ","_").lower())
#    _title = 'System Throughput {}'.format(title)
    _title = title
    stats={}

    _xval = xval
    if xlab == "":
        _xlab = xname
    else:
        _xlab = xlab
    for v in vval:
        _v = v
        speedup[_v] = [0] * len(xval)
        tpt[_v] = [0] * len(xval)
        tpt_sn[_v] = [0] * len(xval)

        for x,xi in zip(xval,range(len(xval))):
            if new_cfgs != {}:
                my_cfg_fmt = cfg_fmt + [xname] + [vname]
                my_cfg = new_cfgs[(x,v)] + [x] + [v]
                my_cfg,my_cfg_fmt = apply_extras(my_cfg_fmt,my_cfg,extras,xname,vname)
            else:
                my_cfg_fmt = cfg_fmt + [xname] + [vname]
                my_cfg = cfg + [x] + [v]
                my_cfg,my_cfg_fmt = apply_extras(my_cfg_fmt,my_cfg,extras,xname,vname)
            if lst != {}:
                my_cfg_fmt = cfg_fmt + [xname] + [vname]
                my_cfg = lst[(x,v)] + [x] + [v]
                my_cfg,my_cfg_fmt = apply_extras(my_cfg_fmt,my_cfg,extras,xname,vname)
                print("fmt ({},{}): {}".format(x,v,my_cfg_fmt))
                print("lst ({},{}): {}".format(x,v,my_cfg))

            cfgs = get_cfgs(my_cfg_fmt, my_cfg)
            cc = cfgs["CC_ALG"]
            nnodes = cfgs["NODE_CNT"]
            single_node_cfgs = dict(cfgs)
            single_node_cfgs["NODE_CNT"] = 1
            cfgs = get_outfile_name(cfgs,my_cfg_fmt)
            single_node_cfgs = get_outfile_name(single_node_cfgs,my_cfg_fmt)
            print(cfgs)
            print(single_node_cfgs)
            if cfgs not in summary.keys() or single_node_cfgs not in summary.keys(): 
                print("Not in summary: {}".format(cfgs))
                print("Not in summary: {}".format(single_node_cfgs))
                continue 
            try:
                s = summary_cl
                tot_txn_cnt = sum(s[cfgs]['txn_cnt'])
                avg_run_time = avg(s[cfgs]['clock_time'])
                tot_txn_cnt_sn = sum(s[single_node_cfgs]['txn_cnt'])
                avg_run_time_sn = avg(s[single_node_cfgs]['clock_time'])
#FIXME
                avg_txn_cnt = avg(s[cfgs]['txn_cnt'])
            except KeyError:
#                print("KeyError: {}, {} {} -- {}".format(tmp,v,x,cfgs))
                tpt[_v][xi] = 0
                tpt_sn[_v][xi] = 1
                continue
            stats = get_summary_stats(stats,summary[cfgs],summary_cl[cfgs],x,v,cc)
            # System Throughput: total txn count / average of all node's run time
            # Per Node Throughput: avg txn count / average of all node's run time
            # Per txn latency: total of all node's run time / total txn count
            tpt[_v][xi] = (tot_txn_cnt/avg_run_time)
            tpt_sn[_v][xi] = (tot_txn_cnt_sn/avg_run_time_sn)
            speedup[_v][xi] = tpt[_v][xi] /  (tpt_sn[_v][xi] * nnodes)
            print("Speedup {} {}: {} / ({} * {}) = {}".format(_v,xi,tpt[_v][xi],tpt_sn[_v][xi],nnodes,speedup[_v][xi]))

    pp = pprint.PrettyPrinter()
    pp.pprint(tpt)
    pp.pprint(speedup)
#    bbox = [0.8,0.35]
    bbox = [1.0,0.95]
    write_summary_file(name,stats,_xval,vval)
    base = 2
#bbox = [0.7,0.9]
    print("Created plot {}".format(name))
    draw_line(name+"nolog",speedup,_xval,ylab='Speedup over linear scaling of single node',xlab=_xlab,title=_title,bbox=bbox,ncol=2,ltitle=vname,ylimit=ylimit,logscale=logscale,logscalex=False,legend=legend,base=base)
    if logscalex:
        _xlab = _xlab + " (Log Scale)"
    print(_xval)
    draw_line(name,speedup,_xval,ylab='Speedup over linear scaling of single node',xlab=_xlab,title=_title,bbox=bbox,ncol=2,ltitle=vname,ylimit=ylimit,logscale=logscale,logscalex=logscalex,legend=legend,base=base)


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
#                my_cfg[my_cfg_fmt.index("MAX_TXN_IN_FLIGHT")] /= my_cfg[my_cfg_fmt.index("NODE_CNT")]

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
        draw_line(name,lat,_xval,ylab='Latency-{}% (Sec)'.format(stat),xlab=_xlab,title=_title,bbox=bbox,ncol=2,ltitle=vname) 


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
#                my_cfg[my_cfg_fmt.index("MAX_TXN_IN_FLIGHT")] /= my_cfg[my_cfg_fmt.index("NODE_CNT")]

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
        draw_line(name,lat,_xval,ylab='Latency-{}% (Sec)'.format(stat),xlab=_xlab,title=_title,bbox=bbox,ncol=2,ltitle=vname) 


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
    

def abort_rate(xval,
        vval,
        summary,
        summary_cl,
        cfg_fmt=[],
        cfg=[],
        lst={},
        xname="MPR",
        vname="CC_ALG",
        title="",
        extras={},
        name="",
        xlab="",
        new_cfgs = {},
        logscalex=False
        ):
    tpt = {}
    if name == "":
        name = 'abortrate_{}_{}_{}'.format(xname.lower(),vname.lower(),title.replace(" ","_").lower())
    _title = 'Abort Rate {}'.format(title)

    for v in vval:
        if vname == "NETWORK_DELAY":
            _v = (float(v.replace("UL","")))/1000000
        else:
            _v = v
        tpt[_v] = [0] * len(xval)

        for x,xi in zip(xval,range(len(xval))):
            if new_cfgs != {}:
                my_cfg_fmt = cfg_fmt + [xname] + [vname]
                my_cfg = new_cfgs[(x,v)] + [x] + [v]
                my_cfg,my_cfg_fmt = apply_extras(my_cfg_fmt,my_cfg,extras,xname,vname)
            else:
                my_cfg_fmt = cfg_fmt + [xname] + [vname]
                my_cfg = cfg + [x] + [v]
                my_cfg,my_cfg_fmt = apply_extras(my_cfg_fmt,my_cfg,extras,xname,vname)
            if lst != {}:
                my_cfg_fmt = cfg_fmt + [xname] + [vname]
                my_cfg = lst[(x,v)] + [x] + [v]
                my_cfg,my_cfg_fmt = apply_extras(my_cfg_fmt,my_cfg,extras,xname,vname)
                print("fmt ({},{}): {}".format(x,v,my_cfg_fmt))
                print("lst ({},{}): {}".format(x,v,my_cfg))

            cfgs = get_cfgs(my_cfg_fmt, my_cfg)
            cc = cfgs["CC_ALG"]
            nnodes = cfgs["NODE_CNT"]
            cfgs = get_outfile_name(cfgs,my_cfg_fmt)
            print(cfgs)
            tmp = 0
            if cfgs not in summary.keys(): 
                print("Not in summary: {}".format(cfgs))
                continue            
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
    draw_line(name,tpt,xval,ylab='Average Aborts / Txn',xlab=xname,title=_title,bbox=bbox,ncol=2,ltitle=vname) 



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

    draw_line(name,tpt,mpr,ylab='Throughput (Txn/sec)',xlab='Multi-Partition Rate',title=_title,bbox=[0.5,0.95],ltitle=vname) 

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

    draw_line(name,tport_lat,mpr,ylab='Latency (s)',xlab='Multi-Partition Rate',title='Average Message Latency',bbox=[0.5,0.95],ltitle=vname) 

def stacks_general(xval,summary,
        keys,
        key_names=[],
        xname='MPR',
        normalized=False,
        cfg_fmt=[],
        cfg=[],
        title='',
        extras={}
        ):

    global plot_cnt
    if normalized:
        name = 'stack_norm_plot{}'.format(plot_cnt)
        _title = title + 'Normalized '
    else:
        name = 'stack_plot{}'.format(plot_cnt)
        _title = title
    plot_cnt += 1
    pp = pprint.PrettyPrinter()
    _ymax = 0

    if len(key_names) > 0:
        assert(len(key_names) == len(keys))
    else:
        key_names = keys

    data = [[0 for i in range(len(xval))] for t in range(len(keys))]
    for x,xi in zip(xval,range(len(xval))):
        my_cfg_fmt = cfg_fmt + [xname]
        my_cfg = cfg + [x]
        my_cfg,my_cfg_fmt = apply_extras(my_cfg_fmt,my_cfg,extras,xname,'')

        cfgs = get_cfgs(my_cfg_fmt, my_cfg)
        cfgs = get_outfile_name(cfgs,my_cfg_fmt)
        if cfgs not in summary.keys(): 
            print("Not in summary: {}".format(cfgs))
            continue 
        print(cfgs)
        total = 0
        for k,ki in zip(keys,range(len(keys))):
            try:
#                print("{} {} -- {} {}".format(k,ki,x,xi))
#                print(summary[cfgs][k])
                data[ki][xi] = avg(summary[cfgs][k])
#                data[ki][xi] = sum(summary[cfgs][k]) / sum(summary[cfgs]['txn_cnt'])
#                pp.pprint(data)
                total += data[ki][xi]
            except KeyError:
                print("KeyError: {} {}".format(x,cfg))
        if normalized:
            for k,ki in zip(keys,range(len(keys))):
                data[ki][xi] = data[ki][xi] / total
            total = 1
        if total > _ymax:
            _ymax = total
    for k in key_names:
        ki = key_names.index(k)
#        pp.pprint(key_names)
#        pp.pprint(data)
#        print("{}:{}".format( k, ki))
        if sum(data[ki]) == 0:
            del(data[ki])
            key_names.remove(k)


    pp.pprint(key_names)
    pp.pprint(data)
    draw_stack(data,xval,key_names,figname=name,title=_title,ymax=_ymax)
    print("Created plot {}".format(name))

def tput_stack(xval,summary,
        summary_cl,
        normalized=True,
        xname="NODE_CNT",
        cfg_fmt=[],
        cfg=[],
        title='',
        extras = {},
        name='',
        new_cfgs = {},
        legend=False
        ):
    stack_names = [
    'Throughput'
    ]
    global plot_cnt
    pp = pprint.PrettyPrinter()
    if name == "":
        name = 'tput_stack{}'.format(plot_cnt)
    plot_cnt += 1
    _ymax=1.0
#    if normalized:
#        _title = 'Normalized {}'.format(title)
#    else:
#        _title = title
    _title = title
    pp.pprint(cfg)

    if xname == "ABORT_PENALTY":
        _xval = [(float(x.replace("UL","")))/1000000000 for x in xval]
        sort_idxs = sorted(range(len(_xval)),key=lambda x:_xval[x])
        xval = [xval[i] for i in sort_idxs]
        _xval = sorted(_xval)
        _xlab = xname + " (Sec)"
    else:
        _xval = xval
        _xlab = xname
    tpt = [0] * len(xval)


    for x,i in zip(xval,range(len(xval))):
        if new_cfgs != {}:
            my_cfg_fmt = cfg_fmt + [xname]
            my_cfg = new_cfgs[(x,0)] + [x]
            my_cfg,my_cfg_fmt = apply_extras(my_cfg_fmt,my_cfg,extras,xname,'')
        else:
            my_cfg_fmt = cfg_fmt + [xname]
            my_cfg = cfg + [x]
            my_cfg,my_cfg_fmt = apply_extras(my_cfg_fmt,my_cfg,extras,xname,'')

        cfgs = get_cfgs(my_cfg_fmt, my_cfg)
        cfgs = get_outfile_name(cfgs,my_cfg_fmt)
        print(cfgs)
        if cfgs not in summary.keys(): 
            print("Not in summary")
            continue
        try:
            s = summary_cl
#                s = summary
#                print s[cfgs]
#                print s[cfgs]['total_runtime']
            tot_run_time = sum(s[cfgs]['total_runtime'])
            tmp = 1
            tot_txn_cnt = sum(s[cfgs]['txn_cnt'])
            tot_txn_cnt = sum(s[cfgs]['txn_cnt']) - sum(s[cfgs]['post_warmup_txn_cnt'])
#                tot_txn_cnt = sum(s[cfgs]['txn_cnt']) - sum(s[cfgs]["progress"][5]['txn_cnt'])
            print("{} - {} -> {}".format(s[cfgs]['txn_cnt'],s[cfgs]['post_warmup_txn_cnt'],tot_txn_cnt))
            tmp = 2
            avg_run_time = avg(s[cfgs]['total_runtime'])
            avg_run_time = 60
            tmp = 3
#FIXME
            avg_txn_cnt = avg(s[cfgs]['txn_cnt'])
        except KeyError:
            print("KeyError: {}".format(cfgs))
            tpt[i] = 0

        tpt[i] = (tot_txn_cnt/avg_run_time/1000)
    pp = pprint.PrettyPrinter()

    _ymax = max(tpt) * 1.10
    data = [
        tpt
        ]
    pp.pprint(data)
    
    # Quick and dirty label fix by Dana
    if "MAAT" in _xval:
        _xval[_xval.index("MAAT")] = "OCC"
    draw_stack(data,_xval,stack_names,figname=name,title=_title,ymax=_ymax,legend=False,ylab='System Throughput\n(Thousand txn/s)')
    print("Created plot {}".format(name))

# Stack graph of time breakdowns
# mpr: list of MPR values to plot along the x-axis
# nodes: node count to plot
# algos: CC algo to plot
# max_txn: MAX_TXN_PER_PART
# summary: dictionary loaded with results
# normalized: if true, normalize the results
def time_breakdown(xval,summary,
        normalized=True,
        xname="NODE_CNT",
        cfg_fmt=[],
        cfg=[],
        title='',
        extras = {},
        name='',
        new_cfgs = {},
        legend=False
        ):
    stack_names = [
    'Idle',
    'Abort',
    #'Index',
    '2PC',
    'CC Manager',
    'Txn Manager',
    'Useful Work'
    ]
    global plot_cnt
    pp = pprint.PrettyPrinter()
    if name == "":
        name = 'breakdown_{}'.format(plot_cnt)
    plot_cnt += 1
    _ymax=1.0
#    if normalized:
#        _title = 'Normalized {}'.format(title)
#    else:
#        _title = title
    _title = title
    pp.pprint(cfg)

    if xname == "ABORT_PENALTY":
        _xval = [(float(x.replace("UL","")))/1000000000 for x in xval]
        sort_idxs = sorted(range(len(_xval)),key=lambda x:_xval[x])
        xval = [xval[i] for i in sort_idxs]
        _xval = sorted(_xval)
        _xlab = xname + " (Sec)"
    else:
        _xval = xval
        _xlab = xname

    time_idle = [0] * len(xval)
    time_index = [0] * len(xval)
    time_abort = [0] * len(xval)
    time_twopc = [0] * len(xval)
    time_ccman = [0] * len(xval)
    time_work = [0] * len(xval)
    time_overhead = [0] * len(xval)
    total = [1] * len(xval)

    for x,i in zip(xval,range(len(xval))):
        if new_cfgs != {}:
            my_cfg_fmt = cfg_fmt + [xname]
            my_cfg = new_cfgs[(x,0)] + [x]
            my_cfg,my_cfg_fmt = apply_extras(my_cfg_fmt,my_cfg,extras,xname,'')
        else:
            my_cfg_fmt = cfg_fmt + [xname]
            my_cfg = cfg + [x]
            my_cfg,my_cfg_fmt = apply_extras(my_cfg_fmt,my_cfg,extras,xname,'')

        cfgs = get_cfgs(my_cfg_fmt, my_cfg)
        cfgs = get_outfile_name(cfgs,my_cfg_fmt)
        print(cfgs)
        if cfgs not in summary.keys(): 
            print("Not in summary")
            continue
        try:
            time_idle[i] = avg(summary[cfgs]['worker_idle_time'])
            if 'seq_idle_time' in summary[cfgs] and 'sched_idle_time' in summary[cfgs]:
                time_idle[i] = time_idle[i] + avg(summary[cfgs]['seq_idle_time']) + avg(summary[cfgs]['sched_idle_time'])
            time_index[i] = avg(summary[cfgs]['txn_index_time'])
            time_abort[i] = avg(summary[cfgs]['abort_time'])
            time_ccman[i] = avg(summary[cfgs]['txn_manager_time']) + avg(summary[cfgs]['txn_validate_time'])
            if 'seq_process_time' in summary[cfgs] and 'seq_ack_time' in summary[cfgs] and 'seq_prep_time' in summary[cfgs] and 'calvin_sched_time' in summary[cfgs]:
                time_ccman[i] = time_ccman[i] + avg(summary[cfgs]['seq_process_time']) + avg(summary[cfgs]['seq_ack_time']) + avg(summary[cfgs]['seq_prep_time']) + avg(summary[cfgs]['calvin_sched_time'])
            time_twopc[i] =  avg(summary[cfgs]['proc_time_type6']) + avg(summary[cfgs]['proc_time_type11']) + avg(summary[cfgs]['proc_time_type12']) + avg(summary[cfgs]['proc_time_type16']) - avg(summary[cfgs]['txn_validate_time'])
            if time_twopc[i] > avg(summary[cfgs]['txn_cleanup_time']):
                time_twopc[i] -= avg(summary[cfgs]['txn_cleanup_time'])
            time_overhead[i] = avg(summary[cfgs]['txn_cleanup_time']) + avg(summary[cfgs]['txn_table_release_time']) + avg(summary[cfgs]['txn_table_get_time'])
            time_work[i] = avg(summary[cfgs]['txn_process_time'])
#            total[i] = sum(time_index[i] + time_abort[i] + time_ccman[i] + time_twopc[i] + time_work[i] + time_idle[i] + time_overhead[i])
            total[i] = sum(time_abort[i] + time_ccman[i] + time_twopc[i] + time_work[i] + time_idle[i] + time_overhead[i])

        except KeyError:
            print("KeyError: {}".format(cfgs))


    print("time_idle")
    pp.pprint(time_idle)
#    pp.pprint(time_index)
    print("time_abort")
    pp.pprint(time_abort)
    print("time_twopc")
    pp.pprint(time_twopc)
    print("time_cc_man")
    pp.pprint(time_ccman)
    print("time_overhead")
    pp.pprint(time_overhead)
    print("time_work")
    pp.pprint(time_work)
    pp.pprint(total)
    if normalized:
        time_idle = [i / j for i,j in zip(time_idle,total)]
        time_index = [i / j for i,j in zip(time_index,total)]
        time_abort =  [i / j for i,j in zip(time_abort,total)]
        time_twopc =  [i / j for i,j in zip(time_twopc,total)]
        time_ccman =  [i / j for i,j in zip(time_ccman,total)]
        time_work =  [i / j for i,j in zip(time_work,total)]
        time_overhead =  [i / j for i,j in zip(time_overhead,total)]
        _ymax = 1
    else:
        _ymax = max(total)
    data = [
        time_idle,
        time_abort,
        #time_index,
        time_twopc,
        time_ccman, 
        time_overhead, 
        time_work]
    pp.pprint(data)
    
    # Quick and dirty label fix by Dana
    if "MAAT" in _xval:
        _xval[_xval.index("MAAT")] = "OCC"
    draw_stack(data,_xval,stack_names,figname=name,title=_title,ymax=_ymax,legend=legend)
    print("Created plot {}".format(name))



# Stack graph of time breakdowns
# mpr: list of MPR values to plot along the x-axis
# nodes: node count to plot
# algos: CC algo to plot
# max_txn: MAX_TXN_PER_PART
# summary: dictionary loaded with results
# normalized: if true, normalize the results
def time_breakdown_line(xval,vval,summary,
        normalized=True,
        xname="NODE_CNT",
        vname="CC_ALG",
        cfg_fmt=[],
        cfg=[],
        lst={},
        title='',
        extras = {},
        name='',
        xlab="",
        new_cfgs = {},
        logscalex=False,
        legend=False
        ):
    stack_names = [
    'Idle',
    'Abort',
    #'Index',
    '2PC',
    'CC Manager',
    'Txn Manager',
    'Useful Work'
    ]
    global plot_cnt
    pp = pprint.PrettyPrinter()
    if name == "":
        name = 'breakdown_{}'.format(plot_cnt)
    plot_cnt += 1
    _ymax=1.0
#    if normalized:
#        _title = 'Normalized {}'.format(title)
#    else:
#        _title = title
    _title = title
    pp.pprint(cfg)

    if xname == "ABORT_PENALTY":
        _xval = [(float(x.replace("UL","")))/1000000000 for x in xval]
        sort_idxs = sorted(range(len(_xval)),key=lambda x:_xval[x])
        xval = [xval[i] for i in sort_idxs]
        _xval = sorted(_xval)
        _xlab = xname + " (Sec)"
    else:
        _xval = xval
        _xlab = xname

    time_idle = [0] * len(xval)
    time_index = [0] * len(xval)
    time_abort = [0] * len(xval)
    time_twopc = [0] * len(xval)
    time_ccman = [0] * len(xval)
    time_work = [0] * len(xval)
    time_overhead = [0] * len(xval)
    total = [1] * len(xval)
    twopc = {}

    for v in vval:
        _v = v
        twopc[_v] = [0] * len(xval)


        for x,i in zip(xval,range(len(xval))):
            if new_cfgs != {}:
                my_cfg_fmt = cfg_fmt + [xname] + [vname]
                my_cfg = new_cfgs[(x,v)] + [x] + [v]
                my_cfg,my_cfg_fmt = apply_extras(my_cfg_fmt,my_cfg,extras,xname,vname)
            else:
                my_cfg_fmt = cfg_fmt + [xname] + [vname]
                my_cfg = cfg + [x] + [v]
                my_cfg,my_cfg_fmt = apply_extras(my_cfg_fmt,my_cfg,extras,xname,vname)

            cfgs = get_cfgs(my_cfg_fmt, my_cfg)
            cfgs = get_outfile_name(cfgs,my_cfg_fmt)
            print(cfgs)
            if cfgs not in summary.keys(): 
                print("Not in summary")
                continue
            try:
                time_idle[i] = avg(summary[cfgs]['worker_idle_time'])
                if 'seq_idle_time' in summary[cfgs] and 'sched_idle_time' in summary[cfgs]:
                    time_idle[i] = time_idle[i] + avg(summary[cfgs]['seq_idle_time']) + avg(summary[cfgs]['sched_idle_time'])
                time_index[i] = avg(summary[cfgs]['txn_index_time'])
                time_abort[i] = avg(summary[cfgs]['abort_time'])
                time_ccman[i] = avg(summary[cfgs]['txn_manager_time']) + avg(summary[cfgs]['txn_validate_time'])
                if 'seq_process_time' in summary[cfgs] and 'seq_ack_time' in summary[cfgs] and 'seq_prep_time' in summary[cfgs] and 'calvin_sched_time' in summary[cfgs]:
                    time_ccman[i] = time_ccman[i] + avg(summary[cfgs]['seq_process_time']) + avg(summary[cfgs]['seq_ack_time']) + avg(summary[cfgs]['seq_prep_time']) + avg(summary[cfgs]['calvin_sched_time'])
                time_twopc[i] =  avg(summary[cfgs]['proc_time_type6']) + avg(summary[cfgs]['proc_time_type11']) + avg(summary[cfgs]['proc_time_type12']) + avg(summary[cfgs]['proc_time_type16']) - avg(summary[cfgs]['txn_validate_time'])
                if time_twopc[i] > avg(summary[cfgs]['txn_cleanup_time']):
                    time_twopc[i] -= avg(summary[cfgs]['txn_cleanup_time'])
                time_overhead[i] = avg(summary[cfgs]['txn_cleanup_time']) + avg(summary[cfgs]['txn_table_release_time']) + avg(summary[cfgs]['txn_table_get_time'])
                time_work[i] = avg(summary[cfgs]['txn_process_time'])
#            total[i] = sum(time_index[i] + time_abort[i] + time_ccman[i] + time_twopc[i] + time_work[i] + time_idle[i] + time_overhead[i])
                total[i] = sum(time_abort[i] + time_ccman[i] + time_twopc[i] + time_work[i] + time_idle[i] + time_overhead[i])

            except KeyError:
                print("KeyError: {}".format(cfgs))

            if normalized:
                twopc[v][i] = time_twopc[i] / total[i] * 100
                if twopc[v][i] < 0:
                    twopc[v][i] = 0
                time_idle = [k / j for k,j in zip(time_idle,total)]
                time_index = [k / j for k,j in zip(time_index,total)]
                time_abort =  [k / j for k,j in zip(time_abort,total)]
                time_twopc =  [k / j for k,j in zip(time_twopc,total)]
                time_ccman =  [k / j for k,j in zip(time_ccman,total)]
                time_work =  [k / j for k,j in zip(time_work,total)]
                time_overhead =  [k / j for k,j in zip(time_overhead,total)]
                _ymax = 1
            else:
                twopc[v][i] = time_twopc[i]
                _ymax = max(_ymax,total[i]) 
            print("{} {}: {} {} {} {} {} {}".format(v,i,time_idle[i],time_abort[i],time_twopc[i],time_ccman[i], time_overhead[i],time_work[i]))


    # Quick and dirty label fix by Dana
    if "MAAT" in _xval:
        _xval[_xval.index("MAAT")] = "OCC"
    draw_line(name,twopc,_xval,ylab="Percent of Total Time",xlab="Server Count (Log Scale)",ltitle=vname,title=_title,logscalex=logscalex,legend=legend)
    print("Created plot {}".format(name))


# Stack graph of average txn latency breakdown 
# nodes: node count to plot
# algos: CC algo to plot
# summary: dictionary loaded with results
# normalized: if true, normalize the results
def latency_breakdown(xval,summary,
        normalized=True,
        xname="NODE_CNT",
        cfg_fmt=[],
        cfg=[],
        title='',
        extras = {},
        name='',
        new_cfgs = {},
        legend=False
        ):
    stack_names = [
#    'Abort',
    'Network',
    'Other',
    'Message Queue',
    'Work Queue',
    'CC Manager',
    'CC Blocking',
    'Processing'
    ]
    global plot_cnt
    pp = pprint.PrettyPrinter()
    if name == "":
        name = 'breakdown_{}'.format(plot_cnt)
    plot_cnt += 1
    _ymax=1.0
#    if normalized:
#        _title = 'Normalized {}'.format(title)
#    else:
#        _title = title
    _title = title
    pp.pprint(cfg)
    stats={}

    if xname == "ABORT_PENALTY":
        _xval = [(float(x.replace("UL","")))/1000000000 for x in xval]
        sort_idxs = sorted(range(len(_xval)),key=lambda x:_xval[x])
        xval = [xval[i] for i in sort_idxs]
        _xval = sorted(_xval)
        _xlab = xname + " (Sec)"
    else:
        _xval = xval
        _xlab = xname

    time_abort = [0] * len(xval)
    time_network = [0] * len(xval)
    time_msg_q = [0] * len(xval)
    time_work_q = [0] * len(xval)
    time_ccman = [0] * len(xval)
    time_cc_block = [0] * len(xval)
    time_work = [0] * len(xval)
    total = [1] * len(xval)
    txn_cnt = [1] * len(xval)
    time_other = [0] * len(xval)

    ltime_abort = [0] * len(xval)
    ltime_network = [0] * len(xval)
    ltime_msg_q = [0] * len(xval)
    ltime_work_q = [0] * len(xval)
    ltime_ccman = [0] * len(xval)
    ltime_cc_block = [0] * len(xval)
    ltime_work = [0] * len(xval)
    ltotal = [1] * len(xval)

    for x,i in zip(xval,range(len(xval))):
        if new_cfgs != {}:
            my_cfg_fmt = cfg_fmt + [xname]
            my_cfg = new_cfgs[(x,0)] + [x]
            my_cfg,my_cfg_fmt = apply_extras(my_cfg_fmt,my_cfg,extras,xname,'')
        else:
            my_cfg_fmt = cfg_fmt + [xname]
            my_cfg = cfg + [x]
            my_cfg,my_cfg_fmt = apply_extras(my_cfg_fmt,my_cfg,extras,xname,'')

        cfgs = get_cfgs(my_cfg_fmt, my_cfg)
        cc = cfgs["CC_ALG"]
        nc = cfgs["NODE_CNT"]
        cfgs = get_outfile_name(cfgs,my_cfg_fmt)
        print(cfgs)
        if cfgs not in summary.keys(): 
            print("Not in summary")
            continue
        try:
            avg_parts_touched = 1.0
            if "CALVIN" == x:
                txn_cnt[i] = sum(summary[cfgs]['seq_txn_cnt'])
            else:
                txn_cnt[i] = sum(summary[cfgs]['txn_cnt'])
                avg_parts_touched =  avg(summary[cfgs]['avg_parts_touched'])
#            time_abort[i] = avg(summary[cfgs]['lat_l_loc_abort_time']) / txn_cnt[i]
                time_msg_q[i] = (sum(summary[cfgs]['lat_short_msg_queue_time']) + sum(summary[cfgs]['lat_short_batch_time']) ) / txn_cnt[i]
            time_work_q[i] = (sum(summary[cfgs]['lat_short_work_queue_time']) ) / txn_cnt[i]
            time_ccman[i] = (sum(summary[cfgs]['lat_short_cc_time']) ) / txn_cnt[i]
            time_cc_block[i] = (sum(summary[cfgs]['lat_short_cc_block_time']) ) / txn_cnt[i]
            time_work[i] = (sum(summary[cfgs]['lat_short_process_time']) ) / txn_cnt[i]
            total[i] = time_abort[i] + time_msg_q[i] +time_work_q[i] + time_ccman[i] + time_cc_block[i] + time_work[i] + time_network[i]

            time_other[i] = avg(summary[cfgs]['lscl_avg']) - total[i]
            time_network[i] = min((sum(summary[cfgs]['lat_short_network_time']) ) / txn_cnt[i],time_other[i])
            print("NETWORK: {} vs {}".format((sum(summary[cfgs]['lat_short_network_time']) ) / txn_cnt[i], time_other[i]))

#                time_msg_q[i] = (sum(summary[cfgs]['lat_s_loc_msg_queue_time']) + avg(summary[cfgs]['lat_l_rem_msg_queue_time']) * avg_parts_touched) / txn_cnt[i]
#            time_work_q[i] = (sum(summary[cfgs]['lat_s_loc_work_queue_time']) + avg(summary[cfgs]['lat_l_rem_work_queue_time']) * avg_parts_touched) / txn_cnt[i]
#            time_ccman[i] = (sum(summary[cfgs]['lat_s_loc_cc_time']) + avg(summary[cfgs]['lat_l_rem_cc_time']) * avg_parts_touched) / txn_cnt[i]
#            time_cc_block[i] = (sum(summary[cfgs]['lat_s_loc_cc_block_time']) + avg(summary[cfgs]['lat_l_rem_cc_block_time']) * avg_parts_touched) / txn_cnt[i]
#            time_work[i] = (sum(summary[cfgs]['lat_s_loc_process_time']) + avg(summary[cfgs]['lat_l_rem_process_time']) * avg_parts_touched) / txn_cnt[i]
#            print("Avg parts touched " + str(avg(summary[cfgs]['avg_parts_touched'])))

#            time_msg_q[i] = (sum(summary[cfgs]['lat_s_loc_msg_queue_time']) + avg(summary[cfgs]['lat_l_rem_msg_queue_time']) ) / txn_cnt[i]
#            time_work_q[i] = (sum(summary[cfgs]['lat_s_loc_work_queue_time']) + avg(summary[cfgs]['lat_l_rem_work_queue_time']) ) / txn_cnt[i]
#            time_ccman[i] = (sum(summary[cfgs]['lat_s_loc_cc_time']) + avg(summary[cfgs]['lat_l_rem_cc_time']) ) / txn_cnt[i]
#            time_cc_block[i] = (sum(summary[cfgs]['lat_s_loc_cc_block_time']) + avg(summary[cfgs]['lat_l_rem_cc_block_time']) ) / txn_cnt[i]
#            time_work[i] = (sum(summary[cfgs]['lat_s_loc_process_time']) + avg(summary[cfgs]['lat_l_rem_process_time']) ) / txn_cnt[i]
            total[i] = time_abort[i] + time_msg_q[i] +time_work_q[i] + time_ccman[i] + time_cc_block[i] + time_work[i] + time_network[i]

            time_other[i] = avg(summary[cfgs]['lscl_avg']) - total[i]
            total[i] = avg(summary[cfgs]['lscl_avg'])

#            if "CALVIN" == x:
#                time_network[i] = avg(summary[cfgs]['lscl_avg']) - total[i]
#            else:
#                time_network[i] = avg(summary[cfgs]['lscl_avg']) - total[i]
#            total[i] += time_network[i]

            ltime_abort[i] = sum(summary[cfgs]['lat_l_loc_abort_time']) / txn_cnt[i]
            ltime_msg_q[i] = (sum(summary[cfgs]['lat_l_loc_msg_queue_time']) + sum(summary[cfgs]['lat_l_rem_msg_queue_time'])+ sum(summary[cfgs]['lat_s_rem_msg_queue_time'])) / txn_cnt[i]
            ltime_work_q[i] = (sum(summary[cfgs]['lat_l_loc_work_queue_time']) + sum(summary[cfgs]['lat_l_rem_work_queue_time'])+ sum(summary[cfgs]['lat_s_rem_work_queue_time'])) / txn_cnt[i]
            ltime_ccman[i] = (sum(summary[cfgs]['lat_l_loc_cc_time']) + sum(summary[cfgs]['lat_l_rem_cc_time'])+ sum(summary[cfgs]['lat_s_rem_cc_time'])) / txn_cnt[i]
            ltime_cc_block[i] = (sum(summary[cfgs]['lat_l_loc_cc_block_time']) + sum(summary[cfgs]['lat_l_rem_cc_block_time'])+ sum(summary[cfgs]['lat_s_rem_cc_block_time'])) / txn_cnt[i]
            ltime_work[i] = (sum(summary[cfgs]['lat_l_loc_process_time']) + sum(summary[cfgs]['lat_l_rem_process_time'])+ sum(summary[cfgs]['lat_s_rem_process_time'])) / txn_cnt[i]
            ltotal[i] = sum(ltime_abort[i] + ltime_msg_q[i] +ltime_work_q[i] + ltime_ccman[i] + ltime_cc_block[i] + ltime_work[i])

            ltime_network[i] = avg(summary[cfgs]['fscl_avg']) - ltotal[i]
            ltotal[i] += ltime_network[i]

        except KeyError:
            print("KeyError: {}".format(cfgs))
            continue
        stats = get_summary_stats(stats,summary[cfgs],{},x,'',cc)


    if normalized:
        time_abort = [i / j for i,j in zip(time_abort,total)]
        time_network = [i / j for i,j in zip(time_network,total)]
        time_msg_q = [i / j for i,j in zip(time_msg_q,total)]
        time_work_q = [i / j for i,j in zip(time_work_q,total)]
        time_ccman = [i / j for i,j in zip(time_ccman,total)]
        time_cc_block = [i / j for i,j in zip(time_cc_block,total)]
        time_work = [i / j for i,j in zip(time_work,total)]
        time_other = [i / j for i,j in zip(time_other,total)]

        ltime_abort = [i / j for i,j in zip(ltime_abort,ltotal)]
        ltime_network = [i / j for i,j in zip(ltime_network,ltotal)]
        ltime_msg_q = [i / j for i,j in zip(ltime_msg_q,ltotal)]
        ltime_work_q = [i / j for i,j in zip(ltime_work_q,ltotal)]
        ltime_ccman = [i / j for i,j in zip(ltime_ccman,ltotal)]
        ltime_cc_block = [i / j for i,j in zip(ltime_cc_block,ltotal)]
        ltime_work = [i / j for i,j in zip(ltime_work,ltotal)]
        _ymax = 1
        _ymaxl = 1
    else:
        _ymax = max(total)
        _ymaxl = max(ltotal)
    print("time_abort")
    pp.pprint(time_abort)
    print("time_network")
    pp.pprint(time_network)
    print("time_msg_q")
    pp.pprint(time_msg_q)
    print("time_work_q")
    pp.pprint(time_work_q)
    print("time_ccman")
    pp.pprint(time_ccman)
    print("time_cc_block")
    pp.pprint(time_cc_block)
    print("time_work")
    pp.pprint(time_work)
    print("time_other")
    pp.pprint(time_other)
    data = [
#        time_abort,
        time_network,
        time_other,
        time_msg_q,
        time_work_q,
        time_ccman,
        time_cc_block,
        time_work
        ]
    pp.pprint(data)
#write_summary_file(name,stats,_xval,[])
    
    # Quick and dirty label fix by Dana
    if "MAAT" in _xval:
        _xval[_xval.index("MAAT")] = "OCC"
    draw_stack(data,_xval,stack_names,figname="sa"+name,title=_title,ymax=_ymax,legend=False)

    data = [
        ltime_abort,
        ltime_network,
        ltime_msg_q,
        ltime_work_q,
        ltime_ccman,
        ltime_cc_block,
        ltime_work
        ]
#    pp.pprint(data)
 
#    draw_stack(data,_xval,stack_names,figname="fs"+name,title=_title,ymax=_ymax,legend=True)
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


    draw_line(name,ys,xs,ylab='Percent',xlab=xname,title=_title,bbox=[0.8,0.6],ylimit=1.0,ltitle=vname) 


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

    draw_line(name,avgs,mpr,ylab='average ' + value,xlab='Multi-Partition Rate',title='Per Node Throughput',bbox=[0.5,0.95],ltitle=vname) 


