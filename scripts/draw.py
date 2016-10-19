import matplotlib
from matplotlib.table import Table
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os, sys, re, math,itertools
from pylab import *
from helper import *
from textwrap import wrap
import latency_stats as ls
import seaborn as sns


rename = {
    "average": "   avg   "
}

scatterconfig = {
    'START': {'color':'g', 'marker':'o','alpha':0.5},
    'ABORT': {'color':'r', 'marker':'x','alpha':1.0},
    'COMMIT': {'color':'b', 'marker':'o','alpha':0.5},
    'LOCK': {'color':'y', 'marker':'+','alpha':1.0},
    'UNLOCK': {'color':'c', 'marker':'s','alpha':1.0},
}

lineconfig_nopreset = [
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
    "ls='-', lw=2, marker='o', ms=4",
#    "ls='-', lw=2, color='#f15854', marker='o', ms=4",
#    "ls='-', lw=2, color='#faa43a', marker='o', ms=4",
#    "ls='-', lw=2, color='#DECF3F', marker='o', ms=4",
#    "ls='-', lw=2, color='#60BD68', marker='o', ms=4",
#    "ls='--', lw=2, color='#5DA5DA', marker='o', ms=4", #, marker='+', ms=10",
#    "ls='--', lw=2, color='#B276B2', marker='o', ms=6",
#    "ls='-', lw=2, color='#4d4d4d', marker='o', ms=6",
#    "ls='-', lw=2, color='#f15854', marker='D', ms=4",
#    "ls='-', lw=2, color='#faa43a', marker='D', ms=4",
#    "ls='-', lw=2, color='#DECF3F', marker='D', ms=4",
#    "ls='-', lw=2, color='#60BD68', marker='D', ms=4",
#    "ls='--', lw=2, color='#5DA5DA', marker='D', ms=4", #, marker='+', ms=10",
#    "ls='--', lw=2, color='#B276B2', marker='D', ms=6",
#    "ls='-', lw=2, color='#4d4d4d', marker='D', ms=6",
#    "ls='-', lw=2, color='#f15854', marker='^', ms=4",
#    "ls='-', lw=2, color='#faa43a', marker='^', ms=4",
#    "ls='-', lw=2, color='#DECF3F', marker='^', ms=4",
#    "ls='-', lw=2, color='#60BD68', marker='^', ms=4",
#    "ls='--', lw=2, color='#5DA5DA', marker='^', ms=4", #, marker='+', ms=10",
#    "ls='--', lw=2, color='#B276B2', marker='^', ms=6",
#    "ls='-', lw=2, color='#4d4d4d', marker='^', ms=6"
]

lineconfig = {
# CC Algos
#    'DL_DETECT'     : "ls='-', lw=2, color='#f15854', marker='o', ms=4",
#    'NO_WAIT'       : "ls='-', lw=2, color='#faa43a', marker='D', ms=4",
#    'WAIT_DIE'      : "ls='-', lw=2, color='#DECF3F', marker='s', ms=4",
#    'TIMESTAMP'     : "ls='-', lw=2, color='#60BD68', marker='^', ms=4",
#    'MVCC'          : "ls='--', lw=2, color='#5DA5DA', marker='o', ms=4", #, marker='+', ms=10",
#    'OCC'           : "ls='--', lw=2, color='#B276B2', marker='+', ms=6",
#    'HSTORE'        : "ls='-', lw=2, color='#4d4d4d', marker='x', ms=6",
#    'HSTORE_SPEC'   : "ls='--', lw=2, color='#4d4d4d', marker='x', ms=6",
#    'CALVIN'           : "ls='-', lw=2, color='#ff3333', marker='*', ms=4",
#    'Single Server'       : "ls='--', color='#d3d3d3',lw=2, marker='D', ms=8",
    'Single Server'       : "ls='--',lw=2",
    'Serializable Execution'       : "ls='-',lw=2",
    'No Concurrency Control'       : "ls='--',lw=2",
    'NO_WAIT'       : "ls='-', lw=2, marker='D', ms=8",
    'WAIT_DIE'      : "ls='-', lw=2, marker='s', ms=8",
    'TIMESTAMP'     : "ls='-', lw=2, marker='^', ms=8",
    'MVCC'          : "ls='--', lw=2,marker='o', ms=8", #, marker='+', ms=10",
    'OCC'           : "ls='--', lw=2, marker='+', ms=10",
    'HSTORE'        : "ls='-', lw=2, marker='x', ms=10",
    'HSTORE_SPEC'   : "ls='--', lw=2, marker='x', ms=10",
    'CALVIN'           : "ls='-', lw=2, marker='*', ms=8",

    25              : "ls='-', lw=2, color='#4d4d4d', marker='s', ms=6",
    50              : "ls='-', lw=2, color='#5DA5DA', marker='+', ms=6",
    60              : "ls='-', lw=2, color='#4d4d4d', marker='s', ms=6",
    70              : "ls='-', lw=2, color='#faa43a', marker='+', ms=6",
    80              : "ls='-', lw=2, color='#DECF3F', marker='o', ms=6",
    90              : "ls='-', lw=2, color='#60BD68', marker='D', ms=6",
    100             : "ls='-', lw=2, color='#B276B2', marker='s', ms=6",
# Multipartition Rates
    0               : "ls='-', lw=2, color='#ff3333', marker='o', ms=4",
    1               : "ls='-', lw=2, color='#faa43a', marker='D', ms=4",
    5               : "ls='-', lw=2, color='#DECF3F', marker='s', ms=4",
#Skew
    0.0       : "ls='-', lw=2, color='#faa43a', marker='D', ms=4",
    0.01      : "ls='-', lw=2, color='#DECF3F', marker='s', ms=4",
    0.05      : "ls='-', lw=2, color='#faa43a', marker='s', ms=4",
    0.075      : "ls='-', lw=2, color='#4d4d4d', marker='s', ms=4",
    0.1     : "ls='-', lw=2, color='#5DA5DA', marker='^', ms=4",
    0.5      : "ls='-', lw=2, color='#B276B2', marker='s', ms=4",
    0.6      : "ls='-', lw=2, color='#DECF3F', marker='^', ms=4",
    0.8     : "ls='-', lw=2, color='#f15854', marker='D', ms=4",
    0.9          : "ls='--', lw=2, color='#5DA5DA', marker='o', ms=4",
    1.0          : "ls='--', lw=2, color='#60BD68', marker='o', ms=4",
    0.99           : "ls='--', lw=2, color='#B276B2', marker='+', ms=6",
    10.0           : "ls='--', lw=2, color='#B276B2', marker='+', ms=6",
    100.0        : "ls='-', lw=2, color='#4d4d4d', marker='x', ms=6",
#    10              : "ls='-', lw=2, color='#DECF3F', marker='s', ms=4",
#    20              : "ls='-', lw=2, color='#60BD68', marker='^', ms=4",
#    30              : "ls='--', lw=2, color='#5DA5DA', marker='o', ms=4", #, marker='+', ms=10",
#    40              : "ls='--', lw=2, color='#B276B2', marker='+', ms=6",
#    50              : "ls='-', lw=2, color='#f15854', marker='x', ms=6",
}

# data[config] = []
# config : different configurations
# under a config, the list has the values for each benchmark 
# label : the names of benchmarks, matching the list

def draw_bar(filename, data, label, names=None, dots=None, 
        ylabel='Speedup', xlabel='', rotation=30,
        ncol=1, bbox=[0.95, 0.9], colors=None, hatches=None,
        figsize=(9, 3), left=0.1, bottom=0.18, right=0.96, top=None, 
        ylimit=None, xlimit=None,ltitle=''):

    index = range(0, len(label))
    m_label = list(label)
    for i in range(0, len(label)):
        if m_label[i] in rename:
            m_label[i] = rename[ label[i] ]
    fig, ax1 = plt.subplots(figsize=figsize)
    ax1.set_ylabel(ylabel)
    ax1.set_xlabel(xlabel)
    ax1.axhline(0, color='black', lw=1)
    grid(axis='y')
    if dots != None:
        ax2 = ax1.twinx()
        ax2.ticklabel_format(axis='y', style='sci', scilimits=(-2,2))
        ax2.set_ylabel('Total Memory Accesses')
        ax2.set_ylim([0,12e6])
    width = 1.0 / len(data) / 1.6
    if colors == None :
        colors = [0] * len(data)
        for i in range(0, len(data)) :
            colors[i] = [0.1 + 0.7 / len(data) * i] * 3
    n = 0
    bars = [0] * len(data)
    if names == None: names = data.keys()
    if xlimit == None:
        xlimit = (-0.2,len(index) - 0.2)
    ax1.set_xlim(xlimit)
    if ylimit != None:
        ax1.set_ylim(ylimit)
    for cfg in names:
        ind = [x + width*n for x in index]
        hatch = None if hatches == None else hatches[n]
        bars[n] = ax1.bar(ind, data[cfg], width, color=colors[n], hatch=hatch)
        if dots != None:
            ax2.plot([x + width/2 for x in ind], dots[cfg], 'ro')
        n += 1
    plt.xticks([x + width*len(names)/2.0 for x in index], m_label, size='14', rotation=rotation)
    plt.tick_params(
        axis='x',  # changes apply to the x-axis
        which='both',  # both major and minor ticks are affected
        bottom='off',  # ticks along the bottom edge are off
        top='off', # ticks along the top edge are off
        labelbottom='on') # labels along the bottom edge are off

    if title:
        ax1.set_title("\n".join(wrap(title)))
    fig.legend([x[0] for x in bars], names, prop={'size':12}, 
        ncol=ncol, bbox_to_anchor=bbox, labelspacing=0.2) 
    subplots_adjust(left=left, bottom=bottom, right=right, top=top)
    savefig('../figs/' + filename)
    plt.close()

def draw_line2(fname, data, xticks, 
        title = None,
        xlabels = None,
        bbox=(0.9,0.95), ncol=1, 
        ylab='Throughput', logscale=False, 
        logscalex = False,
        ylimit=0, xlimit=None, xlab='Number of Cores',
        legend=True, linenames = None, figsize=(23/3, 10/3), styles=None,ltitle='') :
    fig = figure(figsize=figsize)
    lines = [0] * len(data)
    ax = plt.axes()
    if logscale :
        ax.set_yscale('log')
    if logscalex:
        ax.set_xscale('log')
    n = 0
    if xlabels != None :
        ax.set_xticklabels(xlabels) 
    if linenames == None :
        print(data.keys())
        linenames = sorted(data.keys())
    for i in range(0, len(linenames)) :
        key = linenames[i]
        intlab = {}
        for k in xticks.keys():
            try:
                intlab[k] = [float(x) for x in xticks[k]]
            except ValueError:
                intlab[k] = [float(x[:-2]) for x in xticks[k]]

        style = None
        if styles != None :
            style = styles[key]
        elif key in lineconfig.keys():
            style = lineconfig[key]
        else :
            style = lineconfig_nopreset[i]
        exec("lines[n], = plot(intlab[key], data[key], %s)" % style)
        n += 1
    if ylimit != 0:
        ylim(ylimit)
    if xlimit != None:
        xlim(xlimit)
    plt.gca().set_ylim(bottom=0)
    ylabel(ylab)
    xlabel(xlab)
    if not logscale:
        ticklabel_format(axis='y', style='plain')
        #ticklabel_format(axis='y', style='sci', scilimits=(-3,5))
    if logscalex:
        ax.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
    if legend :
        #fig.legend(lines, linenames, loc='upper right',bbox_to_anchor = (1,1), prop={'size':9}, ncol=ncol)
        fig.legend(lines, linenames, loc='upper right',bbox_to_anchor = bbox, prop={'size':8},ncol=ncol,title=ltitle)
    subplots_adjust(left=0.18, bottom=0.15, right=0.9, top=None)
    if title:
        ax.set_title("\n".join(wrap(title)))
    axes = ax.get_axes()
    axes.yaxis.grid(True,
        linestyle='-',
        which='major',
        color='0.75'
    )
    ax.set_axisbelow(True)

    savefig('../figs/' + fname +'.pdf', bbox_inches='tight')
    plt.close()


def draw_line(fname, data, xticks, 
        title = None,
        xlabels = None,
        bbox=(0.9,0.95), ncol=1, 
        ylab='Throughput', logscale=False, 
        logscalex = False,
        ylimit=0, xlimit=None, xlab='Number of Cores',
        legend=False, linenames = None, figsize=(23/3, 10/3), styles=None,ltitle=''
        ,base=2, num_yticks=6) :

    if len(xticks) <= 6:
        current_palette = sns.color_palette()
    else:
        current_palette = sns.color_palette("hls",len(xticks))
    sns.set_palette("bright")
    sns.set_style("whitegrid")



    fig = figure(figsize=figsize)
    thr = [0] * len(xticks)
    lines = [0] * len(data)
    ax = plt.axes()
    plt.tick_params(axis='both',which='major',labelsize=16)


    if logscale :
        ax.set_yscale('log')
    if logscalex:
        ax.set_xscale('log',basex=base)
    n = 0
    if xlabels != None :
        ax.set_xticklabels([x if i%2 else '' for x,i in zip(xlabels,range(len(xlabels)))]) 
    if linenames == None :
        print(data.keys())
        linenames = sorted(data.keys())
    for i in range(0, len(linenames)) :
        key = linenames[i]
        try:
            intlab = [float(x) for x in xticks]
        except ValueError:
            print("ValError " + key)
            intlab = [float(x[:-2]) for x in xticks]

        style = None
        if styles != None :
            style = styles[key]
        elif key in lineconfig.keys():
            style = lineconfig[key]
        else :
            style = lineconfig_nopreset[i]
        exec("lines[n], = plot(intlab, data[key], %s)" % style)
#        exec("lines[n], = plot(intlab, data[key])")
        n += 1
    if ylimit != 0:
        plt.gca().set_ylim([0,ylimit])
#        ylim(ylimit)
    else:
        plt.gca().set_ylim(bottom=0)
    if xlimit != None:
        if not logscalex:
            xlim(xlimit)
    ax.set_xlim([xticks[0],xticks[len(xticks)-1]])
#    plt.gca().set_ylim(bottom=0)
    ylabel(ylab,fontsize=18)
    xlabel(xlab,fontsize=18)
    if not logscale:
        ticklabel_format(axis='y', style='plain')
        #ticklabel_format(axis='y', style='sci', scilimits=(-3,5))
    if logscalex:
#ax.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
        if xlab == "Network Latency (ms) (Log Scale)" or xlab == "Network Latency (ms)":
            ax.get_xaxis().set_major_formatter(matplotlib.ticker.FormatStrFormatter('%.1f'))
        else:
            ax.get_xaxis().set_major_formatter(matplotlib.ticker.FormatStrFormatter('%d'))
    if legend :
        #fig.legend(lines, linenames, loc='upper right',bbox_to_anchor = (1,1), prop={'size':9}, ncol=ncol)
        fig.legend(lines, linenames, loc='upper center',bbox_to_anchor = (0.4,1), prop={'size':10},ncol=len(linenames)/2)
#        fig.legend(lines, linenames, loc='upper right',bbox_to_anchor = bbox, prop={'size':8},ncol=ncol,title=ltitle)
    if title:
        ax.set_title("\n".join(wrap(title)))
    axes = ax.get_axes()
    axes.yaxis.grid(True,
        linestyle='-',
        which='major',
        color='0.75'
    )
    axes.yaxis.set_major_locator(matplotlib.ticker.MaxNLocator(num_yticks))
    ax.set_axisbelow(True)
    ax.spines['right'].set_color('black')
    ax.spines['left'].set_color('black')
    ax.spines['bottom'].set_color('black')
    ax.spines['top'].set_color('black')

    savefig('../figs/' + fname +'.pdf', bbox_inches='tight')
    plt.close()
    if not legend:
        fig = figure(figsize=((7.2, 0.4)))
        fig.legend(lines, linenames,bbox_to_anchor = (1,1), prop={'size':10},frameon=False,ncol=len(linenames))
        savefig('../figs/' + 'legend.pdf')
        plt.close()
        fig = figure(figsize=((3.9, 0.6)))
        fig.legend(lines, linenames,bbox_to_anchor = (1,1), prop={'size':10},frameon=False,ncol=len(linenames)/2)
        savefig('../figs/' + 'legend_half.pdf')
        plt.close()
#        fig = figure(figsize=((4.2, 0.4)))
#        fig.legend(lines, linenames,bbox_to_anchor = (1,1), prop={'size':10},frameon=True,ncol=len(linenames))
#        savefig('../figs/' + fname + 'legend.pdf')
#        plt.close()

def draw_bars_single(data, xlabels, 
        figname='stack', 
        figsize=(8, 3),
        ylab = 'Throughput',
        xlab = 'Time',
        title = None,
        bbox=[0.95,0.95],ltitle=''):

    fig = figure(figsize=figsize)
    ind = range(0, len(xlabels))

    plots = ()

    #xlabels = [str(x) for x in xlabels]

    xticks( ind,  xlabels, rotation=30, ha='center')
    clr = itertools.cycle(['#4d4d4d','#F15854','#DECF3F','#5DA5DA','#FAA43A','#60BD68'])
    htch = itertools.cycle(['','//','\\','-','\\\\','/'])

    w = 0.8 
    k = 0
    p = plt.bar([i+(w*k) for i in ind], data, color=clr.next(), hatch=htch.next(),width=w)
    plots = plots + (p,)
    k = k+1

    ax = plt.axes()
    ylabel(ylab)
    xlabel(xlab)
    if title:
        ax.set_title("\n".join(wrap(title)))
    legend(plots, xlabels, bbox_to_anchor = bbox, prop={'size':11})
    subplots_adjust(bottom=0.25, right=0.7, top=None)
    savefig('../figs/' + figname + '.pdf', bbox_inches='tight')
    plt.close()


def draw_bars(data, xlabels, 
        figname='stack', 
        figsize=(8, 3),
        ylab = 'Throughput',
        xlab = 'Time',
        title = None,
        bbox=[0.95,0.95],ltitle=''):

    fig = figure(figsize=figsize)
    ind = range(0, len(xlabels))

    plots = ()

    #xlabels = [str(x) for x in xlabels]

    xticks( ind,  xlabels, rotation=30, ha='center')
    clr = itertools.cycle(['#4d4d4d','#F15854','#DECF3F','#5DA5DA','#FAA43A','#60BD68'])
    htch = itertools.cycle(['','//','\\','-','\\\\','/'])

    w = 0.12
    k = 0
    for s in sorted(data.keys()):
        p = plt.bar([i+(w*k) for i in ind], data[s], color=clr.next(), hatch=htch.next(),width=w)
        plots = plots + (p,)
        k = k+1

    ax = plt.axes()
    ylabel(ylab)
    xlabel(xlab)
    if title:
        ax.set_title("\n".join(wrap(title)))
    legend(plots, sorted(data.keys()), bbox_to_anchor = bbox, prop={'size':11})
    subplots_adjust(bottom=0.25, right=0.7, top=None)
    savefig('../figs/' + figname + '.pdf', bbox_inches='tight')
    plt.close()



def draw_stack(data, xlabels, slabels, figname='stack', title=None, figsize=(8, 3),ymin=0, ymax=1,ltitle=''
        ,legend=False,ylab='') :
    fig = figure(figsize=figsize)
    slabels = list(reversed(slabels))
    ind = range(0, len(xlabels))

    plots = ()
    bottom = [0] * len(xlabels)

    ylim([ymin, ymax])
    #xlabels = [str(x) for x in xlabels]
    ylabel(ylab,fontsize=18)

    xticks( ind,  xlabels, rotation=30, ha='center')
    clr = itertools.cycle(['#4d4d4d','#F15854','#DECF3F','#5DA5DA','#FAA43A','#B276B2','#60BD68'])
    htch = itertools.cycle(['','//','\\','-','\\\\','/'])

    for s in range(len(slabels)):
        p = plt.bar(ind, data[s], color=clr.next(), hatch=htch.next(), bottom=bottom)
        plots = plots + (p,)
        bottom = [a + b for a,b in zip(bottom, data[s])]

    if title:
        plt.title("\n".join(wrap(title)))
    if legend:
        fig.legend(reversed(plots), tuple(slabels),loc='right',prop={'size':11})
    subplots_adjust(bottom=0.25, right=0.7, top=None)
    savefig('../figs/' + figname + '.pdf', bbox_inches='tight')
    plt.close()
    fig = figure(figsize=(5.4, 0.3))
    fig.legend(reversed(plots), tuple(slabels), prop={'size':8},ncol=len(slabels), columnspacing=1)
#    fig.legend(reversed(plots), tuple(slabels), bbox_to_anchor = (1,1, 1, 1), prop={'size':10})
    savefig('../figs/breakdown_legend.pdf')
    plt.close()
    fig = figure(figsize=((3.6, 0.6)))
    fig.legend(reversed(plots), tuple(slabels), prop={'size':8},ncol=3)
    savefig('../figs/breakdown_legend_half.pdf')
    plt.close()
    fig = figure(figsize=((1.3, 2)))
    fig.legend(reversed(plots), tuple(slabels), prop={'size':8},ncol=1)
    savefig('../figs/breakdown_legend_stacked.pdf')
    plt.close()


def draw_2line(x, y1, y2, figname="noname", ylimit=None,ltitle=''):
#   fig = figure(figsize=(16/3, 9/3))
    fig, ax1 = plt.subplots(figsize=(21/3, 7.0/3))
    if ylimit != None:
        ax1.set_ylim(ylimit)
    #ax1.set_xscale('log')
    # ax1.plot(x, y1, 'b-', lw=2, marker='o', ms=4)
    color = (0.2,0.2,0.5)
    ax1.plot(range(0, len(x)), y1, ls='-', lw=2, color=color, marker='o', ms=4)
    ax1.set_xlabel('Timeout Threshold')
    ax1.set_xticklabels([str(i) for i in x])
    ax1.set_ylabel('Throughput (Million txn/s)', color=color)
    for tl in ax1.get_yticklabels():
        tl.set_color(color)

    ax2 = ax1.twinx()
    #ax2.set_ylim(bottom=0)
    # ax2.plot(x, y2, 'r-', lw=2, marker='s', ms=4)
    # ax2.plot(range(0, len(x)), y2, 'r--', lw=2, marker='s', ms=4)
    color = (1, 0.5, 0.5)
    ax2.plot(range(0, len(x)), y2, ls='--', lw=2, color=color, marker='s', ms=4)
    ax2.set_ylabel('Abort Rate', color=color)
    for tl in ax2.get_yticklabels():
        tl.set_color(color)
    #ax2.set_xticklabels([str(i) for i in x])
    ax2.set_xticklabels([str(i) for i in x])
    ax2.yaxis.grid(True,
                   linestyle='-',
                   which='major',
                   color='0.75'
    )
    subplots_adjust(left=0.18, bottom=0.15, right=0.9, top=None)

    savefig('../figs/' + figname + '.pdf', bbox_inches='tight')
    plt.close()

def draw_scatter(fname, data, xticks, 
        title = None,
        xlabels = None,
        bbox=(0.9,0.95), ncol=1, 
        ylab='Txn IDs', logscale=False, 
        logscalex = False,
        ylimit=0, xlimit=None, xlab='Time',
        legend=True, linenames = None, figsize=(100,50), styles=None) :
    fig = figure(figsize=figsize)
    thr = [0] * len(xticks)
    lines = [0] * len(data)
    ax = plt.axes()
    if logscale :
        ax.set_yscale('log')
    if logscalex:
        ax.set_xscale('log')
    n = 0
    if xlabels != None :
        ax.set_xticklabels(xlabels) 

#exec "lines[n], = plot(intlab, data[key], %s)" % style
    for i in range(0,len(linenames)):
#exec "lines[n], = scatter(xticks[i], data[i], c=colors[i])"
        c = scatterconfig[linenames[i]]['color']
        m = scatterconfig[linenames[i]]['marker']
        a = scatterconfig[linenames[i]]['alpha']
        lines[i] = plt.scatter(xticks[i],data[i],color=c,marker=m,alpha=a)

    if ylimit != 0:
        ylim(ylimit)
    if xlimit != None:
        xlim(xlimit)
    plt.gca().set_ylim(bottom=0)
    ylabel(ylab)
    xlabel(xlab)
    if not logscale:
        ticklabel_format(axis='y', style='plain')
        #ticklabel_format(axis='y', style='sci', scilimits=(-3,5))
#if logscalex:
#ax.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())

    start,end=ax.get_xlim()
    ax.xaxis.set_ticks(np.arange(start,end,100000))

    if legend :
        fig.legend(lines, linenames, bbox_to_anchor = bbox, prop={'size':9}, ncol=ncol)

    subplots_adjust(left=0.18, bottom=0.15, right=0.9, top=None)
    if title:
        ax.set_title("\n".join(wrap(title)))

    axes = ax.get_axes()
    axes.yaxis.grid(True,
        linestyle='-',
        which='major',
        color='0.75'
    )
    ax.set_axisbelow(True)

    savefig('../figs/' + fname +'.pdf', bbox_inches='tight')
    plt.close()

def draw_lat_matrix(fname,data,title="",lat_type=None,lat_types=None,columns=[],rows=[]):
    bkg_colors=['#CCFFFF','white']
    fmt='{:.3f}'
    fig,ax=plt.subplots()
    ax.set_axis_off()
    tb=Table(ax,bbox=[0,0,1,1])
    nrows,ncols=data.shape
    assert nrows==len(rows)
    assert ncols==len(columns)
    width, height = 1.0 / ncols * 2, 1.0 / nrows * 2 
    for (i,j),val in np.ndenumerate(data):
        idx = [j % 2, (j + 1) % 2][i % 2]
        color = bkg_colors[idx]
        if val:
            if lat_type:
                txt=fmt.format(ls.exec_fn(val,lat_type))
            else:
                assert lat_types
                txt=fmt.format(ls.exec_fn(val,lat_types[i]))
        else:
            txt="-"
        tb.add_cell(i, j, width, height, text=txt, 
                loc='center', facecolor=color)
    # Row Labels...
    for i, label in enumerate(rows):
        tb.add_cell(i, -1, width, height, text=label, loc='right', 
                edgecolor='none', facecolor='none')
    # Column Labels...
    for j, label in enumerate(columns):
        tb.add_cell(-1, j, width, height/2, text=label, loc='center', 
                edgecolor='none', facecolor='none')
    ax.add_table(tb)

    if title:
        ax.set_title("\n".join(wrap(title)), y=1.08)
    savefig('../figs/' + fname +'.pdf', bbox_inches='tight')
    plt.close()

