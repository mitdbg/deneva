DDBMS
=======

DDBMS is a testbed of an OLTP distributed database management system (DBMS). It supports 6 concurrency control algorithms.

This testbed is based on the DBx1000 system, whose concurrency control scalability study can be found in the following paper:

    Staring into the Abyss: An Evaluation of Concurrency Control with One Thousand Cores
    Xiangyao Yu, George Bezerra, Andrew Pavlo, Srinivas Devadas, Michael Stonebraker
    http://voltdb.com/downloads/datasheets_collateral/vdb_whitepaper_staring_into_the_abyss.pdf
    
Setup
------------
DDBMS has three dependences that need to be installed:

* [Boost](https://www.boost.org/)
* [jemalloc](https://github.com/jemalloc/jemalloc/releases/tag/4.0.3)
* [nanomsg](https://github.com/nanomsg/nanomsg/releases/tag/0.6-beta)

To be able to make the code successfully there needs to be a file named obj. Run 

    mkdir obj

Build & Test
------------
To build the database.

    make deps
    make -j

Configuration
-------------

DBMS configurations can be changed in the config.h file. Please refer to README for the meaning of each configuration. Here we only list several most important ones. 

    NODE_CNT          : Number of server nodes in the database
    THREAD_CNT        : Number of worker threads running per server
    WORKLOAD          : Supported workloads include YCSB and TPCC
    CC_ALG            : Concurrency control algorithm. Six algorithms are supported 
                        (NO_WAIT, WAIT_DIE, TIMESTAMP, MVCC, OCC, CALVIN) 
    MAX_TXN_IN_FLIGHT  : Maximum number of active transactions at each server at a given time
    DONE_TIMER        : Amount of time to run experiment
                        
Configurations can also be specified as command argument at runtime. Run the following command for a full list of program argument. 
    
    ./rundb -h

Run
---

The DBMS can be run with 

    ./rundb -nid[N]
    ./runcl -nid[M]

where N and M are the ID of a server and client, respectively

For example, if you are running with a `THREAD_CNT` of 4 you would run 

    ./rundb -nid0
    ./rundb -nid1
    ./runcl -nid2
    ./runcl -nid3

There is also the option to run scripts. From the scripts directory run 
    
    python3 run_experiments -e [experiment]

* List of available experiments can be found [here](https://github.com/mitdbg/deneva/blob/master/scripts/experiments.py)

After running an experiment, the results can be plotted by running 

    python3 plot.py [experiment]
