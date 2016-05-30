#ifndef _CONFIG_H_

#define _CONFIG_H_

/***********************************************/
// Simulation + Hardware
/***********************************************/
#define NODE_CNT 2
#define THREAD_CNT 1
#define REM_THREAD_CNT THREAD_CNT
#define SEND_THREAD_CNT THREAD_CNT
#define CORE_CNT 8
// PART_CNT should be at least NODE_CNT
#define PART_CNT NODE_CNT
#define CLIENT_NODE_CNT 1
#define CLIENT_THREAD_CNT 1
#define CLIENT_REM_THREAD_CNT 1
#define CLIENT_SEND_THREAD_CNT 1
#define CLIENT_RUNTIME false

#define LOAD_METHOD LOAD_MAX
#define LOAD_PER_SERVER 10

// Replication
#define REPLICA_CNT 0
// AA (Active-Active), AP (Active-Passive)
#define REPL_TYPE AP

// each transaction only accesses only 1 virtual partition. But the lock/ts manager and index are not aware of such partitioning. VIRTUAL_PART_CNT describes the request distribution and is only used to generate queries. For HSTORE, VIRTUAL_PART_CNT should be the same as PART_CNT.
#define VIRTUAL_PART_CNT    PART_CNT  
#define PAGE_SIZE         4096 
#define CL_SIZE           64
#define CPU_FREQ          2.6
// enable hardware migration.
#define HW_MIGRATE          false

// # of transactions to run for warmup
#define WARMUP            0
// YCSB or TPCC
#define WORKLOAD TPCC 
// print the transaction latency distribution
#define PRT_LAT_DISTR false
#define STATS_ENABLE        true
#define TIME_ENABLE         true //STATS_ENABLE

#define FIN_BY_TIME true
#define MAX_TXN_IN_FLIGHT 10

#define SERVER_GENERATE_QUERIES false

/***********************************************/
// Memory System
/***********************************************/
// Three different memory allocation methods are supported.
// 1. default libc malloc
// 2. per-thread malloc. each thread has a private local memory
//    pool
// 3. per-partition malloc. each partition has its own memory pool
//    which is mapped to a unique tile on the chip.
#define MEM_ALLIGN          8 

// [THREAD_ALLOC]
#define THREAD_ALLOC        false
#define THREAD_ARENA_SIZE     (1UL << 22) 
#define MEM_PAD           true

// [PART_ALLOC] 
#define PART_ALLOC          false
#define MEM_SIZE          (1UL << 30) 
#define NO_FREE           false

/***********************************************/
// Message Passing
/***********************************************/
#define TPORT_TYPE TCP
#define TPORT_PORT 17000
#define SET_AFFINITY true

#define MAX_TPORT_NAME 128
#define MSG_SIZE 128 // in bytes
#define HEADER_SIZE sizeof(uint32_t)*2 // in bits 
#define MSG_TIMEOUT 5000000000UL // in ns
#define NETWORK_TEST false
#define NETWORK_DELAY_TEST false
#define NETWORK_DELAY 0UL

#define MAX_QUEUE_LEN NODE_CNT * 2

#define PRIORITY_WORK_QUEUE false
#define PRIORITY PRIORITY_ACTIVE
#define MSG_SIZE_MAX 4096
#define MSG_TIME_LIMIT 0

/***********************************************/
// Concurrency Control
/***********************************************/
// WAIT_DIE, NO_WAIT, TIMESTAMP, MVCC, HSTORE, HSTORE_SPEC, OCC, VLL, CALVIN, MAAT
#define CC_ALG CALVIN 
#define ISOLATION_LEVEL SERIALIZABLE
#define YCSB_ABORT_MODE false

// all transactions acquire tuples according to the primary key order.
#define KEY_ORDER         false
// transaction roll back changes after abort
#define ROLL_BACK         true
// per-row lock/ts management or central lock/ts management
#define CENTRAL_MAN         false
#define BUCKET_CNT          31
#define ABORT_PENALTY 10 * 1000000UL   // in ns.
#define ABORT_PENALTY_MAX 5 * 100 * 1000000UL   // in ns.
#define BACKOFF true
// [ INDEX ]
#define ENABLE_LATCH        false
#define CENTRAL_INDEX       false
#define CENTRAL_MANAGER       false
#define INDEX_STRUCT        IDX_HASH
#define BTREE_ORDER         16

// [DL_DETECT] 
#define DL_LOOP_DETECT        100000  // 100 us
#define DL_LOOP_TRIAL       1000  // 1 us
#define NO_DL           KEY_ORDER
#define TIMEOUT           100000000000
// [TIMESTAMP]
#define TS_TWR            false
#define TS_ALLOC          TS_CLOCK
#define TS_BATCH_ALLOC        false
#define TS_BATCH_NUM        1
// [MVCC]
// when read/write history is longer than HIS_RECYCLE_LEN
// the history should be recycled.
#define HIS_RECYCLE_LEN       10
#define MAX_PRE_REQ         MAX_TXN_IN_FLIGHT * NODE_CNT//1024
#define MAX_READ_REQ        MAX_TXN_IN_FLIGHT * NODE_CNT//1024
#define MIN_TS_INTVL        10 * 1000000UL // 10ms
// [OCC]
#define MAX_WRITE_SET       10
#define PER_ROW_VALID       false
// [HSTORE]
// when set to true, hstore will not access the global timestamp.
// This is fine for single partition transactions. 
#define HSTORE_LOCAL_TS       false
// [VLL] 
#define TXN_QUEUE_SIZE_LIMIT    THREAD_CNT
// [CALVIN]
#define SEQ_THREAD_CNT 4 
#define MAX_BATCH_WAITTIME 5000000  //5ms, currently unused


/***********************************************/
// Logging
/***********************************************/
#define LOG_COMMAND         false
#define LOG_REDO          false
#define LOGGING false
#define LOG_BUF_MAX 10
#define LOG_BUF_TIMEOUT 10 * 1000000UL // 10ms

/***********************************************/
// Benchmark
/***********************************************/
// max number of rows touched per transaction
#define MAX_ROW_PER_TXN       64
#define QUERY_INTVL         1UL
#define MAX_TXN_PER_PART 5000
#define FIRST_PART_LOCAL      true
#define MAX_TUPLE_SIZE        1024 // in bytes
#define GEN_BY_MPR false
// ==== [YCSB] ====
// SKEW_METHOD: 
//    ZIPF: use ZIPF_THETA distribution
//    HOT: use ACCESS_PERC of the accesses go to DATA_PERC of the data
#define SKEW_METHOD ZIPF
#define DATA_PERC 100
#define ACCESS_PERC 0.03
#define INIT_PARALLELISM 8
#define SYNTH_TABLE_SIZE 65536
#define ZIPF_THETA 0.3
#define TXN_WRITE_PERC 0.5
#define TUP_WRITE_PERC 0.5
#define SCAN_PERC           0
#define SCAN_LEN          20
#define PART_PER_TXN PART_CNT
#define PERC_MULTI_PART     MPR 
#define REQ_PER_QUERY 10
#define FIELD_PER_TUPLE       10
#define CREATE_TXN_FILE false
#define STRICT_PPT 0
// ==== [TPCC] ====
// For large warehouse count, the tables do not fit in memory
// small tpcc schemas shrink the table size.
#define TPCC_SMALL          false
#define MAX_ITEMS_SMALL 10000
#define CUST_PER_DIST_SMALL 2000
#define MAX_ITEMS_NORM 100000
#define CUST_PER_DIST_NORM 3000
#define MAX_ITEMS_PER_TXN 15
// Some of the transactions read the data but never use them. 
// If TPCC_ACCESS_ALL == fales, then these parts of the transactions
// are not modeled.
#define TPCC_ACCESS_ALL       false 
#define WH_UPDATE         true
#define NUM_WH 2
// % of transactions that access multiple partitions
#define MPR 1.0
#define MPIR 0.01
#define MPR_NEWORDER      20 // In %
enum TPCCTable {TPCC_WAREHOUSE, 
          TPCC_DISTRICT,
          TPCC_CUSTOMER,
          TPCC_HISTORY,
          TPCC_NEWORDER,
          TPCC_ORDER,
          TPCC_ORDERLINE,
          TPCC_ITEM,
          TPCC_STOCK};
enum TPCCTxnType {TPCC_ALL, 
          TPCC_PAYMENT, 
          TPCC_NEW_ORDER, 
          TPCC_ORDER_STATUS, 
          TPCC_DELIVERY, 
          TPCC_STOCK_LEVEL};
extern TPCCTxnType          g_tpcc_txn_type;

//#define TXN_TYPE          TPCC_ALL
#define PERC_PAYMENT 1.0
#define FIRSTNAME_MINLEN      8
#define FIRSTNAME_LEN         16
#define LASTNAME_LEN        16

#define DIST_PER_WH       10
//#define WH_TAB_SIZE NUM_WH
//#define ITEM_TAB_SIZE MAX_ITEMS_SMALL
//#define DIST_TAB_SIZE NUM_WH * DIST_PER_WH
//#define STOC_TAB_SIZE NUM_WH * MAX_ITEMS_SMALL
//#define CUST_TAB_SIZE NUM_WH * DIST_PER_WH * CUST_PER_DIST_SMALL
//#define HIST_TAB_SIZE NUM_WH * DIST_PER_WH * CUST_PER_DIST_SMALL
//#define ORDE_TAB_SIZE NUM_WH * DIST_PER_WH * CUST_PER_DIST_SMALL

/***********************************************/
// TODO centralized CC management. 
/***********************************************/
#define MAX_LOCK_CNT        (20 * THREAD_CNT) 
#define TSTAB_SIZE                  50 * THREAD_CNT
#define TSTAB_FREE                  TSTAB_SIZE 
#define TSREQ_FREE                  4 * TSTAB_FREE
#define MVHIS_FREE                  4 * TSTAB_FREE
#define SPIN                        false

/***********************************************/
// Test cases
/***********************************************/
#define TEST_ALL          true
enum TestCases {
    READ_WRITE,
      CONFLICT
};
extern TestCases          g_test_case;
/***********************************************/
// DEBUG info
/***********************************************/
#define WL_VERB           true
#define IDX_VERB          false
#define VERB_ALLOC          true

#define DEBUG_LOCK          false
#define DEBUG_TIMESTAMP       false
#define DEBUG_SYNTH         false
#define DEBUG_ASSERT        false
#define DEBUG_DISTR true
#define DEBUG_ALLOC false
#define DEBUG_RACE false
#define DEBUG_TIMELINE        false
#define DEBUG_BREAKDOWN       false

/***********************************************/
// MODES
/***********************************************/
// QRY Only do query operations, no 2PC
// TWOPC Only do 2PC, no query work
// SIMPLE Immediately send OK back to client
// NOCC Don't do CC
// NORMAL normal operation
#define MODE NORMAL_MODE


/***********************************************/
// Constant
/***********************************************/
// INDEX_STRUCT
#define IDX_HASH          1
#define IDX_BTREE         2
// WORKLOAD
#define YCSB            1
#define TPCC            2
#define TEST            3
// Concurrency Control Algorithm
#define NO_WAIT           1
#define WAIT_DIE          2
#define DL_DETECT         3
#define TIMESTAMP         4
#define MVCC            5
#define HSTORE            6
#define HSTORE_SPEC           7
#define OCC             8
#define VLL             9
#define CALVIN      10
#define MAAT      11
// TIMESTAMP allocation method.
#define TS_MUTEX          1
#define TS_CAS            2
#define TS_HW           3
#define TS_CLOCK          4
// MODES
// NORMAL < NOCC < QRY_ONLY < SETUP < SIMPLE
#define NORMAL_MODE 1
#define NOCC_MODE 2
#define QRY_ONLY_MODE 3
#define SETUP_MODE 4
#define SIMPLE_MODE 5
// SKEW METHODS
#define ZIPF 1
#define HOT 2
// PRIORITY WORK QUEUE
#define PRIORITY_FCFS 1
#define PRIORITY_ACTIVE 2
#define PRIORITY_HOME 3
// Replication
#define AA 1
#define AP 2
// Load
#define LOAD_MAX 1
#define LOAD_RATE 2
// Transport
#define TCP 1
#define IPC 2
// Isolation levels
#define SERIALIZABLE 1
#define READ_COMMITTED 2 
#define READ_UNCOMMITTED 3 
#define NOLOCK 4 

// Stats and timeout
#define BILLION 1000000000UL // in ns => 1 second
#define MILLION 1000000UL // in ns => 1 second
#define STAT_ARR_SIZE 1024
#define PROG_TIMER 1000 * BILLION // in s
#define BATCH_TIMER 0
#define SEQ_BATCH_TIMER 5 * 1 * MILLION // ~5ms -- same as CALVIN paper
#define DONE_TIMER 1 * 20 * BILLION // ~1 minutes
#define WARMUP_TIMER 0 * 60 * BILLION // ~1 minutes

#define SEED 0
#define SHMEM_ENV true
#define ENVIRONMENT_EC2 false

#endif


