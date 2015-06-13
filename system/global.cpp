#include "global.h"
#include "mem_alloc.h"
#include "stats.h"
#include "dl_detect.h"
#include "manager.h"
#include "query.h"
#include "client_query.h"
#include "plock.h"
#include "occ.h"
#include "specex.h"
#include "vll.h"
#include "transport.h"
#include "query_work_queue.h"
#include "remote_query.h"
#include "txn_pool.h"
#include "client_txn.h"
#include "sequencer.h"

mem_alloc mem_allocator;
Stats stats;
DL_detect dl_detector;
Manager glob_manager;
Query_queue query_queue;
Client_query_queue client_query_queue;
Plock part_lock_man;
OptCC occ_man;
SpecEx spec_man;
#if CC_ALG == VLL
VLLMan vll_man;
#endif 
Transport tport_man;
Remote_query rem_qry_man;
TxnPool txn_pool;
QWorkQueue work_queue;
Client_txn client_man;
Sequencer seq_man;

bool volatile warmup_finish = false;
bool volatile enable_thread_mem_pool = false;
pthread_barrier_t warmup_bar;
#ifndef NOGRAPHITE
carbon_barrier_t enable_barrier;
#endif

ts_t g_abort_penalty = ABORT_PENALTY;
bool g_central_man = CENTRAL_MAN;
UInt32 g_ts_alloc = TS_ALLOC;
bool g_key_order = KEY_ORDER;
bool g_no_dl = NO_DL;
ts_t g_timeout = TIMEOUT;
ts_t g_dl_loop_detect = DL_LOOP_DETECT;
bool g_ts_batch_alloc = TS_BATCH_ALLOC;
UInt32 g_ts_batch_num = TS_BATCH_NUM;
UInt32 g_inflight_max = MAX_TXN_IN_FLIGHT;

bool g_hw_migrate = HW_MIGRATE;

bool g_part_alloc = PART_ALLOC;
bool g_mem_pad = MEM_PAD;
UInt32 g_cc_alg = CC_ALG;
ts_t g_query_intvl = QUERY_INTVL;
UInt32 g_part_per_txn = PART_PER_TXN;
double g_perc_multi_part = PERC_MULTI_PART;
double g_read_perc = READ_PERC;
double g_write_perc = WRITE_PERC;
double g_zipf_theta = ZIPF_THETA;
bool g_prt_lat_distr = PRT_LAT_DISTR;
UInt32 g_node_id = 0;
UInt32 g_node_cnt = NODE_CNT;
UInt32 g_part_cnt = PART_CNT;
UInt32 g_virtual_part_cnt = VIRTUAL_PART_CNT;
UInt32 g_thread_cnt = THREAD_CNT;
UInt32 g_rem_thread_cnt = REM_THREAD_CNT;
UInt64 g_synth_table_size = SYNTH_TABLE_SIZE;
UInt32 g_req_per_query = REQ_PER_QUERY;
UInt32 g_field_per_tuple = FIELD_PER_TUPLE;
UInt32 g_init_parallelism = INIT_PARALLELISM;
UInt32 g_client_node_cnt = CLIENT_NODE_CNT;
UInt32 g_client_thread_cnt = CLIENT_THREAD_CNT;
UInt32 g_client_rem_thread_cnt = CLIENT_REM_THREAD_CNT;
UInt32 g_servers_per_client = 0;
UInt32 g_server_start_node = 0;

// CALVIN
UInt32 g_seq_thread_cnt = SEQ_THREAD_CNT;

double g_mpr = MPR;

UInt32 g_num_wh = NUM_WH;
double g_perc_payment = PERC_PAYMENT;
bool g_wh_update = WH_UPDATE;
char * output_file = NULL;

#if TPCC_SMALL
UInt32 g_max_items = MAX_ITEMS_SMALL;
UInt32 g_cust_per_dist = CUST_PER_DIST_SMALL;
#else 
UInt32 g_max_items = MAX_ITEMS_NORM;
UInt32 g_cust_per_dist = CUST_PER_DIST_NORM;
#endif
