#ifndef _STATS_H_
#define _STATS_H_

enum StatsArrType {ArrInsert, ArrIncr};
class StatsArr {
    public:
      void init(uint64_t size,StatsArrType type);
      void quicksort(int low_idx, int high_idx);
      void resize();
      void insert(uint64_t item);
      void print(FILE * f);
      void print(FILE * f,uint64_t min, uint64_t max); 
      uint64_t get_idx(uint64_t idx); 
      uint64_t get_percentile(uint64_t ile); 
      uint64_t get_avg(); 

      uint64_t * arr;
      uint64_t size;
      uint64_t cnt;
      StatsArrType type;
};

class Stats_thd {
public:
	void init(uint64_t thd_id);
	void clear();

	char _pad2[CL_SIZE];
	uint64_t txn_cnt;
	uint64_t txn_rem_cnt;
	uint64_t txn_sent;
	uint64_t occ_abort_check_cnt;
	uint64_t occ_check_cnt;
	uint64_t abort_cnt;
	uint64_t rem_row_cnt;
	uint64_t abort_rem_row_cnt;
	uint64_t abort_row_cnt;
	uint64_t abort_wr_cnt;
	uint64_t abort_rem_cnt;
	uint64_t abort_rem_txn_row_cnt;
	uint64_t txn_abort_cnt;
	uint64_t rbk_abort_cnt;
  uint64_t abort_from_ts;
	double tot_run_time;
	double run_time;
  double finish_time;
  uint64_t access_cnt;
  uint64_t write_cnt;
  double mbuf_send_time;
  double mq_full;
  double time_clock_rwait;
  double time_clock_wait;
  double time_work;
	double time_man;
	double time_rqry;
	double time_lock_man;
	double rtime_lock_man;
	double time_index;
	double rtime_index;
	double time_wait;
	double time_wait_lock_rem;
	double time_wait_lock;
	double time_wait_rem;
	double time_abort;
	double time_cleanup;
	double time_qq;
	double time_tport_rcv;
	double time_tport_send;
	double time_validate;
	uint64_t time_ts_alloc;
	double time_query;
	double rtime_proc;
	double time_unpack;
	uint64_t wait_cnt;
	double tport_lat;
	double lock_diff;
  double qq_full;
  double qq_cnt;
  double qq_lat;
  double aq_full;

  uint64_t cc_busy_cnt; // # of accesses when lock was owned (not necessarily a conflict)
  uint64_t txn_table_cflt;
  uint64_t txn_table_cflt_size;

  double owned_time;
  double owned_time_rd;
  double owned_time_wr;
  double twopl_time_diff;
  double owned_cnt;
  double owned_cnt_wr;
  double owned_cnt_rd;

  uint64_t *part_cnt;
  uint64_t *part_acc;
  double prof_time_twopc;
  double prof_cc_rel_abort, prof_cc_rel_commit;
  double sthd_prof_1a,sthd_prof_1b, sthd_prof_2, sthd_prof_3, sthd_prof_4, sthd_prof_5a, sthd_prof_5b;
  double rthd_prof_1, rthd_prof_2;
  double thd_prof_thd1, thd_prof_thd2, thd_prof_thd3;
  double thd_prof_thd1a, thd_prof_thd1b, thd_prof_thd1c, thd_prof_thd1d;
  double thd_prof_thd2_loc, thd_prof_thd2_rem;
  double thd_prof_thd2_type[17];
  double thd_prof_thd3_type[17];

  double thd_prof_thd_rfin0,thd_prof_thd_rfin1, thd_prof_thd_rfin2;
  double thd_prof_thd_rprep0,thd_prof_thd_rprep1, thd_prof_thd_rprep2;
  double thd_prof_thd_rqry_rsp0,thd_prof_thd_rqry_rsp1;
  double thd_prof_thd_rqry0,thd_prof_thd_rqry1,thd_prof_thd_rqry2;
  double thd_prof_thd_rack0,thd_prof_thd_rack1, thd_prof_thd_rack2a,thd_prof_thd_rack2,thd_prof_thd_rack3,thd_prof_thd_rack4;
  double thd_prof_thd_rtxn1a,thd_prof_thd_rtxn1b, thd_prof_thd_rtxn2,thd_prof_thd_rtxn3,thd_prof_thd_rtxn4;

  double thd_prof_wl1;
  double thd_prof_row1,thd_prof_row2,thd_prof_row3;
  double thd_prof_cc0,thd_prof_cc1,thd_prof_cc2,thd_prof_cc3;
  double thd_prof_wq1,thd_prof_wq2;
  double thd_prof_wq3,thd_prof_wq4;
  double thd_prof_txn1,thd_prof_txn2;
  double thd_prof_txn_table_add,thd_prof_txn_table_get;
  double thd_prof_txn_table_mints1,thd_prof_txn_table_mints2;
  double thd_prof_txn_table0a,thd_prof_txn_table1a,thd_prof_txn_table2a;
  double thd_prof_txn_table0b,thd_prof_txn_table1b,thd_prof_txn_table2;
  double thd_prof_occ_val4,thd_prof_occ_val1,thd_prof_occ_val2a,thd_prof_occ_val2,thd_prof_occ_val3,thd_prof_occ_val5;
  double thd_prof_mvcc1,thd_prof_mvcc2,thd_prof_mvcc3,thd_prof_mvcc4,thd_prof_mvcc5,thd_prof_mvcc6,thd_prof_mvcc7,thd_prof_mvcc8,thd_prof_mvcc9;
  uint64_t thd_prof_get_txn_cnt;

  double aq_poll;
  double aq_enqueue, aq_dequeue;
  double wq_enqueue, wq_dequeue;
  double all_wq_enqueue, all_wq_dequeue;
  double rem_wq_enqueue, rem_wq_dequeue;
  double new_wq_enqueue, new_wq_dequeue;

  uint64_t msg_batch_size;
  uint64_t msg_batch_bytes;
  uint64_t msg_batch_cnt;

  uint32_t batch_cnt;
  // calvin
  uint64_t seq_txn_cnt;
  uint64_t seq_batch_cnt;
  double time_seq_batch;
  double time_seq_prep;
  double time_seq_ack;

  double time_getqry;
  double client_latency;

	uint64_t cc_wait_cnt; // # of times a committed txn has been blocked in the cc algo
	double cc_wait_time; // time a committed txn has been blocked in this cc algo 
	double cc_hold_time; // time a committed txn has held a lock in this cc algo
	uint64_t cc_wait_abrt_cnt; // # of times an aborted txn has been blocked in the cc algo
	double cc_wait_abrt_time; // time an aborted txn has been blocked in this cc algo 
	double cc_hold_abrt_time; // time an aborted txn has held a lock in this cc algo
  uint64_t cflt_cnt;
  uint64_t cflt_cnt_txn;
	uint64_t mpq_cnt; // multi-partition queries
	uint64_t msg_bytes;
	uint64_t msg_sent_cnt;
	uint64_t msg_rcv_cnt;
	double time_msg_sent; 
  uint64_t spec_abort_cnt;
  uint64_t spec_commit_cnt;

	uint64_t debug1;
	uint64_t debug2;
	uint64_t debug3;
	uint64_t debug4;
	uint64_t debug5;

	uint64_t rlk;
	uint64_t rulk;
	uint64_t rlk_rsp;
	uint64_t rulk_rsp;
	uint64_t rqry;
	uint64_t rqry_rsp;
	uint64_t rfin;
	uint64_t rfwd;
	uint64_t rack;
	uint64_t rprep;
	uint64_t rinit;
	uint64_t rtxn;

  double txn_time_begintxn;
  double txn_time_begintxn2;
  double txn_time_idx;
  double txn_time_man;
  double txn_time_ts;
  double txn_time_abrt;
  double txn_time_clean;
  double txn_time_copy;
  double txn_time_wait;
  double txn_time_twopc;
  double txn_time_q_abrt;
  double txn_time_q_work;
  double txn_time_net;
  double txn_time_misc;

  StatsArr all_abort;
  StatsArr w_cflt;
  StatsArr d_cflt;
  StatsArr cnp_cflt;
  StatsArr c_cflt;
  StatsArr ol_cflt;
  StatsArr s_cflt;
  StatsArr w_abrt;
  StatsArr d_abrt;
  StatsArr cnp_abrt;
  StatsArr c_abrt;
  StatsArr ol_abrt;
  StatsArr s_abrt;
  StatsArr all_lat;

	uint64_t latency;
	//uint64_t * all_lat;

	char _pad[CL_SIZE];
};

class Stats_tmp {
public:
	void init();
	void clear();
	uint64_t mpq_cnt;

  uint64_t cc_wait_cnt;
  double cc_wait_time;
  double cc_hold_time;
	uint64_t cc_wait_abrt_cnt; // # of times an aborted txn has been blocked in the cc algo
	double cc_wait_abrt_time; // time an aborted txn has been blocked in this cc algo 
	double cc_hold_abrt_time; // time an aborted txn has held a lock in this cc algo
  
	char _pad[CL_SIZE - sizeof(double)*3];
};

class Stats {
public:
	// PER THREAD statistics
	Stats_thd ** _stats;
	// stats are first written to tmp_stats, if the txn successfully commits, 
	// copy the values in tmp_stats to _stats
	Stats_tmp ** tmp_stats;
	
	// GLOBAL statistics
	double dl_detect_time;
	double dl_wait_time;
	uint64_t cycle_detect;
	uint64_t deadlock;	

  // CPU / Mem utilization
  long long unsigned int last_ttl_usr, last_ttl_usr_low, last_ttl_sys, last_ttl_idle;
  long long unsigned int * last_usr, *last_usr_low, *last_sys, *last_idle;
  uint64_t last_total;
  clock_t last_clock;
  clock_t lastCPU, lastSysCPU, lastUserCPU;

	void init();
	void init(uint64_t thread_id);
	void clear(uint64_t tid);
	//void add_lat(uint64_t thd_id, uint64_t latency);
	void commit(uint64_t thd_id);
	void abort(uint64_t thd_id);
	void print_client(bool prog); 
	void print_sequencer(bool prog);
	void print(bool prog);
	void print_cnts(FILE * outf);
	void print_lat_distr();
  void print_lat_distr(uint64_t min, uint64_t max); 
	void print_abort_distr();
  uint64_t get_txn_cnts();
  void util_init();
  void print_util();
  int parseLine(char* line);
  void mem_util(FILE * outf);
  void cpu_util(FILE * outf);
  void print_prof(FILE * outf);
};

#endif
