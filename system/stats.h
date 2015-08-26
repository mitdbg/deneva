#ifndef _STATS_H_
#define _STATS_H_

enum StatsArrType {ArrInsert, ArrIncr};
class StatsArr {
    public:
      void init(uint64_t size,StatsArrType type);
      void resize();
      void insert(uint64_t item);
      void print(FILE * f);

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
	uint64_t txn_sent;
	uint64_t abort_cnt;
	uint64_t txn_abort_cnt;
	uint64_t rbk_abort_cnt;
	double tot_run_time;
	double run_time;
    double finish_time;
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

  // calvin
  uint32_t batch_cnt;

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
	uint64_t rack;
	uint64_t rprep;
	uint64_t rinit;
	uint64_t rtxn;

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

	void init();
	void init(uint64_t thread_id);
	void clear(uint64_t tid);
	//void add_lat(uint64_t thd_id, uint64_t latency);
	void commit(uint64_t thd_id);
	void abort(uint64_t thd_id);
	void print_client(bool prog); 
	void print_sequencer(bool prog);
	void print_prog(uint64_t tid);
	void print(bool prog);
	void print_cnts();
	void print_lat_distr();
	void print_abort_distr();
    uint64_t get_txn_cnts();
};

#endif
