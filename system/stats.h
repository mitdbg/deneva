#ifndef _STATS_H_
#define _STATS_H_

class Stats_thd {
public:
	void init(uint64_t thd_id);
	void clear();

	char _pad2[CL_SIZE];
	uint64_t txn_cnt;
	uint64_t abort_cnt;
	uint64_t txn_abort_cnt;
	double run_time;
	double time_man;
	double time_lock_man;
	double rtime_lock_man;
	double time_index;
	double rtime_index;
	double time_wait;
	double rtime_wait_plock;
	double time_wait_lock;
	double time_wait_rem;
	double time_abort;
	double time_cleanup;
	uint64_t time_ts_alloc;
	double time_query;
	double rtime_proc;
	double rtime_unpack;
	double rtime_unpack_ndest;
	uint64_t wait_cnt;
	double tport_lat;
	double lock_diff;

	uint64_t mpq_cnt; // multi-partition queries
	uint64_t msg_bytes;
	uint64_t msg_sent_cnt;
	uint64_t msg_rcv_cnt;
	double time_msg_wait; // time blocking for remote response
	double time_rem_req; // time preparing msg for remote request
	double time_rem; // time preparing msg for remote request

	uint64_t debug1;
	uint64_t debug2;
	uint64_t debug3;
	uint64_t debug4;
	uint64_t debug5;

	uint64_t rqry;
	uint64_t rqry_rsp;
	uint64_t rfin;
	uint64_t rack;
	
	uint64_t latency;
	uint64_t * all_lat;
	uint64_t * all_abort_cnt;
	char _pad[CL_SIZE];
};

class Stats_tmp {
public:
	void init();
	void clear();
	/*
	double time_man;
	double time_index;
	double time_wait;
	double time_wait_lock;
	double time_wait_rem;
	*/
	uint64_t mpq_cnt;
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
	void add_lat(uint64_t thd_id, uint64_t latency);
	void add_abort_cnt(uint64_t thd_id, uint64_t abort_cnt);
	void commit(uint64_t thd_id);
	void abort(uint64_t thd_id);
	void print();
	void print_lat_distr();
	void print_abort_distr();
};

#endif
