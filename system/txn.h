#ifndef _TXN_H_
#define _TXN_H_

#include "global.h"
#include "helper.h"
#include "semaphore.h"
//#include "wl.h"

class workload;
class thread_t;
class row_t;
class table_t;
class base_query;
class INDEX;
class TxnQEntry; 
//class r_query;

// each thread has a txn_man. 
// a txn_man corresponds to a single transaction.

//For VLL
enum TxnType {VLL_Blocked, VLL_Free};

enum TxnState {START,INIT,EXEC,PREP,FIN,DONE};

class Access {
public:
	access_t 	type;
	row_t * 	orig_row;
	row_t * 	data;
	row_t * 	orig_data;
	void cleanup();
};

class txn_man
{
public:
	virtual void init(thread_t * h_thd, workload * h_wl, uint64_t part_id);
  void clear();
	void release();
	thread_t * h_thd;
	workload * h_wl;
	myrand * mrand;
	uint64_t abort_cnt;
    volatile uint64_t ack_cnt;

	virtual RC 		run_txn(base_query * m_query) = 0;
	//virtual RC 		run_rem_txn(base_query * m_query) = 0;
	virtual void 		merge_txn_rsp(base_query * m_query1, base_query *m_query2) = 0;
  virtual bool  conflict(base_query * query1,base_query * query2) = 0;
  virtual void read_keys(base_query * query) = 0; 
  virtual RC acquire_locks(base_query * query) = 0; 
  void register_thd(thread_t * h_thd);
	uint64_t 		get_thd_id();
	uint64_t 		get_node_id();
	workload * 		get_wl();
	void 			set_txn_id(txnid_t txn_id);
	txnid_t 		get_txn_id();

  void      set_pid(uint64_t pid);
  uint64_t get_pid();
	void 			set_ts(ts_t timestamp);
	ts_t 			get_ts();
	void 			set_start_ts(uint64_t start_ts);
	ts_t 			get_start_ts();
  uint64_t get_rsp_cnt(); 
  uint64_t incr_rsp(int i); 
  uint64_t decr_rsp(int i);

	pthread_mutex_t txn_lock;
	row_t * volatile cur_row;
	// [DL_DETECT, NO_WAIT, WAIT_DIE]
	bool volatile 	lock_ready;
	bool volatile 	lock_abort; // forces another waiting txn to abort.
	// [TIMESTAMP, MVCC]
	bool volatile 	ts_ready; 
	// [HSTORE, HSTORE_SPEC]
	int volatile 	ready_part;
	int volatile 	ready_ulk;
  RC        validate();
	RC 				finish(RC rc, uint64_t * parts, uint64_t part_cnt);
	RC 				finish(base_query * query,bool fin);
	void 			cleanup(RC rc);
    RC              rem_fin_txn(base_query * query);

	////////////////////////////////
	// LOGGING
	////////////////////////////////
//	void 			gen_log_entry(int &length, void * log);

protected:	
	void 			insert_row(row_t * row, table_t * table);
public:
  // Home partition id TODO: populate
  uint64_t pid;
  uint64_t parts_locked; 
	// For OCC
	uint64_t 		start_ts;
	uint64_t 		end_ts;
	// following are public for OCC
	int 			row_cnt;
	int	 			wr_cnt;
//	int * 			row_cnts;
	Access **		accesses;
	int 			num_accesses_alloc;
  
  // Internal state
  TxnState state;
  uint64_t penalty_start;

	// For VLL
	TxnType 		vll_txn_type;
  TxnQEntry * vll_entry;

	itemid_t *		index_read(INDEX * index, idx_key_t key, int part_id);
  RC get_lock(row_t * row, access_t type);
	RC get_row(row_t * row, access_t type, row_t *& row_rtn);
  RC get_row_post_wait(row_t *& row_rtn);

  // For Waiting
  row_t * last_row;
  row_t * last_row_rtn;
  access_t last_type;
  RC rc;

  // For HStore
  bool spec;

  // For performance measurements
  uint64_t starttime;
  uint64_t wait_starttime;
  uint64_t cc_wait_cnt;
  double cc_wait_time;
  
private:
	// insert rows
	uint64_t 		insert_cnt;
	row_t * 		insert_rows[MAX_ROW_PER_TXN];
	txnid_t 		txn_id;
	ts_t 			timestamp;
  uint64_t rsp_cnt;
  sem_t rsp_mutex;
};

#endif

