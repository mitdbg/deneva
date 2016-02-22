/*
   Copyright 2015 Rachael Harding

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#ifndef _TXN_H_
#define _TXN_H_

#include "global.h"
#include "helper.h"
#include "semaphore.h"
//#include "wl.h"

class Workload;
class Thread;
class row_t;
class table_t;
class BaseQuery;
class INDEX;
class TxnQEntry; 
class YCSBQuery;
class TPCCQuery;
//class r_query;

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

class Transaction {
public:
	Access **		accesses;
	int 			num_accesses_alloc;
  uint64_t timestamp;
	// For OCC
  uint64_t start_timestamp;
  uint64_t end_timestamp;

  uint64_t write_cnt;
  uint64_t row_cnt;
  // Internal state
  TxnState state;
  std::vector<row_t*> insert_rows;
	txnid_t 		txn_id;
  uint64_t batch_id;
};

/*
   Execution of transactions
   Manipulates/manages Transaction (contains txn-specific data)
   Maintains BaseQuery (contains input args, info about query)
   */
class TxnManager
{
public:
	virtual void init(Workload * h_wl);
  void clear();
  void reset();
	void release();
	Thread * h_thd;
	Workload * h_wl;

	virtual RC 		run_txn() = 0;
  virtual RC run_calvin_txn() = 0; 
  virtual RC acquire_locks() = 0; 
  void register_thd(Thread * h_thd);
	uint64_t 		get_thd_id();
	Workload * 		get_wl();
	void 			set_txn_id(txnid_t txn_id);
	txnid_t 		get_txn_id();
  void set_query(BaseQuery * qry);
  BaseQuery * get_query();
  bool is_done();

  void      set_pid(uint64_t pid);
  uint64_t get_pid();
	void 			set_ts(ts_t timestamp);
	ts_t 			get_ts();
	void 			set_start_ts(uint64_t start_ts);
	ts_t 			get_start_ts();
  uint64_t get_rsp_cnt(); 
  uint64_t get_rsp2_cnt(); 
  uint64_t incr_rsp(int i); 
  uint64_t decr_rsp(int i);
  uint64_t incr_rsp2(int i); 
  uint64_t decr_rsp2(int i);
  uint64_t incr_lr(); 
  uint64_t decr_lr();

  void commit() {assert(false);};
  void abort() {assert(false);};

	pthread_mutex_t txn_lock;
	row_t * volatile cur_row;
	// [DL_DETECT, NO_WAIT, WAIT_DIE]
	bool volatile 	lock_ready;
	// [TIMESTAMP, MVCC]
	bool volatile 	ts_ready; 
	// [HSTORE, HSTORE_SPEC]
	int volatile 	ready_part;
	int volatile 	ready_ulk;
  RC        validate();
	RC 				finish(RC rc, uint64_t * parts, uint64_t part_cnt);
	RC 				finish_local(RC rc, uint64_t * parts, uint64_t part_cnt);
	RC 				finish(BaseQuery * query,bool fin);
	void 			cleanup(RC rc);
  RC              rem_fin_txn(BaseQuery * query);
  RC              loc_fin_txn(BaseQuery * query);
  RC send_remote_reads(BaseQuery * qry); 
  RC calvin_finish(BaseQuery * qry); 

	////////////////////////////////
	// LOGGING
	////////////////////////////////
//	void 			gen_log_entry(int &length, void * log);

protected:	
  int rsp_cnt;
	void 			insert_row(row_t * row, table_t * table);

  // Calvin
  uint32_t lock_ready_cnt;
  bool locking_done;
  

  // For Calvin
  int phase;
  bool phase_rsp;

	itemid_t *		index_read(INDEX * index, idx_key_t key, int part_id);
  RC get_lock(row_t * row, access_t type);
	RC get_row(row_t * row, access_t type, row_t *& row_rtn);
  RC get_row_post_wait(row_t *& row_rtn);

  // For Waiting
  row_t * last_row;
  row_t * last_row_rtn;
  access_t last_type;

  Transaction * txn;
  BaseQuery * query;
  /*
#if WORKLOAD == YCSB
  YCSBQuery * query;
#elif WORKLOAD == TPCC
  TPCCQuery * query;
#endif
*/
};

#endif

