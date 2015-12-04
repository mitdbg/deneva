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

#ifndef _QUERY_H_
#define _QUERY_H_

#include "global.h"
#include "helper.h"

class Workload;
class YCSBQuery;
class TPCCQuery;

class BaseClientQuery {
public:
  uint64_t pid;
  uint64_t return_id;
  uint64_t client_startts; // For sequencer
	RemReqType rtype;
	uint64_t part_num;
	uint64_t * part_to_access;
  // CALVIN
  uint64_t batch_id;
	txnid_t txn_id;
	//virtual void client_query(BaseClientQuery * query, uint64_t dest_id,
//			uint64_t batch_num, txnid_t txn_id) = 0;
};

class BaseQuery : public BaseClientQuery {
public:
	virtual void init(uint64_t thd_id, Workload * h_wl) = 0;
  virtual void reset() = 0;
  virtual BaseQuery * merge(BaseQuery * query) = 0;
  virtual void deep_copy(BaseQuery * qry) = 0;
  virtual bool readonly() = 0;
  void base_reset();
	uint64_t waiting_time;

	// Remote query components
	uint32_t dest_id;
	//txnid_t txn_id;
	uint64_t ts;
	int rem_req_state;

	uint64_t part_cnt;
	uint64_t * parts;
  bool ro;

  // For aborts
  bool abort_restart;
  uint64_t penalty;
  uint64_t penalty_start;
  uint64_t penalty_end;
  uint64_t q_starttime;

  //for debugging;
  uint64_t debug1;
#if MODE==QRY_ONLY_MODE
  uint64_t max_access;
  bool max_done;
#endif

	//RemReqType rtype;
	RC rc;

  // HStore specx
  bool spec;
  bool spec_done;

  uint64_t home_part;
  uint64_t active_part;
  uint64_t dest_part;
	uint64_t dest_part_id;

  // Prevent unnecessary remote messages
  uint64_t part_touched_cnt;
  uint64_t *part_touched;

  // Client components
  uint32_t client_id;

  // OCC
	uint64_t start_ts;
	uint64_t end_ts;

  // MVCC
  uint64_t thd_id;

  // Stats
  double time_q_abrt;
  double time_q_work;
  double time_copy;

  void clear();
  void update_rc(RC rc);
  void set_txn_id(uint64_t _txn_id); 
  void remote_prepare(BaseQuery * query, int dest_id);
  void remote_finish(BaseQuery * query, int dest_id);
  void unpack_finish(BaseQuery * query, void *d);
  void local_rack_query();
  void local_rinit_query(uint64_t part_id);
};

// All the queries for a particular thread.
class Query_thd {
public:
	void init(Workload * h_wl, int thread_id);
	BaseQuery * get_next_query(); 
	int q_idx;
#if WORKLOAD == YCSB
	YCSBQuery * queries;
#else 
	TPCCQuery * queries;
#endif
	char pad[CL_SIZE - sizeof(void *) - sizeof(int)];
};

// TODO we assume a separate task queue for each thread in order to avoid 
// contention in a centralized query queue. In reality, more sofisticated 
// queue model might be implemented.
class Query_queue {
public:
	void init(Workload * h_wl);
	void init(int thread_id);
	BaseQuery * get_next_query(uint64_t thd_id); 
	
private:
	Query_thd ** all_queries;
	Workload * _wl;
};

#endif
