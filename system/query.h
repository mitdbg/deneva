#ifndef _QUERY_H_
#define _QUERY_H_

#include "global.h"
#include "helper.h"

class workload;
class ycsb_query;
class tpcc_query;

enum RemReqType {INIT_DONE,RLK, RULK, RQRY, RFIN, RLK_RSP, RULK_RSP, RQRY_RSP, RACK, RTXN, RINIT, RPREPARE,RPASS};

class base_query {
public:
	virtual void init(uint64_t thd_id, workload * h_wl) = 0;
  virtual void reset() = 0;
  virtual void unpack_rsp(base_query * query, void * d) = 0;
  virtual void unpack(base_query * query, void * d) = 0;
	uint64_t waiting_time;
	uint64_t part_num;
	uint64_t * part_to_access;

	// Remote query components
	uint32_t dest_id;
	uint32_t return_id;
	txnid_t txn_id;
	uint64_t ts;
	int rem_req_state;

	uint64_t part_cnt;
	uint64_t * parts;

	RemReqType rtype;
	RC rc;
	uint64_t pid;

  // OCC
	uint64_t start_ts;
	uint64_t end_ts;

  // MVCC
  uint64_t thd_id;


  void clear();
  void update_rc(RC rc);
  void set_txn_id(uint64_t _txn_id); 
  void remote_prepare(base_query * query, int dest_id);
  void remote_finish(base_query * query, int dest_id);
  void unpack_finish(base_query * query, void *d);
};

// All the queries for a particular thread.
class Query_thd {
public:
	void init(workload * h_wl, int thread_id);
	base_query * get_next_query(); 
	int q_idx;
#if WORKLOAD == YCSB
	ycsb_query * queries;
#else 
	tpcc_query * queries;
#endif
	char pad[CL_SIZE - sizeof(void *) - sizeof(int)];
};

// TODO we assume a separate task queue for each thread in order to avoid 
// contention in a centralized query queue. In reality, more sofisticated 
// queue model might be implemented.
class Query_queue {
public:
	void init(workload * h_wl);
	void init(int thread_id);
	base_query * get_next_query(uint64_t thd_id); 
	
private:
	Query_thd ** all_queries;
	workload * _wl;
};

#endif
