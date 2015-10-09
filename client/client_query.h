#ifndef _CLIENT_QUERY_H_
#define _CLIENT_QUERY_H_

#include "global.h"
#include "helper.h"
#include "query.h"

class workload;
class ycsb_query;
class ycsb_client_query;
class tpcc_query;

//enum RemReqType {INIT_DONE,RLK, RULK, RQRY, RFIN, RLK_RSP, RULK_RSP, RQRY_RSP, RACK, RTXN, RINIT, RPREPARE,RPASS};

//class client_base_query {
//public:
//	virtual void init(uint64_t thd_id, workload * h_wl) = 0;
//    virtual void reset() = 0;
//    virtual void unpack_rsp(base_query * query, void * d) = 0;
//    virtual void unpack(base_query * query, void * d) = 0;
//	uint64_t waiting_time;
//	uint64_t part_num;
//	uint64_t * part_to_access;
//
//	// Remote query components
//	uint32_t dest_id;
//	uint32_t return_id;
//	txnid_t txn_id;
//	uint64_t ts;
//	int rem_req_state;
//
//	uint64_t part_cnt;
//	uint64_t * parts;
//
//	RemReqType rtype;
//	RC rc;
//	uint64_t pid;
//
//  // Coordination Avoid
//  uint64_t * keys;
//  uint64_t num_keys;
//  uint64_t t_type; // txn_type
//
//  // OCC
//	uint64_t start_ts;
//	uint64_t end_ts;
//
//  // MVCC
//  uint64_t thd_id;
//
//
//  void clear();
//  void update_rc(RC rc);
//  void set_txn_id(uint64_t _txn_id); 
//  void remote_prepare(base_query * query, int dest_id);
//  void remote_finish(base_query * query, int dest_id);
//  void unpack_finish(base_query * query, void *d);
//};

// All the queries for a particular thread.
class Client_query_thd {
public:
	void init(workload * h_wl, int thread_id);
  bool done(); 
  void init_txns_file(const char * txn_file);
	base_client_query * get_next_query(uint64_t tid); 
	//base_query * get_next_query(uint64_t tid); 
  volatile uint64_t q_idx;
  static void * threadInitQuery(void * id);
  void init_query();
  int next_tid;
  uint64_t thread_id;
  workload * h_wl;
#if WORKLOAD == YCSB
	ycsb_client_query * queries;
	//ycsb_query * queries;
#else 
	tpcc_query * queries;
#endif
	char pad[CL_SIZE - sizeof(void *) - sizeof(int)];
};

// TODO we assume a separate task queue for each thread in order to avoid 
// contention in a centralized query queue. In reality, more sofisticated 
// queue model might be implemented.
class Client_query_queue {
public:
	void init(workload * h_wl);
	void init(int thread_id);
  bool done(); 
	base_client_query * get_next_query(uint64_t nid, uint64_t tid); 
	//base_query * get_next_query(uint64_t nid, uint64_t tid); 
	
private:
	Client_query_thd ** all_queries;
	workload * _wl;
};

#endif
