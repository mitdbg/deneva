#ifndef _QUERY_H_
#define _QUERY_H_

#include "global.h"
#include "helper.h"

class workload;
class ycsb_query;
class tpcc_query;

//enum RemReqType {INIT_DONE,EXP_DONE,RLK, RULK, RQRY, RFIN, RLK_RSP, RULK_RSP, RQRY_RSP, RACK, RTXN, RINIT, RPREPARE,RPASS,CL_RSP,NO_MSG};

class base_client_query {
  public:
    uint64_t pid;
    uint64_t return_id;
    uint64_t client_startts; // For sequencer
	RemReqType rtype;
    //virtual void client_query(base_client_query * query, uint64_t dest_id) = 0; 
    virtual void unpack_client(base_client_query * query, void * d) = 0; 

	// calvin
	//virtual void client_query(base_client_query * query, uint64_t dest_id,
//			uint64_t batch_num, txnid_t txn_id) = 0;
};

class base_query {
public:
	virtual void init(uint64_t thd_id, workload * h_wl) = 0;
    virtual void reset() = 0;
    virtual void unpack_rsp(base_query * query, void * d) = 0;
    virtual void unpack(base_query * query, void * d) = 0;
    //virtual void client_query(base_query * query, uint64_t dest_id) = 0; 
    virtual void unpack_client(base_query * query, void * d) = 0; 
    virtual base_query * merge(base_query * query) = 0;
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

  // For aborts
  bool abort_restart;
  uint64_t penalty;
  uint64_t penalty_start;
  uint64_t penalty_end;

  // FIXME: for debugging;
  uint64_t debug1;
  // CALVIN
  uint64_t batch_num;

	RemReqType rtype;
	RC rc;
	uint64_t pid;

  // HStore specx
  bool spec;
  bool spec_done;

  uint64_t home_part;
  uint64_t active_part;
  uint64_t dest_part;
	uint64_t dest_part_id;

  // Prevent unnecessary remote messages
  uint64_t part_touched_cnt;
  uint64_t part_touched[MAX_PART_PER_TXN];

    // Client components
    //uint32_t client_node;
    uint32_t client_id;

  // Coordination Avoid
  uint64_t * keys;
  uint64_t num_keys;
  uint64_t t_type; // txn_type

  // OCC
	uint64_t start_ts;
	uint64_t end_ts;

  // MVCC
  uint64_t thd_id;

  uint64_t client_startts;

  // Stats
  double time_q_abrt;
  double time_q_work;
  double time_copy;

  void clear();
  void update_rc(RC rc);
  void set_txn_id(uint64_t _txn_id); 
  void remote_prepare(base_query * query, int dest_id);
  void remote_finish(base_query * query, int dest_id);
  void unpack_finish(base_query * query, void *d);
  void local_rack_query();
  void local_rinit_query(uint64_t part_id);
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
