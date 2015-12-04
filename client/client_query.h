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

#ifndef _CLIENT_QUERY_H_
#define _CLIENT_QUERY_H_

#include "global.h"
#include "helper.h"
#include "query.h"

class Workload;
class YCSBQuery;
class YCSBClientQuery;
class TPCCQuery;
class tpcc_client_query;

//enum RemReqType {INIT_DONE,RLK, RULK, RQRY, RFIN, RLK_RSP, RULK_RSP, RQRY_RSP, RACK, RTXN, RINIT, RPREPARE,RPASS};

//class client_BaseQuery {
//public:
//	virtual void init(uint64_t thd_id, Workload * h_wl) = 0;
//    virtual void reset() = 0;
//    virtual void unpack_rsp(BaseQuery * query, void * d) = 0;
//    virtual void unpack(BaseQuery * query, void * d) = 0;
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
//  void remote_prepare(BaseQuery * query, int dest_id);
//  void remote_finish(BaseQuery * query, int dest_id);
//  void unpack_finish(BaseQuery * query, void *d);
//};

// All the queries for a particular thread.
class Client_query_thd {
public:
	void init(Workload * h_wl, int thread_id);
  bool done(); 
  void init_txns_file(const char * txn_file);
	BaseClientQuery * get_next_query(uint64_t tid); 
	//BaseQuery * get_next_query(uint64_t tid); 
  volatile uint64_t q_idx;
  static void * threadInitQuery(void * id);
  void init_query();
  int next_tid;
  uint64_t thread_id;
  Workload * h_wl;
#if WORKLOAD == YCSB
	YCSBClientQuery * queries;
	//YCSBQuery * queries;
#else 
	tpcc_client_query * queries;
#endif
	char pad[CL_SIZE - sizeof(void *) - sizeof(int)];
};

// TODO we assume a separate task queue for each thread in order to avoid 
// contention in a centralized query queue. In reality, more sofisticated 
// queue model might be implemented.
class Client_query_queue {
public:
	void init(Workload * h_wl);
	void init(int thread_id);
  bool done(); 
	BaseClientQuery * get_next_query(uint64_t nid, uint64_t tid); 
	//BaseQuery * get_next_query(uint64_t nid, uint64_t tid); 
	
private:
	Client_query_thd ** all_queries;
	Workload * _wl;
};

#endif
