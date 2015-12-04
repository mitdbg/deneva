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

#include "query.h"
#include "mem_alloc.h"
#include "wl.h"
#include "table.h"
#include "ycsb_query.h"
#include "tpcc_query.h"

/*************************************************/
//     class Query_queue
/*************************************************/

void 
Query_queue::init(Workload * h_wl) {
	all_queries = new Query_thd * [g_thread_cnt];
	_wl = h_wl;
	for (UInt32 tid = 0; tid < g_thread_cnt; tid ++)
		init(tid);
}

void 
Query_queue::init(int thread_id) {	
	all_queries[thread_id] = (Query_thd *) mem_allocator.alloc(sizeof(Query_thd), thread_id);
	all_queries[thread_id]->init(_wl, thread_id);
}

BaseQuery * 
Query_queue::get_next_query(uint64_t thd_id) { 	
	BaseQuery * query = all_queries[thd_id]->get_next_query();
	return query;
}

void 
Query_thd::init(Workload * h_wl, int thread_id) {
	uint64_t request_cnt;
	q_idx = 0;
	request_cnt = WARMUP / g_thread_cnt + MAX_TXN_PER_PART + 4;
#if WORKLOAD == YCSB	
	queries = (YCSBQuery *) 
		mem_allocator.alloc(sizeof(YCSBQuery) * request_cnt, thread_id);
#elif WORKLOAD == TPCC
	queries = (TPCCQuery *) 
		mem_allocator.alloc(sizeof(TPCCQuery) * request_cnt, thread_id);
#endif
	for (UInt32 qid = 0; qid < request_cnt; qid ++) {
#if WORKLOAD == YCSB	
		new(&queries[qid]) YCSBQuery();
#elif WORKLOAD == TPCC
		new(&queries[qid]) TPCCQuery();
#endif
		queries[qid].init(thread_id, h_wl);
    // Setup
    queries[qid].txn_id = UINT64_MAX;
    queries[qid].rtype = RTXN;
	}


}

BaseQuery * 
Query_thd::get_next_query() {
	BaseQuery * query = &queries[q_idx++];
	return query;
}

void BaseQuery::clear() { 
  dest_id = UINT32_MAX;
  return_id = UINT32_MAX;
  batch_id = UINT64_MAX;
  txn_id = UINT64_MAX;
  ts = UINT64_MAX;
  rtype = NO_MSG;
  rc = RCOK;
  pid = UINT64_MAX;
  client_id = UINT32_MAX;
  debug1 = 0;
  part_touched_cnt = 0;
  spec = false;
  spec_done = false;
  //time_q_abrt = 0;
  //time_q_work = 0;
  //time_copy = 0;
} 

void BaseQuery::base_reset() { 
  part_touched_cnt = 0;
  rtype = RTXN;
  rc = RCOK;
  spec = false;
  spec_done = false;
  abort_restart = true;
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
  active_part = home_part;
#endif
}

void BaseQuery::update_rc(RC rc) {
  if(rc == Abort)
    this->rc = rc;
}

void BaseQuery::set_txn_id(uint64_t _txn_id) { txn_id = _txn_id; } 

// FIXME: Haven't debugged HStore with this
void BaseQuery::local_rinit_query(uint64_t part_id) {
        BaseQuery * tmp_query; 
        qry_pool.get(tmp_query);
        tmp_query->txn_id = this->txn_id;
        tmp_query->ts = this->ts;
        tmp_query->home_part = this->home_part;
        tmp_query->rtype = RINIT;
        tmp_query->active_part = part_id;
        tmp_query->part_cnt = 1;
        tmp_query->parts = new uint64_t[1];
        tmp_query->parts[0] = part_id;
        //work_queue.enqueue(tmp_query);
 
}
