#include "client_query.h"
#include "mem_alloc.h"
#include "wl.h"
#include "table.h"
#include "ycsb_query.h"
#include "tpcc_query.h"

/*************************************************/
//     class Query_queue
/*************************************************/

void 
Client_query_queue::init(workload * h_wl) {
	all_queries = new Client_query_thd * [g_node_cnt];
	_wl = h_wl;
	for (UInt32 tid = 0; tid < g_node_cnt; tid ++)
		init(tid);
}

void 
Client_query_queue::init(int thread_id) {	
	all_queries[thread_id] = (Client_query_thd *) 
        mem_allocator.alloc(sizeof(Client_query_thd), thread_id);
	all_queries[thread_id]->init(_wl, thread_id);
}

base_query * 
Client_query_queue::get_next_query(uint64_t thd_id) { 	
	base_query * query = all_queries[thd_id]->get_next_query();
	return query;
}

void 
Client_query_thd::init(workload * h_wl, int thread_id) {
	uint64_t request_cnt;
	q_idx = 0;
	//request_cnt = WARMUP / g_client_thread_cnt + MAX_TXN_PER_PART + 4;
	request_cnt = MAX_TXN_PER_PART + 4;
    printf("request count = %lu\n", request_cnt);
#if WORKLOAD == YCSB	
	queries = (ycsb_query *) 
	        mem_allocator.alloc(sizeof(ycsb_query) * request_cnt, thread_id);
#elif WORKLOAD == TPCC
	queries = (tpcc_query *) 
	        mem_allocator.alloc(sizeof(tpcc_query) * request_cnt, thread_id);
#endif
	for (UInt32 qid = 0; qid < request_cnt; qid ++) {
#if WORKLOAD == YCSB	
		new(&queries[qid]) ycsb_query();
#elif WORKLOAD == TPCC
		new(&queries[qid]) tpcc_query();
#endif
		//queries[qid].init(thread_id, h_wl, qid % g_node_cnt);
		queries[qid].init(thread_id, h_wl, thread_id);
        // Setup
        queries[qid].txn_id = UINT64_MAX;
        queries[qid].rtype = RTXN;
        //queries[qid].client_id = g_node_id;
	}
}

base_query * 
Client_query_thd::get_next_query() {
    if (q_idx >= MAX_TXN_PER_PART)
        return NULL;
	base_query * query = &queries[q_idx++];
	return query;
}

//void base_query::clear() { 
//  dest_id = UINT32_MAX;
//  return_id = UINT32_MAX;
//  txn_id = UINT64_MAX;
//  ts = UINT64_MAX;
//  rtype = (RemReqType)-1;
//  rc = RCOK;
//  pid = UINT64_MAX;
//} 

//void base_query::update_rc(RC rc) {
//  if(rc == Abort)
//    this->rc = rc;
//}
//
//void base_query::set_txn_id(uint64_t _txn_id) { txn_id = _txn_id; } 
//
//void base_query::remote_prepare(base_query * query, int dest_id) {
//  int total = 5;
//
//#if DEBUG_DISTR
//  printf("Sending RPREPARE %ld\n",query->txn_id);
//#endif
//  void ** data = new void *[total];
//  int * sizes = new int [total];
//  int num = 0;
//  RemReqType rtype = RPREPARE;
//  uint64_t _pid = query->pid;
//  RC rc = query->rc;
//  uint64_t _txn_id = query->txn_id; 
//
//	data[num] = &_txn_id;
//	sizes[num++] = sizeof(txnid_t);
//
//  data[num] = &rtype;
//  sizes[num++] = sizeof(RemReqType);
//  data[num] = &_pid;
//  sizes[num++] = sizeof(uint64_t);
//  data[num] = &rc;
//  sizes[num++] = sizeof(RC);
//  data[num] = &_txn_id;
//  sizes[num++] = sizeof(uint64_t);
//
//  rem_qry_man.send_remote_query(dest_id, data, sizes, num);
//}
//void base_query::remote_finish(base_query * query, int dest_id) {
//  int total = 5;
//
//#if DEBUG_DISTR
//  printf("Sending RFIN %ld\n",query->txn_id);
//#endif
//  void ** data = new void *[total];
//  int * sizes = new int [total];
//  int num = 0;
//  RemReqType rtype = RFIN;
//  uint64_t _pid = query->pid;
//  RC rc = query->rc;
//  uint64_t _txn_id = query->txn_id; 
//
//	data[num] = &_txn_id;
//	sizes[num++] = sizeof(txnid_t);
//
//  data[num] = &rtype;
//  sizes[num++] = sizeof(RemReqType);
//  data[num] = &_pid;
//  sizes[num++] = sizeof(uint64_t);
//  data[num] = &rc;
//  sizes[num++] = sizeof(RC);
//  data[num] = &_txn_id;
//  sizes[num++] = sizeof(uint64_t);
//
//  rem_qry_man.send_remote_query(dest_id, data, sizes, num);
//}
//
//void base_query::unpack_finish(base_query * query, void * d) {
//    char * data = (char *) d;
//    uint64_t ptr = HEADER_SIZE + sizeof(txnid_t) + sizeof(RemReqType);
//    memcpy(&query->pid, &data[ptr], sizeof(uint64_t));
//    ptr += sizeof(uint64_t);
//    memcpy(&query->rc, &data[ptr], sizeof(RC));
//    ptr += sizeof(RC);
//    memcpy(&query->txn_id, &data[ptr], sizeof(uint64_t));
//    ptr += sizeof(uint64_t);
//}
//
