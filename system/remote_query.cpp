#include "remote_query.h"
#include "mem_alloc.h"
/*
	 Query message format:
	 4 Bytes: Message type (RemReqType)
	 */

void Remote_query_queue::init() {
}

r_query * 
Remote_query_queue::get_next_query(uint64_t thd_id) { 	
	r_query * query = r_queries[thd_id]->get_next_query();
	return query;
}

void Remote_query_queue::add_query(uint64_t thd_id, r_query * query) {
	r_query[thd_id] = query;
}

void Remote_query_queue::add_query(const char * buf) {
	// get query from buf
	r_query * rq = (r_query *) buf;
	// Assumption: thd_id == part_id
	add_query(rq->part_id,rq)
}

r_query * Remote_query_queue::new_query(RemReqType type, uint64_t thd_id, uint64_t part_id, uint64_t txn_id, uint64_t ts, uint64_t address = 0, const char * data = NULL) {
	r_query * rq; 
	rq->rtype = type;
	rq->return_id = thd_id;
	rq->part_id = part_id;
	rq->txn_id = txn_id;
	rq->ts = ts;
	rq->address = address;
	rq->data = data;

	return rq;
}
