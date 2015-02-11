#include "remote_query.h"
#include "mem_alloc.h"
#include "tpcc.h"
#include "tpcc_query.h"
#include "query.h"
#include "transport.h"

void Remote_query::init(uint64_t node_id) {
	q_idx = 0;
	_node_id = node_id;
	buf = new char * [g_thread_cnt];
	pthread_cond_init(&cnd,NULL);
	pthread_mutex_init(&mtx,NULL);
}

RC Remote_query::remote_qry(base_query * query, int type, int dest_id) {
#if WORKLOAD == TPCC
	tpcc_query * m_query = (tpcc_query *) query;
	return m_query->remote_qry(m_query,(TPCCRemTxnType)type,dest_id);
#else
	return RCOK
#endif
}

void * Remote_query::send_remote_query(uint64_t dest_id, void ** data, int * sizes, int num, uint64_t tid) {
	tport_man.send_msg(dest_id, data, sizes, num);
	// TODO wait for response from remote node
	// have remote thread signal local thread when a response message comes back
	pthread_cond_wait(&cnd,&mtx);
	return &buf[tid];
}

void Remote_query::send_remote_rsp(uint64_t dest_id, void ** data, int * sizes, int num, uint64_t tid) {
	tport_man.send_msg(dest_id, data, sizes, num);
}

void Remote_query::unpack(base_query * query, void * d, int len) {
	char * data = (char *) d;
	uint64_t ptr = sizeof(uint32_t);
	memcpy(&query->return_id,&data[ptr],sizeof(uint32_t));
	ptr += sizeof(uint32_t);
	memcpy(&query->txn_id,&data[ptr],sizeof(uint32_t));
	ptr += sizeof(uint32_t);
	memcpy(&query->rtype,&data[ptr],sizeof(query->rtype));
	ptr += sizeof(query->rtype);
	switch(query->rtype) {
		case RLK:
		case RULK:
		case RLK_RSP:
		case RULK_RSP:
			// p_lock::unpack
			break;
		case RQRY: {
#if WORKLOAD == TPCC
			tpcc_query * m_query = new tpcc_query;
			m_query->unpack(query,data);
#endif
			break;
							 }
		case RQRY_RSP: {

			buf[GET_THREAD_ID(query->return_id)] = (char *)mem_allocator.alloc(sizeof(char) * len, 0);
			memcpy(&buf[GET_THREAD_ID(query->return_id)],data,len);
			// Signal local thread
			pthread_cond_signal(&cnd);
			break;
									 }
		default:
			assert(false);
	}
}

