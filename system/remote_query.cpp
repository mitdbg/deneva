#include "remote_query.h"
#include "mem_alloc.h"
#include "tpcc.h"
#include "tpcc_query.h"
#include "query.h"
#include "transport.h"
#include "plock.h"

void Remote_query::init(uint64_t node_id, workload * wl) {
	q_idx = 0;
	_node_id = node_id;
	_wl = wl;
}

void Remote_query::remote_qry(base_query * query, int type, int dest_id) {
#if WORKLOAD == TPCC
	tpcc_query * m_query = (tpcc_query *) query;
#endif
	m_query->remote_qry(query,type,dest_id);
}

void Remote_query::send_remote_query(uint64_t dest_id, void ** data, int * sizes, int num, uint64_t tid) {
	tport_man.send_msg(dest_id, data, sizes, num);
}

void Remote_query::signal_end() {
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
			part_lock_man.unpack(query,data);
			break;
		case RLK_RSP:
		case RULK_RSP: 
			part_lock_man.unpack_rsp(query,data);
			break;
		case RQRY: {
#if WORKLOAD == TPCC
			tpcc_query * m_query = new tpcc_query;
#endif
			m_query->unpack(query,data);
			break;
							 }
		case RQRY_RSP: {
#if WORKLOAD == TPCC
			tpcc_query * m_query = new tpcc_query;
#endif
			m_query->unpack_rsp(query,data);
			break;
									 }
		default:
			assert(false);
	}
}

