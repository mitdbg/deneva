#include "remote_query.h"
#include "mem_alloc.h"
#include "tpcc.h"
#include "tpcc_query.h"

void Remote_query::init() {
	q_idx = 0;
}

void Remote_query::unpack(r_query * query, char * data) {
	uint64_t ptr = sizeof(uint32_t);
	memcpy(&query->return_id,&data[ptr],sizeof(query->return_id));
	ptr += sizeof(query->return_id);
	memcpy(&query->rtype,&data[ptr],sizeof(query->rtype));
	ptr += sizeof(query->rtype);
	memcpy(&query->txn_id,&data[ptr],sizeof(query->txn_id));
	ptr += sizeof(query->txn_id);
	switch(query->rtype) {
		case RLK:
		case RULK:
		case RLK_RSP:
		case RULK_RSP:
			// p_lock::unpack
			break;
		case RQRY:
		case RQRY_RSP: {
#if WORKLOAD == TPCC
			tpcc_r_query * m_query = new tpcc_r_query;
			m_query->unpack(query,data);
#endif
			break;
									 }
		default:
			assert(false);
	}
}

