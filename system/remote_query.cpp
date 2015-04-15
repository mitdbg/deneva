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
  pthread_mutex_init(&mtx,NULL);
}

txn_man * Remote_query::get_txn_man(uint64_t thd_id, uint64_t node_id, uint64_t txn_id) {

  txn_man * next_txn = NULL;

  return next_txn;
}

void Remote_query::remote_qry(base_query * query, int type, int dest_id, txn_man * txn) {
  //add_txn_man(0, dest_id, txn->get_txn_id(), txn);
#if WORKLOAD == TPCC
	tpcc_query * m_query = (tpcc_query *) query;
	m_query->remote_qry(query,type,dest_id);
#endif
}

void Remote_query::ack_response(base_query * query) {

	// Maximum number of parameters
	// NOTE: Adjust if parameters sent is changed
  uint64_t total = 2;
	void ** data = new void *[total];
	int * sizes = new int [total];
	int num = 0;

  txnid_t txn_id = query->txn_id;
	RemReqType rtype = RACK;

	data[num] = &txn_id;
	sizes[num++] = sizeof(txnid_t);

	data[num] = &rtype;
	sizes[num++] = sizeof(RemReqType);

  send_remote_query(query->return_id,data,sizes,num);
}

void Remote_query::send_remote_query(uint64_t dest_id, void ** data, int * sizes, int num) {
	tport_man.send_msg(dest_id, data, sizes, num);
}

void Remote_query::remote_rsp(base_query * query, txn_man * txn) {
    add_txn_man(0, query->return_id, txn->get_txn_id(), txn);
    //add_txn_man(txn->get_thd_id(), query->return_id, txn->get_txn_id(), txn);

#if WORKLOAD == TPCC
    tpcc_query * m_query = (tpcc_query *) query;
    m_query->remote_rsp(query);
#endif

}

void Remote_query::send_remote_rsp(uint64_t dest_id, void ** data, int * sizes, int num) {
	tport_man.send_msg(dest_id, data, sizes, num);
}

void Remote_query::unpack(base_query * query, void * d, int len) {
	char * data = (char *) d;
	uint64_t ptr = 0;
	memcpy(&query->dest_id,&data[ptr],sizeof(uint32_t));
	ptr += sizeof(uint32_t);

	memcpy(&query->return_id,&data[ptr],sizeof(uint32_t));
	ptr += sizeof(uint32_t);
	memcpy(&query->txn_id,&data[ptr],sizeof(txnid_t));
	ptr += sizeof(txnid_t);
	memcpy(&query->rtype,&data[ptr],sizeof(query->rtype));
	ptr += sizeof(query->rtype);

	if(query->dest_id != _node_id)
		return;

	switch(query->rtype) {
#if CC_ALG == HSTORE
		case RLK:
		case RULK:
			part_lock_man.unpack(query,data);
			break;
		case RLK_RSP:
		case RULK_RSP: 
			part_lock_man.unpack_rsp(query,data);
			break;
#endif
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
    case RFIN:
      query->unpack_finish(query, data);
      break;
    case RACK:
      break;
		default:
			assert(false);
	}
}

void Remote_query::add_txn_man(uint64_t thd_id, uint64_t node_id, uint64_t txn_id, txn_man *txn) {
}

void Remote_query::cleanup_remote(uint64_t thd_id, uint64_t node_id, uint64_t txn_id, bool free_txn) {

}

ts_t Remote_query::get_min_ts(ts_t min) {
  ts_t min_ts = UINT64_MAX;
  /*
  ts_t cur_ts = UINT64_MAX;
  pthread_mutex_lock(&mtx);
  for (UInt32 i = 0; i < g_node_cnt; i++) {
    txn_node_t cur = txns[0][i];
    while(cur->next != NULL) {
      cur_ts = cur->next->txn->get_ts(); 
      if(cur_ts < min_ts && min_ts >= min)
        min_ts = cur_ts;
      cur = cur->next;
    }
  }
  pthread_mutex_unlock(&mtx);
  */
  return min_ts;
}
