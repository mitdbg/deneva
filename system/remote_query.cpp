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
	//txns = (txn_man **)mem_allocator.alloc(sizeof(txn_man *) * g_thread_cnt, g_thread_cnt);
  uint64_t thd_cnt = g_thread_cnt + g_rem_thread_cnt;
  txns = (txn_node_t **) mem_allocator.alloc(
            sizeof(txn_node_t **) * thd_cnt, 0);
  for (uint64_t i = 0; i < thd_cnt; ++i) {
    txns[i] = (txn_node_t *) mem_allocator.alloc(
                sizeof(txn_node_t) * g_node_cnt, 0);

    for (uint64_t j = 0; j < g_node_cnt; ++j) {
      txn_node_t t_node = (txn_node_t) mem_allocator.alloc(
                    sizeof(struct txn_node), 0);
      memset(t_node, '\0', sizeof(struct txn_node));
      txns[i][j] = t_node;
    }
  }
}

txn_man * Remote_query::get_txn_man(uint64_t thd_id, uint64_t node_id, uint64_t txn_id) {

  txn_man * next_txn = NULL;

  pthread_mutex_lock(&mtx);

  //txn_node_t t_node = txns[thd_id][node_id];
  txn_node_t t_node = txns[0][node_id];
  assert(t_node != NULL);

  while (t_node->next != NULL) {
    t_node = t_node->next;
    if (t_node->txn->get_txn_id() == txn_id) {
      next_txn = t_node->txn;
      break;
      //return t_node->txn;
    }
  }
  assert(next_txn != NULL);

  pthread_mutex_unlock(&mtx);

  return next_txn;
}

void Remote_query::remote_qry(base_query * query, int type, int dest_id, txn_man * txn) {
	//txns[txn->get_thd_id()] = txn;
  add_txn_man(0, dest_id, txn->get_txn_id(), txn);
  //add_txn_man(txn->get_thd_id(), dest_id, txn->get_txn_id(), txn);
#if WORKLOAD == TPCC
	tpcc_query * m_query = (tpcc_query *) query;
	m_query->remote_qry(query,type,dest_id);
#endif
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
		default:
			assert(false);
	}
}

void Remote_query::add_txn_man(uint64_t thd_id, uint64_t node_id, uint64_t txn_id, txn_man *txn) {
    txn_node_t t_node = (txn_node_t) mem_allocator.alloc(sizeof(struct txn_node), g_thread_cnt);
    t_node->txn = txn;

    pthread_mutex_lock(&mtx);

    t_node->next = txns[thd_id][node_id]->next;
    txns[thd_id][node_id]->next = t_node;

    pthread_mutex_unlock(&mtx);
}

void Remote_query::cleanup_remote(uint64_t thd_id, uint64_t node_id, uint64_t txn_id, bool free_txn) {

    pthread_mutex_lock(&mtx);
    //txn_node_t cur = txns[thd_id][node_id];
    txn_node_t cur = txns[0][node_id];

    txn_node_t t_node = NULL;
    while (cur->next != NULL && t_node == NULL) {
        if (cur->next->txn->get_txn_id() == txn_id) {
            t_node = cur->next;
            cur->next = cur->next->next;
        }
        else {
          cur = cur->next;
        }
    }
    assert(t_node != NULL);
    if (free_txn) {
        t_node->txn->release();
        mem_allocator.free(t_node->txn, sizeof(txn_man));
    }
    mem_allocator.free(t_node, sizeof(struct txn_node));

    pthread_mutex_unlock(&mtx);
}
