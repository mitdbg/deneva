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
	//txns = (txn_man **)mem_allocator.alloc(sizeof(txn_man *) * g_thread_cnt, g_thread_cnt);
    uint64_t thd_cnt = g_thread_cnt;
    txns = (txn_node_t **) mem_allocator.alloc(
            sizeof(txn_node_t **) * thd_cnt, g_thread_cnt);
    for (uint64_t i = 0; i < thd_cnt; ++i) {
        txns[i] = (txn_node_t *) mem_allocator.alloc(
                sizeof(txn_node_t) * g_node_cnt, thd_cnt);

        for (uint64_t j = 0; j < g_node_cnt; ++j) {
            txn_node_t t_node = (txn_node_t) mem_allocator.alloc(
                    sizeof(struct txn_node), g_thread_cnt);
            memset(t_node, '\0', sizeof(struct txn_node));
            txns[i][j] = t_node;
        }
    }
}

txn_man * Remote_query::get_txn_man(uint64_t thd_id, uint64_t node_id, uint64_t txn_id) {
	//return txns[tid];
    //printf("Looking up txn_man node for: thd_id: %lu, node_id: %lu, txn_id = %lu\n", thd_id, node_id, txn_id);
    txn_node_t t_node = txns[thd_id][node_id];
    assert(t_node != NULL);

    while (t_node->next != NULL) {
        t_node = t_node->next;
        if (t_node->txn->get_txn_id() == txn_id) {
            return t_node->txn;
        }
    }
    assert(false);
    return NULL;
}

txn_man * Remote_query::save_txn_man(uint64_t thd_id, uint64_t node_id, uint64_t txn_id, txn_man * txn_to_save) {
    txn_node_t t_node = txns[thd_id][node_id];
    assert(t_node != NULL);

    // Make copy of txn manager
    txn_man * copy = (txn_man *) mem_allocator.alloc(
            sizeof(txn_man), thd_id);
    txn_to_save->copy(copy);

    // Check if we need to update an entry in txns
    while (t_node->next != NULL) {
        t_node = t_node->next;
        if (t_node->txn->get_txn_id() == txn_id) {
            assert(txn_to_save->get_txn_id() == copy->get_txn_id());
            t_node->txn = copy;
            return copy;
        }
    }

    // If not in list yet, add it
    add_txn_man(thd_id, node_id, txn_id, copy);
    return copy;
}

void Remote_query::remote_qry(base_query * query, int type, int dest_id, txn_man * txn) {
	//txns[txn->get_thd_id()] = txn;
    add_txn_man(txn->get_thd_id(), dest_id, txn->get_txn_id(), txn);
#if WORKLOAD == TPCC
	tpcc_query * m_query = (tpcc_query *) query;
	m_query->remote_qry(query,type,dest_id);
#endif
}

void Remote_query::send_remote_query(uint64_t dest_id, void ** data, int * sizes, int num) {
	tport_man.send_msg(dest_id, data, sizes, num);
}

void Remote_query::signal_end() {
}

void Remote_query::remote_rsp(base_query * query, txn_man * txn) {
    save_txn_man(GET_THREAD_ID(query->pid), query->return_id, txn->get_txn_id(), txn);

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
	memcpy(&query->txn_id,&data[ptr],sizeof(uint32_t));
	ptr += sizeof(uint32_t);
	memcpy(&query->rtype,&data[ptr],sizeof(query->rtype));
	ptr += sizeof(query->rtype);

	if(query->dest_id != _node_id)
		return;

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

    t_node->next = txns[thd_id][node_id]->next;
    txns[thd_id][node_id]->next = t_node;
    //printf("Creating txn_man node for: thd_id: %lu, node_id: %lu, txn_id = %lu\n", thd_id, node_id, txn_id);
}

void Remote_query::cleanup_remote(uint64_t thd_id, uint64_t node_id, uint64_t txn_id) {
    txn_node_t cur = txns[thd_id][node_id];

    txn_node_t t_node = NULL;
    while (cur->next != NULL && t_node == NULL) {
        if (cur->next->txn->get_txn_id() == txn_id) {
            t_node = cur->next;
            cur->next = cur->next->next;
        }
    }
    free(t_node->txn);
    free(t_node);
}
