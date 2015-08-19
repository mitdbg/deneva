#include "vll.h"
#include "txn.h"
#include "table.h"
#include "row.h"
#include "row_vll.h"
#include "ycsb_query.h"
#include "ycsb.h"
#include "wl.h"
#include "catalog.h"
#include "mem_alloc.h"
#if CC_ALG == VLL

void 
VLLMan::init() {
	_txn_queue_size = 0;
	_txn_queue = NULL;
	_txn_queue_tail = NULL;
}

void
VLLMan::vllMainLoop(txn_man * txn, base_query * query) {
	
  /*
	ycsb_query * m_query = (ycsb_query *) query;
	// access the indexes. This is not in the critical section
	for (uint32_t rid = 0; rid < m_query->request_cnt; rid ++) {
		ycsb_request * req = &m_query->requests[rid];
		ycsb_wl * wl = (ycsb_wl *) txn->get_wl();
		int part_id = wl->key_to_part( req->key );
		INDEX * index = wl->the_index;
		itemid_t * item;
		item = txn->index_read(index, req->key, part_id);
		row_t * row = ((row_t *)item->location);
		// the following line adds the read/write sets to txn->accesses
		txn->get_row(row, req->rtype);
		int cs = row->manager->get_cs();
		INC_STATS(txn->get_thd_id(), debug1, cs);
	}

	bool done = false;
	while (!done) {
		txn_man * front_txn = NULL;
uint64_t t5 = get_sys_clock();
		//pthread_mutex_lock(&_mutex);
uint64_t tt5 = get_sys_clock() - t5;
INC_STATS(txn->get_thd_id(), debug5, tt5);

		
		TxnQEntry * front = _txn_queue;
		if (front)
			front_txn = front->txn;
		// only one worker thread can execute the txn.
		if (front_txn && front_txn->vll_txn_type == VLL_Blocked) {
			front_txn->vll_txn_type = VLL_Free;
			//pthread_mutex_unlock(&_mutex);
			execute(front_txn, query);
			finishTxn( front_txn, front);
		} else {
			// _mutex will be unlocked in beginTxn()
			TxnQEntry * entry = NULL;
			int ok = beginTxn(txn, query, entry);
			if (ok == 2) {
				execute(txn, query);
				finishTxn(txn, entry);
			} 
			assert(ok == 1 || ok == 2);
			done = true;
		}
	}
	return;
  */
}

void
VLLMan::restartQFront() {
  pthread_mutex_lock(&_mutex);
  TxnQEntry * front = _txn_queue;
  txn_man * front_txn = NULL;
	if (front)
		front_txn = front->txn;
	if (front_txn && front_txn->vll_txn_type == VLL_Blocked) {
		front_txn->vll_txn_type = VLL_Free;
    front_txn->rc = RCOK;
    //if(WORKLOAD != YCSB && front_txn->get_txn_id() % g_node_cnt != g_node_id) {
    if(front_txn->get_txn_id() % g_node_cnt != g_node_id) {
      //base_query * qry = txn_pool.get_qry(g_node_id,front_txn->get_txn_id());
  		//rem_qry_man.ack_response(qry);
  		rem_qry_man.ack_response(RCOK,front_txn);
    }
    else {
      txn_pool.restart_txn(front_txn->get_txn_id());
    }
  }
  pthread_mutex_unlock(&_mutex);

}

RC
VLLMan::beginTxn(txn_man * txn, base_query * query) {

  txn->read_keys(query);

  pthread_mutex_lock(&_mutex);

	TxnQEntry * entry = NULL;
	TxnQEntry * prev_entry = NULL;
  RC ret = RCOK;
  /*
	if (_txn_queue_size >= TXN_QUEUE_SIZE_LIMIT)
		ret = 3;
    */

	txn->vll_txn_type = VLL_Free;
	assert(WORKLOAD == YCSB);
	
	TxnQEntry * front = _txn_queue;
	if (front) {
    if(txn->get_ts() < front->txn->get_ts()) {
      ret = Abort;
      txn->rc = Abort;
      query->rc = Abort;
      ycsb_query * m_query = (ycsb_query*) query;
      m_query->txn_rtype = YCSB_FIN;
      goto final;
  }
  }


	for (int rid = 0; rid < txn->row_cnt; rid ++ ) {
		access_t type = txn->accesses[rid]->type;
		if (txn->accesses[rid]->orig_row->manager->insert_access(type))
			txn->vll_txn_type = VLL_Blocked;
	}

	entry = getQEntry();
  entry->txn = txn;
	prev_entry = _txn_queue;
	//LIST_PUT_TAIL(_txn_queue, _txn_queue_tail, entry);
  // Insert into queue in TS order
  while(prev_entry && prev_entry->txn->get_ts() < txn->get_ts()) {
    prev_entry = prev_entry->next;
  }
  if(prev_entry) {
    entry->prev = prev_entry;
    entry->next = prev_entry->next;
    prev_entry->next = entry;
    if(!entry->next)
      _txn_queue_tail = entry;
    else
      entry->next->prev = entry;
  }
  else {
    if(_txn_queue) {
      // Reached end of queue
      entry->prev = _txn_queue_tail;
      _txn_queue_tail->next = entry;
      _txn_queue_tail = entry;
    }
    else {
      // Head of queue
      _txn_queue = entry;
      _txn_queue_tail = entry;
    }
  }
  txn->vll_entry = entry;
	if (txn->vll_txn_type == VLL_Blocked)
		ret = WAIT;
	else 
		ret = RCOK;

final:
  assert((!_txn_queue || _txn_queue->prev == NULL) && (!_txn_queue_tail || _txn_queue_tail->next == NULL));
  // Race condition on this assert:
  //assert(!_txn_queue || _txn_queue->txn->vll_txn_type == VLL_Free);
  assert(entry || ret == Abort);
	pthread_mutex_unlock(&_mutex);
	return ret;
}

void 
VLLMan::finishTxn(txn_man * txn) {
	pthread_mutex_lock(&_mutex);
	TxnQEntry * entry = txn->vll_entry;
  if(entry){

	for (int rid = 0; rid < txn->row_cnt; rid ++ ) {
		access_t type = txn->accesses[rid]->type;
		txn->accesses[rid]->orig_row->manager->remove_access(type);
	}
	  LIST_REMOVE_HT(entry, _txn_queue, _txn_queue_tail);
  }

  assert((!_txn_queue || _txn_queue->prev == NULL) && (!_txn_queue_tail || _txn_queue_tail->next == NULL));

	pthread_mutex_unlock(&_mutex);

  if(entry)
    returnQEntry(entry);

  restartQFront(); 
	//txn->release();
	//mem_allocator.free(txn, 0);
}


TxnQEntry * 
VLLMan::getQEntry() {
	TxnQEntry * entry = (TxnQEntry *) mem_allocator.alloc(sizeof(TxnQEntry), 0);
	entry->prev = NULL;
	entry->next = NULL;
	entry->txn = NULL;
	return entry;
}

void 
VLLMan::returnQEntry(TxnQEntry * entry) {
 	mem_allocator.free(entry, sizeof(TxnQEntry));
}

#endif
