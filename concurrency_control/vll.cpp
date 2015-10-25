#include "vll.h"
#include "txn.h"
#include "table.h"
#include "row.h"
#include "row_vll.h"
#include "tpcc_query.h"
#include "tpcc.h"
#include "ycsb_query.h"
#include "ycsb.h"
#include "wl.h"
#include "catalog.h"
#include "mem_alloc.h"
#include "msg_queue.h"
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
    uint64_t t = get_sys_clock() - front_txn->wait_starttime;
    front_txn->txn_time_wait += t;
    DEBUG("FREE %ld\n",front_txn->get_txn_id());
    front_txn->rc = RCOK;
    //if(WORKLOAD != YCSB && front_txn->get_txn_id() % g_node_cnt != g_node_id) {
    if(front_txn->get_txn_id() % g_node_cnt != g_node_id) {
      //base_query * qry = txn_table.get_qry(g_node_id,front_txn->get_txn_id());
  		//rem_qry_man.ack_response(qry);
  		//rem_qry_man.ack_response(RCOK,front_txn);
      base_query * qry = front_txn->get_query();
      msg_queue.enqueue(qry,RACK,qry->return_id);
    }
    else {
      txn_table.restart_txn(front_txn->get_txn_id());
    }
  }
  pthread_mutex_unlock(&_mutex);

}

RC
VLLMan::beginTxn(txn_man * txn, base_query * query) {

  txn->read_keys(query);
#if MODE != NORMAL_MODE
  return RCOK;
#endif

  pthread_mutex_lock(&_mutex);
  DEBUG("beginTxn %ld\n",txn->get_txn_id());
  uint64_t starttime = get_sys_clock();

  /*
  TxnQEntry * tmp1 = _txn_queue;
  if(tmp1) {
  TxnQEntry * tmp2 = tmp1->next;
  while(tmp1 && tmp2) {
    assert(tmp1->txn->get_ts() > 0);
    assert( tmp1->txn->get_ts() < tmp2->txn->get_ts());
    tmp1 = tmp1->next;
    tmp2 = tmp2->next;
  }
  }
  */

	TxnQEntry * entry = NULL;
	TxnQEntry * prev_entry = NULL;
  RC ret = RCOK;
  /*
	if (_txn_queue_size >= TXN_QUEUE_SIZE_LIMIT)
		ret = 3;
    */

	txn->vll_txn_type = VLL_Free;
	
	TxnQEntry * front = _txn_queue;
	if (front) {
    if(txn->get_ts() < front->txn->get_ts()) {
      INC_STATS(txn->get_thd_id(),abort_from_ts,1);
      ret = Abort;
      txn->rc = Abort;
      query->rc = Abort;
#if WORKLOAD == TPCC
      tpcc_query * m_query = (tpcc_query*) query;
      m_query->txn_rtype = TPCC_FIN;
#elif WORKLOAD == YCSB
      ycsb_query * m_query = (ycsb_query*) query;
      m_query->txn_rtype = YCSB_FIN;
#endif
      goto final;
    }
  }


	for (int rid = 0; rid < txn->row_cnt; rid ++ ) {
		access_t type = txn->accesses[rid]->type;
    DEBUG("insert_access %d %lx\n",(int)type,(uint64_t)txn->accesses[rid]->orig_row->manager);
		if (txn->accesses[rid]->orig_row->manager->insert_access(type)) {
			txn->vll_txn_type = VLL_Blocked;
      INC_STATS(txn->get_thd_id(),cflt_cnt,1);
    }
    txn->vll_row_cnt++;
	}

	entry = getQEntry();
  entry->txn = txn;
	prev_entry = _txn_queue_tail;
  // Insert into queue in TS order
  while(prev_entry && prev_entry->txn->get_ts() > txn->get_ts()) {
    prev_entry = prev_entry->prev;
  }

  if(prev_entry && prev_entry->next) {
    prev_entry = prev_entry->next;
    LIST_INSERT_BEFORE(prev_entry,entry,_txn_queue);
  }
  else {
    LIST_PUT_TAIL(_txn_queue,_txn_queue_tail,entry);
  }
  txn->vll_entry = entry;
	if (txn->vll_txn_type == VLL_Blocked) {
		ret = WAIT;
    txn->wait_starttime = get_sys_clock();
    DEBUG("BLOCKED %ld\n",txn->get_txn_id());
  }
	else {
		ret = RCOK;
    DEBUG("FREE %ld\n",txn->get_txn_id());
  }

final:
  assert((!_txn_queue || _txn_queue->prev == NULL) && (!_txn_queue_tail || _txn_queue_tail->next == NULL));
  // Race condition on this assert:
  //assert(!_txn_queue || _txn_queue->txn->vll_txn_type == VLL_Free);
  assert(entry || ret == Abort);

  /*
  TxnQEntry *tmp1 = _txn_queue;
  if(tmp1) {
  TxnQEntry * tmp2 = tmp1->next;
  while(tmp1 && tmp2) {
    assert(tmp1->prev != tmp1->next);
    assert(tmp1->txn->get_ts() > 0);
    assert( tmp1->txn->get_ts() < tmp2->txn->get_ts());
    tmp1 = tmp1->next;
    tmp2 = tmp2->next;
  }
  }
  */
  INC_STATS(txn->get_thd_id(),txn_time_begintxn2,get_sys_clock() - starttime);
	pthread_mutex_unlock(&_mutex);
	return ret;
}

void 
VLLMan::finishTxn(txn_man * txn) {
	pthread_mutex_lock(&_mutex);
  DEBUG("finishTxn %ld\n",txn->get_txn_id());
	TxnQEntry * entry = txn->vll_entry;
  if(entry){

	for (int rid = 0; rid < txn->vll_row_cnt; rid ++ ) {
		access_t type = txn->accesses[rid]->type;
    DEBUG("remove_access %d %lx\n",(int)type,(uint64_t)txn->accesses[rid]->orig_row->manager);
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
