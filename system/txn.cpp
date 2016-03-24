/*
   Copyright 2015 Rachael Harding

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#include "helper.h"
#include "txn.h"
#include "row.h"
#include "wl.h"
#include "query.h"
#include "thread.h"
#include "mem_alloc.h"
#include "occ.h"
#include "specex.h"
#include "row_occ.h"
#include "row_specex.h"
#include "table.h"
#include "catalog.h"
#include "index_btree.h"
#include "index_hash.h"
#include "vll.h"
#include "msg_queue.h"
#include "pool.h"
#include "message.h"
#include "ycsb_query.h"
#include "array.h"


void Transaction::init() {
  accesses.init(MAX_ROW_PER_TXN);  
  insert_rows.clear();  
  timestamp = UINT64_MAX;
  start_timestamp = UINT64_MAX;
  end_timestamp = UINT64_MAX;
  txn_id = UINT64_MAX;
  batch_id = UINT64_MAX;
  write_cnt = 0;
  row_cnt = 0;
  state = START;
  rc = RCOK;
}

void Transaction::release() {
  DEBUG("Transaction release\n");
  for(uint64_t i = 0; i < accesses.size(); i++)
    mem_allocator.free(accesses[i],sizeof(Access));
  accesses.release();
}

void TxnManager::init(Workload * h_wl) {
  if(!txn) 
    txn = (Transaction*) mem_allocator.alloc(sizeof(Transaction));
  txn->init();
  if(!query) {
#if WORKLOAD == YCSB
    query = (BaseQuery*) mem_allocator.alloc(sizeof(YCSBQuery));
    new(query) YCSBQuery();
#elif WORKLOAD == TPCC
    query = (BaseQuery*) mem_allocator.alloc(sizeof(TPCCQuery));
    new(query) TPCCQuery();
#endif
  }
  query->init();
  reset();

	this->h_wl = h_wl;
	pthread_mutex_init(&txn_lock, NULL);
}

void TxnManager::reset() {
	lock_ready = false;
  lock_ready_cnt = 0;
  locking_done = true;
	ready_part = 0;
  rsp_cnt = 0;

}

RC TxnManager::commit() {
  DEBUG("Commit %ld\n",get_txn_id());
  release_locks(RCOK);
  commit_stats();
#if LOGGING
    LogRecord * record = logger.createRecord(get_txn_id(),L_NOTIFY,0,0);
    if(g_repl_cnt > 0) {
      msg_queue.enqueue(Message::create_message(record,LOG_MSG),g_node_id + g_node_cnt + g_client_node_cnt); 
    }
  logger.enqueueRecord(record);
  return WAIT;
#endif
  return Commit;
}

RC TxnManager::abort() {
  DEBUG("Abort %ld\n",get_txn_id());
  release_locks(Abort);
  commit_stats();
  return Abort;
}

RC TxnManager::start_abort() {
  if(query->partitions_touched.size() > 1) {
    send_finish_messages();
    return Abort;
  } 
  return abort();
}

RC TxnManager::start_commit() {
  RC rc = RCOK;
  if(is_multi_part()) {
    if(!((YCSBQuery*)query)->readonly() || CC_ALG == OCC) {
      // send prepare messages
      send_prepare_messages();
      rc = WAIT_REM;
    } else {
      send_finish_messages();
      commit();
      rc = WAIT_REM;
    }
  } else { // is not multi-part
    rc = validate();
    if(rc == RCOK)
      rc = commit();
  }
  return rc;
}

void TxnManager::send_prepare_messages() {
  rsp_cnt = query->partitions_touched.size() - 1;
  DEBUG("%ld Send PREPARE messages to %d\n",get_txn_id(),rsp_cnt);
  for(uint64_t i = 0; i < query->partitions_touched.size(); i++) {
    if(GET_NODE_ID(query->partitions_touched[i]) == g_node_id) {
      continue;
    }
    msg_queue.enqueue(Message::create_message(this,RPREPARE),GET_NODE_ID(query->partitions_touched[i]));
  }
}

void TxnManager::send_finish_messages() {
  rsp_cnt = query->partitions_touched.size() - 1;
  DEBUG("%ld Send FINISH messages to %d\n",get_txn_id(),rsp_cnt);
  for(uint64_t i = 0; i < query->partitions_touched.size(); i++) {
    if(GET_NODE_ID(query->partitions_touched[i]) == g_node_id) {
      continue;
    }
    msg_queue.enqueue(Message::create_message(this,RFIN),GET_NODE_ID(query->partitions_touched[i]));
  }
}

int TxnManager::received_response(RC rc) {
  assert(txn->rc == RCOK || txn->rc == Abort);
  if(txn->rc == RCOK)
    txn->rc = rc;
  --rsp_cnt;
  return rsp_cnt;
}

bool TxnManager::is_multi_part() {
  return query->partitions.size() > 1;
}

void TxnManager::commit_stats() {
  if(!IS_LOCAL(get_txn_id()))
    return;
  uint64_t timespan = get_sys_clock(); //FIXME

  INC_STATS(get_thd_id(),txn_cnt,1);
  INC_STATS(get_thd_id(), run_time, timespan);
  INC_STATS(get_thd_id(), latency, timespan);
  INC_STATS_ARR(get_thd_id(),all_lat,timespan);
  /*if(abort_cnt > 0) { 
    INC_STATS(get_thd_id(), txn_abort_cnt, 1);
    INC_STATS_ARR(get_thd_id(), all_abort, abort_cnt);
  }*/
  if(query->partitions.size() > 1) {
    INC_STATS(get_thd_id(),mpq_cnt,1);
  }
  /*if(cflt) {
    INC_STATS(get_thd_id(),cflt_cnt_txn,1);
  }*/

  assert(query->partitions_touched.size() > 0);
  INC_STATS(get_thd_id(),part_cnt[query->partitions_touched.size()-1],1);
  for(uint64_t i = 0 ; i < query->partitions.size(); i++) {
    INC_STATS(get_thd_id(),part_acc[query->partitions[i]],1);
  }
}

void TxnManager::register_thd(Thread * h_thd) {
  this->h_thd = h_thd;
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
  this->active_part = GET_PART_ID_FROM_IDX(get_thd_id());
#endif
}

void TxnManager::set_txn_id(txnid_t txn_id) {
	txn->txn_id = txn_id;
}

txnid_t TxnManager::get_txn_id() {
	return txn->txn_id;
}

Workload * TxnManager::get_wl() {
	return h_wl;
}

uint64_t TxnManager::get_thd_id() {
  // FIXME
	//return h_thd->get_thd_id();
  return 0;
}

BaseQuery * TxnManager::get_query() {
	return query;
}
void TxnManager::set_query(BaseQuery * qry) {
	query = qry;
}

void TxnManager::set_timestamp(ts_t timestamp) {
	txn->timestamp = timestamp;
}

ts_t TxnManager::get_timestamp() {
	return txn->timestamp;
}

void TxnManager::set_start_timestamp(uint64_t start_timestamp) {
	txn->start_timestamp = start_timestamp;
}

ts_t TxnManager::get_start_timestamp() {
	return txn->start_timestamp;
}

uint64_t TxnManager::incr_lr() {
  //ATOM_ADD(this->rsp_cnt,i);
  uint64_t result;
  sem_wait(&rsp_mutex);
  result = ++this->lock_ready_cnt;
  sem_post(&rsp_mutex);
  return result;
}

uint64_t TxnManager::decr_lr() {
  //ATOM_SUB(this->rsp_cnt,i);
  uint64_t result;
  sem_wait(&rsp_mutex);
  result = --this->lock_ready_cnt;
  sem_post(&rsp_mutex);
  return result;
}
uint64_t TxnManager::incr_rsp(int i) {
  //ATOM_ADD(this->rsp_cnt,i);
  uint64_t result;
  sem_wait(&rsp_mutex);
  result = ++this->rsp_cnt;
  sem_post(&rsp_mutex);
  return result;
}

uint64_t TxnManager::decr_rsp(int i) {
  //ATOM_SUB(this->rsp_cnt,i);
  uint64_t result;
  sem_wait(&rsp_mutex);
  result = --this->rsp_cnt;
  sem_post(&rsp_mutex);
  return result;
}

void TxnManager::cleanup(RC rc) {
#if CC_ALG == OCC && MODE == NORMAL_MODE
  occ_man.finish(rc,this);
#endif

	ts_t starttime = get_sys_clock();
  uint64_t row_cnt = txn->accesses.get_count();
  assert(txn->accesses.get_count() == txn->row_cnt);
  DEBUG("Cleanup %ld %ld\n",get_txn_id(),row_cnt);
  uint64_t thd_prof_start = starttime;
	for (int rid = row_cnt - 1; rid >= 0; rid --) {
		row_t * orig_r = txn->accesses[rid]->orig_row;
		access_t type = txn->accesses[rid]->type;
		if (type == WR && rc == Abort)
			type = XP;

    uint64_t cc_rel_starttime = get_sys_clock();
		if (ROLL_BACK && type == XP &&
					(CC_ALG == DL_DETECT || 
					CC_ALG == NO_WAIT || 
					CC_ALG == WAIT_DIE ||
          CC_ALG == HSTORE ||
          CC_ALG == HSTORE_SPEC 
          )) 
		{
			orig_r->return_row(type, this, txn->accesses[rid]->orig_data);
		} else {
			orig_r->return_row(type, this, txn->accesses[rid]->data);
		}

    if(rc == Abort) {
      INC_STATS(get_thd_id(),prof_cc_rel_abort,get_sys_clock() - cc_rel_starttime);
    } else {
      INC_STATS(get_thd_id(),prof_cc_rel_commit,get_sys_clock() - cc_rel_starttime);
    }
#if ROLL_BACK && (CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE || CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC)
		if (type == WR) {
      //printf("free 10 %ld\n",get_txn_id());
			txn->accesses[rid]->orig_data->free_row();
      DEBUG_M("TxnManager::cleanup WR free\n");
			mem_allocator.free(txn->accesses[rid]->orig_data, sizeof(row_t));
		}
#endif
		txn->accesses[rid]->data = NULL;
	}

  INC_STATS(get_thd_id(),thd_prof_txn1,get_sys_clock() - thd_prof_start);
  thd_prof_start = get_sys_clock();

#if CC_ALG == VLL && MODE == NORMAL_MODE
  vll_man.finishTxn(this);
  //vll_man.restartQFront();
#endif

	if (rc == Abort) {
		for (UInt32 i = 0; i < txn->insert_rows.size(); i ++) {
			row_t * row = txn->insert_rows[i];
			assert(g_part_alloc == false);
#if CC_ALG != HSTORE && CC_ALG != HSTORE_SPEC && CC_ALG != OCC && MODE == NORMAL_MODE
      DEBUG_M("TxnManager::cleanup row->manager free\n");
			mem_allocator.free(row->manager, 0);
#endif
			row->free_row();
      DEBUG_M("TxnManager::cleanup row free\n");
			mem_allocator.free(row, sizeof(row));
		}
    uint64_t t = get_sys_clock() - starttime;
		INC_STATS(get_thd_id(), time_abort, t);
	} else {
    if(IS_LOCAL(get_txn_id())) {
      INC_STATS(get_thd_id(), write_cnt, txn->write_cnt);
      INC_STATS(get_thd_id(), access_cnt, txn->row_cnt);
    } else {
      INC_STATS(get_thd_id(), rem_row_cnt, txn->row_cnt);
    }
  }
  INC_STATS(get_thd_id(),thd_prof_txn2,get_sys_clock() - thd_prof_start);
}

RC TxnManager::get_lock(row_t * row, access_t type) {
  RC rc = row->get_lock(type, this);
  if(rc == WAIT) {
    INC_STATS(get_thd_id(), cflt_cnt, 1);
  }
  return rc;
}

RC TxnManager::get_row(row_t * row, access_t type, row_t *& row_rtn) {
	uint64_t starttime = get_sys_clock();
  uint64_t timespan;
	RC rc = RCOK;
  Access * access = (Access*) mem_allocator.alloc(sizeof(Access));
  //uint64_t row_cnt = txn->row_cnt;
  //assert(txn->accesses.get_count() - 1 == row_cnt);

  this->last_row = row;
  this->last_type = type;

  rc = row->get_row(type, this, access->data);

	if (rc == Abort || rc == WAIT) {
    row_rtn = NULL;
	  timespan = get_sys_clock() - starttime;
	  INC_STATS(get_thd_id(), time_man, timespan);
	  INC_STATS(get_thd_id(), txn_time_man, timespan);
    INC_STATS(get_thd_id(), cflt_cnt, 1);
    //cflt = true;
#if DEBUG_TIMELINE
    printf("CONFLICT %ld %ld\n",get_txn_id(),get_sys_clock());
#endif
		return rc;
	}
	access->type = type;
	access->orig_row = row;
#if ROLL_BACK && (CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE || CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC)
	if (type == WR) {
    //printf("alloc 10 %ld\n",get_txn_id());
    uint64_t part_id = row->get_part_id();
    DEBUG_M("TxnManager::get_row alloc\n")
		access->orig_data = (row_t *) 
			mem_allocator.alloc(sizeof(row_t));
		access->orig_data->init(row->get_table(), part_id, 0);
		access->orig_data->copy(row);

    // ARIES-style physiological logging
#if LOGGING
    //LogRecord * record = logger.createRecord(LRT_UPDATE,L_UPDATE,get_txn_id(),part_id,row->get_table()->get_table_id(),row->get_primary_key());
    LogRecord * record = logger.createRecord(get_txn_id(),L_UPDATE,row->get_table()->get_table_id(),row->get_primary_key());
    if(g_repl_cnt > 0) {
      // FIXME > 1 replica
      msg_queue.enqueue(Message::create_message(record,LOG_MSG),g_node_id + g_node_cnt + g_client_node_cnt); 
    }
    logger.enqueueRecord(record);
#endif

	}
#endif
	++txn->row_cnt;
	if (type == WR)
		++txn->write_cnt;
	timespan = get_sys_clock() - starttime;
	INC_STATS(get_thd_id(), time_man, timespan);
	INC_STATS(get_thd_id(), txn_time_man, timespan);
	row_rtn  = access->data;

  txn->accesses.add(access);

  if(CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC || CC_ALG == CALVIN)
    assert(rc == RCOK);
  return rc;
}

RC TxnManager::get_row_post_wait(row_t *& row_rtn) {
  assert(CC_ALG != HSTORE && CC_ALG != HSTORE_SPEC);
/*  uint64_t timespan = get_sys_clock() - this->wait_starttime;
  if(get_txn_id() % g_node_cnt == g_node_id) {
    INC_STATS(get_thd_id(),time_wait_lock,timespan);
  }
  else {
    INC_STATS(get_thd_id(),time_wait_lock_rem,timespan);
  } */

	uint64_t starttime = get_sys_clock();
  row_t * row = this->last_row;
  access_t type = this->last_type;
  assert(row != NULL);

  row->get_row_post_wait(type,this,txn->accesses[ txn->row_cnt ]->data);

	txn->accesses[txn->row_cnt]->type = type;
	txn->accesses[txn->row_cnt]->orig_row = row;
#if ROLL_BACK && (CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE)
	if (type == WR) {
	  uint64_t part_id = row->get_part_id();
    //printf("alloc 10 %ld\n",get_txn_id());
    DEBUG_M("TxnManager::get_row_post_wait alloc\n");
		txn->accesses[txn->row_cnt]->orig_data = (row_t *) 
			mem_allocator.alloc(sizeof(row_t));
		txn->accesses[txn->row_cnt]->orig_data->init(row->get_table(), part_id, 0);
		txn->accesses[txn->row_cnt]->orig_data->copy(row);
	}
#endif
	++txn->row_cnt;
	if (type == WR)
		++txn->write_cnt;
	uint64_t timespan = get_sys_clock() - starttime;
	INC_STATS(get_thd_id(), time_man, timespan);
	INC_STATS(get_thd_id(), txn_time_man, timespan);
	this->last_row_rtn  = txn->accesses[txn->row_cnt - 1]->data;
	row_rtn  = txn->accesses[txn->row_cnt - 1]->data;
  return RCOK;

}

void TxnManager::insert_row(row_t * row, table_t * table) {
	if (CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC)
		return;
	assert(txn->insert_rows.size() < MAX_ROW_PER_TXN);
  txn->insert_rows.push_back(row);
}

itemid_t *
TxnManager::index_read(INDEX * index, idx_key_t key, int part_id) {
	uint64_t starttime = get_sys_clock();

	itemid_t * item;
	index->index_read(key, item, part_id, get_thd_id());

  uint64_t t = get_sys_clock() - starttime;
  INC_STATS(get_thd_id(), time_index, t);
  //txn_time_idx += t;

	return item;
}

RC TxnManager::validate() {
#if MODE != NORMAL_MODE
  return RCOK;
#endif
  RC rc = RCOK;
  uint64_t starttime = get_sys_clock();
  if(CC_ALG == OCC && rc == RCOK)
    rc = occ_man.validate(this);
  INC_STATS(0,time_validate,get_sys_clock() - starttime);
  return rc;
}

RC TxnManager::finish(bool fin) {
  // Only home node should execute
  uint64_t starttime = get_sys_clock();
#if CC_ALG != CALVIN
  assert(IS_LOCAL(txn->txn_id));
#endif
  if(query->partitions.size() == 1) {
    RC rc = validate();
    return rc;
  }

#if MODE == QRY_ONLY_MODE || MODE == SETUP_MODE
  return RCOK;
#endif

  // Stats start
  bool readonly = false;

  if(query->partitions_touched.is_empty() || 
      ((CC_ALG != HSTORE && CC_ALG != HSTORE_SPEC && CC_ALG != VLL) 
       && query->partitions_touched.size() == 1 && query->partitions.size() > 1)) {
    txn->state = DONE;
    release_locks(RCOK);
    return RCOK;
  }

  if(!fin) {
    readonly = query->readonly();
    if(readonly && CC_ALG != OCC)
      txn->state = FIN;
    else
      txn->state = PREP;
  }
  // Send prepare message to all participating transaction
  assert(rsp_cnt == 0);
  //for (uint64_t i = 0; i < query->part_num; ++i) {
  //  uint64_t part_node_id = GET_NODE_ID(query->part_to_access[i]);
  for (uint64_t i = 0; i < query->partitions_touched.size(); ++i) {
    uint64_t part_node_id = GET_NODE_ID(query->partitions_touched[i]);
    uint64_t part_id = query->partitions_touched[i];
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
    if(part_id == home_part) {
      continue;
    }
#else
    if(part_node_id == g_node_id) {
      continue;
    }
#endif
    // Check if we have already sent this node an RPREPARE message
    bool sent = false;
    for (uint64_t j = 0; j < i; j++) {
      //if (part_node_id == GET_NODE_ID(query->part_to_access[j])) {
      if (part_id == query->partitions_touched[j]) {
        sent = true;
        break;
      }
    }
    if (sent) 
      continue;

    incr_rsp(1);
    if(fin || (readonly && CC_ALG != OCC)) {
      if(GET_NODE_ID(part_id) != g_node_id) {
        //query->remote_finish(query, part_node_id);    
        msg_queue.enqueue(Message::create_message(query,RFIN),part_node_id);
      }     } else {
      if(GET_NODE_ID(part_id) != g_node_id) {
        //query->remote_prepare(query, part_node_id);    
        msg_queue.enqueue(Message::create_message(query,RPREPARE),part_node_id);
      } 
    }
  }

  uint64_t timespan = get_sys_clock() - starttime;
  INC_STATS(get_thd_id(),time_msg_sent,timespan);

  //if(query->rc != Abort && readonly && CC_ALG!=OCC) {
  if(readonly && CC_ALG!=OCC) {
    txn->state = DONE;
    release_locks(RCOK);
    return RCOK;
  }
  if(rsp_cnt >0) 
    return WAIT_REM;
  else
    return RCOK; //finish(query->rc);

}

void
TxnManager::release() {
  ((YCSBQuery*)query)->release();
  mem_allocator.free(query,sizeof(YCSBQuery));
  txn->release();
  mem_allocator.free(txn,sizeof(Transaction));
}

RC
TxnManager::send_remote_reads(BaseQuery * qry) {
  assert(CC_ALG == CALVIN);
  for(uint64_t i = 0; i < query->active_nodes.size(); i++) {
      msg_queue.enqueue(Message::create_message(query,RFWD),i);
  }
  return RCOK;

}

RC
TxnManager::calvin_finish(BaseQuery * qry) {
  assert(CC_ALG == CALVIN);
  for(uint64_t i = 0; i < query->active_nodes.size(); i++) {
    msg_queue.enqueue(Message::create_message(query,RFIN),i);
  }
  return RCOK;
}

void TxnManager::release_locks(RC rc) {
  // FIXME: Handle aborts?
	uint64_t starttime = get_sys_clock();

  cleanup(rc);

	uint64_t timespan = (get_sys_clock() - starttime);
	INC_STATS(get_thd_id(), time_cleanup,  timespan);
}
