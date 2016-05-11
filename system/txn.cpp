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
#include "msg_queue.h"
#include "pool.h"
#include "message.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "array.h"
#include "maat.h"


void TxnStats::init() {
  starttime=0;
  wait_starttime=0;
  total_process_time=0;
  process_time=0;
  total_local_wait_time=0;
  local_wait_time=0;
  total_remote_wait_time=0;
  remote_wait_time=0;
  total_twopc_time=0;
  twopc_time=0;
}

void TxnStats::reset() {
  wait_starttime=0;
  total_process_time += process_time;
  process_time = 0;
  total_local_wait_time += local_wait_time;
  local_wait_time = 0;
  total_remote_wait_time += remote_wait_time;
  remote_wait_time = 0;
  total_twopc_time += twopc_time;
  twopc_time = 0;
}

void TxnStats::commit_stats(uint64_t thd_id) {
  total_process_time += process_time;
  total_local_wait_time += local_wait_time;
  total_remote_wait_time += remote_wait_time;
  total_twopc_time += twopc_time;
  assert(total_process_time >= process_time);
  INC_STATS(thd_id,txn_total_process_time,total_process_time);
  INC_STATS(thd_id,txn_process_time,process_time);
  INC_STATS(thd_id,txn_total_local_wait_time,total_local_wait_time);
  INC_STATS(thd_id,txn_local_wait_time,local_wait_time);
  INC_STATS(thd_id,txn_total_remote_wait_time,total_remote_wait_time);
  INC_STATS(thd_id,txn_remote_wait_time,remote_wait_time);
  INC_STATS(thd_id,txn_total_twopc_time,total_twopc_time);
  INC_STATS(thd_id,txn_twopc_time,twopc_time);
}


void Transaction::init() {
  timestamp = UINT64_MAX;
  start_timestamp = UINT64_MAX;
  end_timestamp = UINT64_MAX;
  txn_id = UINT64_MAX;
  batch_id = UINT64_MAX;
  DEBUG_M("Transaction::init array insert_rows\n");
  insert_rows.init(g_max_items_per_txn + 10); // FIXME: arbitrary number
  DEBUG_M("Transaction::reset array accesses\n");
  accesses.init(MAX_ROW_PER_TXN);  

  reset(0);
}

void Transaction::reset(uint64_t thd_id) {
  uint64_t prof_starttime = get_sys_clock();
  release_accesses(thd_id);
  accesses.clear();
  INC_STATS(thd_id,mtx[6],get_sys_clock()-prof_starttime);
  release_inserts(thd_id);
  insert_rows.clear();  
  write_cnt = 0;
  row_cnt = 0;
  twopc_state = START;
  rc = RCOK;
  INC_STATS(thd_id,mtx[5],get_sys_clock()-prof_starttime);
}

void Transaction::release_accesses(uint64_t thd_id) {
  for(uint64_t i = 0; i < accesses.size(); i++) {
    access_pool.put(thd_id,accesses[i]);
  }
}

void Transaction::release_inserts(uint64_t thd_id) {
  for(uint64_t i = 0; i < insert_rows.size(); i++) {
    DEBUG_M("Transaction::release insert_rows free\n")
    row_pool.put(thd_id,insert_rows[i]);
  }
}

void Transaction::release(uint64_t thd_id) {
  //uint64_t prof_starttime = get_sys_clock();
  DEBUG("Transaction release\n");
  release_accesses(thd_id);
  DEBUG_M("Transaction::release array accesses free\n")
  accesses.release();
  release_inserts(thd_id);
  DEBUG_M("Transaction::release array insert_rows free\n")
  insert_rows.release();
  //INC_STATS(thd_id,mtx[6],get_sys_clock()-prof_starttime);
}

void TxnManager::init(uint64_t thd_id, Workload * h_wl) {
  uint64_t prof_starttime = get_sys_clock();
  if(!txn)  {
    DEBUG_M("Transaction alloc\n");
    txn_pool.get(thd_id,txn);

  }
  INC_STATS(get_thd_id(),mtx[15],get_sys_clock()-prof_starttime);
  prof_starttime = get_sys_clock();
  //txn->init();
  if(!query) {
    DEBUG_M("TxnManager::init Query alloc\n");
    qry_pool.get(thd_id,query);
  }
  INC_STATS(get_thd_id(),mtx[16],get_sys_clock()-prof_starttime);
  //query->init();
  //reset();
  sem_init(&rsp_mutex, 0, 1);
  return_id = UINT64_MAX;

	this->h_wl = h_wl;
#if CC_ALG == MAAT
  uncommitted_writes = new std::set<uint64_t>();
  uncommitted_writes_y = new std::set<uint64_t>();
  uncommitted_reads = new std::set<uint64_t>();
#endif
  
  ready = true;

  txn_stats.init();
}

// reset after abort
void TxnManager::reset() {
	lock_ready = false;
  lock_ready_cnt = 0;
  locking_done = true;
	ready_part = 0;
  rsp_cnt = 0;
  aborted = false;
  return_id = UINT64_MAX;

  ready = true;

  // MaaT
  greatest_write_timestamp = 0;
  greatest_read_timestamp = 0;
  commit_timestamp = 0;
#if CC_ALG == MAAT
  uncommitted_writes->clear();
  uncommitted_writes_y->clear();
  uncommitted_reads->clear();
#endif

  assert(txn);
  assert(query);
  txn->reset(get_thd_id());

  // Stats
  txn_stats.reset();

}

void
TxnManager::release() {
  uint64_t prof_starttime = get_sys_clock();
  qry_pool.put(get_thd_id(),query);
  INC_STATS(get_thd_id(),mtx[0],get_sys_clock()-prof_starttime);
  query = NULL;
  prof_starttime = get_sys_clock();
  txn_pool.put(get_thd_id(),txn);
  INC_STATS(get_thd_id(),mtx[1],get_sys_clock()-prof_starttime);
  txn = NULL;

#if CC_ALG == MAAT
  delete uncommitted_writes;
  delete uncommitted_writes_y;
  delete uncommitted_reads;
#endif
}

void TxnManager::reset_query() {
#if WORKLOAD == YCSB
  ((YCSBQuery*)query)->reset();
#elif WORKLOAD == TPCC
  ((TPCCQuery*)query)->reset();
#endif
}

RC TxnManager::commit() {
  DEBUG("Commit %ld\n",get_txn_id());
  release_locks(RCOK);
#if CC_ALG == MAAT
  time_table.release(get_thd_id(),get_txn_id());
#endif
  commit_stats();
#if LOGGING
    LogRecord * record = logger.createRecord(get_txn_id(),L_NOTIFY,0,0);
    if(g_repl_cnt > 0) {
      msg_queue.enqueue(get_thd_id(),Message::create_message(record,LOG_MSG),g_node_id + g_node_cnt + g_client_node_cnt); 
    }
  logger.enqueueRecord(record);
  return WAIT;
#endif
  return Commit;
}

RC TxnManager::abort() {
  if(aborted)
    return Abort;
  DEBUG("Abort %ld\n",get_txn_id());
  txn->rc = Abort;
  INC_STATS(get_thd_id(),total_txn_abort_cnt,1);
  if(IS_LOCAL(get_txn_id())) {
    INC_STATS(get_thd_id(), local_txn_abort_cnt, 1);
  }
  aborted = true;
  release_locks(Abort);
#if CC_ALG == MAAT
  //assert(time_table.get_state(get_txn_id()) == MAAT_ABORTED);
  time_table.release(get_thd_id(),get_txn_id());
#endif
  //commit_stats();
  return Abort;
}

RC TxnManager::start_abort() {
  txn->rc = Abort;
  DEBUG("%ld start_abort\n",get_txn_id());
  if(query->partitions_touched.size() > 1) {
    send_finish_messages();
    abort();
    return Abort;
  } 
  return abort();
}

RC TxnManager::start_commit() {
  RC rc = RCOK;
  DEBUG("%ld start_commit RO?%d\n",get_txn_id(),query->readonly());
  if(is_multi_part()) {
    if(!query->readonly() || CC_ALG == OCC || CC_ALG == MAAT) {
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
    else
      start_abort();
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
    msg_queue.enqueue(get_thd_id(),Message::create_message(this,RPREPARE),GET_NODE_ID(query->partitions_touched[i]));
  }
}

void TxnManager::send_finish_messages() {
  rsp_cnt = query->partitions_touched.size() - 1;
  DEBUG("%ld Send FINISH messages to %d\n",get_txn_id(),rsp_cnt);
  for(uint64_t i = 0; i < query->partitions_touched.size(); i++) {
    if(GET_NODE_ID(query->partitions_touched[i]) == g_node_id) {
      continue;
    }
    msg_queue.enqueue(get_thd_id(),Message::create_message(this,RFIN),GET_NODE_ID(query->partitions_touched[i]));
  }
}

int TxnManager::received_response(RC rc) {
  assert(txn->rc == RCOK || txn->rc == Abort);
  if(txn->rc == RCOK)
    txn->rc = rc;
  --rsp_cnt;
  return rsp_cnt;
}

bool TxnManager::waiting_for_response() {
  return rsp_cnt > 0;
}

bool TxnManager::is_multi_part() {
  return query->partitions.size() > 1;
}

void TxnManager::commit_stats() {
  INC_STATS(get_thd_id(),total_txn_commit_cnt,1);
  if(!IS_LOCAL(get_txn_id())) {
    INC_STATS(get_thd_id(),remote_txn_commit_cnt,1);
    return;
  }
  uint64_t timespan = get_sys_clock() - txn_stats.starttime; 

  INC_STATS(get_thd_id(),txn_cnt,1);
  INC_STATS(get_thd_id(),local_txn_commit_cnt,1);
  INC_STATS(get_thd_id(), txn_run_time, timespan);
  if(query->partitions.size() > 1) {
    INC_STATS(get_thd_id(),multi_part_txn_cnt,1);
    INC_STATS(get_thd_id(),multi_part_txn_run_time,timespan);
  } else {
    INC_STATS(get_thd_id(),single_part_txn_cnt,1);
    INC_STATS(get_thd_id(),single_part_txn_run_time,timespan);
  }
  /*if(cflt) {
    INC_STATS(get_thd_id(),cflt_cnt_txn,1);
  }*/

  assert(query->partitions_touched.size() > 0);
  INC_STATS(get_thd_id(),part_cnt[query->partitions_touched.size()-1],1);
  for(uint64_t i = 0 ; i < query->partitions.size(); i++) {
    INC_STATS(get_thd_id(),part_acc[query->partitions[i]],1);
  }
  txn_stats.commit_stats(get_thd_id());
}

void TxnManager::register_thread(Thread * h_thd) {
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
  if(h_thd)
    return h_thd->get_thd_id();
  else
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
  assert((WORKLOAD == YCSB && row_cnt <= g_req_per_query) || (WORKLOAD == TPCC && row_cnt <= g_max_items_per_txn*2 + 3));
  DEBUG("Cleanup %ld %ld\n",get_txn_id(),row_cnt);
	for (int rid = row_cnt - 1; rid >= 0; rid --) {
		row_t * orig_r = txn->accesses[rid]->orig_row;
		access_t type = txn->accesses[rid]->type;
		if (type == WR && rc == Abort && CC_ALG != MAAT)
			type = XP;

		if (ROLL_BACK && type == XP &&
					(CC_ALG == DL_DETECT || 
					CC_ALG == NO_WAIT || 
					CC_ALG == WAIT_DIE ||
          CC_ALG == HSTORE ||
          CC_ALG == HSTORE_SPEC 
          )) 
		{
			orig_r->return_row(rc,type, this, txn->accesses[rid]->orig_data);
		} else {
			orig_r->return_row(rc,type, this, txn->accesses[rid]->data);
		}

#if ROLL_BACK && (CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE || CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC)
		if (type == WR) {
      //printf("free 10 %ld\n",get_txn_id());
			txn->accesses[rid]->orig_data->free_row();
      DEBUG_M("TxnManager::cleanup row_t free\n");
      row_pool.put(get_thd_id(),txn->accesses[rid]->orig_data);
		}
#endif
		txn->accesses[rid]->data = NULL;
	}

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
      row_pool.put(get_thd_id(),row);
		}
		INC_STATS(get_thd_id(), abort_time, get_sys_clock() - starttime);
	} 
}

RC TxnManager::get_lock(row_t * row, access_t type) {
  RC rc = row->get_lock(type, this);
  if(rc == WAIT) {
    INC_STATS(get_thd_id(), txn_wait_cnt, 1);
  }
  return rc;
}

RC TxnManager::get_row(row_t * row, access_t type, row_t *& row_rtn) {
	uint64_t starttime = get_sys_clock();
  uint64_t timespan;
	RC rc = RCOK;
  DEBUG_M("TxnManager::get_row access alloc\n");
  Access * access;
  access_pool.get(get_thd_id(),access);
  //uint64_t row_cnt = txn->row_cnt;
  //assert(txn->accesses.get_count() - 1 == row_cnt);

  this->last_row = row;
  this->last_type = type;

  rc = row->get_row(type, this, access->data);

	if (rc == Abort || rc == WAIT) {
    row_rtn = NULL;
    DEBUG_M("TxnManager::get_row(abort) access free\n");
    access_pool.put(get_thd_id(),access);
	  timespan = get_sys_clock() - starttime;
	  INC_STATS(get_thd_id(), txn_manager_time, timespan);
    INC_STATS(get_thd_id(), txn_conflict_cnt, 1);
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
    DEBUG_M("TxnManager::get_row row_t alloc\n")
    row_pool.get(get_thd_id(),access->orig_data);
		access->orig_data->init(row->get_table(), part_id, 0);
		access->orig_data->copy(row);

    // ARIES-style physiological logging
#if LOGGING
    //LogRecord * record = logger.createRecord(LRT_UPDATE,L_UPDATE,get_txn_id(),part_id,row->get_table()->get_table_id(),row->get_primary_key());
    LogRecord * record = logger.createRecord(get_txn_id(),L_UPDATE,row->get_table()->get_table_id(),row->get_primary_key());
    if(g_repl_cnt > 0) {
      // FIXME > 1 replica
      msg_queue.enqueue(get_thd_id(),Message::create_message(record,LOG_MSG),g_node_id + g_node_cnt + g_client_node_cnt); 
    }
    logger.enqueueRecord(record);
#endif

	}
#endif

	++txn->row_cnt;
	if (type == WR)
		++txn->write_cnt;

  txn->accesses.add(access);

	timespan = get_sys_clock() - starttime;
	INC_STATS(get_thd_id(), txn_manager_time, timespan);
	row_rtn  = access->data;


  if(CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC || CC_ALG == CALVIN)
    assert(rc == RCOK);
  return rc;
}

RC TxnManager::get_row_post_wait(row_t *& row_rtn) {
  assert(CC_ALG != HSTORE && CC_ALG != HSTORE_SPEC);

	uint64_t starttime = get_sys_clock();
  row_t * row = this->last_row;
  access_t type = this->last_type;
  assert(row != NULL);
  DEBUG_M("TxnManager::get_row_post_wait access alloc\n")
  Access * access;
  access_pool.get(get_thd_id(),access);

  row->get_row_post_wait(type,this,access->data);

	access->type = type;
	access->orig_row = row;
#if ROLL_BACK && (CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE)
	if (type == WR) {
	  uint64_t part_id = row->get_part_id();
    //printf("alloc 10 %ld\n",get_txn_id());
    DEBUG_M("TxnManager::get_row_post_wait row_t alloc\n")
    row_pool.get(get_thd_id(),access->orig_data);
		access->orig_data->init(row->get_table(), part_id, 0);
		access->orig_data->copy(row);
	}
#endif

	++txn->row_cnt;
	if (type == WR)
		++txn->write_cnt;


  txn->accesses.add(access);
	uint64_t timespan = get_sys_clock() - starttime;
	INC_STATS(get_thd_id(), txn_manager_time, timespan);
	this->last_row_rtn  = access->data;
	row_rtn  = access->data;
  return RCOK;

}

void TxnManager::insert_row(row_t * row, table_t * table) {
	if (CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC)
		return;
	assert(txn->insert_rows.size() < MAX_ROW_PER_TXN);
  txn->insert_rows.add(row);
}

itemid_t *
TxnManager::index_read(INDEX * index, idx_key_t key, int part_id) {
	uint64_t starttime = get_sys_clock();

	itemid_t * item;
	index->index_read(key, item, part_id, get_thd_id());

  uint64_t t = get_sys_clock() - starttime;
  INC_STATS(get_thd_id(), txn_index_time, t);
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
  if(CC_ALG == MAAT && rc == RCOK) {
    rc = maat_man.validate(this);
    // Note: home node must be last to validate
    if(IS_LOCAL(get_txn_id()) && rc == RCOK) {
      rc = maat_man.find_bound(this);
    }
  }
  INC_STATS(get_thd_id(),txn_validate_time,get_sys_clock() - starttime);
  return rc;
}

RC TxnManager::finish(bool fin) {
  // Only home node should execute
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
      ((CC_ALG != HSTORE && CC_ALG != HSTORE_SPEC) 
       && query->partitions_touched.size() == 1 && query->partitions.size() > 1)) {
    txn->twopc_state = DONE;
    release_locks(RCOK);
    return RCOK;
  }

  if(!fin) {
    readonly = query->readonly();
    if(readonly && CC_ALG != OCC)
      txn->twopc_state = FIN;
    else
      txn->twopc_state = PREP;
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
        msg_queue.enqueue(get_thd_id(),Message::create_message(query,RFIN),part_node_id);
      }     } else {
      if(GET_NODE_ID(part_id) != g_node_id) {
        //query->remote_prepare(query, part_node_id);    
        msg_queue.enqueue(get_thd_id(),Message::create_message(query,RPREPARE),part_node_id);
      } 
    }
  }


  //if(query->rc != Abort && readonly && CC_ALG!=OCC) {
  if(readonly && CC_ALG!=OCC) {
    txn->twopc_state = DONE;
    release_locks(RCOK);
    return RCOK;
  }
  if(rsp_cnt >0) 
    return WAIT_REM;
  else
    return RCOK; //finish(query->rc);

}
RC
TxnManager::send_remote_reads(BaseQuery * qry) {
  assert(CC_ALG == CALVIN);
  for(uint64_t i = 0; i < query->active_nodes.size(); i++) {
      msg_queue.enqueue(get_thd_id(),Message::create_message(query,RFWD),i);
  }
  return RCOK;

}

bool TxnManager::calvin_exec_phase_done() {
  bool ready =  (phase == CALVIN_DONE) && (get_rc() == RCOK);
  return ready;
}

bool TxnManager::calvin_collect_phase_done() {
  bool ready =  (phase == CALVIN_COLLECT_RD) && (get_rsp_cnt() == query->participant_nodes.size()-1);
  return ready;
}

RC
TxnManager::calvin_finish() {
  assert(CC_ALG == CALVIN);
  for(uint64_t i = 0; i < query->active_nodes.size(); i++) {
    msg_queue.enqueue(get_thd_id(),Message::create_message(query,RFIN),i);
  }
  return RCOK;
}

void TxnManager::release_locks(RC rc) {
  // FIXME: Handle aborts?
	uint64_t starttime = get_sys_clock();

  cleanup(rc);

	uint64_t timespan = (get_sys_clock() - starttime);
	INC_STATS(get_thd_id(), txn_cleanup_time,  timespan);
}
