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
#include "remote_query.h"
#include "plock.h"
#include "vll.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "msg_queue.h"
#include "txn_pool.h"

void txn_man::init(workload * h_wl) {
	this->h_wl = h_wl;
	pthread_mutex_init(&txn_lock, NULL);
	lock_ready = false;
	ready_part = 0;
	row_cnt = 0;
	wr_cnt = 0;
	insert_cnt = 0;
  ack_cnt = 0;
  rsp_cnt = 0;
  state = START;
  cc_wait_abrt_cnt = 0;
  cc_wait_abrt_time = 0;
  cc_hold_abrt_time = 0;
  cflt = false;
  clear();

  sem_init(&rsp_mutex, 0, 1);


  txn_time_idx = 0;
  txn_time_man = 0;
  txn_time_ts = 0;
  txn_time_abrt = 0;
  txn_time_clean = 0;
  txn_time_copy = 0;
  txn_time_wait = 0;
  txn_time_twopc = 0;
  txn_time_q_abrt = 0;
  txn_time_q_work = 0;
  txn_time_net = 0;
  txn_time_misc = 0;

	//accesses = (Access **) mem_allocator.alloc(sizeof(Access **), 0);
	accesses = new Access * [MAX_ROW_PER_TXN];
	for (int i = 0; i < MAX_ROW_PER_TXN; i++)
		accesses[i] = NULL;
	num_accesses_alloc = 0;
}

void txn_man::reset() {
	lock_ready = false;
	ready_part = 0;
	row_cnt = 0;
	wr_cnt = 0;
	insert_cnt = 0;
  ack_cnt = 0;
  rsp_cnt = 0;
  state = START;
  cc_wait_abrt_cnt = 0;
  cc_wait_abrt_time = 0;
  cc_hold_abrt_time = 0;
  cflt = false;
  clear();
 txn_time_idx = 0;
  txn_time_man = 0;
  txn_time_ts = 0;
  txn_time_abrt = 0;
  txn_time_clean = 0;
  txn_time_copy = 0;
  txn_time_wait = 0;
  txn_time_twopc = 0;
  txn_time_q_abrt = 0;
  txn_time_q_work = 0;
  txn_time_net = 0;
  txn_time_misc = 0;

	for (int i = 0; i < MAX_ROW_PER_TXN; i++)
		accesses[i] = NULL;
	num_accesses_alloc = 0;


}

void txn_man::clear() {
  cc_wait_cnt = 0;
  cc_wait_time = 0;
  cc_hold_time = 0;
  ready_ulk = 0;
  ready_part = 0;
  row_cnt = 0;
  state = START;
  rc = RCOK;
}

void txn_man::update_stats() {
  INC_STATS(get_thd_id(), cc_wait_cnt, cc_wait_cnt);
  INC_STATS(get_thd_id(), cc_wait_time, cc_wait_time);
  INC_STATS(get_thd_id(), cc_hold_time, cc_hold_time);
  INC_STATS(get_thd_id(), cc_wait_abrt_cnt, cc_wait_abrt_cnt);
  INC_STATS(get_thd_id(), cc_wait_abrt_time, cc_wait_abrt_time);
  INC_STATS(get_thd_id(), cc_hold_abrt_time, cc_hold_abrt_time);

  txn_time_misc = txn_time_misc - (txn_time_idx + txn_time_man + txn_time_ts + txn_time_abrt + txn_time_clean + txn_time_copy + txn_time_wait + txn_time_twopc + txn_time_q_abrt + txn_time_q_work + txn_time_net); 
  INC_STATS(get_thd_id(), txn_time_idx, this->txn_time_idx);
  INC_STATS(get_thd_id(), txn_time_man, this->txn_time_man);
  INC_STATS(get_thd_id(), txn_time_ts, this->txn_time_ts);
  INC_STATS(get_thd_id(), txn_time_abrt, this->txn_time_abrt);
  INC_STATS(get_thd_id(), txn_time_clean, this->txn_time_clean);
  INC_STATS(get_thd_id(), txn_time_copy, this->txn_time_copy);
  INC_STATS(get_thd_id(), txn_time_wait, this->txn_time_wait);
  INC_STATS(get_thd_id(), txn_time_twopc, this->txn_time_twopc);
  INC_STATS(get_thd_id(), txn_time_q_abrt, this->txn_time_q_abrt);
  INC_STATS(get_thd_id(), txn_time_q_work, this->txn_time_q_work);
  INC_STATS(get_thd_id(), txn_time_net, this->txn_time_net);
  INC_STATS(get_thd_id(), txn_time_misc, this->txn_time_misc);
  // FIXME: Only for debugging
#if DEBUG_BREAKDOWN
  double txn_time_total = txn_time_misc + txn_time_idx + txn_time_man + txn_time_ts + txn_time_abrt + txn_time_clean + txn_time_copy + txn_time_wait + txn_time_twopc + txn_time_q_abrt + txn_time_q_work + txn_time_net;
  printf("ID %ld: abrt_cnt: %ld"
      ",idx: %f"
      ",man: %f"
      ",ts: %f"
      ",abrt: %f"
      ",clean: %f"
      ",copy: %f"
      ",wait: %f"
      ",twopc: %f"
      ",q_abrt: %f"
      ",q_work: %f"
      ",net: %f"
      ",misc: %f\n"
      ",total: %f\n"
      ,txn_id
      ,abort_cnt
      ,txn_time_idx / BILLION
      ,txn_time_man / BILLION
      ,txn_time_ts / BILLION
      ,txn_time_abrt / BILLION
      ,txn_time_clean / BILLION
      ,txn_time_copy / BILLION
      ,txn_time_wait / BILLION
      ,txn_time_twopc / BILLION
      ,txn_time_q_abrt / BILLION
      ,txn_time_q_work / BILLION
      ,txn_time_net / BILLION
      ,txn_time_misc / BILLION
      ,txn_time_total / BILLION
      );
#endif

}

void txn_man::register_thd(thread_t * h_thd) {
  this->h_thd = h_thd;
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
  this->active_part = GET_PART_ID_FROM_IDX(get_thd_id());
#endif
}

void txn_man::set_txn_id(txnid_t txn_id) {
	this->txn_id = txn_id;
}

txnid_t txn_man::get_txn_id() {
	return this->txn_id;
}

workload * txn_man::get_wl() {
	return h_wl;
}

uint64_t txn_man::get_thd_id() {
	return h_thd->get_thd_id();
}

base_query * txn_man::get_query() {
	return myquery;
}
void txn_man::set_query(base_query * qry) {
	myquery = qry;
}

uint64_t txn_man::get_node_id() {
	return h_thd->get_node_id();
}

void txn_man::set_pid(uint64_t pid) {
  this->pid = pid;
}

uint64_t txn_man::get_pid() {
  return pid;
}

void txn_man::set_ts(ts_t timestamp) {
	this->timestamp = timestamp;
}

ts_t txn_man::get_ts() {
	return this->timestamp;
}

void txn_man::set_start_ts(uint64_t start_ts) {
	this->start_ts = start_ts;
}

ts_t txn_man::get_start_ts() {
	return this->start_ts;
}

uint64_t txn_man::get_rsp_cnt() {
  return this->rsp_cnt;
}

uint64_t txn_man::incr_rsp(int i) {
  //ATOM_ADD(this->rsp_cnt,i);
  uint64_t result;
  sem_wait(&rsp_mutex);
  result = ++this->rsp_cnt;
  sem_post(&rsp_mutex);
  return result;
}

uint64_t txn_man::decr_rsp(int i) {
  //ATOM_SUB(this->rsp_cnt,i);
  uint64_t result;
  sem_wait(&rsp_mutex);
  result = --this->rsp_cnt;
  sem_post(&rsp_mutex);
  return result;
}

void txn_man::cleanup(RC rc) {
#if CC_ALG == OCC
  occ_man.finish(rc,this);
#endif

	ts_t starttime = get_sys_clock();
  uint64_t thd_prof_start = starttime;
	for (int rid = row_cnt - 1; rid >= 0; rid --) {
		row_t * orig_r = accesses[rid]->orig_row;
		access_t type = accesses[rid]->type;
		if (type == WR && rc == Abort)
			type = XP;

		if (ROLL_BACK && type == XP &&
					(CC_ALG == DL_DETECT || 
					CC_ALG == NO_WAIT || 
					CC_ALG == WAIT_DIE ||
          CC_ALG == HSTORE ||
          CC_ALG == HSTORE_SPEC 
          )) 
		{
			orig_r->return_row(type, this, accesses[rid]->orig_data);
		} else {
			orig_r->return_row(type, this, accesses[rid]->data);
		}
#if ROLL_BACK && (CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE || CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC)
		if (type == WR) {
			accesses[rid]->orig_data->free_row();
			mem_allocator.free(accesses[rid]->orig_data, sizeof(row_t));
		}
#endif
		accesses[rid]->data = NULL;
	}

  INC_STATS(get_thd_id(),thd_prof_txn1,get_sys_clock() - thd_prof_start);
  thd_prof_start = get_sys_clock();

#if CC_ALG == VLL
  vll_man.finishTxn(this);
  //vll_man.restartQFront();
#endif

	if (rc == Abort) {
		for (UInt32 i = 0; i < insert_cnt; i ++) {
			row_t * row = insert_rows[i];
			assert(g_part_alloc == false);
#if CC_ALG != HSTORE && CC_ALG != HSTORE_SPEC && CC_ALG != OCC
			mem_allocator.free(row->manager, 0);
#endif
			row->free_row();
			mem_allocator.free(row, sizeof(row));
		}
    uint64_t t = get_sys_clock() - starttime;
		INC_STATS(get_thd_id(), time_abort, t);
    txn_time_abrt += t;
    last_time_abrt = t;
	}
	row_cnt = 0;
	wr_cnt = 0;
	insert_cnt = 0;
  rsp_cnt = 0;
  //printf("Cleanup: %ld\n",get_txn_id());
#if CC_ALG == DL_DETECT
	dl_detector.clear_dep(get_txn_id());
#endif
  INC_STATS(get_thd_id(),thd_prof_txn2,get_sys_clock() - thd_prof_start);
}

RC txn_man::get_lock(row_t * row, access_t type) {
  rc = row->get_lock(type, this);
  return rc;
}

RC txn_man::get_row(row_t * row, access_t type, row_t *& row_rtn) {
	uint64_t starttime = get_sys_clock();
  uint64_t timespan;
	RC rc = RCOK;
//	assert(row_cnt < MAX_ROW_PER_TXN);
	uint64_t part_id = row->get_part_id();
	if (accesses[row_cnt] == NULL) {
		access_pool.get(accesses[row_cnt]);
		//accesses[row_cnt] = (Access *) 
	//		mem_allocator.alloc(sizeof(Access), part_id);
		num_accesses_alloc ++;
	}

  this->last_row = row;
  this->last_type = type;

  if ( CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC) {
    accesses[ row_cnt ]->data = row;
    rc = RCOK;
  }
  else {
	  rc = row->get_row(type, this, accesses[ row_cnt ]->data);
  }

	if (rc == Abort || rc == WAIT) {
    row_rtn = NULL;
	  timespan = get_sys_clock() - starttime;
	  INC_STATS(get_thd_id(), time_man, timespan);
	  INC_STATS(get_thd_id(), txn_time_man, timespan);
    INC_STATS(get_thd_id(), cflt_cnt, 1);
    cflt = true;
#if DEBUG_TIMELINE
    printf("CONFLICT %ld %ld\n",get_txn_id(),get_sys_clock());
#endif
		return rc;
	}
	accesses[row_cnt]->type = type;
	accesses[row_cnt]->orig_row = row;
#if ROLL_BACK && (CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE || CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC)
	if (type == WR) {
		accesses[row_cnt]->orig_data = (row_t *) 
			mem_allocator.alloc(sizeof(row_t), part_id);
		accesses[row_cnt]->orig_data->init(row->get_table(), part_id, 0);
		accesses[row_cnt]->orig_data->copy(row);
	}
#endif
	row_cnt ++;
	if (type == WR)
		wr_cnt ++;
	timespan = get_sys_clock() - starttime;
	INC_STATS(get_thd_id(), time_man, timespan);
	INC_STATS(get_thd_id(), txn_time_man, timespan);
	row_rtn  = accesses[row_cnt - 1]->data;
  if(CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC || CC_ALG == CALVIN)
    assert(rc == RCOK);
  return rc;
}

RC txn_man::get_row_post_wait(row_t *& row_rtn) {
  assert(CC_ALG != HSTORE && CC_ALG != HSTORE_SPEC);
  uint64_t timespan = get_sys_clock() - this->wait_starttime;
  if(get_txn_id() % g_node_cnt == g_node_id) {
    INC_STATS(get_thd_id(),time_wait_lock,timespan);
  }
  else {
    INC_STATS(get_thd_id(),time_wait_lock_rem,timespan);
  }

	uint64_t starttime = get_sys_clock();
  row_t * row = this->last_row;
  access_t type = this->last_type;
  assert(row != NULL);

  row->get_row_post_wait(type,this,accesses[ row_cnt ]->data);

	accesses[row_cnt]->type = type;
	accesses[row_cnt]->orig_row = row;
#if ROLL_BACK && (CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE)
	if (type == WR) {
	  uint64_t part_id = row->get_part_id();
		accesses[row_cnt]->orig_data = (row_t *) 
			mem_allocator.alloc(sizeof(row_t), part_id);
		accesses[row_cnt]->orig_data->init(row->get_table(), part_id, 0);
		accesses[row_cnt]->orig_data->copy(row);
	}
#endif
	row_cnt ++;
	if (type == WR)
		wr_cnt ++;
	timespan = get_sys_clock() - starttime;
	INC_STATS(get_thd_id(), time_man, timespan);
	INC_STATS(get_thd_id(), txn_time_man, timespan);
	this->last_row_rtn  = accesses[row_cnt - 1]->data;
	row_rtn  = accesses[row_cnt - 1]->data;
  return RCOK;

}

void txn_man::insert_row(row_t * row, table_t * table) {
	if (CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC)
		return;
	assert(insert_cnt < MAX_ROW_PER_TXN);
	insert_rows[insert_cnt ++] = row;
}

itemid_t *
txn_man::index_read(INDEX * index, idx_key_t key, int part_id) {
	uint64_t starttime = get_sys_clock();

	itemid_t * item;
	index->index_read(key, item, part_id, get_thd_id());

  uint64_t t = get_sys_clock() - starttime;
  INC_STATS(get_thd_id(), time_index, t);
  txn_time_idx += t;

	return item;
}

RC txn_man::validate() {
  assert(h_thd->_node_id < g_node_cnt);
  if(rc == WAIT && !this->spec) {
    rc = Abort;
    return rc;
  }
  uint64_t starttime = get_sys_clock();
  assert(rc == Abort || rc == RCOK || this->spec);
  if(CC_ALG == OCC && rc == RCOK)
    rc = occ_man.validate(this);
  else if(CC_ALG == HSTORE_SPEC && this->spec)
    rc = spec_man.validate(this);
  INC_STATS(0,time_validate,get_sys_clock() - starttime);
  return rc;
}

RC txn_man::finish_local(RC rc, uint64_t * parts, uint64_t part_cnt) {
  assert(h_thd->_node_id < g_node_cnt);
	assert(CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC); 
	part_lock_man.rem_unlock(parts, part_cnt, this);
  return RCOK;
}

RC txn_man::finish(RC rc, uint64_t * parts, uint64_t part_cnt) {
#if MODE == QRY_ONLY_MODE || MODE == SETUP_MODE
  return RCOK;
#endif
  assert(h_thd->_node_id < g_node_cnt);
	uint64_t starttime = get_sys_clock();
    if(txn_twopc_starttime > 0)
      txn_time_twopc = starttime - txn_twopc_starttime;
	if (CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC) {
		part_lock_man.rem_unlock(parts, part_cnt, this);
  }
  //if(CC_ALG != HSTORE)
  cleanup(rc);

  // Stats
	uint64_t timespan = (get_sys_clock() - starttime) - last_time_abrt;
  last_time_abrt = 0;
	INC_STATS(get_thd_id(), time_cleanup,  timespan);
	txn_time_clean += timespan;
	return rc;
}

RC txn_man::rem_fin_txn(base_query * query) {
  assert(query->rc == Abort || query->rc == RCOK);
  return finish(query->rc,query->parts,query->part_cnt);
}

RC txn_man::loc_fin_txn(base_query * query) {
  assert(query->rc == Abort || query->rc == RCOK);
  return finish_local(query->rc,query->parts,query->part_cnt);
}


RC txn_man::finish(base_query * query, bool fin) {
  // Only home node should execute
#if CC_ALG != CALVIN
  assert(query->txn_id % g_node_cnt == g_node_id);
#endif
  if(query->part_num == 1) {
    if(CC_ALG == HSTORE_SPEC && txn_table.spec_mode && this->spec) {
      this->state = PREP;
    }
    else if (CC_ALG == HSTORE_SPEC && !txn_table.spec_mode && this->spec) {
      return Abort;
    }
    RC rc = validate();
    if(rc != RCOK)
      return rc;
    return query->rc;
  }

  /*
#if MODE==QRY
  return RCOK;
#endif
*/
#if MODE == QRY_ONLY_MODE || MODE == SETUP_MODE
  return RCOK;
#endif
  //uint64_t starttime = get_sys_clock();

  // Stats start
  txn_twopc_starttime = get_sys_clock();


  if(!fin) {
    this->state = PREP;
  }
  if(query->part_touched_cnt == 0 || 
      ((CC_ALG != HSTORE && CC_ALG != HSTORE_SPEC && CC_ALG != VLL) 
       && query->part_touched_cnt == 1 && query->part_num > 1)) {
    assert(query->rc == Abort);
    this->state = DONE;
		uint64_t part_arr[1];
		part_arr[0] = query->part_to_access[0];
	  finish(query->rc,part_arr,1);
    return RCOK;
  }

  // Send prepare message to all participating transaction
  assert(rsp_cnt == 0);
  //for (uint64_t i = 0; i < query->part_num; ++i) {
  //  uint64_t part_node_id = GET_NODE_ID(query->part_to_access[i]);
  for (uint64_t i = 0; i < query->part_touched_cnt; ++i) {
    uint64_t part_node_id = GET_NODE_ID(query->part_touched[i]);
    uint64_t part_id = query->part_touched[i];
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
    if(part_id == home_part) {
      continue;
    }
#else
    if(part_node_id == get_node_id()) {
      continue;
    }
#endif
    // Check if we have already sent this node an RPREPARE message
    bool sent = false;
    for (uint64_t j = 0; j < i; j++) {
      //if (part_node_id == GET_NODE_ID(query->part_to_access[j])) {
      if (part_id == query->part_touched[j]) {
        sent = true;
        break;
      }
    }
    if (sent) 
      continue;

    incr_rsp(1);
    query->dest_part = part_id;
    if(fin) {
      if(GET_NODE_ID(part_id) != g_node_id) {
        //query->remote_finish(query, part_node_id);    
        msg_queue.enqueue(query,RFIN,part_node_id);
      } else {
        assert(CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC);
         // Model after RFIN
#if WORKLOAD == TPCC
	    base_query * tmp_query = (tpcc_query *) mem_allocator.alloc(sizeof(tpcc_query), 0);
	      tmp_query = new tpcc_query();
#elif WORKLOAD == YCSB
	    base_query * tmp_query = (ycsb_query *) mem_allocator.alloc(sizeof(ycsb_query), 0);
        tmp_query = new ycsb_query();
#endif
        tmp_query->txn_id = query->txn_id;
        tmp_query->ts = query->ts;
        tmp_query->home_part = query->home_part;
        tmp_query->pid = query->pid;
        tmp_query->rtype = RFIN;
        tmp_query->rc = query->rc;
        tmp_query->active_part = part_id;
        tmp_query->part_cnt = 1;
        tmp_query->parts = new uint64_t[1];
        tmp_query->parts[0] = part_id;
        //work_queue.add_query(GET_PART_ID_IDX(tmp_query->active_part),tmp_query);
        work_queue.enqueue(tmp_query);


      }
    } else {
      query->rc = RCOK;
      if(GET_NODE_ID(part_id) != g_node_id) {
        //query->remote_prepare(query, part_node_id);    
        msg_queue.enqueue(query,RPREPARE,part_node_id);
      } else {
        assert(CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC);
         // Model after RPREP
#if WORKLOAD == TPCC
	    base_query * tmp_query = (tpcc_query *) mem_allocator.alloc(sizeof(tpcc_query), 0);
	      tmp_query = new tpcc_query();
#elif WORKLOAD == YCSB
	    base_query * tmp_query = (ycsb_query *) mem_allocator.alloc(sizeof(ycsb_query), 0);
        tmp_query = new ycsb_query();
#endif
        tmp_query->txn_id = query->txn_id;
        tmp_query->ts = query->ts;
        tmp_query->home_part = query->home_part;
        tmp_query->pid = query->pid;
        tmp_query->rtype = RPREPARE;
        tmp_query->rc = query->rc;
        tmp_query->active_part = part_id;
        tmp_query->part_cnt = 1;
        tmp_query->parts = new uint64_t[1];
        tmp_query->parts[0] = part_id;
        //work_queue.add_query(GET_PART_ID_IDX(tmp_query->active_part),tmp_query);
        work_queue.enqueue(tmp_query);


      }
    }
  }
  // After all requests are sent, it's possible that all responses will come back
  //  before we execute the next instructions and this txn will be deleted.
  //  Can't touch anything related to this txn now.

  /*
  uint64_t timespan = get_sys_clock() - starttime;
  assert(h_thd->_node_id < g_node_cnt);
  INC_STATS(get_thd_id(),time_msg_sent,timespan);
  */

  if(rsp_cnt >0) 
    return WAIT_REM;
  else
    return RCOK; //finish(query->rc);

}

void
txn_man::release() {
	for (int i = 0; i < num_accesses_alloc; i++) {
    access_pool.put(accesses[i]);
		//mem_allocator.free(accesses[i], sizeof(Access));
  }
	//mem_allocator.free(accesses, 0);
}

