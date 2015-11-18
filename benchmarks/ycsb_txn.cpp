#include "global.h"
#include "helper.h"
#include "ycsb.h"
#include "ycsb_query.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_btree.h"
#include "catalog.h"
#include "manager.h"
#include "row_lock.h"
#include "row_ts.h"
#include "row_mvcc.h"
#include "mem_alloc.h"
#include "query.h"
#include "msg_queue.h"

void ycsb_txn_man::init(workload * h_wl) {
	txn_man::init(h_wl);
	_wl = (ycsb_wl *) h_wl;
}

bool ycsb_txn_man::conflict(base_query * query1,base_query * query2){
  return false;
}

void ycsb_txn_man::merge_txn_rsp(base_query * query1, base_query *query2) { 
	ycsb_query * m_query1 = (ycsb_query *) query1;
	ycsb_query * m_query2 = (ycsb_query *) query2;

  if(m_query1->rc == Abort) {
    m_query2->rc = m_query1->rc;
    m_query2->txn_rtype = YCSB_FIN;
    //assert(GET_NODE_ID(m_query2->pid) == g_node_id);
    //assert(GET_NODE_ID(m_query1->pid) == g_node_id);
  }

}

void ycsb_txn_man::read_keys(base_query * query) {
  assert(CC_ALG == VLL);
	ycsb_query * m_query = (ycsb_query *) query;
	// access the indexes. This is not in the critical section
	for (uint32_t rid = 0; rid < m_query->request_cnt; rid ++) {
		ycsb_request * req = &m_query->requests[rid];
		uint64_t part_id = _wl->key_to_part( req->key );
    if(GET_NODE_ID(part_id) != g_node_id)
      continue;
		INDEX * index = _wl->the_index;
		itemid_t * item;
		item = index_read(index, req->key, part_id);
		row_t * row = ((row_t *)item->location);
    row_t * row_local;
		// the following line adds the read/write sets to txn->accesses
		get_row(row, req->acctype, row_local);
	}
}

RC ycsb_txn_man::acquire_locks(base_query * query) {
  assert(CC_ALG == VLL || CC_ALG == CALVIN);
	ycsb_query * m_query = (ycsb_query *) query;
  locking_done = false;
  RC rc = RCOK;
  incr_lr();
	for (uint32_t rid = 0; rid < m_query->request_cnt; rid ++) {
		ycsb_request * req = &m_query->requests[rid];
		uint64_t part_id = _wl->key_to_part( req->key );
    if(GET_NODE_ID(part_id) != g_node_id)
      continue;
		INDEX * index = _wl->the_index;
		itemid_t * item;
		item = index_read(index, req->key, part_id);
		row_t * row = ((row_t *)item->location);
		RC rc2 = get_lock(row,req->acctype);
    if(rc2 != RCOK) {
      rc = rc2;
    }
	}
  if(decr_lr() == 0) {
    if(ATOM_CAS(lock_ready,false,true))
      rc = RCOK;
  }
  /*
  if(rc == WAIT && lock_ready_cnt == 0) {
    if(ATOM_CAS(lock_ready,false,true))
    //lock_ready = true;
      rc = RCOK;
  }
  */
  locking_done = true;
  return rc;
}


RC ycsb_txn_man::run_txn(base_query * query) {
  /*
#if MODE==TWOPC
  ycsb_query * m_query = (ycsb_query*) query;
  m_query->rem_req_state = YCSB_FIN;
	return finish(query,false);
#endif
*/
#if MODE == SETUP_MODE
  return RCOK;
#endif
  RC rc = RCOK;
  rem_done = false;
  fin = false;
  uint64_t thd_prof_start = get_sys_clock();

#if CC_ALG == CALVIN
  rc = run_ycsb(query);
  return rc;
#endif
#if DEBUG_DISTR
	ycsb_query * m_query = (ycsb_query *) query;
  if(IS_LOCAL(m_query->txn_id) && m_query->txn_rtype == YCSB_0 && m_query->rid == 0) {
    printf("REQ %ld: %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld\n",m_query->txn_id
        ,GET_NODE_ID(m_query->requests[0].key)
        ,GET_NODE_ID(m_query->requests[1].key)
        ,GET_NODE_ID(m_query->requests[2].key)
        ,GET_NODE_ID(m_query->requests[3].key)
        ,GET_NODE_ID(m_query->requests[4].key)
        ,GET_NODE_ID(m_query->requests[5].key)
        ,GET_NODE_ID(m_query->requests[6].key)
        ,GET_NODE_ID(m_query->requests[7].key)
        ,GET_NODE_ID(m_query->requests[8].key)
        ,GET_NODE_ID(m_query->requests[9].key)
        );
  }
#endif

  // Resume query after hold
  if(query->rc == WAIT_REM) {
    rtn_ycsb_state(query);
  }

  if((CC_ALG != HSTORE && CC_ALG != HSTORE_SPEC) && this->rc == WAIT) {
    assert(query->rc == WAIT || query->rc == RCOK);
    get_row_post_wait(row);
    next_ycsb_state(query);
    this->rc = RCOK;
  }

  do {
    rc = run_txn_state(query);
    if(rc != RCOK)
      break;
    next_ycsb_state(query);
  } while(!fin && !rem_done);

  assert(rc != WAIT_REM || GET_NODE_ID(query->pid) == g_node_id);

  INC_STATS(get_thd_id(),thd_prof_wl1,get_sys_clock() - thd_prof_start);
  return rc;

}

void ycsb_txn_man::next_ycsb_state(base_query * query) {
	ycsb_query * m_query = (ycsb_query *) query;
  switch(m_query->txn_rtype) {
    case YCSB_0:
      m_query->txn_rtype = YCSB_1;
      break;
      //m_query->req = m_query->requests[m_query->rid];
    case YCSB_1:
      /*
      if(GET_NODE_ID(m_query->pid) != g_node_id) {
        rem_done = true;
        break;
      }
      */
      m_query->rid++;
      if(m_query->rid < m_query->request_cnt && m_query->rc !=Abort ) {
        m_query->txn_rtype = YCSB_0;
        m_query->req = m_query->requests[m_query->rid];
        m_query->rc = RCOK;
      }
      else {
        if(GET_NODE_ID(m_query->pid) == g_node_id) {
          m_query->txn_rtype = YCSB_FIN;
        } else {
          rem_done = true;
          break;
        }
      }
    case YCSB_FIN:
      break;
    default:
      assert(false);
  }
}
void ycsb_txn_man::rtn_ycsb_state(base_query * query) {
	ycsb_query * m_query = (ycsb_query *) query;

  switch(m_query->txn_rtype) {
    case YCSB_0:
      m_query->rid+=m_query->rqry_req_cnt;
      //m_query->rid++;
      if(m_query->rid < m_query->request_cnt && m_query->rc != Abort) {
        m_query->rc = RCOK;
        m_query->txn_rtype = YCSB_0;
        m_query->req = m_query->requests[m_query->rid];
      }
      else {
        if(m_query->rc != Abort)
          m_query->rc = RCOK;
        m_query->txn_rtype = YCSB_FIN;
        assert(GET_NODE_ID(m_query->pid) == g_node_id);
      }
      break;
    case YCSB_1:
      assert(false);
    case YCSB_FIN:
      if(m_query->rc != Abort)
        m_query->rc = RCOK;
      break;
    default:
      assert(false);
  }
}

RC ycsb_txn_man::run_txn_state(base_query * query) {
	ycsb_query * m_query = (ycsb_query *) query;
	//ycsb_request * req = &m_query->requests[m_query->rid];
	ycsb_request * req = &m_query->req;
	uint64_t part_id = _wl->key_to_part( req->key );
  /*
#if CC_ALG == HSTORE || CC_ALG == HSTORE_PART
  bool loc = part_id == query->active_part;
#else
*/
  bool loc = GET_NODE_ID(part_id) == get_node_id();
//#endif

	RC rc = RCOK;

	switch (m_query->txn_rtype) {
		case YCSB_0 :
      if(loc) {
        rc = run_ycsb_0(req,row);
      } else {
        assert(GET_NODE_ID(m_query->pid) == g_node_id);

#if MODE==QRY_ONLY_MODE

        query->max_access = 0;
        for(uint64_t i = 0; i < m_query->request_cnt; i++) {
          if((uint64_t)_wl->key_to_part(m_query->requests[i].key) == part_id)
            query->max_access++;
        }
#endif
        this->rem_row_cnt++;

        m_query->rqry_req_cnt = 0;
        for(uint64_t i = m_query->rid; i < m_query->request_cnt; i++) {
          if(GET_NODE_ID(_wl->key_to_part(m_query->requests[i].key)) == get_node_id())
            break;
          m_query->rqry_req_cnt++;
        }
        assert(m_query->rqry_req_cnt > 0);
        query->dest_part = part_id;
        query->dest_id = GET_NODE_ID(part_id);
        query->rem_req_state = YCSB_0;
        rc = WAIT_REM;
      }
      break;
		case YCSB_1 :
      rc = run_ycsb_1(req->acctype,row);
      break;
    case YCSB_FIN :
      fin = true;
      query->rem_req_state = YCSB_FIN;
      assert(GET_NODE_ID(m_query->pid) == g_node_id);
		  return finish(m_query,false);
    default:
			assert(false);
  }

  if(rc == WAIT) {
    assert(CC_ALG != HSTORE && CC_ALG != HSTORE_SPEC);
    return rc;
  }
  m_query->rc = rc;
  if(rc == Abort && !fin && GET_NODE_ID(m_query->pid) == g_node_id) {
    query->rem_req_state = YCSB_FIN;
    rc = finish(m_query,false);
    if(rc == RCOK)
      rc = m_query->rc;
  }
  return rc;
}

RC ycsb_txn_man::run_ycsb_0(ycsb_request * req,row_t *& row_local) {
    RC rc = RCOK;
		int part_id = _wl->key_to_part( req->key );
		access_t type = req->acctype;
    // TODO: remove for parallel YCSB!
#if CC_ALG == VLL
    get_row_vll(type,row_local);
    return rc;
#endif
	  itemid_t * m_item;
		m_item = index_read(_wl->the_index, req->key, part_id);

		row_t * row = ((row_t *)m_item->location);
			
		rc = get_row(row, type,row_local);
    return rc;

}

RC ycsb_txn_man::run_ycsb_1(access_t acctype, row_t * row_local) {
  if (acctype == RD || acctype == SCAN) {
    int fid = 0;
		char * data = row_local->get_data();
		uint64_t fval __attribute__ ((unused));
    fval = *(uint64_t *)(&data[fid * 100]);
    //INC_STATS(get_thd_id(), debug1, fval);

  } else {
    assert(acctype == WR);
		int fid = 0;
	  //char * data = row->get_data();
	  char * data = row_local->get_data();
	  *(uint64_t *)(&data[fid * 100]) = 0;
  } 
  return RCOK;
}
RC ycsb_txn_man::run_calvin_txn(base_query * query) {
	ycsb_query * m_query = (ycsb_query *) query;
  RC rc = RCOK;
  while(rc == RCOK && this->phase < 6) {
    switch(this->phase) {
      case 1:
        // Phase 1: Read/write set analysis
        participant_cnt = 0;
        active_cnt = 0;
        for(uint64_t i = 0; i < g_node_cnt; i++) {
          participant_nodes[i] = false;
          active_nodes[i] = false;
        }

        for(uint64_t i = 0; i < m_query->request_cnt; i++) {
          uint64_t req_nid = GET_NODE_ID(_wl->key_to_part(m_query->requests[i].key));
          if(!participant_nodes[req_nid]) {
            participant_cnt++;
            participant_nodes[req_nid] = true;
          }
          if(m_query->requests[i].acctype == WR && !active_nodes[req_nid]) {
            active_cnt++;
            active_nodes[req_nid] = true;
          }
        }
        this->phase = 2;
        break;
      case 2:
        // Phase 2: Perform local reads
        rc = run_ycsb(query);
        //release_read_locks(query);

        this->phase = 3;
        break;
      case 3:
        // Phase 3: Serve remote reads
        rc = send_remote_reads(query);
        this->phase = 4;
        break;
      case 4:
        // Phase 4: Collect remote reads
        this->phase = 5;
        break;
      case 5:
        // Phase 5: Execute transaction / perform local writes
        rc = run_ycsb(query);
        query->rc = rc;
        rc = calvin_finish(query);
        this->phase = 6;
        break;
      default:
        assert(false);
    }
  }
  return rc;
}

RC ycsb_txn_man::run_ycsb(base_query * query) {
  RC rc = RCOK;
	ycsb_query * m_query = (ycsb_query *) query;
  assert(CC_ALG == CALVIN);
  
  for (uint64_t i = 0; i < m_query->request_cnt; i++) {
	  ycsb_request * req = &m_query->requests[i];
    if(this->phase == 2 && req->acctype == WR)
      continue;
    if(this->phase == 5 && req->acctype == RD)
      continue;

		uint64_t part_id = _wl->key_to_part( req->key );
    bool loc = GET_NODE_ID(part_id) == get_node_id();

    if(!loc)
      continue;

    rc = run_ycsb_0(req,row);
    assert(rc == RCOK);
    if(rc != RCOK)
      break;
    rc = run_ycsb_1(req->acctype,row);
    assert(rc == RCOK);
  }
	m_query->rc = rc;
  return rc;

}

