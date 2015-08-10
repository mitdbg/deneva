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

void ycsb_txn_man::init(thread_t * h_thd, workload * h_wl, uint64_t thd_id) {
	txn_man::init(h_thd, h_wl, thd_id);
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
#if CC_ALG == VLL || CC_ALG == CALVIN
	ycsb_query * m_query = (ycsb_query *) query;
	for (uint32_t rid = 0; rid < m_query->request_cnt; rid ++) {
		ycsb_request * req = &m_query->requests[rid];
		uint64_t part_id = _wl->key_to_part( req->key );
    if(GET_NODE_ID(part_id) != g_node_id)
      continue;
		INDEX * index = _wl->the_index;
		itemid_t * item;
		item = index_read(index, req->key, part_id);
		row_t * row = ((row_t *)item->location);
		rc = get_lock(row,req->acctype);
    if(rc != RCOK)
      break;
	}
#endif
  return rc;
}


RC ycsb_txn_man::run_txn(base_query * query) {
  RC rc = RCOK;
  rem_done = false;
  fin = false;

#if CC_ALG == CALVIN
  rc = run_ycsb(query);
  return rc;
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

  return rc;

}

void ycsb_txn_man::next_ycsb_state(base_query * query) {
	ycsb_query * m_query = (ycsb_query *) query;
  switch(m_query->txn_rtype) {
    case YCSB_0:
      m_query->txn_rtype = YCSB_1;
      //m_query->req = m_query->requests[m_query->rid];
    case YCSB_1:
      if(GET_NODE_ID(m_query->pid) != g_node_id) {
        rem_done = true;
        break;
      }
      m_query->rid++;
      if(m_query->rid < m_query->request_cnt) {
        m_query->txn_rtype = YCSB_0;
        m_query->req = m_query->requests[m_query->rid];
      }
      else {
        m_query->txn_rtype = YCSB_FIN;
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
      m_query->rid++;
      if(m_query->rid < m_query->request_cnt) {
        m_query->txn_rtype = YCSB_0;
        m_query->req = m_query->requests[m_query->rid];
      }
      else {
        m_query->txn_rtype = YCSB_FIN;
      }
      break;
    case YCSB_1:
      assert(false);
    case YCSB_FIN:
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
#if TWOPC_ONLY
        rc = RCOK;
        break;
#endif
        assert(GET_NODE_ID(m_query->pid) == g_node_id);

        query->dest_part = part_id;
        query->dest_id = GET_NODE_ID(part_id);
        query->rem_req_state = YCSB_0;
        rc = WAIT_REM;
      }
      break;
		case YCSB_1 :
#if TWOPC_ONLY
      if(!loc) {
        rc = RCOK;
        break;
      }
#endif
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
#if !NOGRAPHITE
		if (g_hw_migrate && part_id != CarbonGetHostTileId()) 
			CarbonMigrateThread(part_id);
#endif  
    // TODO: remove for parallel YCSB!
#if CC_ALG == VLL
    return rc;
#endif
	  itemid_t * m_item;
		m_item = index_read(_wl->the_index, req->key, part_id);

		row_t * row = ((row_t *)m_item->location);
		access_t type = req->acctype;
			
		rc = get_row(row, type,row_local);
    return rc;

}

RC ycsb_txn_man::run_ycsb_1(access_t acctype, row_t * row_local) {
  if (acctype == RD || acctype == SCAN) {
    int fid = 0;
		char * data = row_local->get_data();
		uint64_t fval = *(uint64_t *)(&data[fid * 100]);
    INC_STATS(get_thd_id(), debug1, fval);

  } else {
    assert(acctype == WR);
		int fid = 0;
	  //char * data = row->get_data();
	  char * data = row_local->get_data();
	  *(uint64_t *)(&data[fid * 100]) = 0;
  } 
  return RCOK;
}

RC ycsb_txn_man::run_ycsb(base_query * query) {
  RC rc = RCOK;
	ycsb_query * m_query = (ycsb_query *) query;
  assert(CC_ALG == CALVIN);
  
  for (uint64_t i = 0; i < m_query->request_cnt; i++) {
	  ycsb_request * req = &m_query->requests[i];

		uint64_t part_id = _wl->key_to_part( req->key );
    bool loc = GET_NODE_ID(part_id) == get_node_id();
    assert(loc);

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

  // Sends ack back to Calvin sequencer
  return finish(m_query,false);
}


/*
RC ycsb_txn_man::run_txn(base_query * query) {
	RC rc;
	ycsb_query * m_query = (ycsb_query *) query;
	ycsb_wl * wl = (ycsb_wl *) h_wl;
	itemid_t * m_item;
  	row_cnt = 0;

//	Catalog * schema = _wl->the_table->get_schema();
	for (UInt32 rid = 0; rid < m_query->request_cnt; rid ++) {
		ycsb_request * req = &m_query->requests[rid];
		int part_id = wl->key_to_part( req->key );
#if !NOGRAPHITE
		if (g_hw_migrate && part_id != CarbonGetHostTileId()) 
			CarbonMigrateThread(part_id);
#endif  
		bool finish_req = false;
		UInt32 iteration = 0;
		while ( !finish_req ) {
			if (iteration == 0) {
				m_item = index_read(_wl->the_index, req->key, part_id);
			} 
#if INDEX_STRUCT == IDX_BTREE
			else {
				_wl->the_index->index_next(get_thd_id(), m_item);
				if (m_item == NULL)
					break;
			}
#endif
			row_t * row = ((row_t *)m_item->location);
			row_t * row_local; 
			access_t type = req->acctype;
			
			row_local = get_row(row, type);
			if (row_local == NULL) {
				rc = Abort;
				goto final;
			}

			// Computation //
			// Only do computation when there are more than 1 requests.
            if (m_query->request_cnt > 1) {
                if (req->acctype == RD || req->acctype == SCAN) {
//                    for (int fid = 0; fid < schema->get_field_cnt(); fid++) {
						int fid = 0;
						char * data = row_local->get_data();
						uint64_t fval = *(uint64_t *)(&data[fid * 100]);
						INC_STATS(get_thd_id(), debug1, fval);
  //                  }
                } else {
                    assert(req->acctype == WR);
//					for (int fid = 0; fid < schema->get_field_cnt(); fid++) {
						int fid = 0;
						char * data = row->get_data();
						*(uint64_t *)(&data[fid * 100]) = 0;
//					}
                } 
            }


			iteration ++;
			if (req->acctype == RD || req->acctype == WR || iteration == req->scan_len)
				finish_req = true;
		}
	}
	rc = RCOK;
final:
	rc = finish(rc);
	return rc;
}
*/
