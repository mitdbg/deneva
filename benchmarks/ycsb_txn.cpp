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
#include "message.h"

void YCSBTxnManager::init(Workload * h_wl) {
	TxnManager::init(h_wl);
	_wl = (YCSBWorkload *) h_wl;
  reset();
	TxnManager::reset();
}

void YCSBTxnManager::reset() {
  state = YCSB_0;
  next_record_id = 0;
	TxnManager::reset();
}

RC YCSBTxnManager::acquire_locks() {
  assert(CC_ALG == CALVIN);
  YCSBQuery* ycsb_query = (YCSBQuery*) query;
  locking_done = false;
  RC rc = RCOK;
  incr_lr();
	for (uint32_t rid = 0; rid < ycsb_query->requests.size(); rid ++) {
		ycsb_request * req = ycsb_query->requests[rid];
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


RC YCSBTxnManager::run_txn() {
  RC rc = RCOK;

#if CC_ALG == CALVIN
  rc = run_ycsb(query);
  return rc;
#endif
  if(IS_LOCAL(txn->txn_id) && state == YCSB_0 && next_record_id == 0) {
    DEBUG("Running txn %ld\n",txn->txn_id);
    //query->print();
    query->partitions_touched.add_unique(GET_PART_ID(0,g_node_id));
  }


  while(rc == RCOK && !is_done()) {
    rc = run_txn_state();
  }

  if(IS_LOCAL(get_txn_id())) {
    if(is_done() && rc == RCOK) 
      rc = start_commit();
    else if(rc == Abort)
      rc = start_abort();
  }

  return rc;

}

RC YCSBTxnManager::run_txn_post_wait() {
    get_row_post_wait(row);
    next_ycsb_state();
    return RCOK;
}

bool YCSBTxnManager::is_done() {
  return next_record_id == ((YCSBQuery*)query)->requests.size();
}

void YCSBTxnManager::next_ycsb_state() {
  switch(state) {
    case YCSB_0:
      state = YCSB_1;
      break;
    case YCSB_1:
      next_record_id++;
      if(!IS_LOCAL(txn->txn_id) || !is_done()) {
        state = YCSB_0;
      }
      else {
        state = YCSB_FIN;
      }
      break;
    case YCSB_FIN:
      break;
    default:
      assert(false);
  }
}

bool YCSBTxnManager::is_local_request(uint64_t idx) {
  return GET_NODE_ID(_wl->key_to_part(((YCSBQuery*)query)->requests[idx]->key)) == g_node_id;
}

RC YCSBTxnManager::send_remote_request() {
  YCSBQuery* ycsb_query = (YCSBQuery*) query;
  ycsb_query->partitions_touched.add_unique(GET_PART_ID(0,GET_NODE_ID(ycsb_query->requests[next_record_id]->key)));
  uint64_t dest_node_id = GET_NODE_ID(ycsb_query->requests[next_record_id]->key);
  msg_queue.enqueue(Message::create_message(this,RQRY),dest_node_id);
  return WAIT_REM;
}

void YCSBTxnManager::copy_remote_requests(YCSBQueryMessage * msg) {
  YCSBQuery* ycsb_query = (YCSBQuery*) query;
  msg->requests.init(ycsb_query->requests.size());
  uint64_t dest_node_id = GET_NODE_ID(ycsb_query->requests[next_record_id]->key);
  while(next_record_id < ycsb_query->requests.size() && !is_local_request(next_record_id) && GET_NODE_ID(ycsb_query->requests[next_record_id]->key) == dest_node_id) {
    ycsb_request * req = (ycsb_request*) mem_allocator.alloc(sizeof(ycsb_request));
    req->copy(ycsb_query->requests[next_record_id++]);
    msg->requests.add(req);
  }
}

RC YCSBTxnManager::run_txn_state() {
  YCSBQuery* ycsb_query = (YCSBQuery*) query;
	ycsb_request * req = ycsb_query->requests[next_record_id];
	uint64_t part_id = _wl->key_to_part( req->key );
  bool loc = GET_NODE_ID(part_id) == g_node_id;

	RC rc = RCOK;

	switch (state) {
		case YCSB_0 :
      if(loc) {
        rc = run_ycsb_0(req,row);
      } else {
        rc = send_remote_request();
        
      }

      break;
		case YCSB_1 :
      rc = run_ycsb_1(req->acctype,row);
      break;
    case YCSB_FIN :
      state = YCSB_FIN;
      break;
    default:
			assert(false);
  }

  if(rc == RCOK)
    next_ycsb_state();

  return rc;
}

RC YCSBTxnManager::run_ycsb_0(ycsb_request * req,row_t *& row_local) {
    RC rc = RCOK;
		int part_id = _wl->key_to_part( req->key );
		access_t type = req->acctype;
	  itemid_t * m_item;
		m_item = index_read(_wl->the_index, req->key, part_id);

		row_t * row = ((row_t *)m_item->location);
			
		rc = get_row(row, type,row_local);
    return rc;

}

RC YCSBTxnManager::run_ycsb_1(access_t acctype, row_t * row_local) {
  if (acctype == RD || acctype == SCAN) {
    int fid = 0;
		char * data = row_local->get_data();
		uint64_t fval __attribute__ ((unused));
    fval = *(uint64_t *)(&data[fid * 100]);

  } else {
    assert(acctype == WR);
		int fid = 0;
	  char * data = row_local->get_data();
	  *(uint64_t *)(&data[fid * 100]) = 0;
  } 
  return RCOK;
}
RC YCSBTxnManager::run_calvin_txn() {
  RC rc = RCOK;
  YCSBQuery* ycsb_query = (YCSBQuery*) query;
  uint64_t participant_cnt;
  uint64_t active_cnt;
  uint64_t participant_nodes[10];
  uint64_t active_nodes[10];
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

        for(uint64_t i = 0; i < ycsb_query->requests.size(); i++) {
          uint64_t req_nid = GET_NODE_ID(_wl->key_to_part(ycsb_query->requests[i]->key));
          if(!participant_nodes[req_nid]) {
            participant_cnt++;
            participant_nodes[req_nid] = true;
          }
          if(ycsb_query->requests[i]->acctype == WR && !active_nodes[req_nid]) {
            active_cnt++;
            active_nodes[req_nid] = true;
          }
        }
        ATOM_ADD(this->phase,1); //2
        break;
      case 2:
        // Phase 2: Perform local reads
        rc = run_ycsb();
        //release_read_locks(query);

        ATOM_ADD(this->phase,1); //3
        break;
      case 3:
        // Phase 3: Serve remote reads
        rc = send_remote_reads(ycsb_query);
        if(active_nodes[g_node_id]) {
          ATOM_ADD(this->phase,1); //4
          if(get_rsp_cnt() == participant_cnt-1) {
            rc = RCOK;
          } else {
            DEBUG("Phase4 (%ld,%ld)\n",txn->txn_id,txn->batch_id);
            rc = WAIT;
          }
        } else { // Done
          rc = RCOK;
          ATOM_ADD(this->phase,3); //6
        }

        break;
      case 4:
        // Phase 4: Collect remote reads
        ATOM_ADD(this->phase,1); //5
        break;
      case 5:
        // Phase 5: Execute transaction / perform local writes
        rc = run_ycsb();
        rc = calvin_finish(ycsb_query);
        ATOM_ADD(this->phase,1); //6
        // FIXME
        /*
        if(get_rsp2_cnt() == active_cnt-1) {
          rc = RCOK;
        } else {
        DEBUG("Phase6 (%ld,%ld)\n",txn->txn_id,txn->batch_id);
            rc = WAIT;
        }
        */
        break;
      default:
        assert(false);
    }
  }
  return rc;
}

RC YCSBTxnManager::run_ycsb() {
  RC rc = RCOK;
  assert(CC_ALG == CALVIN);
  YCSBQuery* ycsb_query = (YCSBQuery*) query;
  
  for (uint64_t i = 0; i < ycsb_query->requests.size(); i++) {
	  ycsb_request * req = ycsb_query->requests[i];
    if(this->phase == 2 && req->acctype == WR)
      continue;
    if(this->phase == 5 && req->acctype == RD)
      continue;

		uint64_t part_id = _wl->key_to_part( req->key );
    bool loc = GET_NODE_ID(part_id) == g_node_id;

    if(!loc)
      continue;

    rc = run_ycsb_0(req,row);
    assert(rc == RCOK);
    if(rc != RCOK)
      break;
    rc = run_ycsb_1(req->acctype,row);
    assert(rc == RCOK);
  }
  return rc;

}

