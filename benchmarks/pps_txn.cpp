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

#include "pps.h"
#include "pps_query.h"
#include "query.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_btree.h"
#include "pps_const.h"
#include "transport.h"
#include "msg_queue.h"
#include "message.h"

void PPSTxnManager::init(uint64_t thd_id, Workload * h_wl) {
	TxnManager::init(thd_id, h_wl);
	_wl = (PPSWorkload *) h_wl;
  reset();
	TxnManager::reset();
}

void PPSTxnManager::reset() {
  PPSQuery* pps_query = (PPSQuery*) query;
  state = PPS_PAYMENT0;
  if(pps_query->txn_type == PPS_PAYMENT) {
    state = PPS_PAYMENT0;
  } else if (pps_query->txn_type == PPS_NEW_ORDER) {
    state = PPS_NEWORDER0;
  }
	TxnManager::reset();
}

RC PPSTxnManager::run_txn_post_wait() {
    get_row_post_wait(row);
    next_pps_state();
    return RCOK;
}


RC PPSTxnManager::run_txn() {
#if MODE == SETUP_MODE
  return RCOK;
#endif
  RC rc = RCOK;

#if CC_ALG == CALVIN
  rc = run_calvin_txn();
  return rc;
#endif

  if(IS_LOCAL(txn->txn_id) && (state == PPS_PAYMENT0 || state == PPS_NEWORDER0)) {
    DEBUG("Running txn %ld\n",txn->txn_id);
#if DISTR_DEBUG
    query->print();
#endif
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

bool PPSTxnManager::is_done() {
  bool done = false;
  PPSQuery* pps_query = (PPSQuery*) query;
  done = state == PPS_FIN;
  
  return done;
}

RC PPSTxnManager::acquire_locks() {
  uint64_t starttime = get_sys_clock();
  assert(CC_ALG == CALVIN);
  locking_done = false;
  RC rc = RCOK;
  RC rc2;
  INDEX * index;
  itemid_t * item;
  row_t* row;
  uint64_t key;
  incr_lr();
  PPSQuery* pps_query = (PPSQuery*) query;

  switch(pps_query->txn_type) {
    case PPS_PARTS:
    default: assert(false);
  }
  if(decr_lr() == 0) {
    if(ATOM_CAS(lock_ready,false,true))
      rc = RCOK;
  }
  txn_stats.wait_starttime = get_sys_clock();
  locking_done = true;
  INC_STATS(get_thd_id(),calvin_sched_time,get_sys_clock() - starttime);
  return rc;
}


void PPSTxnManager::next_pps_state() {
  //PPSQuery* pps_query = (PPSQuery*) query;

  switch(state) {
    case PPS_PAYMENT_S:
      state = PPS_PAYMENT0;
      break;
    case PPS_PAYMENT0:
      state = PPS_PAYMENT1;
      break;
    case PPS_PAYMENT1:
      state = PPS_PAYMENT2;
      break;
    case PPS_PAYMENT2:
      state = PPS_PAYMENT3;
      break;
    case PPS_PAYMENT3:
      state = PPS_PAYMENT4;
      break;
    case PPS_PAYMENT4:
      state = PPS_PAYMENT5;
      break;
    case PPS_PAYMENT5:
      state = PPS_FIN;
      break;
    case PPS_NEWORDER_S:
      state = PPS_NEWORDER0;
      break;
    case PPS_NEWORDER0:
      state = PPS_NEWORDER1;
      break;
    case PPS_NEWORDER1:
      state = PPS_NEWORDER2;
      break;
    case PPS_NEWORDER2:
      state = PPS_NEWORDER3;
      break;
    case PPS_NEWORDER3:
      state = PPS_NEWORDER4;
      break;
    case PPS_NEWORDER4:
      state = PPS_NEWORDER5;
      break;
    case PPS_NEWORDER5:
      if(!IS_LOCAL(txn->txn_id) || !is_done()) {
        state = PPS_NEWORDER6;
      }
      else {
        state = PPS_FIN;
      }
      break;
    case PPS_NEWORDER6: // loop pt 1
      state = PPS_NEWORDER7;
      break;
    case PPS_NEWORDER7:
      state = PPS_NEWORDER8;
      break;
    case PPS_NEWORDER8: // loop pt 2
      state = PPS_NEWORDER9;
      break;
    case PPS_NEWORDER9:
      ++next_item_id;
      if(!IS_LOCAL(txn->txn_id) || !is_done()) {
        state = PPS_NEWORDER6;
      }
      else {
        state = PPS_FIN;
      }
      break;
    case PPS_FIN:
      break;
    default:
      assert(false);
  }

}

bool PPSTxnManager::is_local_item(uint64_t idx) {
  PPSQuery* pps_query = (PPSQuery*) query;
	uint64_t ol_supply_w_id = pps_query->items[idx]->ol_supply_w_id;
  uint64_t part_id_ol_supply_w = wh_to_part(ol_supply_w_id);
  return GET_NODE_ID(part_id_ol_supply_w) == g_node_id;
}


RC PPSTxnManager::send_remote_request() {
  assert(IS_LOCAL(get_txn_id()));
  PPSQuery* pps_query = (PPSQuery*) query;
  PPSRemTxnType next_state = PPS_FIN;
	uint64_t w_id = pps_query->w_id;
  uint64_t c_w_id = pps_query->c_w_id;
  uint64_t dest_node_id = UINT64_MAX;
  dest_node_id = GET_NODE_ID(wh_to_part(w_id));
  PPSQueryMessage * msg = (PPSQueryMessage*)Message::create_message(this,RQRY);
  msg->state = state;
  query->partitions_touched.add_unique(GET_PART_ID(0,dest_node_id));
  msg_queue.enqueue(get_thd_id(),msg,dest_node_id);
  state = next_state;
  return WAIT_REM;
}

void PPSTxnManager::copy_remote_items(PPSQueryMessage * msg) {
  PPSQuery* pps_query = (PPSQuery*) query;
  msg->items.init(pps_query->items.size());
  if(pps_query->txn_type == PPS_PAYMENT)
    return;
  uint64_t dest_node_id = GET_NODE_ID(wh_to_part(pps_query->items[next_item_id]->ol_supply_w_id));
  while(next_item_id < pps_query->items.size() && !is_local_item(next_item_id) && GET_NODE_ID(wh_to_part(pps_query->items[next_item_id]->ol_supply_w_id)) == dest_node_id) {
    Item_no * req = (Item_no*) mem_allocator.alloc(sizeof(Item_no));
    req->copy(pps_query->items[next_item_id++]);
    msg->items.add(req);
  }
}


RC PPSTxnManager::run_txn_state() {
  PPSQuery* pps_query = (PPSQuery*) query;
	RC rc = RCOK;

	switch (state) {
		case PPS_PAYMENT0 :
      if(w_loc)
			  rc = run_payment_0(w_id, d_id, d_w_id, h_amount, row);
      else {
        rc = send_remote_request();
      }
			break;
		case PPS_PAYMENT1 :
			rc = run_payment_1(w_id, d_id, d_w_id, h_amount, row);
      break;
    case PPS_FIN :
      state = PPS_FIN;
      break;
		default:
			assert(false);
	}

  if(rc == RCOK)
    next_pps_state();
  return rc;
}

inline RC PPSTxnManager::run_getpart_0(uint64_t part_key, row_t *& r_local) {
  RC rc;
  /*
    SELECT * FROM PARTS WHERE PART_KEY = :part_key; 
   */
		INDEX * index = _wl->i_parts;
		item = index_read(index, key, parts_to_part(part_key));
		assert(item != NULL);
		r_loc = (row_t *) item->location;
    rc = get_row(r_loc, RD, r_local);
}
inline RC PPSTxnManager::run_getpart_1(uint64_t part_key, row_t *& r_local) {
  /*
    SELECT * FROM PARTS WHERE PART_KEY = :part_key; 
   */
  char * fields = new char[100];
  assert(r_local != NULL);
  r_local->get_value(FIELD1,fields);
  r_local->get_value(FIELD2,fields);
  r_local->get_value(FIELD3,fields);
  r_local->get_value(FIELD4,fields);
  r_local->get_value(FIELD5,fields);
  r_local->get_value(FIELD6,fields);
  r_local->get_value(FIELD7,fields);
  r_local->get_value(FIELD8,fields);
  r_local->get_value(FIELD9,fields);
  r_local->get_value(FIELD10,fields);
  return RCOK;
}


inline RC PPSTxnManager::run_getproduct_0(uint64_t product_key, row_t *& r_local) {
  /*
    SELECT * FROM PRODUCTS WHERE PRODUCT_KEY = :product_key; 
   */
  RC rc;
		INDEX * index = _wl->i_products;
		item = index_read(index, key, product_to_part(product_key));
		assert(item != NULL);
		r_loc = (row_t *) item->location;
    rc = get_row(r_loc, RD, r_local);
}
inline RC PPSTxnManager::run_getproduct_1(uint64_t product_key, row_t *& r_local) {
  /*
    SELECT * FROM PRODUCTS WHERE PRODUCT_KEY = :product_key; 
   */
  assert(r_local);
  char * fields = new char[100];
  assert(r_local != NULL);
  r_local->get_value(FIELD1,fields);
  r_local->get_value(FIELD2,fields);
  r_local->get_value(FIELD3,fields);
  r_local->get_value(FIELD4,fields);
  r_local->get_value(FIELD5,fields);
  r_local->get_value(FIELD6,fields);
  r_local->get_value(FIELD7,fields);
  r_local->get_value(FIELD8,fields);
  r_local->get_value(FIELD9,fields);
  r_local->get_value(FIELD10,fields);
  return RCOK;
}

inline RC PPSTxnManager::run_getsupplier_0(uint64_t supplier_key, row_t *& r_local) {
  /*
    SELECT * FROM SUPPLIERS WHERE SUPPLIER_KEY = :supplier_key; 
   */
  RC rc;
		INDEX * index = _wl->i_products;
		item = index_read(index, key, product_to_part(product_key));
		assert(item != NULL);
		r_loc = (row_t *) item->location;
    rc = get_row(r_loc, RD, r_local);

}
inline RC PPSTxnManager::run_getsupplier_1(uint64_t supplier_key, row_t *& r_local) {
  /*
    SELECT * FROM SUPPLIERS WHERE SUPPLIER_KEY = :supplier_key; 
   */
  assert(r_local);
  char * fields = new char[100];
  assert(r_local != NULL);
  r_local->get_value(FIELD1,fields);
  r_local->get_value(FIELD2,fields);
  r_local->get_value(FIELD3,fields);
  r_local->get_value(FIELD4,fields);
  r_local->get_value(FIELD5,fields);
  r_local->get_value(FIELD6,fields);
  r_local->get_value(FIELD7,fields);
  r_local->get_value(FIELD8,fields);
  r_local->get_value(FIELD9,fields);
  r_local->get_value(FIELD10,fields);
  return RCOK;

}
inline RC PPSTxnManager::run_getpartsbyproduct_0(uint64_t product_key, row_t *& r_wh_local) {
  /*
    SELECT FIELD1, FIELD2, FIELD3 FROM PRODUCTS WHERE PRODUCT_KEY = ? 
   */
}
inline RC PPSTxnManager::run_getpartsbyproduct_1(uint64_t product_key, row_t *& r_wh_local) {
  /*
    SELECT FIELD1, FIELD2, FIELD3 FROM PRODUCTS WHERE PRODUCT_KEY = ? 
   */
}

inline RC PPSTxnManager::run_getpartsbyproduct_2(uint64_t product_key, row_t *& r_wh_local) {
  /*
    SELECT PART_KEY FROM USES WHERE PRODUCT_KEY = ? 
   */
}
inline RC PPSTxnManager::run_getpartsbyproduct_3(uint64_t product_key, row_t *& r_wh_local) {
  /*
    SELECT PART_KEY FROM USES WHERE PRODUCT_KEY = ? 
   */
}
inline RC PPSTxnManager::run_getpartsbyproduct_4(uint64_t part_key, row_t *& r_wh_local) {
  /*
   SELECT FIELD1, FIELD2, FIELD3 FROM PARTS WHERE PART_KEY = ? 
   */
}
inline RC PPSTxnManager::run_getpartsbyproduct_5(uint64_t part_key, row_t *& r_wh_local) {
  /*
   SELECT FIELD1, FIELD2, FIELD3 FROM PARTS WHERE PART_KEY = ? 
   */
}

inline RC PPSTxnManager::run_getpartsbysupplier_0(uint64_t supplier_key, row_t *& r_wh_local) {
  /*
    SELECT FIELD1, FIELD2, FIELD3 FROM supplierS WHERE supplier_KEY = ? 
   */
}
inline RC PPSTxnManager::run_getpartsbysupplier_1(uint64_t supplier_key, row_t *& r_wh_local) {
  /*
    SELECT FIELD1, FIELD2, FIELD3 FROM supplierS WHERE supplier_KEY = ? 
   */
}

inline RC PPSTxnManager::run_getpartsbysupplier_2(uint64_t supplier_key, row_t *& r_wh_local) {
  /*
    SELECT PART_KEY FROM USES WHERE supplier_KEY = ? 
   */
}
inline RC PPSTxnManager::run_getpartsbysupplier_3(uint64_t supplier_key, row_t *& r_wh_local) {
  /*
    SELECT PART_KEY FROM USES WHERE supplier_KEY = ? 
   */
}
inline RC PPSTxnManager::run_getpartsbysupplier_4(uint64_t part_key, row_t *& r_wh_local) {
  /*
   SELECT FIELD1, FIELD2, FIELD3 FROM PARTS WHERE PART_KEY = ? 
   */
}
inline RC PPSTxnManager::run_getpartsbysupplier_5(uint64_t part_key, row_t *& r_wh_local) {
  /*
   SELECT FIELD1, FIELD2, FIELD3 FROM PARTS WHERE PART_KEY = ? 
   */
}

RC PPSTxnManager::run_calvin_txn() {
  RC rc = RCOK;
  uint64_t starttime = get_sys_clock();
  PPSQuery* pps_query = (PPSQuery*) query;
  DEBUG("(%ld,%ld) Run calvin txn\n",txn->txn_id,txn->batch_id);
  while(!calvin_exec_phase_done() && rc == RCOK) {
    DEBUG("(%ld,%ld) phase %d\n",txn->txn_id,txn->batch_id,this->phase);
    switch(this->phase) {
      case CALVIN_RW_ANALYSIS:
        // Phase 1: Read/write set analysis
        calvin_expected_rsp_cnt = pps_query->get_participants(_wl);
        if(query->participant_nodes[g_node_id] == 1) {
          calvin_expected_rsp_cnt--;
        }

        DEBUG("(%ld,%ld) expects %d responses; %ld participants, %ld active\n",txn->txn_id,txn->batch_id,calvin_expected_rsp_cnt,query->participant_nodes.size(),query->active_nodes.size());

        this->phase = CALVIN_LOC_RD;
        break;
      case CALVIN_LOC_RD:
        // Phase 2: Perform local reads
        DEBUG("(%ld,%ld) local reads\n",txn->txn_id,txn->batch_id);
        rc = run_pps_phase2();
        //release_read_locks(pps_query);

        this->phase = CALVIN_SERVE_RD;
        break;
      case CALVIN_SERVE_RD:
        // Phase 3: Serve remote reads
        if(query->participant_nodes[g_node_id] == 1) {
          rc = send_remote_reads();
        }
        if(query->active_nodes[g_node_id] == 1) {
          this->phase = CALVIN_COLLECT_RD;
          if(calvin_collect_phase_done()) {
            rc = RCOK;
          } else {
            assert(calvin_expected_rsp_cnt > 0);
            DEBUG("(%ld,%ld) wait in collect phase; %d / %d rfwds received\n",txn->txn_id,txn->batch_id,rsp_cnt,calvin_expected_rsp_cnt);
            rc = WAIT;
          }
        } else { // Done
          rc = RCOK;
          this->phase = CALVIN_DONE;
        }
        break;
      case CALVIN_COLLECT_RD:
        // Phase 4: Collect remote reads
        this->phase = CALVIN_EXEC_WR;
        break;
      case CALVIN_EXEC_WR:
        // Phase 5: Execute transaction / perform local writes
        DEBUG("(%ld,%ld) execute writes\n",txn->txn_id,txn->batch_id);
        rc = run_pps_phase5();
        this->phase = CALVIN_DONE;
        break;
      default:
        assert(false);
    }
  }
  txn_stats.process_time += get_sys_clock() - starttime;
  txn_stats.wait_starttime = get_sys_clock();
  return rc;
  
}


RC PPSTxnManager::run_pps_phase2() {
  PPSQuery* pps_query = (PPSQuery*) query;
  RC rc = RCOK;
  assert(CC_ALG == CALVIN);

	switch (pps_query->txn_type) {
		case PPS_PAYMENT :
      break;
		case PPS_NEW_ORDER :
        break;
    default: assert(false);
  }
  return rc;
}

RC PPSTxnManager::run_pps_phase5() {
  PPSQuery* pps_query = (PPSQuery*) query;
  RC rc = RCOK;
  assert(CC_ALG == CALVIN);

	switch (pps_query->txn_type) {
		case PPS_PAYMENT :
      if(w_loc) {
      }
      if(c_w_loc) {
      }
      break;
		case PPS_NEW_ORDER :
      if(w_loc) {
      }
        break;
    default: assert(false);
  }
  return rc;

}

