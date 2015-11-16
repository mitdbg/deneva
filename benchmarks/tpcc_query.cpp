#include "query.h"
#include "tpcc_query.h"
#include "tpcc.h"
#include "tpcc_helper.h"
#include "mem_alloc.h"
#include "wl.h"
#include "table.h"

void tpcc_client_query::client_init(uint64_t thd_id, workload * h_wl) {
	//double x = (double)(rand() % 100) / 100.0;
	//part_to_access = (uint64_t *) 
	//	mem_allocator.alloc(sizeof(uint64_t) * g_part_cnt, thd_id);
	//pid = GET_PART_ID(0,g_node_id);
	//// TODO
	//if (x < g_perc_payment)
	//	gen_payment(pid);
	//else 
	//	gen_new_order(pid);
    client_init(thd_id, h_wl, g_node_id);
}

void tpcc_client_query::client_init(uint64_t thd_id, workload * h_wl, uint64_t node_id) {
	double x = (double)(rand() % 100) / 100.0;
	part_to_access = (uint64_t *) 
		mem_allocator.alloc(sizeof(uint64_t) * g_part_cnt, thd_id);
	pid = GET_PART_ID(0,node_id);
	// TODO
	if (x < g_perc_payment)
		gen_payment(pid);
	else 
		gen_new_order(pid);
}

void tpcc_query::init(uint64_t thd_id, workload * h_wl) {
	txn_rtype = TPCC_PAYMENT0;
}

void tpcc_query::deep_copy(base_query * qry) {
	tpcc_query * m_qry = (tpcc_query *) qry;
  this->return_id = m_qry->return_id;
  this->rtype = m_qry->rtype;
  this->batch_id = m_qry->batch_id;
  this->txn_id = m_qry->txn_id;
  this->txn_rtype = m_qry->txn_rtype;
  this->txn_type = m_qry->txn_type;
  this->w_id = m_qry->w_id;
  this->c_id = m_qry->c_id;
  this->d_id = m_qry->d_id;
  this->w_id = m_qry->d_w_id;
  this->w_id = m_qry->c_w_id;
  this->w_id = m_qry->c_d_id;
  memcpy(this->c_last,m_qry->c_last,LASTNAME_LEN);
  this->h_amount = m_qry->h_amount;
  this->by_last_name = m_qry->by_last_name;
  this->rbk = m_qry->rbk;
  this->ol_cnt = m_qry->ol_cnt;
  this->o_entry_d = m_qry->o_entry_d;
  this->o_carrier_id = m_qry->o_carrier_id;
  this->ol_delivery_d = m_qry->ol_delivery_d;
  this->ol_supply_w_id = m_qry->ol_supply_w_id;
  this->ol_quantity = m_qry->ol_quantity;
  this->ol_number = m_qry->ol_number;
  this->o_id = m_qry->o_id;
  this->ol_amount = m_qry->ol_amount;
  for(uint64_t i = 0; i < m_qry->ol_cnt; i++) {
    this->ol_i_id = m_qry->ol_i_id;
    this->ol_supply_w_id = m_qry->ol_supply_w_id;
    this->ol_quantity = m_qry->ol_quantity;
  }

  
}
uint64_t tpcc_query::participants(bool *& pps,workload * wl) {
  int n = 0;
  for(uint64_t i = 0; i < g_node_cnt; i++)
    pps[i] = false;
  uint64_t id;

  switch(txn_type) {
    case TPCC_PAYMENT:
      id = GET_NODE_ID(wh_to_part(w_id));
      if(!pps[id]) {
        pps[id] = true;
        n++;
      }
      id = GET_NODE_ID(wh_to_part(c_w_id));
      if(!pps[id]) {
        pps[id] = true;
        n++;
      }
      break;
    case TPCC_NEW_ORDER: 
      id = GET_NODE_ID(wh_to_part(w_id));
      if(!pps[id]) {
        pps[id] = true;
        n++;
      }
      /*
      id = GET_NODE_ID(wh_to_part(c_w_id));
      if(!pps[id]) {
        pps[id] = true;
        n++;
      }
      id = GET_NODE_ID(wh_to_part(d_w_id));
      if(!pps[id]) {
        pps[id] = true;
        n++;
      }
      */
      for(uint64_t i = 0; i < ol_cnt; i++) {
        uint64_t req_nid = GET_NODE_ID(wh_to_part(items[i].ol_supply_w_id));
        if(!pps[req_nid]) {
          pps[req_nid] = true;
          n++;
        }
      }
      break;
    default: assert(false);
  }

  return n;
}

bool tpcc_query::readonly() {
  ro = false;
  return false;
}



base_query * tpcc_query::merge(base_query * query) {
	tpcc_query * m_query = (tpcc_query *) query;
  RemReqType old_rtype = this->rtype;
  this->rtype = m_query->rtype;
  switch(m_query->rtype) {
    case RINIT:
      assert(false);
      break;
    case RPREPARE:
      if(m_query->rc == Abort)
        this->rc = Abort;
      break;
    case RQRY:
      assert(m_query->pid == this->pid);
      //assert(m_query->ts == this->ts);
#if CC_ALG == MVCC 
      this->thd_id = m_query->thd_id;
#endif
#if CC_ALG == OCC 
      this->start_ts = m_query->start_ts;
#endif
      this->txn_rtype = m_query->txn_rtype;
      switch(m_query->txn_rtype) {
        case TPCC_PAYMENT0 :
          this->w_id = m_query->w_id;
          this->d_id = m_query->d_id;
          this->d_w_id = m_query->d_w_id;
          this->h_amount = m_query->h_amount;
          break;
        case TPCC_PAYMENT4 :
          this->w_id = m_query->w_id;
          this->d_id = m_query->d_id;
          this->c_id = m_query->c_id;
          this->c_w_id = m_query->c_w_id;
          this->c_d_id = m_query->c_d_id;
          memcpy(this->c_last,m_query->c_last,sizeof(this->c_last));
          this->h_amount = m_query->h_amount;
          this->by_last_name = m_query->by_last_name;
          break;
        case TPCC_NEWORDER0 :
          this->w_id = m_query->w_id;
          this->d_id = m_query->d_id;
          this->c_id = m_query->c_id;
          this->remote = m_query->remote;
          this->ol_cnt = m_query->ol_cnt;
          break;
        case TPCC_NEWORDER6 :
          this->ol_i_id = m_query->ol_i_id;
          break;
        case TPCC_NEWORDER8 :
          this->w_id = m_query->w_id;
          this->d_id = m_query->d_id;
          this->remote = m_query->remote;
          this->ol_i_id = m_query->ol_i_id;
          this->ol_supply_w_id = m_query->ol_supply_w_id;
          this->ol_quantity = m_query->ol_quantity;
          this->ol_number = m_query->ol_number;
          this->o_id = m_query->o_id;
          break;
        default: assert(false);
 
      }
      break;
    case RQRY_RSP:
      //if(m_query->rc == Abort || this->rc == WAIT || this->rc == WAIT_REM) {
      if(m_query->rc == Abort) {
        this->rc = m_query->rc;
        this->txn_rtype = TPCC_FIN;
      }
      break;
    case RFIN:
      assert(this->pid == m_query->pid);
      this->rc = m_query->rc;
      if(this->part_cnt == 0) {
        this->part_cnt = m_query->part_cnt;
        this->parts = m_query->parts;
      }
      break;
    case RACK:
      if(m_query->rc == Abort || this->rc == WAIT || this->rc == WAIT_REM) {
        this->rc = m_query->rc;
      }
    case RTXN:
      break;
    case RFWD:
      this->o_id = m_query->o_id;
      this->rtype = old_rtype;
      break;
    default:
      assert(false);
      break;
  }
  return this;

}

void tpcc_query::reset() {

  if(txn_type == TPCC_PAYMENT) {
    txn_rtype = TPCC_PAYMENT0;
  } else if(txn_type == TPCC_NEW_ORDER) {
    txn_rtype = TPCC_NEWORDER0;
  }
  /*
  switch(txn_type) {
    case TPCC_PAYMENT:
      txn_rtype = TPCC_PAYMENT0;
      break;
    case TPCC_NEW_ORDER:
      txn_rtype = TPCC_NEWORDER0;
      break;
    default:
      assert(false);
      break;
  }
  */
}

/*
uint64_t tpcc_query::get_keys(uint64_t *& arr) {
  uint64_t num_keys = txn_type == TPCC_PAYMENT ? 4 : 3 + ol_cnt;
  uint64_t keys[num_keys];
  uint64_t n = 0;

  uint64_t w_key = w_id;
  keys[n++] = w_key;
  uint64_t d_key = txn_type == TPCC_PAYMENT ? distKey(d_id, d_w_id) : distKey(d_id, w_id);
  keys[n++] = d_key;
  uint64_t cnp_key = txn_type == TPCC_PAYMENT ? custNPKey(c_last, c_d_id, c_w_id) : UINT64_MAX;
  if(txn_type == TPCC_PAYMENT)
    keys[n++] = cnp_key;
  uint64_t c_key =  txn_type == TPCC_PAYMENT ? custKey(c_id, c_d_id, c_w_id) : custKey(c_id, d_id, w_id);
  keys[n++] = c_key;

  if(txn_type == TPCC_NEWORDER) {
    for(uint64_t i = 0; i < ol_cnt; i++) {
      // item table is replicated at every node
      //uint64_t ol_key = items[i].ol_i_id;
      uint64_t s_key = stockKey(items[i].ol_i_id, items[i].ol_supply_w_id);
      keys[n++] = s_key;
    }
  }

  arr = keys;
  return size;
}
*/


void tpcc_client_query::gen_payment(uint64_t thd_id) {
	txn_type = TPCC_PAYMENT;
	if (FIRST_PART_LOCAL) {
    while(wh_to_part(w_id = URand(1, g_num_wh)) != thd_id) {}
		//w_id = thd_id % g_num_wh + 1;
  }
	else
		w_id = URand(1, g_num_wh);
	d_w_id = w_id;

	uint64_t part_id = wh_to_part(w_id);
	part_to_access[0] = part_id;
	part_num = 1;

	d_id = URand(1, g_dist_per_wh);
	h_amount = URand(1, 5000);
  rbk = false;
	double x = (double)(rand() % 10000) / 10000;
	int y = URand(1, 100);

	//if(x > g_mpr) { 
	if(x > 0.15) { 
		// home warehouse
		c_d_id = d_id;
		c_w_id = w_id;
	} else {	
		// remote warehouse
		c_d_id = URand(1, g_dist_per_wh);
		if(g_num_wh > 1) {
			while((c_w_id = URand(1, g_num_wh)) == w_id) {}
			if (wh_to_part(w_id) != wh_to_part(c_w_id)) {
				part_to_access[1] = wh_to_part(c_w_id);
				part_num = 2;
			}
		} else 
			c_w_id = w_id;
	}
	if(y <= 60) {
		// by last name
		by_last_name = true;
		Lastname(NURand(255,0,999),c_last);
	} else {
		// by cust id
		by_last_name = false;
		c_id = NURand(1023, 1, g_cust_per_dist);
	}
}

void tpcc_client_query::gen_new_order(uint64_t thd_id) {
	txn_type = TPCC_NEW_ORDER;
	if (FIRST_PART_LOCAL) {
    while(wh_to_part(w_id = URand(1, g_num_wh)) != thd_id) {}
		//w_id = thd_id % g_num_wh + 1;
  }
	else
		w_id = URand(1, g_num_wh);
	d_id = URand(1, g_dist_per_wh);
	c_id = NURand(1023, 1, g_cust_per_dist);
  // FIXME: No rollback
	//rbk = URand(1, 100) == 1 ? true : false;
	rbk = false;
	ol_cnt = URand(5, g_max_items_per_txn);
	o_entry_d = 2013;
	items = (Item_no *) mem_allocator.alloc(sizeof(Item_no) * ol_cnt, thd_id);
	part_to_access[0] = wh_to_part(w_id);
	part_num = 1;

  double r_mpr = (double)(rand() % 10000) / 10000;
  uint64_t part_limit;
  if(r_mpr < g_mpr)
    part_limit = g_part_per_txn;
  else
    part_limit = 1;
  //printf("\nr_mpr: %f/%f,g_mpitem: %f, limit: %ld\n",r_mpr,g_mpr,g_mpitem,part_limit);


	for (UInt32 oid = 0; oid < ol_cnt; oid ++) {

		while(1) {
			UInt32 i;
#if CONTENTION
			items[oid].ol_i_id = 2000 + oid;
#else
			items[oid].ol_i_id = NURand(8191, 1, g_max_items);
#endif
			for (i = 0; i < oid; i++) {
				if (items[i].ol_i_id == items[oid].ol_i_id) {
					break;
				}
			}
			if(i == oid)
				break;
		} 

		//UInt32 x = URand(1, 100);

    double r_rem = (double)(rand() % 100000) / 100000;
    //printf("rr: %f, ",r_rem);
		//if (r_rem > g_mpitem || r_mpr > g_mpr || g_num_wh == 1) {
		if (r_rem > 0.01 || r_mpr > g_mpr || g_num_wh == 1) {
			// home warehouse
			items[oid].ol_supply_w_id = w_id;
		}
		else  {
      while(1) {
        items[oid].ol_supply_w_id = URand(1, g_num_wh);
        //printf("%d: %ld,",oid,items[oid].ol_supply_w_id);
        if(items[oid].ol_supply_w_id == w_id)
          continue;
        uint32_t j;
        for(j = 0; j < part_num; j++) {
          if(part_to_access[j] == wh_to_part(items[oid].ol_supply_w_id))
            break;
        }
        if( j < part_num)
          break;
        if( part_num < part_limit ) {
          part_to_access[part_num++] = wh_to_part(items[oid].ol_supply_w_id);
          break;
        }
        else 
          continue;
      }
		}
		items[oid].ol_quantity = URand(1, 10);
	}
	// Remove duplicate items
  /*
	for (UInt32 i = 0; i < ol_cnt; i ++) {
		for (UInt32 j = 0; j < i; j++) {
			if (items[i].ol_i_id == items[j].ol_i_id) {
				for (UInt32 k = i; k < ol_cnt - 1; k++)
					items[k] = items[k + 1];
				ol_cnt --;
				i--;
			}
		}
	}
  */
	for (UInt32 i = 0; i < ol_cnt; i ++) {
		for (UInt32 j = 0; j < i; j++) 
			assert(items[i].ol_i_id != items[j].ol_i_id);
	}

	// update part_to_access
  /*
	for (UInt32 i = 0; i < ol_cnt; i ++) {
		UInt32 j;
		for (j = 0; j < part_num; j++ ) 
			if (part_to_access[j] == wh_to_part(items[i].ol_supply_w_id))
				break;
		if (j == part_num) // not found! add to it.
		part_to_access[part_num ++] = wh_to_part( items[i].ol_supply_w_id );
	}
  */

}

/*
void 
tpcc_query::gen_order_status(uint64_t thd_id) {
	txn_type = TPCC_ORDER_STATUS;
	//txn_rtype = TPCC_ORDER_STATUS0;
	if (FIRST_PART_LOCAL)
		w_id = thd_id % g_num_wh + 1;
	else
		w_id = URand(1, g_num_wh);
	d_id = URand(1, g_dist_per_wh);
	c_w_id = w_id;
	c_d_id = d_id;
	int y = URand(1, 100);
	if(y <= 60) {
		// by last name
		by_last_name = true;
		Lastname(NURand(255,0,999),c_last);
	} else {
		// by cust id
		by_last_name = false;
		c_id = NURand(1023, 1, g_cust_per_dist);
	}
}
*/

/*
void 
tpcc_query::gen_delivery(uint64_t thd_id) {
/	type = TPCC_DELIVERY;
//	if (FIRST_PART_LOCAL)
		w_id = thd_id % g_num_wh + 1;
//	else
//		w_id = URand(1, g_num_wh);
	o_carrier_id = URand(1, 10);
	ol_delivery_d = 2014;
}
*/
//uint64_t tpcc_query::wh_to_part(uint64_t wid) {
//	uint64_t part_id;
//	assert(g_part_cnt <= g_num_wh);
//	part_id = wid % g_part_cnt;
//	return part_id;
//}
