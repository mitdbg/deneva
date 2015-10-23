#include "tpcc.h"
#include "tpcc_query.h"
#include "tpcc_helper.h"
#include "query.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_btree.h"
#include "tpcc_const.h"
#include "remote_query.h"
#include "transport.h"

void tpcc_txn_man::init(workload * h_wl) {
	txn_man::init(h_wl);
	_wl = (tpcc_wl *) h_wl;
	_qry = new tpcc_query;
  rc = RCOK;
}

void tpcc_txn_man::merge_txn_rsp(base_query * query1, base_query * query2) {
	tpcc_query * m_query1 = (tpcc_query *) query1;
	tpcc_query * m_query2 = (tpcc_query *) query2;

  if(m_query1->rc == Abort) {
    m_query2->rc = m_query1->rc;
    m_query2->txn_rtype = TPCC_FIN;
  }

}

RC tpcc_txn_man::run_txn(base_query * query) {
	//tpcc_query * m_query = (tpcc_query *) query;
  //assert(query->rc == RCOK);
#if MODE == SETUP_MODE
  return RCOK;
#endif
  RC rc = RCOK;
  rem_done = false;
  fin = false;
  uint64_t thd_prof_start = get_sys_clock();

#if CC_ALG == CALVIN
  assert(false);
  return rc;
#endif
  // Resume query after hold
  if(query->rc == WAIT_REM) {
    rtn_tpcc_state(query);
  }

  if((CC_ALG != HSTORE && CC_ALG != HSTORE_SPEC) && this->rc == WAIT) {
    assert(query->rc == WAIT || query->rc == RCOK);
    get_row_post_wait(row);
    next_tpcc_state(query);
    this->rc = RCOK;
  }

  do {
    rc = run_txn_state(query);
    if(rc != RCOK)
      break;
    next_tpcc_state(query);
  } while(!fin && !rem_done);

  assert(rc != WAIT_REM || GET_NODE_ID(query->pid) == g_node_id);
  INC_STATS(get_thd_id(),thd_prof_wl1,get_sys_clock() - thd_prof_start);

  return rc;

}

void tpcc_txn_man::rtn_tpcc_state(base_query * query) {
	tpcc_query * m_query = (tpcc_query *) query;

  switch(m_query->txn_rtype) {
    case TPCC_PAYMENT0:
    case TPCC_PAYMENT1:
    case TPCC_PAYMENT2:
    case TPCC_PAYMENT3:
      m_query->txn_rtype = TPCC_PAYMENT4;
      break;
    case TPCC_PAYMENT4:
    case TPCC_PAYMENT5:
      m_query->txn_rtype = TPCC_FIN;
      break;
    case TPCC_NEWORDER0:
    case TPCC_NEWORDER1:
    case TPCC_NEWORDER2:
    case TPCC_NEWORDER3:
    case TPCC_NEWORDER4:
    case TPCC_NEWORDER5:
      if(m_query->ol_number < m_query->ol_cnt) {
        m_query->txn_rtype = TPCC_NEWORDER6;
		    m_query->ol_i_id = m_query->items[m_query->ol_number].ol_i_id;
		    m_query->ol_supply_w_id = m_query->items[m_query->ol_number].ol_supply_w_id;
		    m_query->ol_quantity = m_query->items[m_query->ol_number].ol_quantity;
      }
      else {
        m_query->txn_rtype = TPCC_FIN;
      }
      break;
    case TPCC_NEWORDER6: // loop pt 1
    case TPCC_NEWORDER7:
      m_query->txn_rtype = TPCC_NEWORDER8;
      break;
    case TPCC_NEWORDER8: // loop pt 2
    case TPCC_NEWORDER9:
      m_query->ol_number = m_query->ol_number+1;
      if(m_query->ol_number < m_query->ol_cnt) {
        m_query->txn_rtype = TPCC_NEWORDER6;
		    m_query->ol_i_id = m_query->items[m_query->ol_number].ol_i_id;
		    m_query->ol_supply_w_id = m_query->items[m_query->ol_number].ol_supply_w_id;
		    m_query->ol_quantity = m_query->items[m_query->ol_number].ol_quantity;
      }
      else {
        m_query->txn_rtype = TPCC_FIN;
      }
      break;
    case TPCC_FIN:
      break;
    default:
      assert(false);
  }


}

void tpcc_txn_man::next_tpcc_state(base_query * query) {
	tpcc_query * m_query = (tpcc_query *) query;

  switch(m_query->txn_rtype) {
    case TPCC_PAYMENT_S:
      m_query->txn_rtype = TPCC_PAYMENT0;
      break;
    case TPCC_PAYMENT0:
      m_query->txn_rtype = TPCC_PAYMENT1;
      break;
    case TPCC_PAYMENT1:
      m_query->txn_rtype = TPCC_PAYMENT2;
      break;
    case TPCC_PAYMENT2:
      m_query->txn_rtype = TPCC_PAYMENT3;
      break;
    case TPCC_PAYMENT3:
      if(GET_NODE_ID(m_query->pid) != g_node_id) {
        rem_done = true;
        break;
      }
      m_query->txn_rtype = TPCC_PAYMENT4;
      break;
    case TPCC_PAYMENT4:
      m_query->txn_rtype = TPCC_PAYMENT5;
      break;
    case TPCC_PAYMENT5:
      if(GET_NODE_ID(m_query->pid) != g_node_id) {
        rem_done = true;
        break;
      }
      m_query->txn_rtype = TPCC_FIN;
      break;
    case TPCC_NEWORDER_S:
      m_query->ol_number = 0;
      m_query->txn_rtype = TPCC_NEWORDER0;
      break;
    case TPCC_NEWORDER0:
      m_query->ol_number = 0;
      if(GET_NODE_ID(m_query->pid) == g_node_id) 
        m_query->txn_rtype = TPCC_NEWORDER1;
      break;
    case TPCC_NEWORDER1:
      m_query->txn_rtype = TPCC_NEWORDER2;
      break;
    case TPCC_NEWORDER2:
      m_query->txn_rtype = TPCC_NEWORDER3;
      break;
    case TPCC_NEWORDER3:
      m_query->txn_rtype = TPCC_NEWORDER4;
      break;
    case TPCC_NEWORDER4:
      m_query->txn_rtype = TPCC_NEWORDER5;
      break;
    case TPCC_NEWORDER5:
      if(GET_NODE_ID(m_query->pid) != g_node_id) {
        rem_done = true;
        break;
      }
      if(m_query->ol_number < m_query->ol_cnt) {
        m_query->txn_rtype = TPCC_NEWORDER6;
		    m_query->ol_i_id = m_query->items[m_query->ol_number].ol_i_id;
		    m_query->ol_supply_w_id = m_query->items[m_query->ol_number].ol_supply_w_id;
		    m_query->ol_quantity = m_query->items[m_query->ol_number].ol_quantity;
      }
      else {
        m_query->txn_rtype = TPCC_FIN;
      }
      break;
    case TPCC_NEWORDER6: // loop pt 1
      m_query->txn_rtype = TPCC_NEWORDER7;
      break;
    case TPCC_NEWORDER7:
      if(GET_NODE_ID(m_query->pid) != g_node_id) 
        rem_done = true;
      m_query->txn_rtype = TPCC_NEWORDER8;
      break;
    case TPCC_NEWORDER8: // loop pt 2
      m_query->txn_rtype = TPCC_NEWORDER9;
      break;
    case TPCC_NEWORDER9:
      if(GET_NODE_ID(m_query->pid) != g_node_id) {
        rem_done = true;
        break;
      }
      m_query->ol_number = m_query->ol_number+1;
      if(m_query->ol_number < m_query->ol_cnt) {
        m_query->txn_rtype = TPCC_NEWORDER6;
		    m_query->ol_i_id = m_query->items[m_query->ol_number].ol_i_id;
		    m_query->ol_supply_w_id = m_query->items[m_query->ol_number].ol_supply_w_id;
		    m_query->ol_quantity = m_query->items[m_query->ol_number].ol_quantity;
        //m_query->rc = RCOK; // ??
      }
      else {
        m_query->txn_rtype = TPCC_FIN;
      }
      break;
    case TPCC_FIN:
      break;
    default:
      assert(false);
  }

}

RC tpcc_txn_man::run_txn_state(base_query * query) {
	tpcc_query * m_query = (tpcc_query *) query;
	uint64_t w_id = m_query->w_id;
  uint64_t d_id = m_query->d_id;
  uint64_t c_id = m_query->c_id;
  uint64_t d_w_id = m_query->d_w_id;
  uint64_t c_w_id = m_query->c_w_id;
  uint64_t c_d_id = m_query->c_d_id;
	char * c_last = m_query->c_last;
  double h_amount = m_query->h_amount;
	bool by_last_name = m_query->by_last_name;
	bool remote = m_query->remote;
	uint64_t ol_cnt = m_query->ol_cnt;
	uint64_t o_entry_d = m_query->o_entry_d;
	uint64_t ol_i_id = m_query->ol_i_id;
	uint64_t ol_supply_w_id = m_query->ol_supply_w_id;
	uint64_t ol_quantity = m_query->ol_quantity;
	uint64_t ol_number = m_query->ol_number;
  uint64_t o_id = m_query->o_id;

	uint64_t part_id_w = wh_to_part(w_id);
	uint64_t part_id_c_w = wh_to_part(c_w_id);
  uint64_t part_id_ol_supply_w = wh_to_part(ol_supply_w_id);
  bool w_loc = GET_NODE_ID(part_id_w) == get_node_id();
  bool c_w_loc = GET_NODE_ID(part_id_c_w) == get_node_id();
  bool ol_supply_w_loc = GET_NODE_ID(part_id_ol_supply_w) == get_node_id();

	RC rc = RCOK;

	switch (m_query->txn_rtype) {
		case TPCC_PAYMENT0 :
      if(w_loc)
			  rc = run_payment_0(w_id, d_id, d_w_id, h_amount, row);
      else {
        assert(GET_NODE_ID(m_query->pid) == g_node_id);

        this->rem_row_cnt++;

        query->dest_id = GET_NODE_ID(part_id_w);
        query->rem_req_state = TPCC_PAYMENT0;
        rc = WAIT_REM;
      }
			break;
		case TPCC_PAYMENT1 :
			rc = run_payment_1(w_id, d_id, d_w_id, h_amount, row);
      break;
		case TPCC_PAYMENT2 :
			rc = run_payment_2(w_id, d_id, d_w_id, h_amount, row);
      break;
		case TPCC_PAYMENT3 :
			rc = run_payment_3(w_id, d_id, d_w_id, h_amount, row);
      break;
		case TPCC_PAYMENT4 :
      if(c_w_loc)
			  rc = run_payment_4( w_id,  d_id, c_id, c_w_id,  c_d_id, c_last, h_amount, by_last_name, row); 
      else {
        assert(GET_NODE_ID(m_query->pid) == g_node_id);
        this->rem_row_cnt++;
        query->dest_id = GET_NODE_ID(part_id_c_w);
        query->rem_req_state = TPCC_PAYMENT4;
        rc = WAIT_REM;
      }
			break;
		case TPCC_PAYMENT5 :
			rc = run_payment_5( w_id,  d_id, c_id, c_w_id,  c_d_id, c_last, h_amount, by_last_name, row); 
      break;
		case TPCC_NEWORDER0 :
      if(w_loc)
			  rc = new_order_0( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &m_query->o_id, row); 
      else {
		    //rem_qry_man.remote_qry(query,TPCC_NEWORDER0,GET_NODE_ID(part_id_w),this);
        assert(GET_NODE_ID(m_query->pid) == g_node_id);
        this->rem_row_cnt++;
        query->dest_id = GET_NODE_ID(part_id_w);
        query->rem_req_state = TPCC_NEWORDER0;
        rc = WAIT_REM;
      }
			break;
		case TPCC_NEWORDER1 :
			rc = new_order_1( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &m_query->o_id, row); 
      break;
		case TPCC_NEWORDER2 :
			rc = new_order_2( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &m_query->o_id, row); 
      break;
		case TPCC_NEWORDER3 :
			rc = new_order_3( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &m_query->o_id, row); 
      break;
		case TPCC_NEWORDER4 :
			rc = new_order_4( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &m_query->o_id, row); 
      break;
		case TPCC_NEWORDER5 :
			rc = new_order_5( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &m_query->o_id, row); 
      break;
		case TPCC_NEWORDER6 :
			rc = new_order_6(ol_i_id, row);
			break;
		case TPCC_NEWORDER7 :
			rc = new_order_7(ol_i_id, row);
			break;
		case TPCC_NEWORDER8 :
      if(ol_supply_w_loc) {
			  rc = new_order_8( w_id, d_id, remote, ol_i_id, ol_supply_w_id, ol_quantity,  ol_number, o_id, row); 
      }
      else {
			  //rem_qry_man.remote_qry(query,TPCC_NEWORDER8,GET_NODE_ID(part_id_ol_supply_w),this);
        assert(GET_NODE_ID(m_query->pid) == g_node_id);
        this->rem_row_cnt++;
        query->dest_id = GET_NODE_ID(part_id_ol_supply_w);
        query->rem_req_state = TPCC_NEWORDER8;
        rc = WAIT_REM;
      }
			break;
		case TPCC_NEWORDER9 :
			rc = new_order_9( w_id, d_id, remote, ol_i_id, ol_supply_w_id, ol_quantity,  ol_number, o_id, row); 
      break;
    case TPCC_FIN :
      fin = true;
      query->rem_req_state = TPCC_FIN;
      assert(GET_NODE_ID(m_query->pid) == g_node_id);
      if(m_query->rbk)
        query->rc = Abort;
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
    query->rem_req_state = TPCC_FIN;
    rc = finish(m_query,false);
    if(rc == RCOK)
      rc = m_query->rc;
  }
  return rc;
}

inline RC tpcc_txn_man::run_payment_0(uint64_t w_id, uint64_t d_id, uint64_t d_w_id, double h_amount, row_t *& r_wh_local) {

	uint64_t key;
	itemid_t * item;
/*====================================================+
    	EXEC SQL UPDATE warehouse SET w_ytd = w_ytd + :h_amount
		WHERE w_id=:w_id;
	+====================================================*/
	/*===================================================================+
		EXEC SQL SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name
		INTO :w_street_1, :w_street_2, :w_city, :w_state, :w_zip, :w_name
		FROM warehouse
		WHERE w_id=:w_id;
	+===================================================================*/


  RC rc;
	key = w_id;
	INDEX * index = _wl->i_warehouse; 
	item = index_read(index, key, wh_to_part(w_id));
	assert(item != NULL);
	row_t * r_wh = ((row_t *)item->location);
	if (g_wh_update)
		rc = get_row(r_wh, WR, r_wh_local);
	else 
		rc = get_row(r_wh, RD, r_wh_local);

  if(rc == WAIT)
    INC_STATS_ARR(0,w_cflt,key);
  if(rc == Abort)
    INC_STATS_ARR(0,w_abrt,key);
  return rc;
}

inline RC tpcc_txn_man::run_payment_1(uint64_t w_id, uint64_t d_id, uint64_t d_w_id, double h_amount, row_t * r_wh_local) {

  assert(r_wh_local != NULL);
/*====================================================+
    	EXEC SQL UPDATE warehouse SET w_ytd = w_ytd + :h_amount
		WHERE w_id=:w_id;
	+====================================================*/
	/*===================================================================+
		EXEC SQL SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name
		INTO :w_street_1, :w_street_2, :w_city, :w_state, :w_zip, :w_name
		FROM warehouse
		WHERE w_id=:w_id;
	+===================================================================*/


	double w_ytd;
	r_wh_local->get_value(W_YTD, w_ytd);
	if (g_wh_update) {
		r_wh_local->set_value(W_YTD, w_ytd + h_amount);
	}
  return RCOK;
}

inline RC tpcc_txn_man::run_payment_2(uint64_t w_id, uint64_t d_id, uint64_t d_w_id, double h_amount, row_t *& r_dist_local) {
	/*=====================================================+
		EXEC SQL UPDATE district SET d_ytd = d_ytd + :h_amount
		WHERE d_w_id=:w_id AND d_id=:d_id;
	+=====================================================*/
	uint64_t key;
	itemid_t * item;
	key = distKey(d_id, d_w_id);
	item = index_read(_wl->i_district, key, wh_to_part(w_id));
	assert(item != NULL);
	row_t * r_dist = ((row_t *)item->location);
	RC rc = get_row(r_dist, WR, r_dist_local);
  if(rc == WAIT)
    INC_STATS_ARR(0,d_cflt,key);
  if(rc == Abort)
    INC_STATS_ARR(0,d_abrt,key);
  return rc;

}

inline RC tpcc_txn_man::run_payment_3(uint64_t w_id, uint64_t d_id, uint64_t d_w_id, double h_amount, row_t * r_dist_local) {
  assert(r_dist_local != NULL);

	/*=====================================================+
		EXEC SQL UPDATE district SET d_ytd = d_ytd + :h_amount
		WHERE d_w_id=:w_id AND d_id=:d_id;
	+=====================================================*/
	double d_ytd;
	r_dist_local->get_value(D_YTD, d_ytd);
	r_dist_local->set_value(D_YTD, d_ytd + h_amount);

	return RCOK;
}

inline RC tpcc_txn_man::run_payment_4(uint64_t w_id, uint64_t d_id,uint64_t c_id,uint64_t c_w_id, uint64_t c_d_id, char * c_last, double h_amount, bool by_last_name, row_t *& r_cust_local) { 
	/*====================================================================+
		EXEC SQL SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name
		INTO :d_street_1, :d_street_2, :d_city, :d_state, :d_zip, :d_name
		FROM district
		WHERE d_w_id=:w_id AND d_id=:d_id;
	+====================================================================*/

	itemid_t * item;
	uint64_t key;
	row_t * r_cust;
	if (by_last_name) { 
		/*==========================================================+
			EXEC SQL SELECT count(c_id) INTO :namecnt
			FROM customer
			WHERE c_last=:c_last AND c_d_id=:c_d_id AND c_w_id=:c_w_id;
		+==========================================================*/
		/*==========================================================================+
			EXEC SQL DECLARE c_byname CURSOR FOR
			SELECT c_first, c_middle, c_id, c_street_1, c_street_2, c_city, c_state,
			c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since
			FROM customer
			WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_last=:c_last
			ORDER BY c_first;
			EXEC SQL OPEN c_byname;
		+===========================================================================*/

		key = custNPKey(c_last, c_d_id, c_w_id);
		// XXX: the list is not sorted. But let's assume it's sorted... 
		// The performance won't be much different.
		INDEX * index = _wl->i_customer_last;
		item = index_read(index, key, wh_to_part(c_w_id));
		assert(item != NULL);
		
		int cnt = 0;
		itemid_t * it = item;
		itemid_t * mid = item;
		while (it != NULL) {
			cnt ++;
			it = it->next;
			if (cnt % 2 == 0)
				mid = mid->next;
		}
		r_cust = ((row_t *)mid->location);
		
		/*============================================================================+
			for (n=0; n<namecnt/2; n++) {
				EXEC SQL FETCH c_byname
				INTO :c_first, :c_middle, :c_id,
					 :c_street_1, :c_street_2, :c_city, :c_state, :c_zip,
					 :c_phone, :c_credit, :c_credit_lim, :c_discount, :c_balance, :c_since;
			}
			EXEC SQL CLOSE c_byname;
		+=============================================================================*/
		// XXX: we don't retrieve all the info, just the tuple we are interested in
	}

	else { // search customers by cust_id
		/*=====================================================================+
			EXEC SQL SELECT c_first, c_middle, c_last, c_street_1, c_street_2,
			c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim,
			c_discount, c_balance, c_since
			INTO :c_first, :c_middle, :c_last, :c_street_1, :c_street_2,
			:c_city, :c_state, :c_zip, :c_phone, :c_credit, :c_credit_lim,
			:c_discount, :c_balance, :c_since
			FROM customer
			WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_id=:c_id;
		+======================================================================*/
		key = custKey(c_id, c_d_id, c_w_id);
		INDEX * index = _wl->i_customer_id;
		item = index_read(index, key, wh_to_part(c_w_id));
		assert(item != NULL);
		r_cust = (row_t *) item->location;
	}

  	/*======================================================================+
	   	EXEC SQL UPDATE customer SET c_balance = :c_balance, c_data = :c_new_data
   		WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id;
   	+======================================================================*/
	RC rc  = get_row(r_cust, WR, r_cust_local);
  if(rc == WAIT) {
    if(by_last_name) {
      INC_STATS_ARR(0,cnp_cflt,key);
    } else
      INC_STATS_ARR(0,c_cflt,key);
  }
  if(rc == Abort) {
    if(by_last_name) {
      INC_STATS_ARR(0,cnp_abrt,key);
    } else
      INC_STATS_ARR(0,c_abrt,key);
  }

  return rc;
}


inline RC tpcc_txn_man::run_payment_5(uint64_t w_id, uint64_t d_id,uint64_t c_id,uint64_t c_w_id, uint64_t c_d_id, char * c_last, double h_amount, bool by_last_name, row_t * r_cust_local) { 
  assert(r_cust_local != NULL);
	double c_balance;
	double c_ytd_payment;
	double c_payment_cnt;

	r_cust_local->get_value(C_BALANCE, c_balance);
	r_cust_local->set_value(C_BALANCE, c_balance - h_amount);
	r_cust_local->get_value(C_YTD_PAYMENT, c_ytd_payment);
	r_cust_local->set_value(C_YTD_PAYMENT, c_ytd_payment + h_amount);
	r_cust_local->get_value(C_PAYMENT_CNT, c_payment_cnt);
	r_cust_local->set_value(C_PAYMENT_CNT, c_payment_cnt + 1);

	// FIXME? c_credit not used
	//char * c_credit = r_cust_local->get_value(C_CREDIT);

	/*=============================================================================+
	  EXEC SQL INSERT INTO
	  history (h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data)
	  VALUES (:c_d_id, :c_w_id, :c_id, :d_id, :w_id, :datetime, :h_amount, :h_data);
	  +=============================================================================*/
	row_t * r_hist;
	uint64_t row_id;
	// Which partition should we be inserting into?
	_wl->t_history->get_new_row(r_hist, wh_to_part(c_w_id), row_id);
	r_hist->set_value(H_C_ID, c_id);
	r_hist->set_value(H_C_D_ID, c_d_id);
	r_hist->set_value(H_C_W_ID, c_w_id);
	r_hist->set_value(H_D_ID, d_id);
	r_hist->set_value(H_W_ID, w_id);
	int64_t date = 2013;		
	r_hist->set_value(H_DATE, date);
	r_hist->set_value(H_AMOUNT, h_amount);
	insert_row(r_hist, _wl->t_history);

	return RCOK;
}



// new_order 0
inline RC tpcc_txn_man::new_order_0(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote, uint64_t  ol_cnt,uint64_t  o_entry_d, uint64_t * o_id, row_t *& r_wh_local) {
	uint64_t key;
	itemid_t * item;
	/*=======================================================================+
	EXEC SQL SELECT c_discount, c_last, c_credit, w_tax
		INTO :c_discount, :c_last, :c_credit, :w_tax
		FROM customer, warehouse
		WHERE w_id = :w_id AND c_w_id = w_id AND c_d_id = :d_id AND c_id = :c_id;
	+========================================================================*/
	key = w_id;
	INDEX * index = _wl->i_warehouse; 
	item = index_read(index, key, wh_to_part(w_id));
	assert(item != NULL);
	row_t * r_wh = ((row_t *)item->location);
  RC rc = get_row(r_wh, RD, r_wh_local);
  if(rc == WAIT)
    INC_STATS_ARR(0,w_cflt,key);
  if(rc == Abort)
    INC_STATS_ARR(0,w_abrt,key);
  return rc;
}

inline RC tpcc_txn_man::new_order_1(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote, uint64_t  ol_cnt,uint64_t  o_entry_d, uint64_t * o_id, row_t * r_wh_local) {
  assert(r_wh_local != NULL);
	double w_tax;
	r_wh_local->get_value(W_TAX, w_tax); 
  return RCOK;
}

inline RC tpcc_txn_man::new_order_2(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote, uint64_t  ol_cnt,uint64_t  o_entry_d, uint64_t * o_id, row_t *& r_cust_local) {
	uint64_t key;
	itemid_t * item;
	key = custKey(c_id, d_id, w_id);
	INDEX * index = _wl->i_customer_id;
	item = index_read(index, key, wh_to_part(w_id));
	assert(item != NULL);
	row_t * r_cust = (row_t *) item->location;
  RC rc = get_row(r_cust, RD, r_cust_local);
  if(rc == WAIT)
    INC_STATS_ARR(0,c_cflt,key);
  if(rc == Abort)
    INC_STATS_ARR(0,c_abrt,key);
  return rc;
}

inline RC tpcc_txn_man::new_order_3(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote, uint64_t  ol_cnt,uint64_t  o_entry_d, uint64_t * o_id, row_t * r_cust_local) {
  assert(r_cust_local != NULL);
	uint64_t c_discount;
	//char * c_last;
	//char * c_credit;
	r_cust_local->get_value(C_DISCOUNT, c_discount);
	//c_last = r_cust_local->get_value(C_LAST);
	//c_credit = r_cust_local->get_value(C_CREDIT);
  return RCOK;
}
 	
inline RC tpcc_txn_man::new_order_4(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote, uint64_t  ol_cnt,uint64_t  o_entry_d, uint64_t * o_id, row_t *& r_dist_local) {
	uint64_t key;
	itemid_t * item;
	/*==================================================+
	EXEC SQL SELECT d_next_o_id, d_tax
		INTO :d_next_o_id, :d_tax
		FROM district WHERE d_id = :d_id AND d_w_id = :w_id;
	EXEC SQL UPDATE d istrict SET d _next_o_id = :d _next_o_id + 1
		WH ERE d _id = :d_id AN D d _w _id = :w _id ;
	+===================================================*/
	key = distKey(d_id, w_id);
	item = index_read(_wl->i_district, key, wh_to_part(w_id));
	assert(item != NULL);
	row_t * r_dist = ((row_t *)item->location);
  RC rc = get_row(r_dist, WR, r_dist_local);
  if(rc == WAIT)
    INC_STATS_ARR(0,d_cflt,key);
  if(rc == Abort)
    INC_STATS_ARR(0,d_abrt,key);
  return rc;
}

inline RC tpcc_txn_man::new_order_5(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote, uint64_t  ol_cnt,uint64_t  o_entry_d, uint64_t * o_id, row_t * r_dist_local) {
  assert(r_dist_local != NULL);
	//double d_tax;
	//int64_t o_id;
	//d_tax = *(double *) r_dist_local->get_value(D_TAX);
  // Coordination Avoidance: Is this where the lock should be?
	*o_id = *(int64_t *) r_dist_local->get_value(D_NEXT_O_ID);
	(*o_id) ++;
	r_dist_local->set_value(D_NEXT_O_ID, *o_id);

	// return o_id
	/*========================================================================================+
	EXEC SQL INSERT INTO ORDERS (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)
		VALUES (:o_id, :d_id, :w_id, :c_id, :datetime, :o_ol_cnt, :o_all_local);
	+========================================================================================*/
	row_t * r_order;
	uint64_t row_id;
	_wl->t_order->get_new_row(r_order, wh_to_part(w_id), row_id);
	r_order->set_value(O_ID, *o_id);
	r_order->set_value(O_C_ID, c_id);
	r_order->set_value(O_D_ID, d_id);
	r_order->set_value(O_W_ID, w_id);
	r_order->set_value(O_ENTRY_D, o_entry_d);
	r_order->set_value(O_OL_CNT, ol_cnt);
	int64_t all_local = (remote? 0 : 1);
	r_order->set_value(O_ALL_LOCAL, all_local);
	insert_row(r_order, _wl->t_order);
	/*=======================================================+
    EXEC SQL INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id)
        VALUES (:o_id, :d_id, :w_id);
    +=======================================================*/
	row_t * r_no;
	_wl->t_neworder->get_new_row(r_no, wh_to_part(w_id), row_id);
	r_no->set_value(NO_O_ID, *o_id);
	r_no->set_value(NO_D_ID, d_id);
	r_no->set_value(NO_W_ID, w_id);
	insert_row(r_no, _wl->t_neworder);

	return RCOK;
}



// new_order 1
// Read from replicated read-only item table
inline RC tpcc_txn_man::new_order_6(uint64_t ol_i_id, row_t *& r_item_local) {
		uint64_t key;
		itemid_t * item;
		/*===========================================+
		EXEC SQL SELECT i_price, i_name , i_data
			INTO :i_price, :i_name, :i_data
			FROM item
			WHERE i_id = :ol_i_id;
		+===========================================*/
		key = ol_i_id;
		item = index_read(_wl->i_item, key, 0);
		assert(item != NULL);
		row_t * r_item = ((row_t *)item->location);

    RC rc = get_row(r_item, RD, r_item_local);
    if(rc == WAIT)
      INC_STATS_ARR(0,ol_cflt,key);
    if(rc == Abort)
      INC_STATS_ARR(0,ol_abrt,key);
    return rc;
}

inline RC tpcc_txn_man::new_order_7(uint64_t ol_i_id, row_t * r_item_local) {
  assert(r_item_local != NULL);
		int64_t i_price;
		//char * i_name;
		//char * i_data;
		
		r_item_local->get_value(I_PRICE, i_price);
		//i_name = r_item_local->get_value(I_NAME);
		//i_data = r_item_local->get_value(I_DATA);


	return RCOK;
}

// new_order 2
inline RC tpcc_txn_man::new_order_8(uint64_t w_id,uint64_t  d_id,bool remote, uint64_t ol_i_id, uint64_t ol_supply_w_id, uint64_t ol_quantity,uint64_t  ol_number,uint64_t  o_id, row_t *& r_stock_local) {
		uint64_t key;
		itemid_t * item;

		/*===================================================================+
		EXEC SQL SELECT s_quantity, s_data,
				s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05,
				s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10
			INTO :s_quantity, :s_data,
				:s_dist_01, :s_dist_02, :s_dist_03, :s_dist_04, :s_dist_05,
				:s_dist_06, :s_dist_07, :s_dist_08, :s_dist_09, :s_dist_10
			FROM stock
			WHERE s_i_id = :ol_i_id AND s_w_id = :ol_supply_w_id;
		EXEC SQL UPDATE stock SET s_quantity = :s_quantity
			WHERE s_i_id = :ol_i_id
			AND s_w_id = :ol_supply_w_id;
		+===============================================*/

		key = stockKey(ol_i_id, ol_supply_w_id);
		INDEX * index = _wl->i_stock;
		item = index_read(index, key, wh_to_part(ol_supply_w_id));
		assert(item != NULL);
		row_t * r_stock = ((row_t *)item->location);
    // In Coordination Avoidance, this record must be protected!
    RC rc = get_row(r_stock, WR, r_stock_local);
    if(rc == WAIT)
      INC_STATS_ARR(0,s_cflt,key);
    if(rc == Abort)
      INC_STATS_ARR(0,s_abrt,key);
    return rc;
}
		
inline RC tpcc_txn_man::new_order_9(uint64_t w_id,uint64_t  d_id,bool remote, uint64_t ol_i_id, uint64_t ol_supply_w_id, uint64_t ol_quantity,uint64_t  ol_number,uint64_t  o_id, row_t * r_stock_local) {
  assert(r_stock_local != NULL);
		// XXX s_dist_xx are not retrieved.
		UInt64 s_quantity;
		int64_t s_remote_cnt;
		s_quantity = *(int64_t *)r_stock_local->get_value(S_QUANTITY);
#if !TPCC_SMALL
		int64_t s_ytd;
		int64_t s_order_cnt;
		char * s_data;
		r_stock_local->get_value(S_YTD, s_ytd);
		r_stock_local->set_value(S_YTD, s_ytd + ol_quantity);
    // In Coordination Avoidance, this record must be protected!
		r_stock_local->get_value(S_ORDER_CNT, s_order_cnt);
		r_stock_local->set_value(S_ORDER_CNT, s_order_cnt + 1);
		s_data = r_stock_local->get_value(S_DATA);
#endif
		if (remote) {
			s_remote_cnt = *(int64_t*)r_stock_local->get_value(S_REMOTE_CNT);
			s_remote_cnt ++;
			r_stock_local->set_value(S_REMOTE_CNT, &s_remote_cnt);
		}
		uint64_t quantity;
		if (s_quantity > ol_quantity + 10) {
			quantity = s_quantity - ol_quantity;
		} else {
			quantity = s_quantity - ol_quantity + 91;
		}
		r_stock_local->set_value(S_QUANTITY, &quantity);

		/*====================================================+
		EXEC SQL INSERT
			INTO order_line(ol_o_id, ol_d_id, ol_w_id, ol_number,
				ol_i_id, ol_supply_w_id,
				ol_quantity, ol_amount, ol_dist_info)
			VALUES(:o_id, :d_id, :w_id, :ol_number,
				:ol_i_id, :ol_supply_w_id,
				:ol_quantity, :ol_amount, :ol_dist_info);
		+====================================================*/
		row_t * r_ol;
		uint64_t row_id;
		_wl->t_orderline->get_new_row(r_ol, wh_to_part(ol_supply_w_id), row_id);
		r_ol->set_value(OL_O_ID, &o_id);
		r_ol->set_value(OL_D_ID, &d_id);
		r_ol->set_value(OL_W_ID, &w_id);
		r_ol->set_value(OL_NUMBER, &ol_number);
		r_ol->set_value(OL_I_ID, &ol_i_id);
#if !TPCC_SMALL
		r_ol->set_value(OL_SUPPLY_W_ID, &ol_supply_w_id);
		r_ol->set_value(OL_QUANTITY, &ol_quantity);
		r_ol->set_value(OL_AMOUNT, &ol_amount);
#endif		
		insert_row(r_ol, _wl->t_orderline);

	return RCOK;
}

/*
void tpcc_txn_man::read_keys(base_query * query) {
	tpcc_query * m_query = (tpcc_query *) query;
  m_query->txn_type = query->t_type;
  uint64_t w_key;
  uint64_t d_key;
  uint64_t cnp_key;
  uint64_t c_key;
  int num_skeys = query->num_keys - 4;
  if(num_skeys < 0)
    num_skeys = 0;
  uint64_t s_key[num_skeys];

  uint64_t wr_set_size = 1+num_skeys;
  uint64_t wr_set[wr_set_size];
  uint64_t w = 0;
  
  uint64_t n = 0;

  w_key = query->keys[n++];
  d_key = query->keys[n++];
  wr_set[w++] = d_key;
  if((TPCCTxnType)query->t_type == TPCC_PAYMENT)
    cnp_key = query->keys[n++];
  c_key = query->keys[n++];
  for(int i = 0; i < num_skeys; i++) {
    s_key[i] = keys[n++];
    wr_set[w++] = s_key[i];
  }

  uint64_t w_part = wh_to_part(w_key);
  bool w_loc = GET_NODE_ID(w_part) == get_node_id();
  bool d_loc = GET_NODE_ID(wh_to_part(w_from_distKey(d_key))) == get_node_id();
  bool c_loc = GET_NODE_ID(wh_to_part(w_from_custKey(c_key))) == get_node_id();
  bool cnp_loc = GET_NODE_ID(wh_to_part(w_from_custNPKey(c_key))) == get_node_id();
  n = 0;
  void * data[];
  uint64_t sizes[];
  uint64_t wr_set_size;

  if(w_loc) {
    // index read, get row
		INDEX * index = _wl->i_warehouse;
		item = index_read(index, w_key, w_part);
	  assert(item != NULL);
	  row_t * r_wh = ((row_t *)item->location);
    RC rc = get_row(r_wh, RD, r_wh_local, wr_set, wr_set_size);
    assert(rc == RCOK);
    double w_tax;
	  r_wh_local->get_value(W_TAX, w_tax); 
    data[n] = w_tax;
    sizes[n++] = sizeof(double); 
    wr_set_size = r_wh_local->manager->get_wr_set_size();
    data[n] = wr_set_size;
    sizes[n++] = sizeof(double); 
    data[n] = r_wh_local->manager->get_wr_set();
    sizes[n++] = sizeof(double); 
  }
  if(d_loc) {
    // index read, get row
	  item = index_read(_wl->i_district, d_key, w_part);
	  assert(item != NULL);
	  row_t * r_dist = ((row_t *)item->location);
    RC rc = get_row(r_dist, WR, r_dist_local, wr_set, wr_set_size);
    // In Coordination Avoidance, this record must be protected!
	  *o_id = *(int64_t *) r_dist_local->get_value(D_NEXT_O_ID);
  }
  if(c_loc) {
    // index read, get row
	  INDEX * index = _wl->i_customer_id;
	  item = index_read(index, c_key, w_part);
	  assert(item != NULL);
	  row_t * r_cust = (row_t *) item->location;
    RC rc = get_row(r_cust, RD, r_cust_local, wr_set, wr_set_size);
	  uint64_t c_discount;
	  r_cust_local->get_value(C_DISCOUNT, c_discount);
    data[n] = c_discount;
    sizes[n++] = sizeof(uint64_t); 
  }
  if(m_query->txn_type == TPCC_PAYMENT && cnp_loc) {
    // index read, get row
  }
  for(int i = 0; i < num_skeys; i++) {
    uint64_t s_part = wh_to_part(w_from_stockKey(s_key[i]));
    bool s_loc = GET_NODE_ID(s_part) == get_node_id();
    if(s_loc) {
      // index read, get row
		  INDEX * index = _wl->i_stock;
		  item = index_read(index, s_key[i], s_part);
		  assert(item != NULL);
		  row_t * r_stock = ((row_t *)item->location);
      RC rc = get_row(r_stock, WR, r_stock_local, wr_set, wr_set_size);
		  UInt64 s_quantity;
		  s_quantity = *(int64_t *)r_stock_local->get_value(S_QUANTITY);
		  r_stock_local->get_value(S_YTD, s_ytd);
		  r_stock_local->get_value(S_ORDER_CNT, s_order_cnt);
		  s_data = r_stock_local->get_value(S_DATA);
		  if (remote) {
			  s_remote_cnt = *(int64_t*)r_stock_local->get_value(S_REMOTE_CNT);
        data[n] = s_remote_cnt;
        sizes[n++] = sizeof(int64_t); 
      }
      data[n] = s_quantity;
      sizes[n++] = sizeof(int64_t); 
      data[n] = s_ytd;
      sizes[n++] = sizeof(double); 
      data[n] = s_order_cnt;
    }
  }

  // Send data back with ts and write sets
}

void tpcc_txn_man::read_keys_again(base_query * query) {
	tpcc_query * m_query = (tpcc_query *) query;
}
*/

/*
void tpcc_txn_man::get_keys(base_query * query) {
	tpcc_query * m_query = (tpcc_query *) query;
  uint64_t num_keys = m_query->txn_type == TPCC_PAYMENT ? 4 : 3 + m_query->ol_cnt;
  uint64_t keys[num_keys];
  uint64_t n = 0;

  uint64_t w_key = m_query->w_id;
  keys[n++] = w_key;
  uint64_t d_key = m_query->txn_type == TPCC_PAYMENT ? distKey(m_query->d_id, m_query->d_w_id) : distKey(m_query->d_id, m_query->w_id);
  keys[n++] = d_key;
  uint64_t cnp_key = m_query->txn_type == TPCC_PAYMENT ? custNPKey(m_query->c_last, m_query->c_d_id, m_query->c_w_id) : UINT64_MAX;
  if(m_query->txn_type == TPCC_PAYMENT)
    keys[n++] = cnp_key;
  uint64_t c_key =  m_query->txn_type == TPCC_PAYMENT ? custKey(m_query->c_id, m_query->c_d_id, m_query->c_w_id) : custKey(m_query->c_id, m_query->d_id, m_query->w_id);
  keys[n++] = c_key;

  if(m_query->txn_type == TPCC_NEWORDER) {
    for(uint64_t i = 0; i < m_query->ol_cnt; i++) {
      // item table is replicated at every node
      //uint64_t ol_key = m_query->items[i].ol_i_id;
      uint64_t s_key = stockKey(m_query->items[i].ol_i_id, m_query->items[i].ol_supply_w_id);
      keys[n++] = s_key;
    }
  }

  // return keys and n

}
*/

// Check for potential conflict for same row by comparing keys
bool tpcc_txn_man::conflict(base_query * query1,base_query * query2) {
	tpcc_query * m_query1 = (tpcc_query *) query1;
	tpcc_query * m_query2 = (tpcc_query *) query2;

  uint64_t q1_w_key = m_query1->w_id;
  uint64_t q1_d_key = m_query1->txn_type == TPCC_PAYMENT ? distKey(m_query1->d_id, m_query1->d_w_id) : distKey(m_query1->d_id, m_query1->w_id);
  uint64_t q1_cnp_key = m_query1->txn_type == TPCC_PAYMENT ? custNPKey(m_query1->c_last, m_query1->c_d_id, m_query1->c_w_id) : UINT64_MAX;
  uint64_t q1_c_key =  m_query1->txn_type == TPCC_PAYMENT ? custKey(m_query1->c_id, m_query1->c_d_id, m_query1->c_w_id) : custKey(m_query1->c_id, m_query1->d_id, m_query1->w_id);
  uint64_t q2_w_key = m_query2->w_id;
  uint64_t q2_d_key = m_query2->txn_type == TPCC_PAYMENT ? distKey(m_query2->d_id, m_query2->d_w_id) : distKey(m_query2->d_id, m_query2->w_id);
  uint64_t q2_cnp_key = m_query2->txn_type == TPCC_PAYMENT ? custNPKey(m_query2->c_last, m_query2->c_d_id, m_query2->c_w_id) : UINT64_MAX;
  uint64_t q2_c_key =  m_query2->txn_type == TPCC_PAYMENT ? custKey(m_query2->c_id, m_query2->c_d_id, m_query2->c_w_id) : custKey(m_query2->c_id, m_query2->d_id, m_query2->w_id);

  if(q1_w_key == q2_w_key)
    return true;
  if(q1_d_key == q2_d_key)
    return true;
  if(q1_cnp_key == q2_cnp_key)
    return true;
  if(q1_c_key == q2_c_key)
    return true;

  if(m_query1->txn_type == TPCC_PAYMENT || m_query2->txn_type == TPCC_PAYMENT)
    return false;

  for(uint64_t i = 0; i < m_query1->ol_cnt; i++) {
    uint64_t q1_ol_key = m_query1->items[i].ol_i_id;
    uint64_t q1_s_key = stockKey(m_query1->items[i].ol_i_id, m_query1->items[i].ol_supply_w_id);
    for(uint64_t j = 0; j < m_query2->ol_cnt; j++) {
      uint64_t q2_ol_key = m_query2->items[j].ol_i_id;
      uint64_t q2_s_key = stockKey(m_query2->items[j].ol_i_id, m_query2->items[j].ol_supply_w_id);
      if(q1_ol_key == q2_ol_key)
        return true;
      if(q1_s_key == q2_s_key)
        return true;
    }
  }

  return false;
}


