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

void tpcc_txn_man::init(thread_t * h_thd, workload * h_wl, uint64_t thd_id) {
	txn_man::init(h_thd, h_wl, thd_id);
	_wl = (tpcc_wl *) h_wl;
}

void tpcc_txn_man::rem_txn_rsp(base_query * query) {
}

RC tpcc_txn_man::run_rem_txn(base_query * query) {
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

	RC rc = RCOK;

	switch (m_query->type) {
		case TPCC_PAYMENT0 :
			rc = run_payment_0(w_id, d_id, d_w_id, h_amount);
			break;
		case TPCC_PAYMENT1 :
			rc = run_payment_1( w_id,  d_id, c_id, c_w_id,  c_d_id, c_last, h_amount, by_last_name); 
			break;
		case TPCC_NEWORDER0 :
			rc = new_order_0( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &m_query->o_id); 
			break;
		case TPCC_NEWORDER1 :
			rc = new_order_1(ol_i_id);
			break;
		case TPCC_NEWORDER2 :
			rc = new_order_2( w_id, d_id, remote, ol_i_id, ol_supply_w_id, ol_quantity,  ol_number, o_id); 
			break;
		default:
			assert(false);
	}
	// return ack and any values to remote node
	m_query->remote_rsp(query,rc);
	//m_query->pack(query,data,sizes,&num, rc);
	return finish(rc);
}

RC tpcc_txn_man::run_txn(base_query * query) {
	tpcc_query * m_query = (tpcc_query *) query;
	switch (m_query->type) {
		case TPCC_PAYMENT :
			return run_payment(m_query); break;
		case TPCC_NEW_ORDER :
			return run_new_order(m_query); break;
/*		case TPCC_ORDER_STATUS :
			return run_order_status(m_query); break;
		case TPCC_DELIVERY :
			return run_delivery(m_query); break;
		case TPCC_STOCK_LEVEL :
			return run_stock_level(m_query); break;*/
		default:
			assert(false);
	}
}



RC tpcc_txn_man::run_payment(tpcc_query * query) {
	RC rc = RCOK;
	
	uint64_t part_id;
	uint64_t w_id = query->w_id;
  uint64_t d_id = query->d_id;
  uint64_t d_w_id = query->d_w_id;
  uint64_t c_w_id = query->c_w_id;
  uint64_t c_d_id = query->c_d_id;
  uint64_t c_id = query->c_id;
  double h_amount = query->h_amount;
	bool by_last_name = query->by_last_name;
	char * c_last = query->c_last;

	part_id = wh_to_part(w_id);
	printf("run_payment0 %ld:%ld -> %ld -- %ld\n",get_node_id(),get_thd_id(),part_id,GET_NODE_ID(part_id));

	if(GET_NODE_ID(part_id) == get_node_id()) 
		rc = run_payment_0(w_id, d_id, d_w_id, h_amount);
	else 
		rc = rem_qry_man.remote_qry(query,TPCC_PAYMENT0,GET_NODE_ID(part_id));
	if(rc != RCOK)
		return finish(rc);

	part_id = wh_to_part(c_w_id);
	printf("run_payment1 %ld:%ld -> %ld -- %ld\n",get_node_id(),get_thd_id(),part_id,GET_NODE_ID(part_id));
	if(GET_NODE_ID(part_id) == get_node_id())
		rc = run_payment_1( w_id,  d_id, c_id, c_w_id,  c_d_id, c_last, h_amount, by_last_name); 
	else
		rc = rem_qry_man.remote_qry(query,TPCC_PAYMENT1,GET_NODE_ID(part_id));
	if(rc != RCOK)
		return finish(rc);

	assert( rc == RCOK );
	return finish(rc);
}

RC tpcc_txn_man::run_new_order(tpcc_query * query) {
	RC rc = RCOK;
	
	uint64_t part_id;
	bool remote = query->remote;
	uint64_t w_id = query->w_id;
  uint64_t d_id = query->d_id;
  uint64_t c_id = query->c_id;
	uint64_t ol_cnt = query->ol_cnt;
	uint64_t o_entry_d = query->o_entry_d;
	uint64_t o_id;
	
	part_id = wh_to_part(w_id);
	printf("run_new_order0 %ld:%ld -> %ld -- %ld\n",get_node_id(),get_thd_id(),part_id,GET_NODE_ID(part_id));
	if(GET_NODE_ID(part_id) == get_node_id())
		rc = new_order_0( w_id, d_id, c_id, remote, ol_cnt, o_entry_d, &o_id); 
	else
		rc = rem_qry_man.remote_qry(query,TPCC_NEWORDER0,GET_NODE_ID(part_id));
	if(rc != RCOK)
		return finish(rc);

	o_id = query->o_id;

	for (UInt32 ol_number = 0; ol_number < ol_cnt; ol_number++) {

		uint64_t ol_i_id = query->items[ol_number].ol_i_id;
		uint64_t ol_supply_w_id = query->items[ol_number].ol_supply_w_id;
		uint64_t ol_quantity = query->items[ol_number].ol_quantity;
		/*
			part_id = 0;
			if(GET_NODE_ID(part_id) == get_node_id())
				rc = new_order_1(ol_i_id);
			else
				query->ol_i_id = ol_i_id;
				rc = rem_qry_man.remote_qry(query,TPCC_NEWORDER1,GET_NODE_ID(part_id));
			if(rc != RCOK)
				return finish(rc);

				*/
			part_id = wh_to_part(ol_supply_w_id);
			//printf("run_new_order2 %ld:%ld -> %ld -- %ld\n",get_node_id(),get_thd_id(),part_id,GET_NODE_ID(part_id));
			if(GET_NODE_ID(part_id) == get_node_id())
				rc = new_order_2( w_id, d_id, remote, ol_i_id, ol_supply_w_id, ol_quantity,  ol_number, o_id); 
			else {
				query->ol_i_id = ol_i_id;
				query->ol_supply_w_id = ol_supply_w_id;
				query->ol_quantity = ol_quantity;
				query->ol_number = ol_number;
				rc = rem_qry_man.remote_qry(query,TPCC_NEWORDER2,GET_NODE_ID(part_id));
			}
			if(rc != RCOK)
				return finish(rc);
		//uint64_t i_price;
		//double w_tax;
	}
	return finish(rc);
}

// run_payment 0
inline RC tpcc_txn_man::run_payment_0(uint64_t w_id, uint64_t d_id, uint64_t d_w_id, double h_amount) {

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


	key = w_id;
	INDEX * index = _wl->i_warehouse; 
	item = index_read(index, key, wh_to_part(w_id));
	assert(item != NULL);
	row_t * r_wh = ((row_t *)item->location);
	row_t * r_wh_local;
	if (g_wh_update)
		r_wh_local = get_row(r_wh, WR);
	else 
		r_wh_local = get_row(r_wh, RD);

	if (r_wh_local == NULL) {
		return finish(Abort);
	}
	double w_ytd;
	r_wh_local->get_value(W_YTD, w_ytd);
	if (g_wh_update) {
		r_wh_local->set_value(W_YTD, w_ytd + h_amount);
	}

	/*=====================================================+
		EXEC SQL UPDATE district SET d_ytd = d_ytd + :h_amount
		WHERE d_w_id=:w_id AND d_id=:d_id;
	+=====================================================*/
	key = distKey(d_id, d_w_id);
	item = index_read(_wl->i_district, key, wh_to_part(w_id));
	assert(item != NULL);
	row_t * r_dist = ((row_t *)item->location);
	row_t * r_dist_local = get_row(r_dist, WR);
	if (r_dist_local == NULL) {
		return finish(Abort);
	}

	double d_ytd;
	r_dist_local->get_value(D_YTD, d_ytd);
	r_dist_local->set_value(D_YTD, d_ytd + h_amount);

	return finish(RCOK);
}

//run_payment 1
inline RC tpcc_txn_man::run_payment_1(uint64_t w_id, uint64_t d_id,uint64_t c_id,uint64_t c_w_id, uint64_t c_d_id, char * c_last, double h_amount, bool by_last_name) { 
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
	row_t * r_cust_local = get_row(r_cust, WR);
	if (r_cust_local == NULL) {
		return finish(Abort);
	}
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

	return finish(RCOK);
}



// new_order 0
inline RC tpcc_txn_man::new_order_0(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote, uint64_t  ol_cnt,uint64_t  o_entry_d, uint64_t * o_id) {
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
	row_t * r_wh_local = get_row(r_wh, RD);
	if (r_wh_local == NULL) {
		return finish(Abort);
	}
	double w_tax;
	r_wh_local->get_value(W_TAX, w_tax); 

	key = custKey(c_id, d_id, w_id);
	index = _wl->i_customer_id;
	item = index_read(index, key, wh_to_part(w_id));
	assert(item != NULL);
	row_t * r_cust = (row_t *) item->location;
	row_t * r_cust_local = get_row(r_cust, RD);
	if (r_cust_local == NULL) {
		return finish(Abort); 
	}
	uint64_t c_discount;
	//char * c_last;
	//char * c_credit;
	r_cust_local->get_value(C_DISCOUNT, c_discount);
	//c_last = r_cust_local->get_value(C_LAST);
	//c_credit = r_cust_local->get_value(C_CREDIT);
 	
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
	row_t * r_dist_local = get_row(r_dist, WR);
	if (r_dist_local == NULL) {
		return finish(Abort);
	}
	//double d_tax;
	//int64_t o_id;
	//d_tax = *(double *) r_dist_local->get_value(D_TAX);
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

	return finish(RCOK);
}



// new_order 1
inline RC tpcc_txn_man::new_order_1(uint64_t ol_i_id) {
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

		row_t * r_item_local = get_row(r_item, RD);
		if (r_item_local == NULL) {
			return finish(Abort);
		}
		int64_t i_price;
		//char * i_name;
		//char * i_data;
		
		r_item_local->get_value(I_PRICE, i_price);
		//i_name = r_item_local->get_value(I_NAME);
		//i_data = r_item_local->get_value(I_DATA);


	return finish(RCOK);
}

// new_order 2
inline RC tpcc_txn_man::new_order_2(uint64_t w_id,uint64_t  d_id,bool remote, uint64_t ol_i_id, uint64_t ol_supply_w_id, uint64_t ol_quantity,uint64_t  ol_number,uint64_t  o_id) {
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

		row_t * r_item_local = get_row(r_item, RD);
		if (r_item_local == NULL) {
			return finish(Abort);
		}
		int64_t i_price;
		//char * i_name;
		//char * i_data;
		
		r_item_local->get_value(I_PRICE, i_price);
		//i_name = r_item_local->get_value(I_NAME);
		//i_data = r_item_local->get_value(I_DATA);


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
		row_t * r_stock_local = get_row(r_stock, WR);
		if (r_stock_local == NULL) {
			return finish(Abort);
		}
		
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

	return finish(RCOK);
}
