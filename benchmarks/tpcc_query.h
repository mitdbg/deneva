#ifndef _TPCC_QUERY_H_
#define _TPCC_QUERY_H_

#include "global.h"
#include "helper.h"
#include "query.h"
#include "remote_query.h"

class workload;

// items of new order transaction
struct Item_no {
	uint64_t ol_i_id;
	uint64_t ol_supply_w_id;
	uint64_t ol_quantity;
};

enum TPCCRemTxnType {TPCC_PAYMENT0,TPCC_PAYMENT1,TPCC_NEWORDER0,TPCC_NEWORDER1,TPCC_NEWORDER2};

/*
class tpcc_r_query : public r_query {
public:
	void unpack(r_query * query, char * data);
	void pack(r_query * query, void ** data, int * sizes, int * num, RC rc);
	void remote_rsp(r_query * query, RC rc);
	uint64_t rtn_node_id;
	TPCCRemTxnType type;
	uint64_t w_id;
	uint64_t d_id;
	uint64_t c_id;
	uint64_t d_w_id;
	uint64_t c_w_id;
	uint64_t c_d_id;
	char c_last[LASTNAME_LEN];
	double h_amount;
	bool by_last_name;
	bool remote;
	uint64_t ol_cnt;
	uint64_t o_entry_d;
	uint64_t ol_i_id;
	uint64_t ol_supply_w_id;
	uint64_t ol_quantity;
	uint64_t ol_number;
	uint64_t o_id;

};
*/

class tpcc_query : public base_query {
public:
	void init(uint64_t thd_id, workload * h_wl);
	void remote_qry(base_query * query, int type,int dest_id);
	void remote_rsp(base_query * query, RC rc);
	void unpack(base_query * query, void * d);
	void unpack_rsp(base_query * query, void * d);
	void pack(base_query * query, void ** data, int * sizes, int * num, RC rc);
	uint64_t rtn_node_id;
	TPCCTxnType type;
	TPCCRemTxnType rtype;
	/**********************************************/	
	// common txn input for both payment & new-order
	/**********************************************/	
	uint64_t w_id;
	uint64_t d_id;
	uint64_t c_id;
	/**********************************************/	
	// txn input for payment
	/**********************************************/	
	uint64_t d_w_id;
	uint64_t c_w_id;
	uint64_t c_d_id;
	char c_last[LASTNAME_LEN];
	double h_amount;
	bool by_last_name;
	/**********************************************/	
	// txn input for new-order
	/**********************************************/
	Item_no * items;
	bool rbk;
	bool remote;
	uint64_t ol_cnt;
	uint64_t o_entry_d;
	// Input for delivery
	uint64_t o_carrier_id;
	uint64_t ol_delivery_d;
	// for order-status


	// Other
	uint64_t ol_i_id;
	uint64_t ol_supply_w_id;
	uint64_t ol_quantity;
	uint64_t ol_number;
	uint64_t o_id;

private:
	// warehouse id to partition id mapping
//	uint64_t wh_to_part(uint64_t wid);
	void gen_payment(uint64_t thd_id);
	void gen_new_order(uint64_t thd_id);
	void gen_order_status(uint64_t thd_id);
};

#endif
