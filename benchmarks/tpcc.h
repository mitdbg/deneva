#ifndef _TPCC_H_
#define _TPCC_H_

#include "wl.h"
#include "txn.h"
#include "remote_query.h"
#include "query.h"
#include "row.h"

class table_t;
class INDEX;
class tpcc_query;

class tpcc_wl : public workload {
public:
	RC init();
	RC init_table();
	RC init_schema(const char * schema_file);
	RC get_txn_man(txn_man *& txn_manager);
	table_t * 		t_warehouse;
	table_t * 		t_district;
	table_t * 		t_customer;
	table_t *		t_history;
	table_t *		t_neworder;
	table_t *		t_order;
	table_t *		t_orderline;
	table_t *		t_item;
	table_t *		t_stock;

	INDEX * 	i_item;
	INDEX * 	i_warehouse;
	INDEX * 	i_district;
	INDEX * 	i_customer_id;
	INDEX * 	i_customer_last;
	INDEX * 	i_stock;
	INDEX * 	i_order; // key = (w_id, d_id, o_id)
//	INDEX * 	i_order_wdo; // key = (w_id, d_id, o_id)
//	INDEX * 	i_order_wdc; // key = (w_id, d_id, c_id)
	INDEX * 	i_orderline; // key = (w_id, d_id, o_id)
	INDEX * 	i_orderline_wd; // key = (w_id, d_id). 
	
	// XXX HACK
	// For delivary. Only one txn can be delivering a warehouse at a time.
	// *_delivering[warehouse_id] -> the warehouse is delivering.
	bool ** delivering;
//	bool volatile ** delivering;

private:
	uint64_t num_wh;
	void init_tab_item(int id);
	void init_tab_wh();
	void init_tab_dist(uint64_t w_id);
	void init_tab_stock(int id,uint64_t w_id);
	// init_tab_cust initializes both tab_cust and tab_hist.
	void init_tab_cust(int id, uint64_t d_id, uint64_t w_id);
	void init_tab_hist(uint64_t c_id, uint64_t d_id, uint64_t w_id);
	void init_tab_order(int id,uint64_t d_id, uint64_t w_id);
	
	UInt32 perm_count;
	uint64_t * perm_c_id;
	void init_permutation();
	uint64_t get_permutation();

	static void * threadInitItem(void * This);
	static void * threadInitWh(void * This);
	static void * threadInitDist(void * This);
	static void * threadInitStock(void * This);
	static void * threadInitCust(void * This);
	static void * threadInitHist(void * This);
	static void * threadInitOrder(void * This);
};

  struct thr_args{
    tpcc_wl * wl;
    UInt32 id;
    UInt32 tot;
  };

class tpcc_txn_man : public txn_man
{
public:
	void init( workload * h_wl);
  bool conflict(base_query * query1,base_query * query2);
  void read_keys(base_query * query){ assert(false);}; 
  RC acquire_locks(base_query * query); 
	RC run_txn(base_query * query);
	RC run_calvin_txn(base_query * query); 
	RC run_rem_txn(base_query * query);
  RC run_tpcc_phase2(base_query * query); 
  RC run_tpcc_phase5(base_query * query); 
	void rem_txn_rsp(base_query * rtn_query,base_query * loc_query); 
	void rem_txn_rsp(base_query * query); 
	//void pack(r_query * query, void ** data, int * sizes, int * num);
	//void unpack(r_query * query, char * data);
private:
	tpcc_wl * _wl;
	volatile RC _rc;
	tpcc_query * _qry;
  row_t * row;
  bool rem_done;
  bool fin;

void merge_txn_rsp(base_query * query1, base_query * query2);
//RC run_txn(base_query * query, bool rem);
void rtn_tpcc_state(base_query * query);
void next_tpcc_state(base_query * query);
RC run_txn_state(base_query * query);

	RC run_payment_0(uint64_t w_id, uint64_t d_id, uint64_t d_w_id, double h_amount, row_t *& r_wh_local);
	RC run_payment_1(uint64_t w_id, uint64_t d_id, uint64_t d_w_id, double h_amount, row_t * r_wh_local);
	RC run_payment_2(uint64_t w_id, uint64_t d_id, uint64_t d_w_id, double h_amount, row_t *& r_dist_local);
	RC run_payment_3(uint64_t w_id, uint64_t d_id, uint64_t d_w_id, double h_amount, row_t * r_dist_local);
	RC run_payment_4(uint64_t w_id, uint64_t d_id,uint64_t c_id,uint64_t c_w_id, uint64_t c_d_id, char * c_last, double h_amount, bool by_last_name, row_t *& r_cust_local); 
	RC run_payment_5(uint64_t w_id, uint64_t d_id,uint64_t c_id,uint64_t c_w_id, uint64_t c_d_id, char * c_last, double h_amount, bool by_last_name, row_t * r_cust_local); 
	RC new_order_0(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote, uint64_t  ol_cnt,uint64_t  o_entry_d, uint64_t * o_id, row_t *& r_wh_local);
	RC new_order_1(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote, uint64_t  ol_cnt,uint64_t  o_entry_d, uint64_t * o_id, row_t * r_wh_local);
	RC new_order_2(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote, uint64_t  ol_cnt,uint64_t  o_entry_d, uint64_t * o_id, row_t *& r_cust_local);
	RC new_order_3(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote, uint64_t  ol_cnt,uint64_t  o_entry_d, uint64_t * o_id, row_t * r_cust_local);
	RC new_order_4(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote, uint64_t  ol_cnt,uint64_t  o_entry_d, uint64_t * o_id, row_t *& r_dist_local);
	RC new_order_5(uint64_t w_id, uint64_t d_id, uint64_t c_id, bool remote, uint64_t  ol_cnt,uint64_t  o_entry_d, uint64_t * o_id, row_t * r_dist_local);
	RC new_order_6(uint64_t ol_i_id, row_t *& r_item_local);
	RC new_order_7(uint64_t ol_i_id, row_t * r_item_local);
	RC new_order_8(uint64_t w_id,uint64_t  d_id,bool remote, uint64_t ol_i_id, uint64_t ol_supply_w_id, uint64_t ol_quantity,uint64_t  ol_number,uint64_t  o_id, row_t *& r_stock_local);
	RC new_order_9(uint64_t w_id,uint64_t  d_id,bool remote, uint64_t ol_i_id, uint64_t ol_supply_w_id, uint64_t ol_quantity,uint64_t  ol_number,uint64_t ol_amount, uint64_t  o_id, row_t * r_stock_local);
	RC run_order_status(tpcc_query * query);
	RC run_delivery(tpcc_query * query);
	RC run_stock_level(tpcc_query * query);
};

#endif
