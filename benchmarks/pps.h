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

#ifndef _PPS_H_
#define _PPS_H_

#include "wl.h"
#include "txn.h"
#include "query.h"
#include "row.h"

class PPSQuery;
class PPSQueryMessage;
struct Item_no;

class table_t;
class INDEX;
class PPSQuery;
enum PPSRemTxnType {
  PPS_PAYMENT_S=0,
  PPS_PAYMENT0,
  PPS_NEWORDER_S,
  PPS_NEWORDER0,
  PPS_FIN,
  PPS_RDONE};


class PPSWorkload : public Workload {
public:
	RC init();
	RC init_table();
	RC init_schema(const char * schema_file);
	RC get_txn_man(TxnManager *& txn_manager);
	table_t * 		t_suppliers;
	table_t * 		t_products;
	table_t * 		t_parts;
	table_t * 		t_supplies;
	table_t * 		t_uses;

	INDEX * 	i_supplies;
	INDEX * 	i_uses;
	
private:
	void init_tab_suppliers();
	void init_tab_products();
	void init_tab_parts();
	void init_tab_supplies();
	void init_tab_uses();
	
	static void * threadInitSuppliers(void * This);
	static void * threadInitProducts(void * This);
	static void * threadInitParts(void * This);
	static void * threadInitSupplies(void * This);
	static void * threadInitUses(void * This);
};

  struct thr_args{
    PPSWorkload * wl;
    UInt32 id;
    UInt32 tot;
  };

class PPSTxnManager : public TxnManager
{
public:
	void init(uint64_t thd_id, Workload * h_wl);
  void reset();
  RC acquire_locks(); 
	RC run_txn();
	RC run_txn_post_wait();
	RC run_calvin_txn(); 
  RC run_pps_phase2(); 
  RC run_pps_phase5(); 
	PPSRemTxnType state;
  void copy_remote_items(PPSQueryMessage * msg); 
private:
	PPSWorkload * _wl;
	volatile RC _rc;
  row_t * row;

  uint64_t next_item_id;

  void next_pps_state();
  RC run_txn_state();
  bool is_done();
  bool is_local_item(uint64_t idx);
  RC send_remote_request(); 

};

#endif
