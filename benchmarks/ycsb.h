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

#ifndef _SYNTH_BM_H_
#define _SYNTH_BM_H_

#include "wl.h"
#include "txn.h"
#include "global.h"
#include "helper.h"

class YCSBQuery;
class ycsb_request;

class YCSBWorkload : public Workload {
public :
	RC init();
	RC init_table();
	RC init_schema(const char * schema_file);
	RC get_txn_man(TxnManager *& txn_manager);
	int key_to_part(uint64_t key);
	INDEX * the_index;
	table_t * the_table;
private:
	void init_table_parallel();
	void * init_table_slice();
	static void * threadInitTable(void * This) {
		((YCSBWorkload *)This)->init_table_slice(); 
		return NULL;
	}
	pthread_mutex_t insert_lock;
	//  For parallel initialization
	static int next_tid;
};

class YCSBTxnManager : public TxnManager
{
public:
	void init(Workload * h_wl);
  bool conflict(BaseQuery * query1,BaseQuery * query2);
  void read_keys(BaseQuery * query); 
  RC acquire_locks(BaseQuery * query); 
	RC run_txn(BaseQuery * query);
	RC run_rem_txn(BaseQuery * query);
	RC run_calvin_txn(BaseQuery * query);
	void rem_txn_rsp(BaseQuery * query);
private:
void 		merge_txn_rsp(BaseQuery * m_query1, BaseQuery *m_query2) ;
void rtn_ycsb_state(BaseQuery * query);
void next_ycsb_state(BaseQuery * query);
RC run_txn_state(BaseQuery * query);
RC run_ycsb_0(ycsb_request * req,row_t *& row_local);
RC run_ycsb_1(access_t acctype, row_t * row_local);
RC run_ycsb(BaseQuery * query);
	uint64_t row_cnt;
  bool fin;
  bool rem_done;
  row_t * row;
	YCSBWorkload * _wl;
};

#endif
