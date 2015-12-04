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

#ifndef _REMOTE_QUERY_H_
#define _REMOTE_QUERY_H_

#include "global.h"
#include "helper.h"
#include "query.h"
#include "pool.h"

#define MAX_DATA_SIZE 100 
class YCSBQuery;
class ycsb_TxnManager;
class TPCCQuery;
class tpcc_TxnManager;
class TxnManager;
class BaseQuery;


class Remote_query {
public:
	void init(uint64_t node_id, Workload * wl);
	TxnManager * get_txn_man(uint64_t thd_id, uint64_t node_id, uint64_t txn_id);
	TxnManager * save_txn_man(uint64_t thd_id, uint64_t node_id, uint64_t txn_id, TxnManager * txn_to_save);
	void unmarshall(void * d, uint64_t len);
  void unmarshall(BaseQuery *& query,char * data,  uint64_t & ptr,uint64_t dest_id,uint64_t return_id); 
  
	int q_idx;
private:
	pthread_cond_t cnd;
	pthread_mutex_t mtx;
	
	uint64_t _node_id;
	Workload * _wl;

};
#endif
