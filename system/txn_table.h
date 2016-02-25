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

#ifndef _TXN_TABLE_H_
#define _TXN_TABLE_H_

#include "global.h"
#include "helper.h"

class TxnManager;
class BaseQuery;
class row_t;

struct txn_node {
  TxnManager * txn_man;
  uint64_t return_id; // Client ID or Home partition ID
  uint64_t client_startts; // For sequencer
  uint64_t abort_penalty;
  txn_node * next;
  txn_node * prev;

};

typedef txn_node * txn_node_t;


struct pool_node {
 public:
   txn_node_t head;
   txn_node_t tail;
   pthread_mutex_t mtx;
  pthread_cond_t cond_m;
  pthread_cond_t cond_a;
  volatile bool modify;
  int access;
  uint64_t cnt;
  uint64_t min_ts;

};
typedef pool_node * pool_node_t;

typedef std::map<uint64_t,txn_node_t> TxnMap;
typedef std::map<uint64_t,void*> TsMap;
typedef std::pair<uint64_t,txn_node_t> TxnMapPair;
typedef std::pair<uint64_t,void*> TsMapPair;

class TxnTable {
public:
  void init();
  uint64_t get_cnt() {return cnt;}
  bool empty(uint64_t node_id);
  void add_txn(TxnManager * txn);
  TxnManager* get_transaction_manager(uint64_t txn_id,uint64_t batch_id);
  void dump();
  void restart_txn(uint64_t txn_id,uint64_t batch_id);
  void delete_all();
  void delete_txn(uint64_t txn_id, uint64_t batch_id);
  uint64_t get_min_ts(); 
  void snapshot(); 

  void spec_next(uint64_t tid);
  void start_spec_ex(uint64_t tid);
  void end_spec_ex();
  void commit_spec_ex(int r,uint64_t tid);

  //uint64_t inflight_cnt;
  bool * spec_mode;
  int (*compare_uint64)(void* leftp,void* rightp);

private:
	uint64_t _node_id;

  pthread_mutex_t mtx;
  pthread_cond_t cond_m;
  pthread_cond_t cond_a;
  volatile bool modify;
  int access;
  uint64_t cnt;

  uint64_t table_min_ts;
//  TxnMap pool;
  uint64_t pool_size;
  pool_node * pool;
  TsMap ts_pool;

};

#endif
