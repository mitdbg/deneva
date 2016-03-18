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

#include "global.h"
#include "txn_table.h"
#include "tpcc_query.h"
#include "tpcc.h"
#include "ycsb_query.h"
#include "ycsb.h"
#include "query.h"
#include "txn.h"
#include "mem_alloc.h"
#include "row.h"
#include "pool.h"
#include "work_queue.h"
#include "message.h"

#define MODIFY_START(i) {\
    pthread_mutex_lock(&pool[i].mtx);\
    while(pool[i].modify || pool[i].access > 0)\
      pthread_cond_wait(&pool[i].cond_m,&pool[i].mtx);\
    pool[i].modify = true; \
    pthread_mutex_unlock(&pool[i].mtx); }

#define MODIFY_END(i) {\
  pthread_mutex_lock(&pool[i].mtx);\
  pool[i].modify = false;\
  pthread_cond_signal(&pool[i].cond_m); \
  pthread_cond_broadcast(&pool[i].cond_a); \
  pthread_mutex_unlock(&pool[i].mtx); }

#define ACCESS_START(i) {\
  pthread_mutex_lock(&pool[i].mtx);\
  while(pool[i].modify)\
      pthread_cond_wait(&pool[i].cond_a,&pool[i].mtx);\
  pool[i].access++;\
  pthread_mutex_unlock(&pool[i].mtx); }

#define ACCESS_END(i) {\
  pthread_mutex_lock(&pool[i].mtx);\
  pool[i].access--;\
  pthread_cond_signal(&pool[i].cond_m);\
  pthread_mutex_unlock(&pool[i].mtx); }

void TxnTable::init() {
  pthread_mutex_init(&mtx,NULL);
  pthread_cond_init(&cond_m,NULL);
  pthread_cond_init(&cond_a,NULL);
  modify = false;
  access = 0;
  cnt = 0;
  table_min_ts = UINT64_MAX;
  pool_size = g_inflight_max * g_node_cnt * 2 + 1;
  pool = (pool_node_t) mem_allocator.alloc(sizeof(pool_node) * pool_size);
  for(uint32_t i = 0; i < pool_size;i++) {
    pool[i].head = NULL;
    pool[i].tail = NULL;
    pool[i].cnt = 0;
    pthread_mutex_init(&pool[i].mtx,NULL);
    pthread_cond_init(&pool[i].cond_m,NULL);
    pthread_cond_init(&pool[i].cond_a,NULL);
    pool[i].modify = false;
    pool[i].access = 0;
    pool[i].min_ts = UINT64_MAX;
  }
}

bool TxnTable::empty(uint64_t node_id) {
  return ts_pool.empty();
}

void TxnTable::dump() {
  for(uint64_t i = 0; i < pool_size;i++) {
    if(pool[i].cnt  == 0)
      continue;
    ACCESS_START(i);
      txn_node_t t_node = pool[i].head;

      while (t_node != NULL) {
        printf("TT (%ld,%ld)\n",t_node->txn_man->get_txn_id(),t_node->txn_man->get_batch_id()
            );
        t_node = t_node->next;
      }
      
    ACCESS_END(i);
  }
}

void TxnTable::add_txn(TxnManager * txn_man) {

  DEBUG_R("Add (%ld,%ld)\n",txn_man->get_txn_id(),txn_man->get_batch_id());
  uint64_t thd_prof_start = get_sys_clock();
  uint64_t txn_id = txn_man->get_txn_id();

  TxnManager * next_txn = NULL;

  MODIFY_START(txn_id % pool_size);

  txn_node_t t_node = pool[txn_id % pool_size].head;

  while (t_node != NULL) {
#if CC_ALG == CALVIN
    if (t_node->txn_man->get_txn_id() == txn_id && t_node->txn_man->get_batch_id() == txn_man->get_batch_id()) {
      next_txn = t_node->txn;
      break;
    }
#else
    if (t_node->txn_man->get_txn_id() == txn_id) {
      next_txn = t_node->txn_man;
      break;
    }
#endif
    t_node = t_node->next;
  }

  if(next_txn == NULL) {
    //t_node = (txn_node_t) mem_allocator.alloc(sizeof(struct txn_node));
  pthread_mutex_lock(&mtx);
    ts_pool.insert(TsMapPair(txn_man->get_timestamp(),NULL));
  pthread_mutex_unlock(&mtx);
    //txn_table_pool.get(t_node);
    t_node = (txn_node *) mem_allocator.alloc(sizeof(struct txn_node));
    t_node->txn_man = txn_man;
    LIST_PUT_TAIL(pool[txn_id % pool_size].head,pool[txn_id % pool_size].tail,t_node);
    pool[txn_id % pool_size].cnt++;
    if(pool[txn_id % pool_size].cnt > 1) {
      INC_STATS(0,txn_table_cflt,1);
      INC_STATS(0,txn_table_cflt_size,pool[txn_id % pool_size].cnt-1);
    }
    ATOM_ADD(cnt,1);
  }
  else {
    if(txn_man->get_timestamp() != t_node->txn_man->get_timestamp()) {
  pthread_mutex_lock(&mtx);
      ts_pool.erase(t_node->txn_man->get_timestamp());
      ts_pool.insert(TsMapPair(txn_man->get_timestamp(),NULL));
  pthread_mutex_unlock(&mtx);
    }
    t_node->txn_man = txn_man;
  }

  MODIFY_END(txn_id % pool_size);
  INC_STATS(0,thd_prof_txn_table_add,get_sys_clock() - thd_prof_start);
}

TxnManager * TxnTable::get_transaction_manager(uint64_t txn_id,uint64_t batch_id){

  uint64_t thd_prof_start = get_sys_clock();
  INC_STATS(0,thd_prof_get_txn_cnt,1);
  ACCESS_START(txn_id % pool_size);

  txn_node_t t_node = pool[txn_id % pool_size].head;
  TxnManager * txn_man = NULL;

  while (t_node != NULL) {
#if CC_ALG == CALVIN
    if (t_node->txn_man->get_txn_id() == txn_id && t_node->txn_man->get_batch_id() == batch_id) {
#else
    if (t_node->txn_man->get_txn_id() == txn_id) {
#endif
      txn_man = t_node->txn_man;
      break;
    }
    t_node = t_node->next;
  }


  ACCESS_END(txn_id % pool_size);
  if(!txn_man) {
  MODIFY_START(txn_id % pool_size);
  txn_pool.get(txn_man);
  // create new entry and add to table
  /*
  pthread_mutex_lock(&mtx);
    ts_pool.insert(TsMapPair(txn_man->get_timestamp(),NULL));
  pthread_mutex_unlock(&mtx);
  */
    //txn_table_pool.get(t_node);
    t_node = (txn_node *) mem_allocator.alloc(sizeof(struct txn_node));
    txn_man->set_txn_id(txn_id);
    t_node->txn_man = txn_man;
    LIST_PUT_TAIL(pool[txn_id % pool_size].head,pool[txn_id % pool_size].tail,t_node);
    pool[txn_id % pool_size].cnt++;
    if(pool[txn_id % pool_size].cnt > 1) {
      INC_STATS(0,txn_table_cflt,1);
      INC_STATS(0,txn_table_cflt_size,pool[txn_id % pool_size].cnt-1);
    }
    ATOM_ADD(cnt,1);

  MODIFY_END(txn_id % pool_size);
  }
  INC_STATS(0,thd_prof_txn_table_get,get_sys_clock() - thd_prof_start);
  return txn_man;

}

void TxnTable::restart_txn(uint64_t txn_id,uint64_t batch_id){
  MODIFY_START(txn_id % pool_size);

  txn_node_t t_node = pool[txn_id % pool_size].head;

  while (t_node != NULL) {
#if CC_ALG == CALVIN
    if (t_node->txn_man->get_txn_id() == txn_id && t_node->txn_man_man->get_batch_id() == batch_id) {
#else
    if (t_node->txn_man->get_txn_id() == txn_id) {
#endif
      if(txn_id % g_node_cnt == g_node_id)
        work_queue.enqueue(0,Message::create_message(t_node->txn_man,RTXN),false);
      else
        work_queue.enqueue(0,Message::create_message(t_node->txn_man,RQRY),false);
      break;
    }
    t_node = t_node->next;
  }

  MODIFY_END(txn_id % pool_size);

}

void TxnTable::delete_txn(uint64_t txn_id, uint64_t batch_id){
  uint64_t thd_prof_start = get_sys_clock();
  uint64_t starttime = thd_prof_start;

  MODIFY_START(txn_id % pool_size);

  txn_node_t t_node = pool[txn_id % pool_size].head;

  while (t_node != NULL) {
#if CC_ALG == CALVIN
    if (t_node->txn_man->get_txn_id() == txn_id && t_node->txn_man->get_batch_id() == batch_id) {
#else
    if (t_node->txn_man->get_txn_id() == txn_id) {
#endif
      LIST_REMOVE_HT(t_node,pool[txn_id % pool_size].head,pool[txn_id % pool_size].tail);
      pool[txn_id % pool_size].cnt--;
      ATOM_SUB(cnt,1);
      break;
    }
    t_node = t_node->next;
  }
  pthread_mutex_lock(&mtx);
  TsMap::iterator it2 = ts_pool.find(t_node->txn_man->get_timestamp());
  if(it2 != ts_pool.end())
    ts_pool.erase(it2);
  pthread_mutex_unlock(&mtx);

  MODIFY_END(txn_id % pool_size)

  if(t_node != NULL) {
    INC_STATS(0,thd_prof_txn_table1a,get_sys_clock() - thd_prof_start);
    thd_prof_start = get_sys_clock();

    if(t_node->txn_man) {
      t_node->txn_man->release();
      txn_pool.put(t_node->txn_man);
    }
    
    DEBUG_R("Delete (%ld,%ld)\n",txn_id,batch_id);
    //txn_table_pool.put(t_node);
    mem_allocator.free(t_node,sizeof(t_node));
    INC_STATS(0,thd_prof_txn_table2a,get_sys_clock() - thd_prof_start);
  }
  else {

    INC_STATS(0,thd_prof_txn_table1b,get_sys_clock() - thd_prof_start);
  }
  INC_STATS(0,thd_prof_txn_table2,get_sys_clock() - starttime);
}

uint64_t TxnTable::get_min_ts() {

  uint64_t min = UINT64_MAX;
  pthread_mutex_lock(&mtx);
  TsMap::iterator it = ts_pool.lower_bound(0);
  if(it != ts_pool.end())
    min = it->first;
  pthread_mutex_unlock(&mtx);
  return min;

}

void TxnTable::snapshot() {
}

