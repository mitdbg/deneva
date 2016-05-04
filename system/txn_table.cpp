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
  DEBUG_M("TxnTable::init pool_node alloc\n");
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

TxnManager * TxnTable::get_transaction_manager(uint64_t thd_id, uint64_t txn_id,uint64_t batch_id){
  DEBUG("TxnTable::get_txn_manager %ld / %ld\n",txn_id,pool_size);
  uint64_t starttime = get_sys_clock();

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
    DEBUG_M("TxnTable::get_transaction_manager txn_node alloc\n");
    t_node = (txn_node *) mem_allocator.alloc(sizeof(struct txn_node));
    txn_pool.get(txn_man);
    //txn_man = (TxnManager*) mem_allocator.alloc(sizeof(YCSBTxnManager));
    //txn_man->init(NULL);
  // create new entry and add to table
  /*
  pthread_mutex_lock(&mtx);
    ts_pool.insert(TsMapPair(txn_man->get_timestamp(),NULL));
  pthread_mutex_unlock(&mtx);
  */
    //txn_table_pool.get(t_node);
    txn_man->set_txn_id(txn_id);
    t_node->txn_man = txn_man;
    LIST_PUT_TAIL(pool[txn_id % pool_size].head,pool[txn_id % pool_size].tail,t_node);
    pool[txn_id % pool_size].cnt++;
    if(pool[txn_id % pool_size].cnt > 1) {
      INC_STATS(thd_id,txn_table_cflt_cnt,1);
      INC_STATS(thd_id,txn_table_cflt_size,pool[txn_id % pool_size].cnt-1);
    }
    ATOM_ADD(cnt,1);
    INC_STATS(thd_id,txn_table_new_cnt,1);

  MODIFY_END(txn_id % pool_size);
  }
  INC_STATS(thd_id,txn_table_get_time,get_sys_clock() - starttime);
  INC_STATS(thd_id,txn_table_get_cnt,1);
  return txn_man;

}

void TxnTable::restart_txn(uint64_t thd_id, uint64_t txn_id,uint64_t batch_id){
  MODIFY_START(txn_id % pool_size);

  txn_node_t t_node = pool[txn_id % pool_size].head;

  while (t_node != NULL) {
#if CC_ALG == CALVIN
    if (t_node->txn_man->get_txn_id() == txn_id && t_node->txn_man->get_batch_id() == batch_id) {
#else
    if (t_node->txn_man->get_txn_id() == txn_id) {
#endif
      if(txn_id % g_node_cnt == g_node_id)
        work_queue.enqueue(thd_id,Message::create_message(t_node->txn_man,RTXN_CONT),false);
      else
        work_queue.enqueue(thd_id,Message::create_message(t_node->txn_man,RQRY_CONT),false);
      break;
    }
    t_node = t_node->next;
  }

  MODIFY_END(txn_id % pool_size);

}

void TxnTable::release_transaction_manager(uint64_t thd_id, uint64_t txn_id, uint64_t batch_id){
  uint64_t starttime = get_sys_clock();

  MODIFY_START(txn_id % pool_size);

  txn_node_t t_node = pool[txn_id % pool_size].head;

  while (t_node != NULL) {
#if CC_ALG == CALVIN
    if (t_node->txn_man->get_txn_id() == txn_id && t_node->txn_man->get_batch_id() == batch_id) {
#else
    if (t_node->txn_man->get_txn_id() == txn_id) {
#endif
      LIST_REMOVE_HT(t_node,pool[txn_id % pool_size].head,pool[txn_id % pool_size].tail);
      --pool[txn_id % pool_size].cnt;
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

  assert(t_node);
  assert(t_node->txn_man);

  DEBUG_R("TxnTable::release (%ld,%ld)\n",txn_id,batch_id);

  t_node->txn_man->release();
  DEBUG_M("TxnTable::release_transaction_manager TxnManager free\n");
#if WORKLOAD == YCSB
  mem_allocator.free(t_node->txn_man,sizeof(YCSBTxnManager));
#elif WORKLOAD == TPCC
  mem_allocator.free(t_node->txn_man,sizeof(TPCCTxnManager));
#endif
  //txn_pool.put(t_node->txn_man);
    
  //txn_table_pool.put(t_node);
  DEBUG_M("TxnTable::release_transaction_manager txn_node free\n");
  mem_allocator.free(t_node,sizeof(txn_node));

  INC_STATS(thd_id,txn_table_release_time,get_sys_clock() - starttime);
  INC_STATS(thd_id,txn_table_release_cnt,1);

}

void TxnTable::delete_txn(uint64_t txn_id, uint64_t batch_id){

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

    if(t_node->txn_man) {
      t_node->txn_man->release();
      txn_pool.put(t_node->txn_man);
    }
    
    DEBUG_R("Delete (%ld,%ld)\n",txn_id,batch_id);
    //txn_table_pool.put(t_node);
  DEBUG_M("TxnTable::delete_transaction txn_node free\n");
    mem_allocator.free(t_node,sizeof(txn_node));
  }
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

