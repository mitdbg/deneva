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

void TxnTable::init() {
  pool_size = g_inflight_max * g_node_cnt * 2 + 1;
  DEBUG_M("TxnTable::init pool_node alloc\n");
  pool = (pool_node **) mem_allocator.align_alloc(sizeof(pool_node*) * pool_size);
  for(uint32_t i = 0; i < pool_size;i++) {
    pool[i] = (pool_node *) mem_allocator.align_alloc(sizeof(struct pool_node));
    pool[i]->head = NULL;
    pool[i]->tail = NULL;
    pool[i]->cnt = 0;
    pool[i]->modify = false;
    pool[i]->min_ts = UINT64_MAX;
  }
}

void TxnTable::dump() {
  for(uint64_t i = 0; i < pool_size;i++) {
    if(pool[i]->cnt  == 0)
      continue;
      txn_node_t t_node = pool[i]->head;

      while (t_node != NULL) {
        printf("TT (%ld,%ld)\n",t_node->txn_man->get_txn_id(),t_node->txn_man->get_batch_id()
            );
        t_node = t_node->next;
      }
      
  }
}

bool TxnTable::is_matching_txn_node(txn_node_t t_node, uint64_t txn_id, uint64_t batch_id){
  assert(t_node);
#if CC_ALG == CALVIN
    return (t_node->txn_man->get_txn_id() == txn_id && t_node->txn_man->get_batch_id() == batch_id); 
#else
    return (t_node->txn_man->get_txn_id() == txn_id); 
#endif
}


TxnManager * TxnTable::get_transaction_manager(uint64_t thd_id, uint64_t txn_id,uint64_t batch_id){
  DEBUG("TxnTable::get_txn_manager %ld / %ld\n",txn_id,pool_size);
  uint64_t starttime = get_sys_clock();
  uint64_t pool_id = txn_id % pool_size;

  uint64_t mtx_starttime = starttime;
  // set modify bit for this pool: txn_id % pool_size
  while(!ATOM_CAS(pool[pool_id]->modify,false,true)) { };
  INC_STATS(thd_id,mtx[7],get_sys_clock()-mtx_starttime);

  txn_node_t t_node = pool[pool_id]->head;
  TxnManager * txn_man = NULL;

  uint64_t prof_starttime = get_sys_clock();
  while (t_node != NULL) {
    if(is_matching_txn_node(t_node,txn_id,batch_id)) {
      txn_man = t_node->txn_man;
      break;
    }
    t_node = t_node->next;
  }
  INC_STATS(thd_id,mtx[10],get_sys_clock()-prof_starttime);


  if(!txn_man) {
    prof_starttime = get_sys_clock();
    txn_table_pool.get(t_node);
    txn_man_pool.get(txn_man);
    txn_man->set_txn_id(txn_id);
    t_node->txn_man = txn_man;
    LIST_PUT_TAIL(pool[pool_id]->head,pool[pool_id]->tail,t_node);
    /*
    ++pool[pool_id]->cnt;
    if(pool[pool_id]->cnt > 1) {
      INC_STATS(thd_id,txn_table_cflt_cnt,1);
      INC_STATS(thd_id,txn_table_cflt_size,pool[pool_id]->cnt-1);
    }
    INC_STATS(thd_id,txn_table_new_cnt,1);
    */
  INC_STATS(thd_id,mtx[11],get_sys_clock()-prof_starttime);

  }
  // unset modify bit for this pool: txn_id % pool_size
  ATOM_CAS(pool[pool_id]->modify,true,false);

  INC_STATS(thd_id,txn_table_get_time,get_sys_clock() - starttime);
  INC_STATS(thd_id,txn_table_get_cnt,1);
  return txn_man;

}

void TxnTable::restart_txn(uint64_t thd_id, uint64_t txn_id,uint64_t batch_id){
  uint64_t pool_id = txn_id % pool_size;
  // set modify bit for this pool: txn_id % pool_size
  while(!ATOM_CAS(pool[pool_id]->modify,false,true)) { };

  txn_node_t t_node = pool[pool_id]->head;

  while (t_node != NULL) {
    if(is_matching_txn_node(t_node,txn_id,batch_id)) {
      if(IS_LOCAL(txn_id))
        work_queue.enqueue(thd_id,Message::create_message(t_node->txn_man,RTXN_CONT),false);
      else
        work_queue.enqueue(thd_id,Message::create_message(t_node->txn_man,RQRY_CONT),false);
      break;
    }
    t_node = t_node->next;
  }

  // unset modify bit for this pool: txn_id % pool_size
  ATOM_CAS(pool[pool_id]->modify,true,false);

}

void TxnTable::release_transaction_manager(uint64_t thd_id, uint64_t txn_id, uint64_t batch_id){
  uint64_t starttime = get_sys_clock();

  uint64_t pool_id = txn_id % pool_size;
  uint64_t mtx_starttime = starttime;
  // set modify bit for this pool: txn_id % pool_size
  while(!ATOM_CAS(pool[pool_id]->modify,false,true)) { };
  INC_STATS(thd_id,mtx[9],get_sys_clock()-mtx_starttime);

  txn_node_t t_node = pool[pool_id]->head;

  uint64_t prof_starttime = get_sys_clock();
  while (t_node != NULL) {
    if(is_matching_txn_node(t_node,txn_id,batch_id)) {
      LIST_REMOVE_HT(t_node,pool[txn_id % pool_size]->head,pool[txn_id % pool_size]->tail);
      //--pool[pool_id]->cnt;
      break;
    }
    t_node = t_node->next;
  }
  INC_STATS(thd_id,mtx[12],get_sys_clock()-prof_starttime);

  // unset modify bit for this pool: txn_id % pool_size
  ATOM_CAS(pool[pool_id]->modify,true,false);

  prof_starttime = get_sys_clock();
  assert(t_node);
  assert(t_node->txn_man);

  txn_man_pool.put(t_node->txn_man);
    
  txn_table_pool.put(t_node);
  INC_STATS(thd_id,mtx[13],get_sys_clock()-prof_starttime);

  INC_STATS(thd_id,txn_table_release_time,get_sys_clock() - starttime);
  INC_STATS(thd_id,txn_table_release_cnt,1);

}

uint64_t TxnTable::get_min_ts(uint64_t thd_id) {

  uint64_t starttime = get_sys_clock();
  uint64_t min_ts = UINT64_MAX;
  for(uint64_t i = 0 ; i < pool_size; i++) {
    uint64_t pool_min_ts = pool[i]->min_ts;
    if(pool_min_ts < min_ts)
      min_ts = pool_min_ts;
  }

  INC_STATS(thd_id,txn_table_min_ts_time,get_sys_clock() - starttime);
  return min_ts;

}

