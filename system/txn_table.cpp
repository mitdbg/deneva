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
#include "plock.h"
#include "txn_pool.h"

#define MODIFY_START(i) {\
    pthread_mutex_lock(&mtx);\
    while(modify || access > 0)\
      pthread_cond_wait(&cond_m,&mtx);\
    modify = true; \
    pthread_mutex_unlock(&mtx); }

#define MODIFY_END(i) {\
  pthread_mutex_lock(&mtx);\
  modify = false;\
  pthread_cond_signal(&cond_m); \
  pthread_cond_broadcast(&cond_a); \
  pthread_mutex_unlock(&mtx); }

#define ACCESS_START(i) {\
  pthread_mutex_lock(&mtx);\
  while(modify)\
      pthread_cond_wait(&cond_a,&mtx);\
  access++;\
  pthread_mutex_unlock(&mtx); }

#define ACCESS_END(i) {\
  pthread_mutex_lock(&mtx);\
  access--;\
  pthread_cond_signal(&cond_m);\
  pthread_mutex_unlock(&mtx); }

void TxnTable::init() {
  pthread_mutex_init(&mtx,NULL);
  pthread_cond_init(&cond_m,NULL);
  pthread_cond_init(&cond_a,NULL);
  modify = false;
  access = 0;
  cnt = 0;
  table_min_ts = UINT64_MAX;
}

bool TxnTable::empty(uint64_t node_id) {
  return pool.empty();
}

void TxnTable::add_txn(uint64_t node_id, txn_man * txn, base_query * qry) {

  uint64_t thd_prof_start = get_sys_clock();
  txn->set_query(qry);
  uint64_t txn_id = txn->get_txn_id();
  assert(txn_id == qry->txn_id);
  txn_node_t t_node;

  txn_table_pool.get(t_node);
  t_node->txn = txn;
  t_node->qry = qry;
  MODIFY_START(0);
  std::pair<TxnMap::iterator,bool> tp = pool.insert(TxnMapPair(txn_id,t_node));
  
  if(!tp.second) {
    txn_table_pool.put(t_node);
    t_node = tp.first->second;
    if(txn->get_ts() != t_node->txn->get_ts()) {
      ts_pool.erase(t_node->txn->get_ts());
      ts_pool.insert(TsMapPair(txn->get_ts(),NULL));
    }
    t_node->qry = qry;
    t_node->txn = txn;
  } else {
    ts_pool.insert(TsMapPair(txn->get_ts(),NULL));
  }

  MODIFY_END(0);
  INC_STATS(0,thd_prof_txn_table_add,get_sys_clock() - thd_prof_start);
}
void TxnTable::get_txn(uint64_t node_id, uint64_t txn_id,txn_man *& txn,base_query *& qry){

  uint64_t thd_prof_start = get_sys_clock();
  txn_node_t t_node;
  txn = NULL;
  qry = NULL;
  INC_STATS(0,thd_prof_get_txn_cnt,1);
  ACCESS_START(0);
  TxnMap::iterator it = pool.find(txn_id);
  if(it != pool.end()) {
    t_node = it->second;
    txn = t_node->txn;
    qry = t_node->qry;
  }
  ACCESS_END(0);
  INC_STATS(0,thd_prof_txn_table_get,get_sys_clock() - thd_prof_start);

}

void TxnTable::restart_txn(uint64_t txn_id){
  txn_node_t t_node;
  MODIFY_START(0);
  TxnMap::iterator it = pool.find(txn_id);
  if(it != pool.end()) {
    t_node = it->second;
    if(txn_id % g_node_cnt == g_node_id)
      t_node->qry->rtype = RTXN;
    else
      t_node->qry->rtype = RQRY;
    work_queue.enqueue(t_node->qry);
  }
  MODIFY_END(0);


}

void TxnTable::delete_txn(uint64_t node_id, uint64_t txn_id){
  txn_node_t t_node;
  uint64_t thd_prof_start = get_sys_clock();
  uint64_t starttime = thd_prof_start;
  MODIFY_START(0);
  TxnMap::iterator it = pool.find(txn_id);
  if(it != pool.end()) {
    t_node = it->second;
    pool.erase(it);
  }
  TsMap::iterator it2 = ts_pool.find(t_node->txn->get_ts());
  if(it2 != ts_pool.end())
    ts_pool.erase(it2);
  MODIFY_END(0);

  if(t_node != NULL) {
    INC_STATS(0,thd_prof_txn_table1a,get_sys_clock() - thd_prof_start);
    thd_prof_start = get_sys_clock();
    assert(!t_node->txn->spec || t_node->txn->state == DONE);
#if WORKLOAD == TPCC
    t_node->txn->release();
    mem_allocator.free(t_node->txn, sizeof(tpcc_txn_man));
    if(t_node->qry->txn_id % g_node_cnt != node_id) {
      mem_allocator.free(t_node->qry, sizeof(tpcc_query));
    }
#elif WORKLOAD == YCSB
    if(t_node->txn) {
      //t_node->txn->release();
      //mem_allocator.free(t_node->txn, sizeof(ycsb_txn_man));
      t_node->txn->release();
      txn_pool.put(t_node->txn);
    }
    
    if(t_node->qry) {
      //YCSB_QUERY_FREE(t_node->qry)
      qry_pool.put(t_node->qry);
    }
#endif
    //mem_allocator.free(t_node, sizeof(struct txn_node));
    txn_table_pool.put(t_node);
    INC_STATS(0,thd_prof_txn_table2a,get_sys_clock() - thd_prof_start);
  }
  else {

    INC_STATS(0,thd_prof_txn_table1b,get_sys_clock() - thd_prof_start);
  }
  INC_STATS(0,thd_prof_txn_table2,get_sys_clock() - starttime);
}

uint64_t TxnTable::get_min_ts() {

  uint64_t min = UINT64_MAX;
  ACCESS_START(0)
  TsMap::iterator it = ts_pool.lower_bound(0);
  if(it != ts_pool.end())
    min = it->first;
  ACCESS_END(0)
  return min;

}

void TxnTable::snapshot() {
}

