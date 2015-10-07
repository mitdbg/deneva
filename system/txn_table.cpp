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
  for(uint64_t i = 0; i < g_part_cnt / g_node_cnt; i++) {
      spec_mode[i] = false;
  }
  //inflight_cnt = 0;
  pthread_mutex_init(&mtx,NULL);
  pthread_cond_init(&cond_m,NULL);
  pthread_cond_init(&cond_a,NULL);
  pool_size = g_inflight_max * g_node_cnt * 2 + 1;
  pool = (pool_node_t) mem_allocator.alloc(sizeof(pool_node) * pool_size , g_thread_cnt);
  modify = false;
  access = 0;
  cnt = 0;
  for(uint32_t i = 0; i < pool_size;i++) {
    pool[i].head = NULL;
    pool[i].tail = NULL;
    pool[i].cnt = 0;
    pthread_mutex_init(&pool[i].mtx,NULL);
    pthread_cond_init(&pool[i].cond_m,NULL);
    pthread_cond_init(&pool[i].cond_a,NULL);
    pool[i].modify = false;
    pool[i].access = 0;
  }
}

bool TxnTable::empty(uint64_t node_id) {
  //return head == NULL;
  for(uint32_t i = 0; i < pool_size;i++) {
    if(pool[i].head != NULL)
      return false;
  }
  return true;
}

void TxnTable::add_txn(uint64_t node_id, txn_man * txn, base_query * qry) {

  uint64_t thd_prof_start = get_sys_clock();
  txn->set_query(qry);
  uint64_t txn_id = txn->get_txn_id();
  assert(txn_id == qry->txn_id);
  txn_man * next_txn = NULL;
  assert(txn->get_txn_id() == qry->txn_id);

  MODIFY_START(txn_id % pool_size);

  txn_node_t t_node = pool[txn_id % pool_size].head;

  while (t_node != NULL) {
    if (t_node->txn->get_txn_id() == txn_id) {
      next_txn = t_node->txn;
      break;
    }
    t_node = t_node->next;
  }

  if(next_txn == NULL) {
    //t_node = (txn_node_t) mem_allocator.alloc(sizeof(struct txn_node), g_thread_cnt);
    txn_table_pool.get(t_node);
    t_node->txn = txn;
    t_node->qry = qry;
    LIST_PUT_TAIL(pool[txn_id % pool_size].head,pool[txn_id % pool_size].tail,t_node);
    pool[txn_id % pool_size].cnt++;
    if(pool[txn_id % pool_size].cnt > 1) {
      INC_STATS(0,txn_table_cflt,1);
      INC_STATS(0,txn_table_cflt_size,pool[txn_id % pool_size].cnt-1);
    }
    cnt++;
  }
  else {
    t_node->txn = txn;
    t_node->qry = qry;
  }

  MODIFY_END(txn_id % pool_size);
  INC_STATS(0,thd_prof_txn_table_add,get_sys_clock() - thd_prof_start);
}
void TxnTable::get_txn(uint64_t node_id, uint64_t txn_id,txn_man *& txn,base_query *& qry){

  uint64_t thd_prof_start = get_sys_clock();
  //ACCESS_START(txn_id % pool_size);
  MODIFY_START(txn_id % pool_size);

  txn_node_t t_node = pool[txn_id % pool_size].head;

  while (t_node != NULL) {
    if (t_node->txn->get_txn_id() == txn_id) {
      txn = t_node->txn;
      qry = t_node->qry;
      assert(txn->get_txn_id() == qry->txn_id);
      break;
    }
    t_node = t_node->next;
  }
  if(!t_node) {
    txn = NULL;
    qry = NULL;
  }

  //ACCESS_END(txn_id % pool_size);
  MODIFY_END(txn_id % pool_size);
  INC_STATS(0,thd_prof_txn_table_get,get_sys_clock() - thd_prof_start);

}

/*
txn_man * TxnTable::get_txn(uint64_t node_id, uint64_t txn_id){
  txn_man * next_txn = NULL;

  ACCESS_START();

  txn_node_t t_node = pool[txn_id % pool_size].head;

  while (t_node != NULL) {
    if (t_node->txn->get_txn_id() == txn_id) {
      next_txn = t_node->txn;
      break;
    }
    t_node = t_node->next;
  }

  ACCESS_END();
  return next_txn;
}
*/

void TxnTable::restart_txn(uint64_t txn_id){

  //ACCESS_START(txn_id % pool_size);
  MODIFY_START(txn_id % pool_size);

  txn_node_t t_node = pool[txn_id % pool_size].head;

  while (t_node != NULL) {
    if (t_node->txn->get_txn_id() == txn_id) {
      if(txn_id % g_node_cnt == g_node_id)
        t_node->qry->rtype = RTXN;
      else
        t_node->qry->rtype = RQRY;
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
      //work_queue.add_query(t_node->qry->active_part/g_node_cnt,t_node->qry);
      work_queue.enqueue(t_node->qry);
#else
      //work_queue.add_query(0,t_node->qry);
      work_queue.enqueue(t_node->qry);
#endif
      break;
    }
    t_node = t_node->next;
  }

  MODIFY_END(txn_id % pool_size);
  //ACCESS_END(txn_id % pool_size);

}

/*
base_query * TxnTable::get_qry(uint64_t node_id, uint64_t txn_id){
  base_query * next_qry = NULL;

  ACCESS_START();

  txn_node_t t_node = pool[txn_id % pool_size].head;

  while (t_node != NULL) {
    if (t_node->txn->get_txn_id() == txn_id) {
      next_qry = t_node->qry;
      break;
    }
    t_node = t_node->next;
  }

  ACCESS_END();

  return next_qry;
}
*/

void TxnTable::delete_all() {
  for(uint64_t i=0;i < pool_size;i++) {
    txn_node_t t_node = pool[i].head;

    while (t_node != NULL) {
      LIST_REMOVE_HT(t_node,pool[i].head,pool[i].tail);
      pool[i].cnt--;
      cnt--;
      if(t_node->txn) {
        t_node->txn->release();
        txn_pool.put(t_node->txn);
      }
      if(t_node->qry) {
        qry_pool.put(t_node->qry);
      }
      t_node = t_node->next;
    }

  }
}

void TxnTable::delete_txn(uint64_t node_id, uint64_t txn_id){
  uint64_t thd_prof_start = get_sys_clock();
  uint64_t starttime = thd_prof_start;
  uint64_t thd_prof_tmp;
  MODIFY_START(txn_id % pool_size);

  thd_prof_tmp = get_sys_clock() - thd_prof_start;
  thd_prof_start = get_sys_clock();

  txn_node_t t_node = pool[txn_id % pool_size].head;

  while (t_node != NULL) {
    if (t_node->txn->get_txn_id() == txn_id) {
      LIST_REMOVE_HT(t_node,pool[txn_id % pool_size].head,pool[txn_id % pool_size].tail);
      pool[txn_id % pool_size].cnt--;
      cnt--;
      break;
    }
    t_node = t_node->next;
  }

  MODIFY_END(txn_id % pool_size)

  if(t_node != NULL) {
    INC_STATS(0,thd_prof_txn_table1a,get_sys_clock() - thd_prof_start);
    INC_STATS(0,thd_prof_txn_table0a,thd_prof_tmp);
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
      /*
      t_node->txn->release();
      mem_allocator.free(t_node->txn, sizeof(ycsb_txn_man));
      */
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
    INC_STATS(0,thd_prof_txn_table0b,thd_prof_tmp);
  }
  INC_STATS(0,thd_prof_txn_table2,get_sys_clock() - starttime);

}

void TxnTable::snapshot() {
  uint64_t total = 0;
  uint64_t abrt_total = 0;
  uint64_t wait_total = 0;
  uint64_t exec_total = 0;
  uint64_t abrt_loc = 0;
  uint64_t wait_loc = 0;
  uint64_t wait_rem = 0;
  uint64_t exec_loc = 0;
  uint64_t other_total = 0;
  uint64_t other_loc = 0;
  for(uint32_t i = 0;i < pool_size; i++) {
    MODIFY_START(i);
    txn_node_t t_node = pool[i].head;
    while (t_node != NULL) {
      total++;
      if((t_node->txn->state == EXEC || t_node->txn->state == PREP || t_node->txn->state == DONE|| t_node->txn->state == FIN) && t_node->txn->rc == RCOK && (t_node->qry->rc != WAIT &&t_node->qry->rc != WAIT_REM)) {
        exec_total++;
        if(t_node->txn->get_txn_id() % g_node_cnt == g_node_id)
          exec_loc++;
      }
      else if(t_node->txn->rc == WAIT || t_node->txn->rc == WAIT_REM || t_node->qry->rc == WAIT || t_node->qry->rc == WAIT_REM) {
        wait_total++;
        if(t_node->txn->get_txn_id() % g_node_cnt == g_node_id) {
          wait_loc++;
          if(t_node->txn->rc == WAIT_REM || t_node->txn->rc == WAIT)
            wait_rem++;
        }
      }
      else if((t_node->txn->rc == Abort || t_node->txn->state == START) && t_node->txn->abort_cnt > 0) {
        abrt_total++;
        if(t_node->txn->get_txn_id() % g_node_cnt == g_node_id)
          abrt_loc++;
      }
      else {
        other_total++;
        if(t_node->txn->get_txn_id() % g_node_cnt == g_node_id)
          other_loc++;
      }
      /*
      else {
        printf("%d %d %ld %d\n",t_node->txn->state,t_node->txn->rc,((ycsb_query*)t_node->qry)->req.key,t_node->txn->get_txn_id() % g_node_cnt == g_node_id);
      }
      */
      t_node = t_node->next;
    }
    MODIFY_END(i);
  }
  printf("TOTAL: %ld\n",total);
  printf("EXEC: %ld / %ld / %ld\n",exec_total,exec_loc,exec_total-exec_loc);
  printf("WAIT: %ld / %ld -- %ld / %ld\n",wait_total,wait_loc,wait_rem,wait_total-wait_loc);
  printf("ABORT/START: %ld/ %ld / %ld\n",abrt_total,abrt_loc,abrt_total-abrt_loc);
  printf("OTHER: %ld/ %ld / %ld\n",other_total,other_loc,other_total-other_loc);
}

uint64_t TxnTable::get_min_ts() {

  uint64_t min = UINT64_MAX;

  for(uint32_t i = 0; i < pool_size; i++) {
    ACCESS_START(i)
    txn_node_t t_node = pool[i].head;
    while (t_node != NULL) {
      if(t_node->txn->get_ts() < min)
        min = t_node->txn->get_ts();
      t_node = t_node->next;
    }
    ACCESS_END(i);
  }

  return min;
}

void TxnTable::spec_next(uint64_t tid) {
  assert(CC_ALG == HSTORE_SPEC);
  if(!spec_mode[tid])
    return;


  for(uint32_t i = 0; i < pool_size; i++) {
  MODIFY_START(i);
  txn_node_t t_node = pool[i].head;

  while (t_node != NULL) {
    if (t_node->txn->get_txn_id() % g_node_cnt == g_node_id  // txn is local to this node
        && t_node->qry->active_part/g_node_cnt == tid // txn is local to this thread's part
        && t_node->qry->part_num == 1  // is a single part txn
        && t_node->txn->state == INIT // hasn't started executing yet
        && !t_node->txn->spec  // is not currently speculative
        && t_node->qry->penalty_end < get_sys_clock()) { // is not currently in an abort penalty phase
      t_node->txn->spec = true;
      t_node->qry->spec = true;
      t_node->txn->state = EXEC;
      // unlock causes deadlock
      /*
			uint64_t part_arr_s[1];
			part_arr_s[0] = g_node_id;
      part_lock_man.rem_unlock(part_arr_s,1,t_node->txn);
      */
      t_node->txn->rc = RCOK;
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
      //printf("SPEC %ld\n",t_node->qry->txn_id);
      //work_queue.add_query(t_node->qry->active_part/g_node_cnt,t_node->qry);
      work_queue.enqueue(t_node->qry);
#else
      //work_queue.add_query(0,t_node->qry);
      work_queue.enqueue(t_node->qry);
#endif
    }
    t_node = t_node->next;
  }
  MODIFY_END(i);
  }

}

void TxnTable::start_spec_ex(uint64_t tid) {
  assert(CC_ALG == HSTORE_SPEC);
  spec_mode[tid] = true;

  /*
  ACCESS_START();


  txn_node_t t_node = txns[0];

  while (t_node->next != NULL) {
    t_node = t_node->next;
    if (t_node->txn->get_txn_id() % g_node_cnt == g_node_id && t_node->qry->part_num == 1 && t_node->txn->state == INIT) {
      t_node->txn->spec = true;
      t_node->txn->state = EXEC;
      work_queue.add_query(t_node->qry);
    }
  }

  ACCESS_END();
  */

}

void TxnTable::commit_spec_ex(int r, uint64_t tid) {
  assert(CC_ALG == HSTORE_SPEC);
  RC rc = (RC) r;


  spec_mode[tid] = false;

  for(uint32_t i = 0; i < pool_size; i++) {
  ACCESS_START(i);
  txn_node_t t_node = pool[i].head;
  //txn_node_t t_node = txns[0];

  while (t_node != NULL) {
    if (t_node->txn->get_txn_id() % g_node_cnt == g_node_id && t_node->qry->active_part/g_node_cnt == tid && t_node->qry->part_num == 1 && t_node->txn->state == PREP && t_node->txn->spec && !t_node->txn->spec_done) {
      t_node->txn->validate();
      t_node->txn->finish(rc,t_node->qry->part_to_access,t_node->qry->part_num);
      t_node->txn->state = DONE;
      t_node->qry->rtype = RPASS;
      t_node->txn->spec_done = true;
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
      //printf("SPEC END %ld\n",t_node->qry->txn_id);
      //work_queue.add_query(t_node->qry->active_part/g_node_cnt,t_node->qry);
      work_queue.enqueue(t_node->qry);
#else
      //work_queue.add_query(0,t_node->qry);
      work_queue.enqueue(t_node->qry);
#endif
    }
    else if (t_node->txn->get_txn_id() % g_node_cnt == g_node_id && t_node->qry->active_part/g_node_cnt == tid && t_node->qry->part_num == 1 && t_node->txn->state != PREP && t_node->txn->state != DONE && !t_node->txn->spec_done && t_node->txn->spec) {
      // Why is this here?
      t_node->qry->rtype = RPASS;
      t_node->txn->rc = Abort;
      t_node->txn->finish(Abort,t_node->qry->part_to_access,t_node->qry->part_num);
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
      //printf("SPEC ABRT %ld\n",t_node->qry->txn_id);
      //work_queue.add_query(t_node->qry->active_part/g_node_cnt,t_node->qry);
      work_queue.enqueue(t_node->qry);
#else
      //work_queue.add_query(0,t_node->qry);
      work_queue.enqueue(t_node->qry);
#endif
    }
    // FIXME: what if txn is already in work queue or is currently being executed?
    t_node = t_node->next;
  }
  ACCESS_END(i);
  }



}
