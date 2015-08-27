#include "global.h"
#include "txn_pool.h"
#include "tpcc_query.h"
#include "tpcc.h"
#include "ycsb_query.h"
#include "ycsb.h"
#include "query.h"
#include "txn.h"
#include "mem_alloc.h"
#include "row.h"
#include "plock.h"

#define MODIFY_START() {\
    pthread_mutex_lock(&mtx);\
    while(modify || access > 0)\
      pthread_cond_wait(&cond_m,&mtx);\
    modify = true; \
    pthread_mutex_unlock(&mtx); }

#define MODIFY_END() {\
  pthread_mutex_lock(&mtx);\
  modify = false;\
  pthread_cond_signal(&cond_m); \
  pthread_cond_broadcast(&cond_a); \
  pthread_mutex_unlock(&mtx); }

#define ACCESS_START() {\
  pthread_mutex_lock(&mtx);\
  while(modify)\
      pthread_cond_wait(&cond_a,&mtx);\
  access++;\
  pthread_mutex_unlock(&mtx); }

#define ACCESS_END() {\
  pthread_mutex_lock(&mtx);\
  access--;\
  pthread_cond_signal(&cond_m);\
  pthread_mutex_unlock(&mtx); }

void TxnPool::init() {
  for(uint64_t i = 0; i < g_part_cnt / g_node_cnt; i++) {
      spec_mode[i] = false;
  }
  //inflight_cnt = 0;
  pthread_mutex_init(&mtx,NULL);
  pthread_cond_init(&cond_m,NULL);
  pthread_cond_init(&cond_a,NULL);
  head = NULL;
  tail = NULL;
  modify = false;
  access = 0;
}

bool TxnPool::empty(uint64_t node_id) {
  //return txns[0]->next == NULL;
  return head == NULL;
}

void TxnPool::add_txn(uint64_t node_id, txn_man * txn, base_query * qry) {

  MODIFY_START();

  uint64_t txn_id = txn->get_txn_id();
  assert(txn_id == qry->txn_id);
  txn_man * next_txn = NULL;
  txn_node_t t_node = head;

  while (t_node != NULL) {
    if (t_node->txn->get_txn_id() == txn_id) {
      next_txn = t_node->txn;
      break;
    }
    t_node = t_node->next;
  }


  if(next_txn == NULL) {
    t_node = (txn_node_t) mem_allocator.alloc(sizeof(struct txn_node), g_thread_cnt);
    t_node->txn = txn;
    t_node->qry = qry;
    LIST_PUT_TAIL(head,tail,t_node);
  }
  else {
    t_node->txn = txn;
    t_node->qry = qry;
  }

  MODIFY_END();
}

txn_man * TxnPool::get_txn(uint64_t node_id, uint64_t txn_id){
  txn_man * next_txn = NULL;

  ACCESS_START();

  txn_node_t t_node = head;

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

void TxnPool::restart_txn(uint64_t txn_id){

  ACCESS_START();

  txn_node_t t_node = head;

  while (t_node != NULL) {
    if (t_node->txn->get_txn_id() == txn_id) {
      if(txn_id % g_node_cnt == g_node_id)
        t_node->qry->rtype = RTXN;
      else
        t_node->qry->rtype = RQRY;
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
      work_queue.add_query(t_node->qry->active_part/g_node_cnt,t_node->qry);
#else
      work_queue.add_query(0,t_node->qry);
#endif
      break;
    }
    t_node = t_node->next;
  }

  ACCESS_END();

}

base_query * TxnPool::get_qry(uint64_t node_id, uint64_t txn_id){
  base_query * next_qry = NULL;

  ACCESS_START();

  txn_node_t t_node = head;

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


void TxnPool::delete_txn(uint64_t node_id, uint64_t txn_id){
  MODIFY_START();

  txn_node_t t_node = head;

  while (t_node != NULL) {
    if (t_node->txn->get_txn_id() == txn_id) {
      LIST_REMOVE_HT(t_node,head,tail);
      break;
    }
    t_node = t_node->next;
  }

  if(t_node != NULL) {
    assert(!t_node->txn->spec || t_node->txn->state == DONE);
    t_node->txn->release();
    printf("FREE %ld\n",t_node->txn->get_txn_id());
#if WORKLOAD == TPCC
    mem_allocator.free(t_node->txn, sizeof(tpcc_txn_man));
    if(t_node->qry->txn_id % g_node_cnt != node_id) {
      mem_allocator.free(t_node->qry, sizeof(tpcc_query));
    }
#elif WORKLOAD == YCSB
    mem_allocator.free(t_node->txn, sizeof(ycsb_txn_man));
      if(((ycsb_query*)t_node->qry)->requests)
        mem_allocator.free(((ycsb_query*)t_node->qry)->requests, sizeof(ycsb_request)*((ycsb_query*)t_node->qry)->request_cnt);
      mem_allocator.free(t_node->qry, sizeof(ycsb_query));
#endif
    mem_allocator.free(t_node, sizeof(struct txn_node));
  }

  MODIFY_END()

}

uint64_t TxnPool::get_min_ts() {
  ACCESS_START()

  txn_node_t t_node = head;
  uint64_t min = UINT64_MAX;

  while (t_node != NULL) {
    if(t_node->txn->get_ts() < min)
      min = t_node->txn->get_ts();
    t_node = t_node->next;
  }

  ACCESS_END();
  return min;
}

void TxnPool::spec_next(uint64_t tid) {
  assert(CC_ALG == HSTORE_SPEC);
  if(!spec_mode[tid])
    return;

  MODIFY_START();

  txn_node_t t_node = head;

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
      work_queue.add_query(t_node->qry->active_part/g_node_cnt,t_node->qry);
#else
      work_queue.add_query(0,t_node->qry);
#endif
    }
    t_node = t_node->next;
  }

  MODIFY_END();
}

void TxnPool::start_spec_ex(uint64_t tid) {
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

void TxnPool::commit_spec_ex(int r, uint64_t tid) {
  assert(CC_ALG == HSTORE_SPEC);
  RC rc = (RC) r;

  ACCESS_START();

  spec_mode[tid] = false;

  txn_node_t t_node = head;
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
      work_queue.add_query(t_node->qry->active_part/g_node_cnt,t_node->qry);
#else
      work_queue.add_query(0,t_node->qry);
#endif
    }
    else if (t_node->txn->get_txn_id() % g_node_cnt == g_node_id && t_node->qry->active_part/g_node_cnt == tid && t_node->qry->part_num == 1 && t_node->txn->state != PREP && t_node->txn->state != DONE && !t_node->txn->spec_done && t_node->txn->spec) {
      // Why is this here?
      t_node->qry->rtype = RPASS;
      t_node->txn->rc = Abort;
      t_node->txn->finish(Abort,t_node->qry->part_to_access,t_node->qry->part_num);
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
      //printf("SPEC ABRT %ld\n",t_node->qry->txn_id);
      work_queue.add_query(t_node->qry->active_part/g_node_cnt,t_node->qry);
#else
      work_queue.add_query(0,t_node->qry);
#endif
    }
    // FIXME: what if txn is already in work queue or is currently being executed?
    t_node = t_node->next;
  }

  ACCESS_END();


}
