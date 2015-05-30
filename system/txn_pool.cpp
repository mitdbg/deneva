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

void TxnPool::init() {
  spec_mode = false;
  //inflight_cnt = 0;
  pthread_mutex_init(&mtx,NULL);
  txns = (txn_node_t *) mem_allocator.alloc(
            sizeof(txn_node_t *) * g_node_cnt, 0);
  for (uint64_t i = 0; i < g_node_cnt; i++) {
    txn_node_t t_node = (txn_node_t) mem_allocator.alloc(
                  sizeof(struct txn_node), 0);
    memset(t_node, '\0', sizeof(struct txn_node));
    txns[i] = t_node;
  }
}

bool TxnPool::empty(uint64_t node_id) {
  return txns[0]->next == NULL;
}

void TxnPool::add_txn(uint64_t node_id, txn_man * txn, base_query * qry) {

    pthread_mutex_lock(&mtx);

  uint64_t txn_id = txn->get_txn_id();
  assert(txn_id == qry->txn_id);
  //assert(txn_id % g_node_cnt == node_id);
  txn_man * next_txn = NULL;
  txn_node_t t_node = txns[0];
  //txn_node_t t_node = txns[node_id];

  while (t_node->next != NULL) {
    t_node = t_node->next;
    if (t_node->txn->get_txn_id() == txn_id) {
      next_txn = t_node->txn;
      break;
    }
  }

  if(next_txn == NULL) {
    t_node = (txn_node_t) mem_allocator.alloc(sizeof(struct txn_node), g_thread_cnt);
    t_node->txn = txn;
    t_node->qry = qry;
    t_node->next = txns[0]->next;
    txns[0]->next = t_node;
  }
  else {
    t_node->txn = txn;
    t_node->qry = qry;
  }


    pthread_mutex_unlock(&mtx);

}

txn_man * TxnPool::get_txn(uint64_t node_id, uint64_t txn_id){
  txn_man * next_txn = NULL;
  //assert(txn_id % g_node_cnt == node_id);

  pthread_mutex_lock(&mtx);

  txn_node_t t_node = txns[0];
  //txn_node_t t_node = txns[node_id];

  while (t_node->next != NULL) {
    t_node = t_node->next;
    //assert(t_node->txn->get_txn_id() == t_node->qry->txn_id);
    if (t_node->txn->get_txn_id() == txn_id) {
      next_txn = t_node->txn;
      break;
    }
  }
  //assert(next_txn != NULL);

  pthread_mutex_unlock(&mtx);

  return next_txn;
}

void TxnPool::restart_txn(uint64_t txn_id){

  pthread_mutex_lock(&mtx);

  txn_node_t t_node = txns[0];
  //txn_node_t t_node = txns[node_id];

  while (t_node->next != NULL) {
    t_node = t_node->next;
    if (t_node->txn->get_txn_id() == txn_id) {
      if(txn_id % g_node_cnt == g_node_id)
        t_node->qry->rtype = RTXN;
      else
        t_node->qry->rtype = RQRY;
      work_queue.add_query(t_node->qry);
      break;
    }
  }

  pthread_mutex_unlock(&mtx);

}

base_query * TxnPool::get_qry(uint64_t node_id, uint64_t txn_id){
  base_query * next_qry = NULL;

  //assert(txn_id % g_node_cnt == node_id);

  pthread_mutex_lock(&mtx);

  txn_node_t t_node = txns[0];
  //txn_node_t t_node = txns[node_id];

  while (t_node->next != NULL) {
    t_node = t_node->next;
    if (t_node->txn->get_txn_id() == txn_id) {
      next_qry = t_node->qry;
      break;
    }
  }
  assert(next_qry != NULL);

  pthread_mutex_unlock(&mtx);

  return next_qry;
}


void TxnPool::delete_txn(uint64_t node_id, uint64_t txn_id){
  //assert(txn_id % g_node_cnt == node_id);

  pthread_mutex_lock(&mtx);
  //printf("Delete: %ld\n",txn_id);

  txn_node_t t_node = txns[0];
  //txn_node_t t_node = txns[node_id];
  txn_node_t node = NULL;

  //while (t_node->next != NULL && node == NULL) {
  while (t_node->next != NULL && node == NULL) {
    if (t_node->next->txn->get_txn_id() == txn_id) {
      node = t_node->next;
      t_node->next = t_node->next->next; 
      break;
    }
    t_node = t_node->next;
  }

  //assert(node != NULL);
  if(node != NULL) {
    node->txn->release();
#if WORKLOAD == TPCC
    mem_allocator.free(node->txn, sizeof(tpcc_txn_man));
    if(node->qry->txn_id % g_node_cnt != node_id) {
      mem_allocator.free(node->qry, sizeof(tpcc_query));
    }
#elif WORKLOAD == YCSB
    mem_allocator.free(node->txn, sizeof(ycsb_txn_man));
    if(node->qry->txn_id % g_node_cnt != node_id) {
      mem_allocator.free(node->qry, sizeof(ycsb_query));
    }
#endif
    mem_allocator.free(node, sizeof(struct txn_node));
  }

  pthread_mutex_unlock(&mtx);

}

uint64_t TxnPool::get_min_ts() {
  pthread_mutex_lock(&mtx);
  txn_node_t t_node = txns[0];
  uint64_t min = UINT64_MAX;

  while (t_node->next != NULL) {
    if(t_node->next->txn->get_ts() < min)
      min = t_node->next->txn->get_ts();
    t_node = t_node->next;
  }

  pthread_mutex_unlock(&mtx);
  return min;
}

void TxnPool::start_spec_ex() {
  assert(CC_ALG == HSTORE_SPEC);

  pthread_mutex_lock(&mtx);

  spec_mode = true;

  txn_node_t t_node = txns[0];

  while (t_node->next != NULL) {
    t_node = t_node->next;
    if (t_node->txn->get_txn_id() % g_node_cnt == g_node_id && t_node->qry->part_num == 1 && t_node->txn->state == INIT) {
      t_node->txn->spec = true;
      t_node->txn->state = EXEC;
      work_queue.add_query(t_node->qry);
    }
  }

  pthread_mutex_unlock(&mtx);

}

void TxnPool::commit_spec_ex(int r) {
  assert(CC_ALG == HSTORE_SPEC);
  RC rc = (RC) r;

  pthread_mutex_lock(&mtx);

  spec_mode = false;

  txn_node_t t_node = txns[0];

  while (t_node->next != NULL) {
    t_node = t_node->next;
    if (t_node->txn->get_txn_id() % g_node_cnt == g_node_id && t_node->qry->part_num == 1 && t_node->txn->state == PREP && t_node->txn->spec) {
      if(rc != Abort)
        rc = t_node->txn->validate();
      if(rc == Abort) {
        INC_STATS(0,spec_abort_cnt,1);
      }
      else {
        INC_STATS(0,spec_commit_cnt,1);
      }
      t_node->txn->finish(rc,t_node->qry->part_to_access,t_node->qry->part_num);
      t_node->txn->state = DONE;
      t_node->qry->rtype = RPASS;
      work_queue.add_query(t_node->qry);
    }
  }

  pthread_mutex_unlock(&mtx);


}
