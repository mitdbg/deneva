#include "query_work_queue.h"
#include "mem_alloc.h"
#include "query.h"
#include "concurrentqueue.h"


void QWorkQueue::init() {
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
  q_len = g_part_cnt / g_node_cnt;
#else
  q_len = 1;
#endif
  hash = (QHash *) mem_allocator.alloc(sizeof(QHash), 0);
  hash->init();
  queue = (QWorkQueueHelper *) mem_allocator.alloc(sizeof(QWorkQueueHelper) * q_len, 0);
  for(int i = 0;i<q_len;i++) {
    queue[i].init(hash); 
  }
  active_txns = (uint64_t*) mem_allocator.alloc(sizeof(uint64_t) * g_thread_cnt,0);
  for(uint64_t i = 0; i < g_thread_cnt; i++) {
    active_txns[i] = UINT64_MAX;
  }
  pthread_mutex_init(&mtx,NULL);
  pthread_cond_init(&cond,NULL);
  wq_cnt = 0;
  rem_wq_cnt = 0;
  new_wq_cnt = 0;
  abrt_cnt = 0;
  aq_head = NULL;
}

int inline QWorkQueue::get_idx(int input) {
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
    int idx = input;
#else
    int idx = 0;
#endif
    return idx;
}

void QWorkQueue::abort_finish(uint64_t time) {
  /*
    for(int idx=0;idx<q_len;idx++)
      queue[idx].abort_finish(time);
      */
} 

void QWorkQueue::enqueue(uint64_t thd_id, base_query * qry) {
  uint64_t starttime = get_sys_clock();
  qry->q_starttime = starttime;
#if CC_ALG == CALVIN
  if(thd_id >= g_thread_cnt && g_thread_cnt > 1)
    new_wq.enqueue(qry);
  else
    wq.enqueue(qry);
#else
  switch(PRIORITY) {
    case PRIORITY_FCFS:
      ATOM_ADD(wq_cnt,1);
      wq.enqueue(qry);
      INC_STATS(thd_id,wq_enqueue,get_sys_clock() - starttime);
      break;
    case PRIORITY_ACTIVE:
      if(qry->txn_id == UINT64_MAX) {
        ATOM_ADD(new_wq_cnt,1);
        new_wq.enqueue(qry);
        INC_STATS(thd_id,new_wq_enqueue,get_sys_clock() - starttime);
      }
      else {
        ATOM_ADD(wq_cnt,1);
        wq.enqueue(qry);
        INC_STATS(thd_id,wq_enqueue,get_sys_clock() - starttime);
        }
      break;
    case PRIORITY_HOME:
      if(qry->txn_id == UINT64_MAX) {
        ATOM_ADD(new_wq_cnt,1);
        new_wq.enqueue(qry);
        INC_STATS(thd_id,new_wq_enqueue,get_sys_clock() - starttime);
      }
      else if (IS_REMOTE(qry->txn_id)) {
        ATOM_ADD(rem_wq_cnt,1);
        rem_wq.enqueue(qry);
        INC_STATS(thd_id,rem_wq_enqueue,get_sys_clock() - starttime);
      }
      else {
        ATOM_ADD(wq_cnt,1);
        wq.enqueue(qry);
        INC_STATS(thd_id,wq_enqueue,get_sys_clock() - starttime);
      }
      break;
    default: assert(false);
  }
#endif
  //DEBUG("ENQUEUE %ld %ld %ld %ld\n",qry->txn_id,wq_cnt,rem_wq_cnt,new_wq_cnt);
  INC_STATS(thd_id,all_wq_enqueue,get_sys_clock() - starttime);
}
//TODO: do we need to has qry id here?
// If we do, maybe add in check as an atomic var in base_query
// Current hash implementation requires an expensive mutex
bool QWorkQueue::dequeue(uint64_t thd_id, base_query *& qry) {
  bool valid;
  uint64_t prof_starttime = get_sys_clock();
#if CC_ALG == CALVIN
  if(g_thread_cnt > 1 && thd_id ==0)
    valid = new_wq.try_dequeue(qry);
  else
    valid = wq.try_dequeue(qry);
#else
  uint64_t starttime = prof_starttime;
  valid = wq.try_dequeue(qry);
  INC_STATS(thd_id,wq_dequeue,get_sys_clock() - starttime);
  if(valid) {
    ATOM_SUB(wq_cnt,1);
  }
  if(PRIORITY == PRIORITY_HOME) {
    if(!valid) {
      starttime = get_sys_clock();
      valid = rem_wq.try_dequeue(qry);
      INC_STATS(thd_id,rem_wq_dequeue,get_sys_clock() - starttime);
      if(valid) {
        ATOM_SUB(rem_wq_cnt,1);
      }
    }
  }
  if(PRIORITY == PRIORITY_HOME || PRIORITY == PRIORITY_ACTIVE) {
    if(!valid) {
      starttime = get_sys_clock();
      valid = new_wq.try_dequeue(qry);
      INC_STATS(thd_id,new_wq_dequeue,get_sys_clock() - starttime);
      if(valid) {
        ATOM_SUB(new_wq_cnt,1);
      }
    }
  }
#endif


  if(!ISCLIENT && valid && qry->txn_id != UINT64_MAX) {
    if(set_active(thd_id, qry->txn_id)) {
      enqueue(thd_id,qry);
      qry = NULL;
      valid = false;
    }
  }
  if(valid) {
    uint64_t t = get_sys_clock() - qry->q_starttime;
    qry->time_q_work += t;
    INC_STATS(0,qq_cnt,1);
    INC_STATS(0,qq_lat,t);
    DEBUG("DEQUEUE %ld %ld %ld %ld\n",qry->txn_id,wq_cnt,rem_wq_cnt,new_wq_cnt);
  }
  INC_STATS(thd_id,all_wq_dequeue,get_sys_clock() - prof_starttime);
  return valid;
}

bool QWorkQueue::set_active(uint64_t thd_id, uint64_t txn_id) {
  bool result = false;
  pthread_mutex_lock(&mtx);
  while(true) {
    uint64_t i = 0;
    for(i = 0; i < g_thread_cnt; i++) {
      if(i == thd_id)
        continue;
      if(active_txns[i] == txn_id) {
        result = true;
        goto end;
      }
    }
    if(i==g_thread_cnt)
      break;
    //pthread_cond_wait(&cond,&mtx);
  }
  active_txns[thd_id] = txn_id;
  //pthread_cond_broadcast(&cond);
end:
  pthread_mutex_unlock(&mtx);
  return result;
} 
void QWorkQueue::delete_active(uint64_t thd_id, uint64_t txn_id) {
  pthread_mutex_lock(&mtx);
  //assert(active_txns[thd_id] == txn_id);
  active_txns[thd_id] = UINT64_MAX;
  pthread_cond_broadcast(&cond);
  pthread_mutex_unlock(&mtx);
}

void QWorkQueue::update_hash(int tid, uint64_t id) {
  set_active(tid,id);
  //hash->update_qhash(id);
}

void QWorkQueue::done(int tid, uint64_t id) {
  delete_active(tid,id);
  /*
  int idx = get_idx(tid);
    return queue[idx].done(id);
    */
}

void QWorkQueue::add_abort_query(int tid, base_query * qry) {
  uint64_t starttime = get_sys_clock();
  if(qry->penalty == 0)
    qry->penalty = g_abort_penalty;
#if BACKOFF
  else {
    if(qry->penalty * 2 < g_abort_penalty_max)
      qry->penalty = qry->penalty * 2;
  }
#endif
  qry->penalty_end = starttime + qry->penalty;
  aq.enqueue(qry);
  ATOM_ADD(abrt_cnt,1);
  //queue[idx].add_abort_query(qry);
  INC_STATS(tid,aq_enqueue,get_sys_clock() - starttime);
}

bool QWorkQueue::poll_abort(int tid) {
  uint64_t starttime = get_sys_clock();
  //bool result = queue[idx].poll_abort();
  bool result = false;
  if(!aq_head && aq.size_approx() > 0) {
    aq.try_dequeue(aq_head);
  }
  if(aq_head && aq_head->penalty_end < starttime)
    result = true;
  INC_STATS(0,aq_poll,get_sys_clock() - starttime);
  return result;
} 

void QWorkQueue::process_aborts() {
    base_query * qry = NULL;
    uint64_t starttime = get_sys_clock();
    bool valid = aq.try_dequeue(qry);
    int restarts = 0;
    if(valid) 
      restarts = queue[0].add_abort_query(qry);
    else
      restarts = queue[0].check_abort_query();
    ATOM_SUB(abrt_cnt,restarts);
    INC_STATS(0,aq_dequeue,get_sys_clock() - starttime);

}

base_query * QWorkQueue::get_next_abort_query(int tid) {
  uint64_t starttime = get_sys_clock();
  //base_query * rtn_qry = queue[idx].get_next_abort_query();
  base_query * rtn_qry = NULL;
  if(aq_head && aq_head->penalty_end < starttime) {
    ATOM_SUB(abrt_cnt,1);
    rtn_qry = aq_head;
    aq_head = NULL;
  }
  INC_STATS(0,aq_dequeue,get_sys_clock() - starttime);
  return rtn_qry;
  /*
  if(rtn_qry) {
    ATOM_SUB(abrt_cnt,1);
  }
  return rtn_qry;
  */
}

/**********************
  Hash table
  *********************/
void QHash::init() {
  id_hash_size = 1069;
  //id_hash_size = g_inflight_max*g_node_cnt;
  id_hash = new id_entry_t[id_hash_size];
  for(uint64_t i = 0; i < id_hash_size; i++) {
    id_hash[i] = NULL;
  }
  pthread_mutex_init(&mtx,NULL);
}

void QHash::lock() {
  pthread_mutex_lock(&mtx);
}

void QHash::unlock() {
  pthread_mutex_unlock(&mtx);
}

bool QHash::in_qhash(uint64_t id) {
  if( id == UINT64_MAX)
    return true;
  id_entry_t bin = id_hash[id % id_hash_size];
  while(bin && bin->id != id) {
    bin = bin->next;
  }
  if(bin)
    return true;
  return false;

}

void QHash::add_qhash(uint64_t id) {
  if(id == UINT64_MAX)
    return;
  id_entry * entry = new id_entry;
  entry->id = id;
  entry->next = id_hash[id % id_hash_size];
  id_hash[id % id_hash_size] = entry;
}

void QHash::update_qhash(uint64_t id) {
  pthread_mutex_lock(&mtx);
  id_entry * entry = new id_entry;
  entry->id = id;
  entry->next = id_hash[id % id_hash_size];
  id_hash[id % id_hash_size] = entry;
  pthread_mutex_unlock(&mtx);
}

void QHash::remove_qhash(uint64_t id) {
  if(id ==UINT64_MAX)
    return;
  pthread_mutex_lock(&mtx);

  id_entry_t bin = id_hash[id % id_hash_size];
  id_entry_t prev = NULL;
  while(bin && bin->id != id) {
    prev = bin;
    bin = bin->next;
  }
  assert(bin);

  if(!prev)
    id_hash[id % id_hash_size] = bin->next;
  else
    prev->next = bin->next;

  pthread_mutex_unlock(&mtx);

}

/*************************
  Query Work Queue Helper
  ************************/
void QWorkQueueHelper::init(QHash * hash) {
  cnt = 0;
  head = NULL;
  tail = NULL;
  this->hash = hash;
  last_add_time = 0;
  pthread_mutex_init(&mtx,NULL);

}

bool QWorkQueueHelper::poll_next_query() {
  return cnt > 0;
}

void QWorkQueueHelper::finish(uint64_t time) {
  if(last_add_time != 0)
    INC_STATS(0,qq_full,time - last_add_time);
}

void QWorkQueueHelper::abort_finish(uint64_t time) {
  if(last_add_time != 0)
    INC_STATS(0,aq_full,time - last_add_time);
}

void QWorkQueueHelper::add_query(base_query * qry) {

  wq_entry_t entry = (wq_entry_t) mem_allocator.alloc(sizeof(struct wq_entry), 0);
  entry->qry = qry;
  entry->next  = NULL;
  entry->starttime = get_sys_clock();
  assert(qry->rtype <= NO_MSG);

  pthread_mutex_lock(&mtx);

  //printf("Add Query %ld %d\n",qry->txn_id,qry->rtype);
  wq_entry_t n = head;
#if !PRIORITY_WORK_QUEUE
  n = NULL;
#endif

  while(n) {
    assert(CC_ALG==HSTORE_SPEC || n->qry != qry);
#if CC_ALG == HSTORE_SPEC
    if(n->qry == qry)
      goto final;
#endif
    
#if PRIORITY_WORK_QUEUE
    /*
#if CC_ALG == HSTORE_SPEC
    if(qry->spec)
      break;
#else
*/
  if(qry->txn_id == UINT64_MAX || qry->abort_restart) {
    n = NULL;
    break;
  }
    if(n->qry->txn_id > entry->qry->txn_id)
      break;
//#endif
#endif
    n = n->next;
  }

  if(n) {
    LIST_INSERT_BEFORE(n,entry,head);
  }
  else {
    LIST_PUT_TAIL(head,tail,entry);
  }
  cnt++;

#if CC_ALG == HSTORE_SPEC
final:
#endif
  if(last_add_time == 0)
    last_add_time = get_sys_clock();

  pthread_mutex_unlock(&mtx);
}

void QWorkQueueHelper::remove_query(base_query* qry) {

  pthread_mutex_lock(&mtx);

  if(cnt > 0) {
    wq_entry_t next = head;
    while(next) {
      if(next->qry == qry) {
        LIST_REMOVE_HT(next,head,tail);
        cnt--;
        mem_allocator.free(next,sizeof(struct wq_entry));
        break;
      }
      next = next->next;
    }

  }

  pthread_mutex_unlock(&mtx);

}

base_query * QWorkQueueHelper::get_next_query_client() {
  base_query * next_qry = NULL;

  pthread_mutex_lock(&mtx);
  if(cnt > 0) {
    wq_entry_t next = head;
    next_qry = next->qry;
    LIST_REMOVE_HT(next,head,tail);
    cnt--;

    mem_allocator.free(next,sizeof(struct wq_entry));
  }
  pthread_mutex_unlock(&mtx);
  return next_qry;
}

base_query * QWorkQueueHelper::get_next_query(int id) {
  base_query * next_qry = NULL;

  uint64_t thd_prof_start = get_sys_clock();
  pthread_mutex_lock(&mtx);
    INC_STATS(id,thd_prof_wq1,get_sys_clock() - thd_prof_start);
    thd_prof_start = get_sys_clock();
  hash->lock();
    INC_STATS(id,thd_prof_wq2,get_sys_clock() - thd_prof_start);
    thd_prof_start = get_sys_clock();

  uint64_t starttime = get_sys_clock();

  assert( ( (cnt == 0) && head == NULL && tail == NULL) || ( (cnt > 0) && head != NULL && tail !=NULL) );

  wq_entry_t next = NULL;
  if(cnt > 0) {
    next = head;
    while(next) {
      if(next->qry->txn_id == UINT64_MAX || !hash->in_qhash(next->qry->txn_id)) {
        next_qry = next->qry;
        if(next_qry->txn_id != UINT64_MAX) {
          hash->add_qhash(next_qry->txn_id);
          assert(hash->in_qhash(next_qry->txn_id));
        }
        LIST_REMOVE_HT(next,head,tail);
        cnt--;

      uint64_t t = get_sys_clock() - next->starttime;
      next_qry->time_q_work += t;
      INC_STATS(0,qq_cnt,1);
      INC_STATS(0,qq_lat,t);

        mem_allocator.free(next,sizeof(struct wq_entry));
        break;
      }
      next = next->next;
    }

  }

  if(cnt == 0 && last_add_time != 0) {
    INC_STATS(0,qq_full,get_sys_clock() - last_add_time);
    last_add_time = 0;
  }

  /*
  wq_entry_t n = head;
  while(n) {
    assert(n->qry != next_qry);
    n = n->next;
  }
  */

    INC_STATS(id,thd_prof_wq3,get_sys_clock() - thd_prof_start);
    thd_prof_start = get_sys_clock();

  INC_STATS(0,time_qq,get_sys_clock() - starttime);
  hash->unlock();
//  if(next_qry)
//    printf("Get Query %ld %d\n",next_qry->txn_id,next_qry->rtype);
  pthread_mutex_unlock(&mtx);
    INC_STATS(id,thd_prof_wq4,get_sys_clock() - thd_prof_start);
  return next_qry;
}


// Remove hash
void QWorkQueueHelper::done(uint64_t id) {
  //hash->remove_qhash(id);
}

bool QWorkQueueHelper::poll_abort() {
  bool ready = false;
  pthread_mutex_lock(&mtx);
  wq_entry_t elem = head;
  if(elem)
    ready = (get_sys_clock() >= elem->qry->penalty_end);
  pthread_mutex_unlock(&mtx);
    //return (get_sys_clock() - elem->qry->penalty_start) >= g_abort_penalty;
  return ready;
}

int QWorkQueueHelper::add_abort_query(base_query * qry) {

  int restarts = 0;
  wq_entry_t entry = (wq_entry_t) mem_allocator.alloc(sizeof(struct wq_entry), 0);

  /*
  if(qry->penalty == 0)
    qry->penalty = g_abort_penalty;
#if BACKOFF
  else {
    if(qry->penalty * 2 < g_abort_penalty_max)
      qry->penalty = qry->penalty * 2;
  }
#endif

  qry->penalty_end = get_sys_clock() + qry->penalty;
*/
  entry->qry = qry;
  entry->next  = NULL;
  entry->starttime = get_sys_clock();
  assert(qry->rtype <= NO_MSG);
  uint64_t starttime = get_sys_clock();

  pthread_mutex_lock(&mtx);

  wq_entry_t n = head;
  while(n) {
    if(n->qry->penalty_end < starttime) {
      LIST_REMOVE_HT(n,head,tail);
      INC_STATS(0,txn_time_q_abrt,starttime - n->starttime);
      work_queue.enqueue(0,n->qry);
      mem_allocator.free(n,sizeof(struct wq_entry));
      restarts ++;
      n = head;
      continue;
    }
    if(n->qry->penalty_end >= entry->qry->penalty_end)
      break;
    n = n->next;
  }

  if(n) {
    LIST_INSERT_BEFORE(n,entry,head);
  }
  else {
    LIST_PUT_TAIL(head,tail,entry);
  }

  cnt++;

  if(last_add_time == 0)
    last_add_time = get_sys_clock();

  pthread_mutex_unlock(&mtx);
  return restarts;
}

int QWorkQueueHelper::check_abort_query() {

  int restarts = 0;
  uint64_t starttime = get_sys_clock();
  pthread_mutex_lock(&mtx);

  wq_entry_t n = head;
  while(n) {
    if(n->qry->penalty_end < starttime) {
      LIST_REMOVE_HT(n,head,tail);
      INC_STATS(0,txn_time_q_abrt,starttime - n->starttime);
      work_queue.enqueue(0,n->qry);
      restarts++;
      mem_allocator.free(n,sizeof(struct wq_entry));
      n = head;
    } else
      break;
  }
 
  pthread_mutex_unlock(&mtx);
  return restarts;
}

base_query * QWorkQueueHelper::get_next_abort_query() {
  base_query * next_qry = NULL;
  wq_entry_t elem = NULL;

  pthread_mutex_lock(&mtx);

  if(cnt > 0) {
    elem = head;
    assert(elem);
    if(get_sys_clock() > elem->qry->penalty_end) {
      LIST_REMOVE_HT(elem,head,tail);
      //LIST_GET_HEAD(head,tail,elem);
      next_qry = elem->qry;
      if(next_qry)
        next_qry->time_q_abrt += get_sys_clock() - elem->starttime;
      cnt--;

      mem_allocator.free(elem,sizeof(struct wq_entry));
    }

  }

  if(cnt == 0 && last_add_time != 0) {
    INC_STATS(0,aq_full,get_sys_clock() - last_add_time);
    last_add_time = 0;
  }

  assert((head && tail && cnt > 0) || (!head && !tail && cnt ==0));


  pthread_mutex_unlock(&mtx);
  return next_qry;
}
