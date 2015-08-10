#include "query_work_queue.h"
#include "mem_alloc.h"
#include "query.h"

void QWorkQueue::init() {
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
  q_len = PART_CNT / NODE_CNT;
#else
  q_len = 1;
#endif
  hash = (QHash *) mem_allocator.alloc(sizeof(QHash), 0);
  hash->init();
  queue = (QWorkQueueHelper *) mem_allocator.alloc(sizeof(QWorkQueueHelper) * q_len, 0);
  for(int i = 0;i<q_len;i++) {
    queue[i].init(hash); 
  }
}

int inline QWorkQueue::get_idx(int input) {
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
    int idx = input;
#else
    int idx = 0;
#endif
    return idx;
}

bool QWorkQueue::poll_next_query(int tid) {
  int idx = get_idx(tid);
    return queue[idx].poll_next_query();
}
void QWorkQueue::finish(uint64_t time) {
    for(int idx=0;idx<q_len;idx++)
      queue[idx].finish(time);
} 
void QWorkQueue::abort_finish(uint64_t time) {
    for(int idx=0;idx<q_len;idx++)
      queue[idx].abort_finish(time);
} 

// FIXME: Access only 1 partition at a time; what if qry is being restarted at only 1 partition?
void QWorkQueue::add_query(int tid, base_query * qry) {
  int idx = get_idx(tid);
    queue[idx].add_query(qry);
}

// FIXME: Only should abort at 1 partition
void QWorkQueue::add_abort_query(int tid, base_query * qry) {
  int idx = get_idx(tid);
    queue[idx].add_query(qry);
}

base_query * QWorkQueue::get_next_query(int tid) {
  int idx = get_idx(tid);
    return queue[idx].get_next_query();
}

void QWorkQueue::remove_query(int tid, base_query * qry) {
  int idx = get_idx(tid);
    queue[idx].remove_query(qry);
}

void QWorkQueue::update_hash(int tid, uint64_t id) {
  hash->update_hash(id);
  /*
  int idx = get_idx(tid);
    queue[idx].update_hash(id);
    */
}

void QWorkQueue::done(int tid, uint64_t id) {
  int idx = get_idx(tid);
    return queue[idx].done(id);
}
bool QWorkQueue::poll_abort(int tid) {
  int idx = get_idx(tid);
    return queue[idx].poll_abort();
} 
base_query * QWorkQueue::get_next_abort_query(int tid) {
  int idx = get_idx(tid);
    return queue[idx].get_next_abort_query();
}

/**********************
  Hash table
  *********************/
void QHash::init() {
  id_hash_size = 1069;
  //id_hash_size = MAX_TXN_IN_FLIGHT*g_node_cnt;
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

bool QHash::in_hash(uint64_t id) {
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

void QHash::add_hash(uint64_t id) {
  if(id == UINT64_MAX)
    return;
  id_entry * entry = new id_entry;
  entry->id = id;
  entry->next = id_hash[id % id_hash_size];
  id_hash[id % id_hash_size] = entry;
}

void QHash::update_hash(uint64_t id) {
  pthread_mutex_lock(&mtx);
  id_entry * entry = new id_entry;
  entry->id = id;
  entry->next = id_hash[id % id_hash_size];
  id_hash[id % id_hash_size] = entry;
  pthread_mutex_unlock(&mtx);
}

void QHash::remove_hash(uint64_t id) {
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
  assert(qry->rtype <= CL_RSP);

  pthread_mutex_lock(&mtx);

  wq_entry_t n = head;
  while(n) {
    assert(n->qry != qry);
#if PRIORITY_WORK_QUEUE
#if CC_ALG == HSTORE_SPEC
    if(qry->spec)
      break;
#else
    if(n->qry->txn_id > entry->qry->txn_id)
      break;
#endif
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
        break;
      }
      next = next->next;
    }

  }

  pthread_mutex_unlock(&mtx);

}


base_query * QWorkQueueHelper::get_next_query() {
  base_query * next_qry = NULL;

  pthread_mutex_lock(&mtx);
  hash->lock();

  uint64_t starttime = get_sys_clock();

  assert( ( (cnt == 0) && head == NULL && tail == NULL) || ( (cnt > 0) && head != NULL && tail !=NULL) );

  if(cnt > 0) {
    wq_entry_t next = head;
    while(next) {
      if(next->qry->txn_id == UINT64_MAX || !hash->in_hash(next->qry->txn_id)) {
        next_qry = next->qry;
        hash->add_hash(next_qry->txn_id);
        LIST_REMOVE_HT(next,head,tail);
        cnt--;
        assert(hash->in_hash(next_qry->txn_id));
        break;
      }
      next = next->next;
    }

    if(next_qry) {
      uint64_t t = get_sys_clock() - next->starttime;
      next_qry->time_q_work += t;
      INC_STATS(0,qq_cnt,1);
      INC_STATS(0,qq_lat,t);
    }
  }

  if(cnt == 0 && last_add_time != 0) {
    INC_STATS(0,qq_full,get_sys_clock() - last_add_time);
    last_add_time = 0;
  }

  wq_entry_t n = head;
  while(n) {
    assert(n->qry != next_qry);
    n = n->next;
  }


  INC_STATS(0,time_qq,get_sys_clock() - starttime);
  INC_STATS(0,time_qq,get_sys_clock() - starttime);
  hash->unlock();
  pthread_mutex_unlock(&mtx);
  return next_qry;
}


// Remove hash
void QWorkQueueHelper::done(uint64_t id) {
  hash->remove_hash(id);
}

bool QWorkQueueHelper::poll_abort() {
  wq_entry_t elem = head;
  if(elem)
    return (get_sys_clock() >= elem->qry->penalty_end);
    //return (get_sys_clock() - elem->qry->penalty_start) >= g_abort_penalty;
  return false;
}

void QWorkQueueHelper::add_abort_query(base_query * qry) {

  wq_entry_t entry = (wq_entry_t) mem_allocator.alloc(sizeof(struct wq_entry), 0);

  if(qry->penalty == 0)
    qry->penalty = g_abort_penalty;
#if BACKOFF
  else
    qry->penalty = qry->penalty * 2;
#endif

  qry->penalty_end = get_sys_clock() + qry->penalty;
  entry->qry = qry;
  entry->next  = NULL;
  entry->starttime = get_sys_clock();
  assert(qry->rtype <= CL_RSP);

  pthread_mutex_lock(&mtx);

  wq_entry_t n = head;
  while(n) {
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
  assert((head && tail));

  if(last_add_time == 0)
    last_add_time = get_sys_clock();

  pthread_mutex_unlock(&mtx);
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
