#include "query_work_queue.h"
#include "mem_alloc.h"
#include "query.h"

void QWorkQueue::init() {
  cnt = 0;
  head = NULL;
  tail = NULL;
  last_add_time = 0;
  pthread_mutex_init(&mtx,NULL);

  id_hash_size = 1069;
  //id_hash_size = MAX_TXN_IN_FLIGHT*g_node_cnt;
  id_hash = new id_entry_t[id_hash_size];
  for(uint64_t i = 0; i < id_hash_size; i++) {
    id_hash[i] = NULL;
  }
}

bool QWorkQueue::poll_next_query() {
  return cnt > 0;
}

void QWorkQueue::finish(uint64_t time) {
  if(last_add_time != 0)
    INC_STATS(0,qq_full,time - last_add_time);
}

void QWorkQueue::abort_finish(uint64_t time) {
  if(last_add_time != 0)
    INC_STATS(0,aq_full,time - last_add_time);
}

void QWorkQueue::add_query(base_query * qry) {

  wq_entry_t entry = (wq_entry_t) mem_allocator.alloc(sizeof(struct wq_entry), 0);
  entry->qry = qry;
  entry->next  = NULL;
  assert(qry->rtype <= CL_RSP);

  pthread_mutex_lock(&mtx);

  wq_entry_t n = head;
  while(n) {
    assert(n->qry != qry);
    n = n->next;
  }

  if(cnt > 0) {
    tail->next = entry;
  }
  if(cnt == 0) {
    head = entry;
  }
  tail = entry;
  cnt++;

  if(last_add_time == 0)
    last_add_time = get_sys_clock();

  pthread_mutex_unlock(&mtx);
}

base_query * QWorkQueue::get_next_query() {
  base_query * next_qry = NULL;

  pthread_mutex_lock(&mtx);

  assert( ( (cnt == 0) && head == NULL && tail == NULL) || ( (cnt > 0) && head != NULL && tail !=NULL) );

  if(cnt > 0) {
    wq_entry_t next = head;
    wq_entry_t prev = NULL;
    while(next) {
      if(next->qry->txn_id == UINT64_MAX || !in_hash(next->qry->txn_id)) {
        next_qry = next->qry;
        add_hash(next_qry->txn_id);
        if(prev == NULL)
          head = head->next;
        else
          prev->next = next->next; 
        cnt--;
        if(next == tail) 
          tail = prev;
        assert(in_hash(next_qry->txn_id));
        break;
      }
      prev = next;
      next = next->next;
    }

    if(cnt == 0) {
      tail = NULL;
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

  pthread_mutex_unlock(&mtx);
  return next_qry;
}

bool QWorkQueue::in_hash(uint64_t id) {
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

void QWorkQueue::add_hash(uint64_t id) {
  if(id == UINT64_MAX)
    return;
  id_entry * entry = new id_entry;
  entry->id = id;
  entry->next = id_hash[id % id_hash_size];
  id_hash[id % id_hash_size] = entry;
}

void QWorkQueue::update_hash(uint64_t id) {
  pthread_mutex_lock(&mtx);
  id_entry * entry = new id_entry;
  entry->id = id;
  entry->next = id_hash[id % id_hash_size];
  id_hash[id % id_hash_size] = entry;
  pthread_mutex_unlock(&mtx);
}


// Remove hash
void QWorkQueue::done(uint64_t id) {
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

bool QWorkQueue::poll_abort() {
  wq_entry_t elem = head;
  if(elem)
    return (get_sys_clock() - elem->qry->penalty_start) >= g_abort_penalty;
  return false;
}

base_query * QWorkQueue::get_next_abort_query() {
  base_query * next_qry = NULL;

  pthread_mutex_lock(&mtx);

  if(cnt > 0) {
    wq_entry_t elem = head;
    assert(elem);
    if((get_sys_clock() - elem->qry->penalty_start) >= g_abort_penalty) {
      next_qry = elem->qry;
      head = head->next;
      cnt--;
    }

    if(cnt == 0) {
      tail = NULL;
      assert(head == NULL);
    }
  }

  if(cnt == 0 && last_add_time != 0) {
    INC_STATS(0,aq_full,get_sys_clock() - last_add_time);
    last_add_time = 0;
  }

  pthread_mutex_unlock(&mtx);
  return next_qry;
}
