#include "query_work_queue.h"
#include "mem_alloc.h"
#include "query.h"

void QWorkQueue::init() {
  cnt = 0;
  head = NULL;
  tail = NULL;
  last_add_time = 0;
  pthread_mutex_init(&mtx,NULL);
}

bool QWorkQueue::poll_next_query() {
  return cnt > 0;
}

void QWorkQueue::add_query(base_query * qry) {

  wq_entry_t entry = (wq_entry_t) mem_allocator.alloc(sizeof(struct wq_entry), 0);
  entry->qry = qry;
  entry->next  = NULL;
  assert(qry->rtype <= RPASS);

  pthread_mutex_lock(&mtx);

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
    next_qry = head->qry;
    head = head->next;
    cnt--;

    if(cnt == 0) {
      tail = NULL;
    }
  }

  if(cnt == 0) {
    INC_STATS(0,qq_full,get_sys_clock() - last_add_time);
    last_add_time = 0;
  }

  pthread_mutex_unlock(&mtx);
  return next_qry;
}
