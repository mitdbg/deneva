#include "msg_queue.h"
#include "mem_alloc.h"
#include "query.h"
#include "txn_pool.h"

void MessageQueue::init() {
  cnt = 0;
  head = NULL;
  tail = NULL;
  pthread_mutex_init(&mtx,NULL);
}
void MessageQueue::enqueue(base_query * qry,RemReqType type,uint64_t dest) {
  if(qry) {
    DEBUG("Queueing Message %d txn %ld dest %ld\n",type,qry->txn_id,dest);
  } else {
    DEBUG("Queueing Message %d txn NULL dest %ld\n",type,dest);
  }
  //msg_entry_t entry = (msg_entry_t) mem_allocator.alloc(sizeof(struct msg_entry), 0);
  msg_entry_t entry;
  msg_pool.get(entry);
  entry->qry = qry;
  //entry->qry = (base_query*) qry;
  entry->dest = dest;
  entry->type = type;
  entry->next  = NULL;
  entry->prev  = NULL;
  entry->starttime = get_sys_clock();

  mq.enqueue(entry);
  /*
  pthread_mutex_lock(&mtx);

  LIST_PUT_TAIL(head,tail,entry);
  cnt++;

  if(last_add_time == 0)
    last_add_time = entry->starttime;

  pthread_mutex_unlock(&mtx);
  */

}

RemReqType MessageQueue::dequeue(base_query *& qry, uint64_t & dest) {
  msg_entry * entry;
  RemReqType t;
  bool r = mq.try_dequeue(entry);
  if(r) {
    qry = entry->qry;
    t = entry->type;
    dest = entry->dest;
    msg_pool.put(entry);
  } else {
    qry = NULL;
    t = NO_MSG;
    dest = UINT64_MAX;
  }
  /*
  pthread_mutex_lock(&mtx);
  if(cnt > 0) {
    entry = head;
    LIST_GET_HEAD(head,tail,entry);
    qry = entry->qry;
    t = entry->type;
    dest = entry->dest;
    mem_allocator.free(entry,sizeof(struct msg_entry));
    cnt--;
  }
  else {
    qry = NULL;
    t = NO_MSG;
    dest = UINT64_MAX;
  }

  if(cnt == 0 && last_add_time != 0) {
    INC_STATS(0,mq_full,get_sys_clock() - last_add_time);
    last_add_time = 0;
  }

  pthread_mutex_unlock(&mtx);
  */
  return t;
}
