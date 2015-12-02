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
  msg_entry_t entry;
  msg_pool.get(entry);
  entry->qry = qry;
  entry->dest = dest;
  entry->type = type;
  entry->next  = NULL;
  entry->prev  = NULL;
  entry->starttime = get_sys_clock();

  mq.enqueue(entry);

}

uint64_t MessageQueue::dequeue(base_query *& qry, RemReqType & type, uint64_t & dest) {
  msg_entry * entry;
  uint64_t time;
  bool r = mq.try_dequeue(entry);
  if(r) {
    qry = entry->qry;
    type = entry->type;
    dest = entry->dest;
    time = entry->starttime;
    msg_pool.put(entry);
  } else {
    qry = NULL;
    type = NO_MSG;
    dest = UINT64_MAX;
    time = 0;
  }
  return time;
}
