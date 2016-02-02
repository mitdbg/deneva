/*
   Copyright 2015 Rachael Harding

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#include "msg_queue.h"
#include "mem_alloc.h"
#include "query.h"
#include "pool.h"

void MessageQueue::init() {
  cnt = 0;
  head = NULL;
  tail = NULL;
  pthread_mutex_init(&mtx,NULL);
}

void MessageQueue::enqueue(BaseQuery * qry,RemReqType type,uint64_t dest) {
  msg_entry_t entry;
  msg_pool.get(entry);
  if(type == RFIN) {
    BaseQuery * qry2;
    qry_pool.get(qry2);
    qry2->pid = qry->pid;
    qry2->rc = qry->rc;
    qry2->txn_id = qry->txn_id;
    qry2->batch_id = qry->batch_id;
    qry2->ro = qry->ro;
    qry2->rtype = qry->rtype;
    entry->qry = qry2;
  } else {
    entry->qry = qry;
  }
  entry->dest = dest;
  entry->type = type;
  entry->next  = NULL;
  entry->prev  = NULL;
  entry->starttime = get_sys_clock();
  ATOM_ADD(cnt,1);
#ifdef MSG_QUEUE_CONCURRENT_QUEUE
  mq.enqueue(entry);
#else
  pthread_mutex_lock(&mtx);
  LIST_PUT_TAIL(head,tail,entry);
  pthread_mutex_unlock(&mtx);
#endif


}

uint64_t MessageQueue::dequeue(BaseQuery *& qry, RemReqType & type, uint64_t & dest) {
  msg_entry * entry;
  uint64_t time;
#ifdef MSG_QUEUE_CONCURRENT_QUEUE
  bool r = mq.try_dequeue(entry);
#else
  pthread_mutex_lock(&mtx);
  LIST_GET_HEAD(head,tail,entry);
  pthread_mutex_unlock(&mtx);
  bool r = entry != NULL;
#endif
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
