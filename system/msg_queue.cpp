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
  entry->qry = qry;
  entry->dest = dest;
  entry->type = type;
  entry->next  = NULL;
  entry->prev  = NULL;
  entry->starttime = get_sys_clock();

  mq.enqueue(entry);

}

uint64_t MessageQueue::dequeue(BaseQuery *& qry, RemReqType & type, uint64_t & dest) {
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
