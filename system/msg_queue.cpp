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
#include "message.h"

void MessageQueue::init() {
  cnt = 0;
#ifndef MSG_QUEUE_CONCURRENT_QUEUE
  head = NULL;
  tail = NULL;
  pthread_mutex_init(&mtx,NULL);
#endif
}

void MessageQueue::enqueue(Message * msg,uint64_t dest) {
  assert(dest < g_node_cnt + g_client_node_cnt + g_repl_cnt*g_node_cnt);
  DEBUG("MQ Enqueue %ld\n",dest)
  msg_entry_t entry;
  msg_pool.get(entry);
  entry->msg = msg;
  entry->next  = NULL;
  entry->prev  = NULL;
  entry->dest = dest;
  entry->starttime = get_sys_clock();
  pthread_mutex_lock(&mtx);
  LIST_PUT_TAIL(head,tail,entry);
  ATOM_ADD(cnt,1);
  pthread_mutex_unlock(&mtx);


}

uint64_t MessageQueue::dequeue(Message *& msg) {
  msg_entry * entry = NULL;
  uint64_t dest = UINT64_MAX;
  pthread_mutex_lock(&mtx);
  uint64_t curr_time = get_sys_clock();
  if(head && (ISCLIENT || (curr_time - head->starttime > g_network_delay))) {
    LIST_GET_HEAD(head,tail,entry);
    ATOM_SUB(cnt,1);
  }
  pthread_mutex_unlock(&mtx);
  if(entry) {
    msg = entry->msg;
    dest = entry->dest;
    DEBUG("MQ Dequeue %ld\n",dest)
    INC_STATS(0,msg_queue_delay_time,curr_time - entry->starttime);
    INC_STATS(0,msg_queue_cnt,1);
    msg_pool.put(entry);
  } else {
    msg = NULL;
    dest = UINT64_MAX;
  }
  return dest;
}
