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
#include <boost/lockfree/queue.hpp>

void MessageQueue::init() {
  //m_queue = new boost::lockfree::queue<msg_entry* > (0);
  m_queue = new boost::lockfree::queue<msg_entry* > * [g_this_send_thread_cnt];
  for(uint64_t i = 0; i < g_this_send_thread_cnt; i++) {
    m_queue[i] = new boost::lockfree::queue<msg_entry* > (0);
  }
  ctr = new  uint64_t * [g_this_send_thread_cnt];
  for(uint64_t i = 0; i < g_this_send_thread_cnt; i++) {
    ctr[i] = (uint64_t*) mem_allocator.align_alloc(sizeof(uint64_t));
    *ctr[i] = i % g_thread_cnt;
  }
}

void MessageQueue::enqueue(uint64_t thd_id, Message * msg,uint64_t dest) {
  DEBUG("MQ Enqueue %ld\n",dest)
  assert(dest < g_total_node_cnt);
  assert(dest != g_node_id);
  msg_entry_t entry;
  msg_pool.get(entry);
  entry->msg = msg;
  entry->dest = dest;
  entry->starttime = get_sys_clock();
  assert(entry->dest < g_total_node_cnt);
  uint64_t mtx_time_start = get_sys_clock();
  uint64_t rand = mtx_time_start % g_this_send_thread_cnt;
  while(!m_queue[rand]->push(entry) && !simulation->is_done()) {}
  INC_STATS(thd_id,mtx[3],get_sys_clock() - mtx_time_start);
  INC_STATS(thd_id,msg_queue_enq_cnt,1);


}

uint64_t MessageQueue::dequeue(uint64_t thd_id, Message *& msg) {
  msg_entry * entry = NULL;
  uint64_t dest = UINT64_MAX;
  uint64_t mtx_time_start = get_sys_clock();
  bool valid = false;
  //uint64_t ctr_id = thd_id % g_this_send_thread_cnt;
  //uint64_t start_ctr = *ctr[ctr_id];
  valid = m_queue[thd_id%g_this_send_thread_cnt]->pop(entry);
  /*
  while(!valid && !simulation->is_done()) {
    ++(*ctr[ctr_id]);
    if(*ctr[ctr_id] >= g_this_thread_cnt)
      *ctr[ctr_id] = 0;
    valid = m_queue[*ctr[ctr_id]]->pop(entry);
    if(*ctr[ctr_id] == start_ctr)
      break;
  }
  */
  INC_STATS(thd_id,mtx[4],get_sys_clock() - mtx_time_start);
  uint64_t curr_time = get_sys_clock();
  if(valid) {
    msg = entry->msg;
    dest = entry->dest;
    assert(dest < g_total_node_cnt);
    DEBUG("MQ Dequeue %ld\n",dest)
    INC_STATS(thd_id,msg_queue_delay_time,curr_time - entry->starttime);
    INC_STATS(thd_id,msg_queue_cnt,1);
    msg_pool.put(entry);
  } else {
    msg = NULL;
    dest = UINT64_MAX;
  }
  INC_STATS(thd_id,mtx[5],get_sys_clock() - curr_time);
  return dest;
}
