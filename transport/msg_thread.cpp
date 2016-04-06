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

#include "msg_thread.h"
#include "msg_queue.h"
#include "message.h"
#include "mem_alloc.h"
#include "transport.h"
#include "query.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "pool.h"
#include "global.h"

void MessageThread::init(uint64_t thd_id) { 
  buffer_cnt = g_node_cnt + g_client_node_cnt + g_repl_cnt * g_node_cnt;
#if CC_ALG == CALVIN
  buffer_cnt++;
#endif
  buffer = (mbuf **) mem_allocator.alloc(sizeof(mbuf*) * buffer_cnt);
  for(uint64_t n = 0; n < buffer_cnt; n++) {
    buffer[n] = (mbuf *)mem_allocator.alloc(sizeof(mbuf));
    buffer[n]->init(n);
    buffer[n]->reset(n);
  }
  _thd_id = thd_id;
  head_qry = NULL;
}

void MessageThread::check_and_send_batches() {
  for(uint64_t n = 0; n < buffer_cnt; n++) {
    if(buffer[n]->ready()) {
      send_batch(n);
    }
  }
}

void MessageThread::send_batch(uint64_t id) {
    mbuf * sbuf = buffer[id];
    assert(sbuf->cnt > 0);
	  ((uint32_t*)sbuf->buffer)[2] = sbuf->cnt;
    INC_STATS(_thd_id,mbuf_send_intv_time,get_sys_clock() - sbuf->starttime);

    DEBUG("Send batch of %ld msgs to %ld\n",sbuf->cnt,id);
    tport_man.send_msg(_thd_id,id,sbuf->buffer,sbuf->ptr);

    INC_STATS(_thd_id,msg_batch_size_msgs,sbuf->cnt);
    INC_STATS(_thd_id,msg_batch_size_bytes,sbuf->ptr);
    INC_STATS(_thd_id,msg_batch_cnt,1);
    sbuf->reset(id);
}

void MessageThread::run() {
  
  Message * msg;
  uint64_t dest;
  mbuf * sbuf;


  dest = msg_queue.dequeue(msg);
  if(!msg) {
    check_and_send_batches();
    return;
  }
  assert(msg);
  assert(dest < g_node_cnt + g_client_node_cnt + g_repl_cnt*g_node_cnt);
  assert(dest != g_node_id);

  sbuf = buffer[dest];

  if(!sbuf->fits(msg->get_size())) {
    send_batch(dest);
  }

  msg->copy_to_buf(&(sbuf->buffer[sbuf->ptr]));
  DEBUG("Buffered Msg %d to %ld\n",msg->rtype,dest);
  sbuf->cnt += 1;
  sbuf->ptr += msg->get_size();
  if(sbuf->starttime == 0)
    sbuf->starttime = get_sys_clock();

  check_and_send_batches();

}

