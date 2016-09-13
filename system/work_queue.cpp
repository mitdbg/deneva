/*
   Copyright 2016 Massachusetts Institute of Technology

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

#include "work_queue.h"
#include "mem_alloc.h"
#include "query.h"
#include "message.h"
#include "client_query.h"
#include <boost/lockfree/queue.hpp>

void QWorkQueue::init() {

  last_sched_dq = NULL;
  sched_ptr = 0;
  seq_queue = new boost::lockfree::queue<work_queue_entry* > (0);
  work_queue = new boost::lockfree::queue<work_queue_entry* > (0);
  new_txn_queue = new boost::lockfree::queue<work_queue_entry* >(0);
  sched_queue = new boost::lockfree::queue<work_queue_entry* > * [g_node_cnt];
  for ( uint64_t i = 0; i < g_node_cnt; i++) {
    sched_queue[i] = new boost::lockfree::queue<work_queue_entry* > (0);
  }

}

void QWorkQueue::sequencer_enqueue(uint64_t thd_id, Message * msg) {
  uint64_t starttime = get_sys_clock();
  assert(msg);
  DEBUG_M("SeqQueue::enqueue work_queue_entry alloc\n");
  work_queue_entry * entry = (work_queue_entry*)mem_allocator.align_alloc(sizeof(work_queue_entry));
  entry->msg = msg;
  entry->rtype = msg->rtype;
  entry->txn_id = msg->txn_id;
  entry->batch_id = msg->batch_id;
  entry->starttime = get_sys_clock();
  assert(ISSERVER);

  DEBUG("Seq Enqueue (%ld,%ld)\n",entry->txn_id,entry->batch_id);
  while(!seq_queue->push(entry) && !simulation->is_done()) {}

  INC_STATS(thd_id,seq_queue_enqueue_time,get_sys_clock() - starttime);
  INC_STATS(thd_id,seq_queue_enq_cnt,1);

}

Message * QWorkQueue::sequencer_dequeue(uint64_t thd_id) {
  uint64_t starttime = get_sys_clock();
  assert(ISSERVER);
  Message * msg = NULL;
  work_queue_entry * entry = NULL;
  bool valid = seq_queue->pop(entry);
  
  if(valid) {
    msg = entry->msg;
    assert(msg);
    DEBUG("Seq Dequeue (%ld,%ld)\n",entry->txn_id,entry->batch_id);
    uint64_t queue_time = get_sys_clock() - entry->starttime;
    INC_STATS(thd_id,seq_queue_wait_time,queue_time);
    INC_STATS(thd_id,seq_queue_cnt,1);
    //DEBUG("DEQUEUE (%ld,%ld) %ld; %ld; %d, 0x%lx\n",msg->txn_id,msg->batch_id,msg->return_node_id,queue_time,msg->rtype,(uint64_t)msg);
  DEBUG_M("SeqQueue::dequeue work_queue_entry free\n");
    mem_allocator.free(entry,sizeof(work_queue_entry));
    INC_STATS(thd_id,seq_queue_dequeue_time,get_sys_clock() - starttime);
  }

  return msg;

}

void QWorkQueue::sched_enqueue(uint64_t thd_id, Message * msg) {
  assert(CC_ALG == CALVIN);
  assert(msg);
  assert(ISSERVERN(msg->return_node_id));
  uint64_t starttime = get_sys_clock();

  DEBUG_M("QWorkQueue::sched_enqueue work_queue_entry alloc\n");
  work_queue_entry * entry = (work_queue_entry*)mem_allocator.alloc(sizeof(work_queue_entry));
  entry->msg = msg;
  entry->rtype = msg->rtype;
  entry->txn_id = msg->txn_id;
  entry->batch_id = msg->batch_id;
  entry->starttime = get_sys_clock();

  DEBUG("Sched Enqueue (%ld,%ld)\n",entry->txn_id,entry->batch_id);
  uint64_t mtx_time_start = get_sys_clock();
  while(!sched_queue[msg->get_return_id()]->push(entry) && !simulation->is_done()) {}
  INC_STATS(thd_id,mtx[37],get_sys_clock() - mtx_time_start);

  INC_STATS(thd_id,sched_queue_enqueue_time,get_sys_clock() - starttime);
  INC_STATS(thd_id,sched_queue_enq_cnt,1);
}

Message * QWorkQueue::sched_dequeue(uint64_t thd_id) {
  uint64_t starttime = get_sys_clock();

  assert(CC_ALG == CALVIN);
  Message * msg = NULL;
  work_queue_entry * entry = NULL;

  bool valid = sched_queue[sched_ptr]->pop(entry);

  if(valid) {

    msg = entry->msg;
    DEBUG("Sched Dequeue (%ld,%ld)\n",entry->txn_id,entry->batch_id);

    uint64_t queue_time = get_sys_clock() - entry->starttime;
    INC_STATS(thd_id,sched_queue_wait_time,queue_time);
    INC_STATS(thd_id,sched_queue_cnt,1);

    DEBUG_M("QWorkQueue::sched_enqueue work_queue_entry free\n");
    mem_allocator.free(entry,sizeof(work_queue_entry));

    if(msg->rtype == RDONE) {
      // Advance to next queue or next epoch
      DEBUG("Sched RDONE %ld %ld\n",sched_ptr,simulation->get_worker_epoch());
      assert(msg->get_batch_id() == simulation->get_worker_epoch());
      if(sched_ptr == g_node_cnt - 1) {
        INC_STATS(thd_id,sched_epoch_cnt,1);
        INC_STATS(thd_id,sched_epoch_diff,get_sys_clock()-simulation->last_worker_epoch_time);
        simulation->next_worker_epoch();
      }
      sched_ptr = (sched_ptr + 1) % g_node_cnt;
      msg->release();
      msg = NULL;

    } else {
      simulation->inc_epoch_txn_cnt();
      DEBUG("Sched msg dequeue %ld (%ld,%ld) %ld\n",sched_ptr,msg->txn_id,msg->batch_id,simulation->get_worker_epoch());
      assert(msg->batch_id == simulation->get_worker_epoch());
    }

    INC_STATS(thd_id,sched_queue_dequeue_time,get_sys_clock() - starttime);
  }


  return msg;

}


void QWorkQueue::enqueue(uint64_t thd_id, Message * msg,bool busy) {
  uint64_t starttime = get_sys_clock();
  assert(msg);
  DEBUG_M("QWorkQueue::enqueue work_queue_entry alloc\n");
  work_queue_entry * entry = (work_queue_entry*)mem_allocator.align_alloc(sizeof(work_queue_entry));
  entry->msg = msg;
  entry->rtype = msg->rtype;
  entry->txn_id = msg->txn_id;
  entry->batch_id = msg->batch_id;
  entry->starttime = get_sys_clock();
  assert(ISSERVER || ISREPLICA);
  DEBUG("Work Enqueue (%ld,%ld) %d\n",entry->txn_id,entry->batch_id,entry->rtype);

  uint64_t mtx_wait_starttime = get_sys_clock();
  if(msg->rtype == CL_QRY) {
    while(!new_txn_queue->push(entry) && !simulation->is_done()) {}
  } else {
    while(!work_queue->push(entry) && !simulation->is_done()) {}
  }
  INC_STATS(thd_id,mtx[13],get_sys_clock() - mtx_wait_starttime);

  if(busy) {
    INC_STATS(thd_id,work_queue_conflict_cnt,1);
  }
  INC_STATS(thd_id,work_queue_enqueue_time,get_sys_clock() - starttime);
  INC_STATS(thd_id,work_queue_enq_cnt,1);
}

Message * QWorkQueue::dequeue(uint64_t thd_id) {
  uint64_t starttime = get_sys_clock();
  assert(ISSERVER || ISREPLICA);
  Message * msg = NULL;
  work_queue_entry * entry = NULL;
  uint64_t mtx_wait_starttime = get_sys_clock();
  bool valid = work_queue->pop(entry);
  if(!valid) {
#if SERVER_GENERATE_QUERIES
    if(ISSERVER) {
      BaseQuery * m_query = client_query_queue.get_next_query(thd_id,thd_id);
      if(m_query) {
        assert(m_query);
        msg = Message::create_message((BaseQuery*)m_query,CL_QRY);
      }
    }
#else
    valid = new_txn_queue->pop(entry);
#endif
  }
  INC_STATS(thd_id,mtx[14],get_sys_clock() - mtx_wait_starttime);
  
  if(valid) {
    msg = entry->msg;
    assert(msg);
    //printf("%ld WQdequeue %ld\n",thd_id,entry->txn_id);
    uint64_t queue_time = get_sys_clock() - entry->starttime;
    INC_STATS(thd_id,work_queue_wait_time,queue_time);
    INC_STATS(thd_id,work_queue_cnt,1);
    if(msg->rtype == CL_QRY) {
      INC_STATS(thd_id,work_queue_new_wait_time,queue_time);
      INC_STATS(thd_id,work_queue_new_cnt,1);
    } else {
      INC_STATS(thd_id,work_queue_old_wait_time,queue_time);
      INC_STATS(thd_id,work_queue_old_cnt,1);
    }
    msg->wq_time = queue_time;
    //DEBUG("DEQUEUE (%ld,%ld) %ld; %ld; %d, 0x%lx\n",msg->txn_id,msg->batch_id,msg->return_node_id,queue_time,msg->rtype,(uint64_t)msg);
    DEBUG("Work Dequeue (%ld,%ld)\n",entry->txn_id,entry->batch_id);
    DEBUG_M("QWorkQueue::dequeue work_queue_entry free\n");
    mem_allocator.free(entry,sizeof(work_queue_entry));
    INC_STATS(thd_id,work_queue_dequeue_time,get_sys_clock() - starttime);
  }

#if SERVER_GENERATE_QUERIES
  if(msg && msg->rtype == CL_QRY) {
    INC_STATS(thd_id,work_queue_new_wait_time,get_sys_clock() - starttime);
    INC_STATS(thd_id,work_queue_new_cnt,1);
  }
#endif
  return msg;
}

