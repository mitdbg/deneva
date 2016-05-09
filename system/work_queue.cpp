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

#include "work_queue.h"
#include "mem_alloc.h"
#include "query.h"
#include "message.h"
#include <boost/lockfree/queue.hpp>

void QWorkQueue::init() {
  pthread_mutex_init(&mtx,NULL);
  pthread_mutex_init(&sched_mtx,NULL);
  pthread_mutex_init(&active_txn_mtx,NULL);

  new_epoch = false;
  last_sched_dq = NULL;
  sched_ptr = 0;

}

void QWorkQueue::lock_enqueue(uint64_t thd_id, Message * msg) {
}

Message * QWorkQueue::lock_dequeue(uint64_t thd_id) {
  return NULL;
}

void QWorkQueue::sched_enqueue(uint64_t thd_id, Message * msg) {
  assert(CC_ALG == CALVIN);
  assert(msg);
  assert(ISSERVERN(msg->return_node_id));

  DEBUG_M("QWorkQueue::sched_enqueue work_queue_entry alloc\n");
  work_queue_entry * entry = (work_queue_entry*)mem_allocator.alloc(sizeof(work_queue_entry));
  entry->msg = msg;
  entry->starttime = get_sys_clock();

  scheduler_queue.push(entry);
}

Message * QWorkQueue::sched_dequeue(uint64_t thd_id) {

  assert(CC_ALG == CALVIN);
  if(scheduler_queue.empty())
    return NULL;
  Message * msg = NULL;

  uint64_t mtx_time_start = get_sys_clock();
  pthread_mutex_lock(&sched_mtx);
  INC_STATS(thd_id,mtx[12],get_sys_clock() - mtx_time_start);
  work_queue_entry * entry = scheduler_queue.top();
  if(!(!entry || simulation->get_worker_epoch() > simulation->get_seq_epoch() || (new_epoch && simulation->epoch_txn_cnt > 0))) {
    Message * msg = entry->msg;
  DEBUG_M("QWorkQueue::sched_enqueue work_queue_entry free\n");
    mem_allocator.free(entry,sizeof(work_queue_entry));
  new_epoch = false;
  if(msg->rtype == RDONE) {
    scheduler_queue.pop();
    DEBUG("RDONE %ld %ld\n",sched_ptr,simulation->get_worker_epoch());
    sched_ptr++;
    if(sched_ptr == g_node_cnt) {
      simulation->next_worker_epoch();
      sched_ptr = 0;
      new_epoch = true;
    }

  } else {
    simulation->inc_epoch_txn_cnt();
    scheduler_queue.pop();
    DEBUG("SDeq %ld (%ld,%ld) %ld\n",sched_ptr,msg->txn_id,msg->batch_id,simulation->get_worker_epoch());
    assert(msg->batch_id == simulation->get_worker_epoch());
  }


  }

  pthread_mutex_unlock(&sched_mtx);

  return msg;

}


void QWorkQueue::enqueue(uint64_t thd_id, Message * msg,bool busy) {
  uint64_t starttime = get_sys_clock();
  assert(msg);
  DEBUG_M("QWorkQueue::enqueue work_queue_entry alloc\n");
  work_queue_entry * entry = (work_queue_entry*)mem_allocator.alloc(sizeof(work_queue_entry));
  entry->msg = msg;
  entry->rtype = msg->rtype;
  entry->txn_id = msg->txn_id;
  entry->batch_id = msg->batch_id;
  entry->starttime = get_sys_clock();
  assert(ISSERVER || ISREPLICA);

  // FIXME: May need alternative queue for some calvin threads
  uint64_t mtx_wait_starttime = get_sys_clock();
  if(msg->rtype == CL_QRY) {
    while(!new_txn_queue.push(entry)) {}
  } else {
    while(!work_queue.push(entry)) {}
  }
  INC_STATS(thd_id,mtx[13],get_sys_clock() - mtx_wait_starttime);

  INC_STATS(thd_id,work_queue_enqueue_time,get_sys_clock() - starttime);
  INC_STATS(thd_id,work_queue_enq_cnt,1);
}

Message * QWorkQueue::dequeue(uint64_t thd_id) {
  uint64_t starttime = get_sys_clock();
  assert(ISSERVER || ISREPLICA);
  Message * msg = NULL;
  work_queue_entry * entry = NULL;
  uint64_t mtx_wait_starttime = get_sys_clock();
  bool valid = work_queue.pop(entry);
  if(!valid) {
    valid = new_txn_queue.pop(entry);
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
    //DEBUG("DEQUEUE (%ld,%ld) %ld; %ld; %d, 0x%lx\n",msg->txn_id,msg->batch_id,msg->return_node_id,queue_time,msg->rtype,(uint64_t)msg);
  DEBUG_M("QWorkQueue::dequeue work_queue_entry free\n");
    mem_allocator.free(entry,sizeof(work_queue_entry));
    INC_STATS(thd_id,work_queue_dequeue_time,get_sys_clock() - starttime);
  } else {
    assert(msg == NULL);
  }
  return msg;
}

