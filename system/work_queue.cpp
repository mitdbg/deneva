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

void QWorkQueue::init() {
  pthread_mutex_init(&mtx,NULL);
  pthread_mutex_init(&sched_mtx,NULL);
  pthread_mutex_init(&active_txn_mtx,NULL);

  new_epoch = false;
  last_sched_dq = NULL;
  sched_ptr = 0;

}

void QWorkQueue::sched_enqueue(Message * msg) {
  assert(CC_ALG == CALVIN);
  assert(msg);
  assert(ISSERVERN(msg->return_node_id));

  work_queue_entry * entry = (work_queue_entry*)mem_allocator.alloc(sizeof(work_queue_entry));
  entry->msg = msg;
  entry->starttime = get_sys_clock();

  scheduler_queue.push(entry);
}

Message * QWorkQueue::sched_dequeue() {

  assert(CC_ALG == CALVIN);
  if(scheduler_queue.empty())
    return NULL;
  Message * msg = NULL;

  pthread_mutex_lock(&sched_mtx);
  work_queue_entry * entry = scheduler_queue.top();
  if(!(!entry || simulation->get_worker_epoch() > simulation->get_sched_epoch() || (new_epoch && simulation->epoch_txn_cnt > 0))) {
    Message * msg = entry->msg;
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
  work_queue_entry * entry = (work_queue_entry*)mem_allocator.alloc(sizeof(work_queue_entry));
  entry->msg = msg;
  entry->rtype = msg->rtype;
  entry->txn_id = msg->txn_id;
  entry->batch_id = msg->batch_id;
  entry->starttime = get_sys_clock();
  assert(ISSERVER);

  // FIXME: May need alternative queue for some calvin threads
  pthread_mutex_lock(&mtx);
  DEBUG("%ld ENQUEUE (%lld,%lld); %ld; %d,0x%lx\n",thd_id,entry->txn_id,entry->batch_id,msg->return_node_id,entry->rtype,(uint64_t)msg);
  work_queue.push(entry);
  pthread_mutex_unlock(&mtx);

  INC_STATS(thd_id,all_wq_enqueue,get_sys_clock() - starttime);
}

Message * QWorkQueue::dequeue() {
  uint64_t starttime = get_sys_clock();
  assert(ISSERVER);
  Message * msg = NULL;
  work_queue_entry * entry = NULL;
  uint64_t prof_starttime = get_sys_clock();
  if(!work_queue.empty()) {
    pthread_mutex_lock(&mtx);
    if(!work_queue.empty()) {
      entry = work_queue.top();
      msg = entry->msg;
      if(activate_txn_id(msg->txn_id)) {
        work_queue.pop();
      } else {
        entry = NULL;
        msg = NULL;
      }
    }
    pthread_mutex_unlock(&mtx);
  }


  if(entry) {
    assert(msg);
    uint64_t queue_time = get_sys_clock() - entry->starttime;
    INC_STATS(0,qq_cnt,1);
    INC_STATS(0,qq_lat,queue_time);
    INC_STATS(0,wq_dequeue,get_sys_clock() - starttime);
    DEBUG("DEQUEUE (%lld,%lld) %ld; %ld; %d, 0x%lx\n",msg->txn_id,msg->batch_id,msg->return_node_id,queue_time,msg->rtype,(uint64_t)msg);
    mem_allocator.free(entry,sizeof(work_queue_entry));
  } else {
    assert(msg == NULL);
  }
  INC_STATS(0,all_wq_dequeue,get_sys_clock() - prof_starttime);
  return msg;
}

void QWorkQueue::deactivate_txn_id(uint64_t txn_id) {
  pthread_mutex_lock(&active_txn_mtx);
  active_txn_ids.erase(txn_id);
  pthread_mutex_unlock(&active_txn_mtx);
}


bool QWorkQueue::activate_txn_id(uint64_t txn_id) {
  if(txn_id == UINT64_MAX)
    return true;
  pthread_mutex_lock(&active_txn_mtx);
  bool success = active_txn_ids.insert(txn_id).second;
  pthread_mutex_unlock(&active_txn_mtx);
  return success;
}


