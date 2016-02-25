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

#ifndef _WORK_QUEUE_H_
#define _WORK_QUEUE_H_


#include "global.h"
#include "helper.h"
#include <queue>
//#include "message.h"

class BaseQuery;
class Workload;
class Message;

struct wq_entry {
  BaseQuery * qry;
  uint64_t starttime;
  struct wq_entry * next;
  struct wq_entry * prev;
};

typedef wq_entry * wq_entry_t;

struct work_queue_entry {
  Message * msg;
  uint64_t batch_id;
  uint64_t txn_id;
  RemReqType rtype;
  uint64_t starttime;
};


struct CompareSchedEntry {
  bool operator()(const work_queue_entry* lhs, const work_queue_entry* rhs) {
    if(lhs->batch_id == rhs->batch_id)
      return lhs->starttime < rhs->starttime;
    return lhs->batch_id < rhs->batch_id;
  }
};
struct CompareWQEntry {
#if PRIORITY == PRIORITY_FCFS
  bool operator()(const work_queue_entry* lhs, const work_queue_entry* rhs) {
    return lhs->starttime < rhs->starttime;
  }
#elif PRIORITY == PRIORITY_ACTIVE
  bool operator()(const work_queue_entry* lhs, const work_queue_entry* rhs) {
    if(lhs->rtype == RTXN && rhs->rtype != RTXN)
      return true;
    if(rhs->rtype == RTXN && lhs->rtype != RTXN)
      return false;
    return lhs->starttime < rhs->starttime;
  }
#elif PRIORITY == PRIORITY_HOME
  bool operator()(const work_queue_entry* lhs, const work_queue_entry* rhs) {
    if(ISLOCAL(lhs->txn_id) && !ISLOCAL(rhs->txn_id))
      return true;
    if(ISLOCAL(rhs->txn_id) && !ISLOCAL(lhs->txn_id))
      return false;
    return lhs->starttime < rhs->starttime;
  }
#endif

};

class QWorkQueue {
public:
  void init();
  void enqueue(uint64_t thd_id,Message * msg,bool busy); 
  Message * dequeue();
  void sched_enqueue(Message * msg); 
  Message * sched_dequeue(); 

  uint64_t get_cnt() {return get_wq_cnt() + get_rem_wq_cnt() + get_new_wq_cnt();}
  uint64_t get_wq_cnt() {return work_queue.size();}
  uint64_t get_sched_wq_cnt() {return scheduler_queue.size();}
  uint64_t get_rem_wq_cnt() {return 0;} 
  uint64_t get_new_wq_cnt() {return 0;}
  //uint64_t get_rem_wq_cnt() {return remote_op_queue.size();}
  //uint64_t get_new_wq_cnt() {return new_query_queue.size();}

  void deactivate_txn_id(uint64_t txn_id); 
  bool activate_txn_id(uint64_t txn_id); 


private:
  std::priority_queue<work_queue_entry*,std::vector<work_queue_entry*>,CompareWQEntry> work_queue;
/*  std::queue<Message*> work_queue;
  std::queue<Message*> new_query_queue;
  std::queue<Message*> remote_op_queue;
  */
  std::priority_queue<work_queue_entry*,std::vector<work_queue_entry*>,CompareSchedEntry> scheduler_queue;
  std::set<uint64_t> active_txn_ids;
  pthread_mutex_t mtx;
  pthread_mutex_t sched_mtx;
  pthread_mutex_t active_txn_mtx;
  uint64_t sched_ptr;
  BaseQuery * last_sched_dq;
  uint64_t curr_epoch;
  bool new_epoch;
  wq_entry_t * sched_head;
  wq_entry_t * sched_tail;

};


#endif
