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

#ifndef _WORK_QUEUE_H_
#define _WORK_QUEUE_H_


#include "global.h"
#include "helper.h"
#include <queue>
#include <boost/lockfree/queue.hpp>
//#include "message.h"

class BaseQuery;
class Workload;
class Message;

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
      return lhs->starttime > rhs->starttime;
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
    if(lhs->rtype == CL_QRY && rhs->rtype != CL_QRY)
      return true;
    if(rhs->rtype == CL_QRY && lhs->rtype != CL_QRY)
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
  Message * dequeue(uint64_t thd_id);
  void sched_enqueue(uint64_t thd_id, Message * msg); 
  Message * sched_dequeue(uint64_t thd_id); 
  void sequencer_enqueue(uint64_t thd_id, Message * msg); 
  Message * sequencer_dequeue(uint64_t thd_id); 

  uint64_t get_cnt() {return get_wq_cnt() + get_rem_wq_cnt() + get_new_wq_cnt();}
  uint64_t get_wq_cnt() {return 0;}
  //uint64_t get_wq_cnt() {return work_queue.size();}
  uint64_t get_sched_wq_cnt() {return 0;}
  uint64_t get_rem_wq_cnt() {return 0;} 
  uint64_t get_new_wq_cnt() {return 0;}
  //uint64_t get_rem_wq_cnt() {return remote_op_queue.size();}
  //uint64_t get_new_wq_cnt() {return new_query_queue.size();}

private:
  boost::lockfree::queue<work_queue_entry* > * work_queue;
  boost::lockfree::queue<work_queue_entry* > * new_txn_queue;
  boost::lockfree::queue<work_queue_entry* > * seq_queue;
  boost::lockfree::queue<work_queue_entry* > ** sched_queue;
  uint64_t sched_ptr;
  BaseQuery * last_sched_dq;
  uint64_t curr_epoch;

};


#endif
