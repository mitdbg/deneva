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
#include "concurrentqueue.h"

//template<typename T> class ConcurrentQueue;
class BaseQuery;
class BaseClientQuery;
class Workload;

struct wq_entry {
  BaseQuery * qry;
  uint64_t starttime;
  struct wq_entry * next;
  struct wq_entry * prev;
};

typedef wq_entry * wq_entry_t;

struct id_entry {
  uint64_t id;
  struct id_entry * next;
};

typedef id_entry * id_entry_t;

class QHash {
public:
  void init();
  void lock();
  void unlock();
  bool in_qhash(uint64_t id);
  void add_qhash(uint64_t id);
  void update_qhash(uint64_t id);
  void remove_qhash(uint64_t id);
private:
  uint64_t id_hash_size;
  id_entry_t * id_hash;
  pthread_mutex_t mtx;
};

// Really a linked list
class QWorkQueueHelper {
public:
  void init(QHash * hash);
  bool poll_next_query();
  uint64_t get_cnt() {return cnt;}
  void finish(uint64_t time); 
  void abort_finish(uint64_t time); 
  void add_query(BaseQuery * qry);
  int add_abort_query(BaseQuery * qry);
  int check_abort_query();
  BaseQuery * get_next_query(int id);
  BaseQuery * get_next_query_client();
  void remove_query(BaseQuery * qry);
  void done(uint64_t id);
  bool poll_abort(); 
  BaseQuery * get_next_abort_query();


private:
  uint64_t cnt;
  pthread_mutex_t mtx;
  pthread_cond_t cond;
  wq_entry_t head;
  wq_entry_t tail;
  uint64_t last_add_time;
  QHash * hash;

};


class QWorkQueue {
public:
  void init(Workload * wl);
  int inline get_idx(int input);
  bool poll_next_query(int tid);
  void finish(uint64_t time); 
  void abort_finish(uint64_t time); 
  void process_aborts(); 
  void enqueue(uint64_t thd_id,BaseQuery * qry,bool busy); 
  void sched_enqueue(BaseQuery * qry); 
  bool sched_dequeue(BaseQuery *& qry); 
  void enqueue_new(uint64_t thd_id,BaseQuery * qry); 
  bool dequeue(uint64_t thd_id, BaseQuery *& qry);
  bool set_active(uint64_t thd_id, uint64_t txn_id);
  void delete_active(uint64_t thd_id, uint64_t txn_id);
  void add_query(int tid, BaseQuery * qry);
  void add_abort_query(int tid, BaseQuery * qry);
  BaseQuery * get_next_query(int tid);
  void remove_query(int tid, BaseQuery * qry);
  void update_hash(int tid, uint64_t id);
  void done(int tid, uint64_t id);
  bool poll_abort(int tid); 
  BaseQuery * get_next_abort_query(int tid);
  uint64_t get_cnt() {return wq_cnt + rem_wq_cnt + new_wq_cnt;}
  uint64_t get_wq_cnt() {return wq_cnt;}
  uint64_t get_sched_wq_cnt() {return sched_wq_cnt;}
  uint64_t get_rem_wq_cnt() {return rem_wq_cnt;}
  uint64_t get_new_wq_cnt() {return new_wq_cnt;}
  uint64_t get_abrt_cnt() {return abrt_cnt;}


private:
  moodycamel::ConcurrentQueue<BaseQuery*,moodycamel::ConcurrentQueueDefaultTraits> wq;
  moodycamel::ConcurrentQueue<BaseQuery*,moodycamel::ConcurrentQueueDefaultTraits> new_wq;
  moodycamel::ConcurrentQueue<BaseQuery*,moodycamel::ConcurrentQueueDefaultTraits> rem_wq;
  moodycamel::ConcurrentQueue<BaseQuery*,moodycamel::ConcurrentQueueDefaultTraits> aq;
  //moodycamel::ConcurrentQueue<BaseQuery*,moodycamel::ConcurrentQueueDefaultTraits> sched_wq;
  BaseQuery * aq_head;
  QWorkQueueHelper * queue;
  QHash * hash;
  int q_len;
  uint64_t wq_cnt;
  uint64_t sched_wq_cnt;
  uint64_t rem_wq_cnt;
  uint64_t new_wq_cnt;
  uint64_t abrt_cnt;
  uint64_t * active_txns;
  pthread_mutex_t mtx;
  pthread_cond_t cond;
  Workload * _wl;
  uint64_t sched_ptr;
  BaseQuery * last_sched_dq;
  uint64_t curr_epoch;
  bool new_epoch;
  wq_entry_t * sched_head;
  wq_entry_t * sched_tail;

};


#endif
