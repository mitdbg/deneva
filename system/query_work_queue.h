#ifndef _WORK_QUEUE_H_
#define _WORK_QUEUE_H_

#include "global.h"
#include "helper.h"
#include "concurrentqueue.h"

//template<typename T> class ConcurrentQueue;
class base_query;
class base_client_query;

struct wq_entry {
  base_query * qry;
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
  void add_query(base_query * qry);
  void add_abort_query(base_query * qry);
  base_query * get_next_query(int id);
  base_query * get_next_query_client();
  void remove_query(base_query * qry);
  void done(uint64_t id);
  bool poll_abort(); 
  base_query * get_next_abort_query();


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
  void init();
  int inline get_idx(int input);
  bool poll_next_query(int tid);
  void finish(uint64_t time); 
  void abort_finish(uint64_t time); 
  void enqueue(uint64_t thd_id,base_query * qry); 
  bool dequeue(uint64_t thd_id, base_query *& qry);
  bool set_active(uint64_t thd_id, uint64_t txn_id);
  void delete_active(uint64_t thd_id, uint64_t txn_id);
  void add_query(int tid, base_query * qry);
  void add_abort_query(int tid, base_query * qry);
  base_query * get_next_query(int tid);
  void remove_query(int tid, base_query * qry);
  void update_hash(int tid, uint64_t id);
  void done(int tid, uint64_t id);
  bool poll_abort(int tid); 
  base_query * get_next_abort_query(int tid);
  uint64_t get_cnt() {return wq_cnt + rem_wq_cnt + new_wq_cnt;}
  uint64_t get_wq_cnt() {return wq_cnt;}
  uint64_t get_rem_wq_cnt() {return rem_wq_cnt;}
  uint64_t get_new_wq_cnt() {return new_wq_cnt;}
  uint64_t get_abrt_cnt() {return abrt_cnt;}


private:
  moodycamel::ConcurrentQueue<base_query*,moodycamel::ConcurrentQueueDefaultTraits> wq;
  moodycamel::ConcurrentQueue<base_query*,moodycamel::ConcurrentQueueDefaultTraits> new_wq;
  moodycamel::ConcurrentQueue<base_query*,moodycamel::ConcurrentQueueDefaultTraits> rem_wq;
  QWorkQueueHelper * queue;
  QHash * hash;
  int q_len;
  uint64_t wq_cnt;
  uint64_t rem_wq_cnt;
  uint64_t new_wq_cnt;
  uint64_t abrt_cnt;
  uint64_t * active_txns;
  pthread_mutex_t mtx;
  pthread_cond_t cond;

};


#endif
