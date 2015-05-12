#ifndef _WORK_QUEUE_H_
#define _WORK_QUEUE_H_

#include "global.h"
#include "helper.h"

class base_query;

struct wq_entry {
  base_query * qry;
  struct wq_entry * next;
};

typedef wq_entry * wq_entry_t;

// Really a linked list
class QWorkQueue {
public:
  void init();
  bool poll_next_query();
  void add_query(base_query * qry);
  base_query * get_next_query();

private:
  pthread_mutex_t mtx;
  wq_entry_t head;
  wq_entry_t tail;
  uint64_t cnt;
  uint64_t last_add_time;

};


#endif
