#ifndef _WWORK_QUEUE_H_
#define _WWORK_QUEUE_H_

#include "global.h"
#include "helper.h"

struct q_entry {
  void * entry;
  struct q_entry * next;
};

typedef q_entry * q_entry_t;

// Really a linked list
class WorkQueue {
public:
  void init();
  bool poll_next_entry();
  void add_entry(void * data);
  void * get_next_entry();
  uint64_t size();

private:
  pthread_mutex_t mtx;
  q_entry_t head;
  q_entry_t tail;
  uint64_t cnt;
  uint64_t last_add_time;
};


#endif
