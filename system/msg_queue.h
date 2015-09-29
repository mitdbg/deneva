#ifndef _MSG_QUEUE_H_
#define _MSG_QUEUE_H_

#include "global.h"
#include "helper.h"
#include "concurrentqueue.h"

class base_query;

struct msg_entry {
  base_query * qry;
  RemReqType type;
  uint64_t dest;
  uint64_t starttime;
  struct msg_entry * next;
  struct msg_entry * prev;
};

typedef msg_entry * msg_entry_t;

class MessageQueue {
public:
  void init();
  void enqueue(base_query * qry,RemReqType type, uint64_t dest);
  RemReqType dequeue(base_query *& qry,uint64_t & dest);
private:
  moodycamel::ConcurrentQueue<msg_entry_t,moodycamel::ConcurrentQueueDefaultTraits> mq;
  uint64_t last_add_time;
  pthread_mutex_t mtx;
  msg_entry_t head;
  msg_entry_t tail;
  uint64_t cnt;

};

#endif
