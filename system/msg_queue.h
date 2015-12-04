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

#ifndef _MSG_QUEUE_H_
#define _MSG_QUEUE_H_

#include "global.h"
#include "helper.h"
#include "concurrentqueue.h"

class BaseQuery;

struct msg_entry {
  BaseQuery * qry;
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
  void enqueue(BaseQuery * qry,RemReqType type, uint64_t dest);
  uint64_t dequeue(BaseQuery *& qry,RemReqType & type,uint64_t & dest);
private:
  moodycamel::ConcurrentQueue<msg_entry_t,moodycamel::ConcurrentQueueDefaultTraits> mq;
  uint64_t last_add_time;
  pthread_mutex_t mtx;
  msg_entry_t head;
  msg_entry_t tail;
  uint64_t cnt;

};

#endif
