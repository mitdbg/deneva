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
#include "lock_free_queue.h"
#include <boost/lockfree/queue.hpp>

class BaseQuery;
class Message;

struct msg_entry {
  Message * msg;
  uint64_t dest;
  uint64_t starttime;
  struct msg_entry * next;
  struct msg_entry * prev;
};

typedef msg_entry * msg_entry_t;

class MessageQueue {
public:
  void init();
  void enqueue(uint64_t thd_id, Message * msg, uint64_t dest);
  uint64_t dequeue(uint64_t thd_id, Message *& msg);
private:
 //LockfreeQueue m_queue;
// This is close to max capacity for boost
  boost::lockfree::queue<msg_entry*, boost::lockfree::capacity<65526> > m_queue;
  pthread_mutex_t mtx;
  msg_entry_t head;
  msg_entry_t tail;
  uint64_t last_add_time;
  uint64_t cnt;

};

#endif
