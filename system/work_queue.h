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
  virtual void * get_next_entry();
  uint64_t size();

protected:
  pthread_mutex_t mtx;
  q_entry_t head;
  q_entry_t tail;
  uint64_t cnt;
  uint64_t last_add_time;
};


#endif
