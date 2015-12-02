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

#include "work_queue.h"
#include "mem_alloc.h"
#include "query.h"

void WorkQueue::init() {
  cnt = 0;
  head = NULL;
  tail = NULL;
  last_add_time = 0;
  pthread_mutex_init(&mtx,NULL);
}

bool WorkQueue::poll_next_entry() {
  return cnt > 0;
}

void WorkQueue::add_entry(void * data) {

  q_entry_t entry = (q_entry_t) mem_allocator.alloc(sizeof(struct q_entry), 0);
  entry->entry = data;
  entry->next  = NULL;
  //assert(qry->rtype <= RPASS);

  pthread_mutex_lock(&mtx);

  if(cnt > 0) {
    tail->next = entry;
  }
  if(cnt == 0) {
    head = entry;
  }
  tail = entry;
  cnt++;

  if(last_add_time == 0)
    last_add_time = get_sys_clock();

  pthread_mutex_unlock(&mtx);
}

void * WorkQueue::get_next_entry() {
  q_entry_t next_entry = NULL;
  void * data = NULL;

  pthread_mutex_lock(&mtx);

  assert( ( (cnt == 0) && head == NULL && tail == NULL) || ( (cnt > 0) && head != NULL && tail !=NULL) );

  if(cnt > 0) {
    next_entry = head;
	data = next_entry->entry;
	assert(data != NULL);
    head = head->next;
	free(next_entry);
    cnt--;

    if(cnt == 0) {
      tail = NULL;
    }
  }

  if(cnt == 0 && last_add_time != 0) {
    INC_STATS(0,qq_full,get_sys_clock() - last_add_time);
    last_add_time = 0;
  }

  pthread_mutex_unlock(&mtx);
  return data;
}

uint64_t WorkQueue::size() {
	return cnt;
}

