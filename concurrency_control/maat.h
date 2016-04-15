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

#ifndef _MAAT_H_
#define _MAAT_H_

#include "row.h"
#include "semaphore.h"


class TxnManager;

enum MAATState { MAAT_RUNNING=0,MAAT_VALIDATED,MAAT_COMMITTED,MAAT_ABORTED};

struct TimeTableEntry{
  uint64_t lower;
  uint64_t upper;
  MAATState state;
  void init() {
    lower = 0;
    upper = UINT64_MAX;
    state = MAAT_RUNNING;
  }
};

class TimeTable {
public:
	void init();
  uint64_t get_commit_timestamp();
private:
  // hash table
  uint64_t hash();

  TimeTableEntry * find_entry(uint64_t id);
	
 	sem_t 	_semaphore;
};

#endif
