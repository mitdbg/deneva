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

#include "global.h"
#include "manager.h"
#include "thread.h"
#include "txn.h"
#include "wl.h"
#include "query.h"
#include "plock.h"
#include "occ.h"
#include "vll.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "mem_alloc.h"
#include "transport.h"
#include "remote_query.h"
#include "math.h"
#include "specex.h"
#include "helper.h"
#include "msg_thread.h"
#include "msg_queue.h"
#include "sequencer.h"
#include "logger.h"

void Thread::init(uint64_t thd_id, uint64_t node_id, Workload * workload) {
	_thd_id = thd_id;
	_node_id = node_id;
	_wl = workload;
  stats.init(_thd_id);
	rdm.init(_thd_id);
}

uint64_t Thread::get_thd_id() { return _thd_id; }
uint64_t Thread::get_node_id() { return _node_id; }

void Thread::tsetup() {
	printf("Setup %ld:%ld\n",_node_id, _thd_id);
  fflush(stdout);
	pthread_barrier_wait( &warmup_bar );

  setup();

	pthread_barrier_wait( &warmup_bar );
	printf("Running %ld:%ld\n",_node_id, _thd_id);
  fflush(stdout);

	run_starttime = get_sys_clock();
  simulation->set_starttime(run_starttime);

}

