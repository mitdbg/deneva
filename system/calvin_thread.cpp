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
#include "calvin_thread.h"
#include "txn.h"
#include "wl.h"
#include "query.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "mem_alloc.h"
#include "transport.h"
#include "math.h"
#include "specex.h"
#include "helper.h"
#include "msg_thread.h"
#include "msg_queue.h"
#include "sequencer.h"
#include "logger.h"
#include "message.h"
#include "work_queue.h"

/*
void CalvinLockThread::setup() {
}

RC CalvinLockThread::run() {
  tsetup();

	RC rc = RCOK;

	while(!simulation->is_done()) {
    m_txn = NULL;

    Message * msg = work_queue.lock_dequeue(_thd_id);

		if(!msg)
			continue;

    assert(msg->get_rtype() == RTXN);
    assert(msg->get_txn_id() != UINT64_MAX);

    m_txn = txn_table.get_transaction_manager(get_thd_id(),msg->get_txn_id(),msg->get_batch_id());
    msg->copy_to_txn(m_txn);
    m_txn->register_thread(this);
    // Acquire locks
    rc = m_txn->acquire_locks();

    if(rc == RCOK) {
      work_queue.enqueue(_thd_id,msg,false);
    }

  }
  printf("FINISH %ld:%ld\n",_node_id,_thd_id);
  fflush(stdout);
  return FINISH;
}

void CalvinSequencerThread::setup() {
}

bool CalvinSequencerThread::is_batch_ready() {
  bool ready = get_sys_clock() - last_batchtime >= g_batch_time_limit;
  return ready;
}

RC CalvinSequencerThread::run() {
  tsetup();

  Message * msg;
	last_batchtime = run_starttime;

	while(!simulation->is_done()) {

    if(is_batch_ready()) {
      simulation->advance_seq_epoch();
      seq_man.send_next_batch(_thd_id);
      last_batchtime = get_sys_clock();
    }

    msg = work_queue.sched_dequeue(_thd_id);

		if(!msg)
			continue;

    switch (msg->get_rtype()) {
      case RTXN:
        // Query from client
        seq_man.process_txn(msg);
        break;
      case RACK:
        // Ack from server
        seq_man.process_ack(msg,get_thd_id());
        break;
      default:
        assert(false);
    }
  }
  printf("FINISH %ld:%ld\n",_node_id,_thd_id);
  fflush(stdout);
  return FINISH;

}

*/
