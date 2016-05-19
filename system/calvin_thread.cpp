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
#include "helper.h"
#include "msg_thread.h"
#include "msg_queue.h"
#include "sequencer.h"
#include "logger.h"
#include "message.h"
#include "work_queue.h"

void CalvinLockThread::setup() {
}

RC CalvinLockThread::run() {
  tsetup();

	RC rc = RCOK;
  TxnManager * txn_man;

	while(!simulation->is_done()) {
    txn_man = NULL;

    Message * msg = work_queue.sched_dequeue(_thd_id);

		if(!msg)
			continue;

    assert(msg->get_rtype() == CL_QRY);
    assert(msg->get_txn_id() != UINT64_MAX);

    txn_man = txn_table.get_transaction_manager(get_thd_id(),msg->get_txn_id(),msg->get_batch_id());
    while(!txn_man->unset_ready()) { }
    msg->copy_to_txn(txn_man);
    txn_man->register_thread(this);
    // Acquire locks
    rc = txn_man->acquire_locks();

    if(rc == RCOK) {
      work_queue.enqueue(_thd_id,msg,false);
    }
    txn_man->set_ready();

  }
  printf("FINISH %ld:%ld\n",_node_id,_thd_id);
  fflush(stdout);
  return FINISH;
}

void CalvinSequencerThread::setup() {
}

bool CalvinSequencerThread::is_batch_ready() {
  bool ready = get_sys_clock() - last_batchtime >= g_seq_batch_time_limit;
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

    msg = work_queue.sequencer_dequeue(_thd_id);

		if(!msg)
			continue;

    switch (msg->get_rtype()) {
      case CL_QRY:
        // Query from client
        DEBUG("SEQ process_txn\n");
        seq_man.process_txn(msg);
        // TODO: Don't free message yet
        break;
      case CALVIN_ACK:
        // Ack from server
        DEBUG("SEQ process_ack (%ld,%ld) from %ld\n",msg->get_txn_id(),msg->get_batch_id(),msg->get_return_id());
        seq_man.process_ack(msg,get_thd_id());
        // TODO: Free message
        msg->release();
        break;
      default:
        assert(false);
    }
  }
  printf("FINISH %ld:%ld\n",_node_id,_thd_id);
  fflush(stdout);
  return FINISH;

}
