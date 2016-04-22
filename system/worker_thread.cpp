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
#include "worker_thread.h"
#include "txn.h"
#include "wl.h"
#include "query.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "mem_alloc.h"
#include "math.h"
#include "specex.h"
#include "helper.h"
#include "msg_thread.h"
#include "msg_queue.h"
#include "work_queue.h"
#include "logger.h"
#include "message.h"
#include "abort_queue.h"
#include "maat.h"

void WorkerThread::send_init_done_to_all_nodes() {
		uint64_t total_nodes = g_node_cnt + g_client_node_cnt + g_node_cnt*g_repl_cnt;
		for(uint64_t i = 0; i < total_nodes; i++) {
			if(i != g_node_id) {
        printf("Send INIT_DONE to %ld\n",i);
        msg_queue.enqueue(Message::create_message(INIT_DONE),i);
			}
		}

}

void WorkerThread::setup() {

	if( get_thd_id() == 0) {
    send_init_done_to_all_nodes();
  }
  _thd_txn_id = 0;

}

void WorkerThread::progress_stats() {
		if(get_thd_id() == 0) {
      uint64_t now_time = get_sys_clock();
      if (now_time - prog_time >= g_prog_timer) {
        prog_time = now_time;
        SET_STATS(get_thd_id(), total_runtime, prog_time - simulation->run_starttime); 

        stats.print(true);
      }
		}

}

void WorkerThread::process(Message * msg) {
  RC rc __attribute__ ((unused));
  DEBUG("%ld Processing %ld %d\n",get_thd_id(),msg->get_txn_id(),msg->get_rtype());
  assert(msg->get_rtype() == CL_QRY || msg->get_txn_id() != UINT64_MAX);
  uint64_t starttime = get_sys_clock();
		switch(msg->get_rtype()) {
			case RPASS:
        //rc = process_rpass(msg);
				break;
			case RPREPARE: 
        rc = process_rprepare(msg);
				break;
			case RQRY:
        rc = process_rqry(msg);
				break;
			case RQRY_CONT:
        rc = process_rqry_cont(msg);
				break;
			case RQRY_RSP:
        rc = process_rqry_rsp(msg);
				break;
			case RFIN: 
        rc = process_rfin(msg);
				break;
			case RACK_PREP:
        rc = process_rack_prep(msg);
				break;
			case RACK_FIN:
        rc = process_rack_rfin(msg);
				break;
			case RTXN_CONT:
        rc = process_rtxn_cont(msg);
				break;
      case CL_QRY:
			case RTXN:
        rc = process_rtxn(msg);
				break;
			case LOG_FLUSHED:
        rc = process_log_flushed(msg);
				break;
			case LOG_MSG:
        rc = process_log_msg(msg);
				break;
			case LOG_MSG_RSP:
        rc = process_log_msg_rsp(msg);
				break;
			default:
        printf("Msg: %d\n",msg->get_rtype());
        fflush(stdout);
				assert(false);
				break;
		}
  uint64_t timespan = get_sys_clock() - starttime;
  INC_STATS(get_thd_id(),worker_process_cnt,1);
  INC_STATS(get_thd_id(),worker_process_time,timespan);
  INC_STATS(get_thd_id(),worker_process_cnt_by_type[msg->rtype],1);
  INC_STATS(get_thd_id(),worker_process_time_by_type[msg->rtype],timespan);
  DEBUG("%ld EndProcessing %d %ld\n",get_thd_id(),msg->get_rtype(),msg->get_txn_id());
}

void WorkerThread::check_if_done(RC rc, TxnManager * txn_man) {
  //TODO: get txn_id in non-hacky way, or change the way commit() is handled
  if(txn_man->waiting_for_response())
    return;
  if(rc == Commit)
    commit(txn_man);
  if(rc == Abort)
    abort(txn_man);
}

void WorkerThread::release_txn_man(TxnManager * txn_man) {
  txn_table.release_transaction_manager(txn_man->get_txn_id(),txn_man->get_batch_id());
}

// Can't use txn_man after this function
void WorkerThread::commit(TxnManager * txn_man) {
  //TxnManager * txn_man = txn_table.get_transaction_manager(txn_id,0);
  //txn_man->release_locks(RCOK);
  //        txn_man->commit_stats();
  assert(txn_man);
  assert(IS_LOCAL(txn_man->get_txn_id()));

  uint64_t timespan = get_sys_clock() - txn_man->get_start_timestamp();
  DEBUG("COMMIT %ld %f -- %f\n",txn_man->get_txn_id(),simulation->seconds_from_start(get_sys_clock()),(double)timespan/ BILLION);

  // Send result back to client
  msg_queue.enqueue(Message::create_message(txn_man,CL_RSP),txn_man->client_id);
  simulation->inc_txn_cnt();
  // remove txn from pool
  release_txn_man(txn_man);
  // Do not use txn_man after this

}

void WorkerThread::abort(TxnManager * txn_man) {

  DEBUG("ABORT %ld -- %f\n",txn_man->get_txn_id(),(double)get_sys_clock() - run_starttime/ BILLION);
  // TODO: TPCC Rollback here
  INC_STATS(get_thd_id(), txn_abort_cnt, 1);

  ++txn_man->abort_cnt;
  txn_man->reset();

  abort_queue.enqueue(txn_man->get_txn_id(),txn_man->get_abort_cnt());

}

RC WorkerThread::run() {
  tsetup();

	while(!simulation->is_done()) {

    progress_stats();

    Message * msg = work_queue.dequeue();

    if(!msg)
      continue;

    process(msg);


    work_queue.deactivate_txn_id(msg->txn_id);

    // delete message
    msg->release();

	}
  printf("FINISH %ld:%ld\n",_node_id,_thd_id);
  fflush(stdout);
  return FINISH;
}

RC WorkerThread::process_rfin(Message * msg) {
  DEBUG("RFIN %ld\n",msg->get_txn_id());

  assert(!IS_LOCAL(msg->get_txn_id()));
  TxnManager * txn_man = txn_table.get_transaction_manager(msg->get_txn_id(),0);
  if(((FinishMessage*)msg)->rc == Abort) {
    txn_man->abort();
    txn_man->reset();
    txn_man->reset_query();
    msg_queue.enqueue(Message::create_message(txn_man,RACK_FIN),GET_NODE_ID(msg->get_txn_id()));
    return Abort;
  } 
  txn_man->commit();
  //if(!txn_man->query->readonly() || CC_ALG == OCC)
  msg_queue.enqueue(Message::create_message(txn_man,RACK_FIN),GET_NODE_ID(msg->get_txn_id()));

  return RCOK;
}

RC WorkerThread::process_rack_prep(Message * msg) {
  DEBUG("RPREP_ACK %ld\n",msg->get_txn_id());

  RC rc = RCOK;

  TxnManager * txn_man = txn_table.get_transaction_manager(msg->get_txn_id(),0);
  int responses_left = txn_man->received_response(((AckMessage*)msg)->rc);
  assert(responses_left >=0);
#if CC_ALG == MAAT
  // Integrate bounds
  uint64_t lower = ((AckMessage*)msg)->lower;
  uint64_t upper = ((AckMessage*)msg)->upper;
  if(lower > time_table.get_lower(msg->get_txn_id())) {
    time_table.set_lower(msg->get_txn_id(),lower);
  }
  if(upper < time_table.get_upper(msg->get_txn_id())) {
    time_table.set_upper(msg->get_txn_id(),upper);
  }
  if(((AckMessage*)msg)->rc != RCOK) {
    time_table.set_state(msg->get_txn_id(),MAAT_ABORTED);
  }
#endif
  if(responses_left > 0) 
    return WAIT;

  // Done waiting 
  if(txn_man->get_rc() == RCOK) {
    rc  = txn_man->validate();
  }
  if(rc == Abort || txn_man->get_rc() == Abort) {
    txn_man->txn->rc = Abort;
    rc = Abort;
  }
  txn_man->send_finish_messages();
  if(rc == Abort) {
    txn_man->abort();
  } else {
    txn_man->commit();
  }

  return rc;
}

RC WorkerThread::process_rack_rfin(Message * msg) {
  DEBUG("RFIN_ACK %ld\n",msg->get_txn_id());

  RC rc = RCOK;

  TxnManager * txn_man = txn_table.get_transaction_manager(msg->get_txn_id(),0);
  int responses_left = txn_man->received_response(((AckMessage*)msg)->rc);
  assert(responses_left >=0);
  if(responses_left > 0) 
    return WAIT;

  // Done waiting 

  if(txn_man->get_rc() == RCOK) {
    //txn_man->commit();
    commit(txn_man);
  } else {
    //txn_man->abort();
    abort(txn_man);
  }
  return rc;
}

RC WorkerThread::process_rqry_rsp(Message * msg) {
  DEBUG("RQRY_RSP %ld\n",msg->get_txn_id());
  assert(IS_LOCAL(msg->get_txn_id()));

  TxnManager * txn_man = txn_table.get_transaction_manager(msg->get_txn_id(),0);
  RC rc = txn_man->run_txn();
  check_if_done(rc,txn_man);
  return rc;

}

RC WorkerThread::process_rqry(Message * msg) {
  DEBUG("RQRY %ld\n",msg->get_txn_id());
  assert(!IS_LOCAL(msg->get_txn_id()));
  RC rc = RCOK;

  // Create new transaction table entry if one does not already exist
  TxnManager * txn_man = txn_table.get_transaction_manager(msg->get_txn_id(),0);
  msg->copy_to_txn(txn_man);

#if CC_ALG == MAAT
          time_table.init(txn_man->get_txn_id());
          assert(time_table.get_lower(txn_man->get_txn_id()) == 0);
          assert(time_table.get_upper(txn_man->get_txn_id()) == UINT64_MAX);
          assert(time_table.get_state(txn_man->get_txn_id()) == MAAT_RUNNING);
#endif

  rc = txn_man->run_txn();

  // Send response
  if(rc != WAIT) {
    msg_queue.enqueue(Message::create_message(txn_man,RQRY_RSP),txn_man->return_id);
  }
  return rc;
}

RC WorkerThread::process_rqry_cont(Message * msg) {
  DEBUG("RQRY_CONT %ld\n",msg->get_txn_id());
  assert(!IS_LOCAL(msg->get_txn_id()));
  RC rc = RCOK;

  // Create new transaction table entry if one does not already exist
  TxnManager * txn_man = txn_table.get_transaction_manager(msg->get_txn_id(),0);
  txn_man->run_txn_post_wait();
  rc = txn_man->run_txn();

  // Send response
  if(rc != WAIT) {
    msg_queue.enqueue(Message::create_message(txn_man,RQRY_RSP),txn_man->return_id);
  }
  return rc;
}


RC WorkerThread::process_rtxn_cont(Message * msg) {
  DEBUG("RTXN_CONT %ld\n",msg->get_txn_id());
  assert(IS_LOCAL(msg->get_txn_id()));
  TxnManager * txn_man = txn_table.get_transaction_manager(msg->get_txn_id(),0);
  txn_man->run_txn_post_wait();
  RC rc = txn_man->run_txn();
  check_if_done(rc,txn_man);
  return RCOK;
}

RC WorkerThread::process_rprepare(Message * msg) {
  DEBUG("RPREP %ld\n",msg->get_txn_id());
    RC rc = RCOK;
  TxnManager * txn_man = txn_table.get_transaction_manager(msg->get_txn_id(),0);

    // Validate transaction
    rc  = txn_man->validate();
    txn_man->set_rc(rc);
    // FIXME: need to add RC into message
    msg_queue.enqueue(Message::create_message(txn_man,RACK_PREP),msg->return_node_id);

    return rc;
}

uint64_t WorkerThread::get_next_txn_id() {
  uint64_t txn_id = ( get_node_id() + get_thd_id() * g_node_cnt) 
							+ (g_thread_cnt * g_node_cnt * _thd_txn_id);
  ++_thd_txn_id;
  return txn_id;
}

RC WorkerThread::process_rtxn(Message * msg) {
        RC rc = RCOK;
        TxnManager * txn_man;
        uint64_t txn_id = UINT64_MAX;

        if(msg->get_rtype() == CL_QRY) {
          // This is a new transaction

					// Only set new txn_id when txn first starts
          txn_id = get_next_txn_id();
          work_queue.activate_txn_id(txn_id);
          msg->txn_id = txn_id;

					// Put txn in txn_table
          txn_man = txn_table.get_transaction_manager(txn_id,0);
					if (CC_ALG == WAIT_DIE) {
            txn_man->set_timestamp(get_next_ts());
          }
          txn_man->txn_stats.starttime = get_sys_clock();
          msg->copy_to_txn(txn_man);
          DEBUG("START %ld %f %lu\n",txn_man->get_txn_id(),simulation->seconds_from_start(get_sys_clock()),txn_man->txn_stats.starttime);

				} else {
          txn_man = txn_table.get_transaction_manager(msg->get_txn_id(),0);
          DEBUG("RESTART %ld %f %lu\n",txn_man->get_txn_id(),simulation->seconds_from_start(get_sys_clock()),txn_man->txn_stats.starttime);
        }

        // FIXME: Make sure we are coming from a new txn or restart, not a wait
          // Get new timestamps
          if(is_cc_new_timestamp()) {
            txn_man->set_timestamp(get_next_ts());
					}

#if CC_ALG == OCC
          txn_man->set_start_timestamp(get_next_ts());
#endif
#if CC_ALG == MAAT
          time_table.init(txn_man->get_txn_id());
          assert(time_table.get_lower(txn_man->get_txn_id()) == 0);
          assert(time_table.get_upper(txn_man->get_txn_id()) == UINT64_MAX);
          assert(time_table.get_state(txn_man->get_txn_id()) == MAAT_RUNNING);
#endif

    rc = init_phase();
    if(rc != RCOK)
      return rc;

    // Execute transaction
    rc = txn_man->run_txn();
  check_if_done(rc,txn_man);
    return rc;
}

RC WorkerThread::init_phase() {
  RC rc = RCOK;
  //m_query->part_touched[m_query->part_touched_cnt++] = m_query->part_to_access[0];
  return rc;
}


RC WorkerThread::process_log_msg(Message * msg) {
  assert(ISREPLICA);
  DEBUG("REPLICA PROCESS %ld\n",msg->get_txn_id());
  LogRecord * record = logger.createRecord(&((LogMessage*)msg)->record);
  logger.enqueueRecord(record);
  return RCOK;
}

RC WorkerThread::process_log_msg_rsp(Message * msg) {
  DEBUG("REPLICA RSP %ld\n",msg->get_txn_id());
  TxnManager * txn_man = txn_table.get_transaction_manager(msg->get_txn_id(),0);
  txn_man->repl_finished = true;
  if(txn_man->log_flushed)
    commit(txn_man);
  return RCOK;
}

RC WorkerThread::process_log_flushed(Message * msg) {
  DEBUG("LOG FLUSHED %ld\n",msg->get_txn_id());
  if(ISREPLICA) {
    msg_queue.enqueue(Message::create_message(msg->txn_id,LOG_MSG_RSP),GET_NODE_ID(msg->txn_id)); 
    return RCOK;
  }

  TxnManager * txn_man = txn_table.get_transaction_manager(msg->get_txn_id(),0);
  txn_man->log_flushed = true;
  if(g_repl_cnt == 0 || txn_man->repl_finished)
    commit(txn_man);
  return RCOK; 
}

bool WorkerThread::is_cc_new_timestamp() {
  return (CC_ALG == HSTORE && !HSTORE_LOCAL_TS)
							|| (CC_ALG == HSTORE_SPEC && !HSTORE_LOCAL_TS)
							|| CC_ALG == MVCC 
							|| CC_ALG == TIMESTAMP;
}

ts_t WorkerThread::get_next_ts() {
	if (g_ts_batch_alloc) {
		if (_curr_ts % g_ts_batch_num == 0) {
			_curr_ts = glob_manager.get_ts(get_thd_id());
			_curr_ts ++;
		} else {
			_curr_ts ++;
		}
		return _curr_ts - 1;
	} else {
		_curr_ts = glob_manager.get_ts(get_thd_id());
		return _curr_ts;
	}
}


