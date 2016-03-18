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

void WorkerThread::send_init_done_to_all_nodes() {
		uint64_t total_nodes = g_node_cnt + g_client_node_cnt;
		for(uint64_t i = 0; i < total_nodes; i++) {
			if(i != g_node_id) {
        msg_queue.enqueue(Message::create_message(INIT_DONE),i);
			}
		}

}

void WorkerThread::setup() {

	if( get_thd_id() == 0) {
    send_init_done_to_all_nodes();
  }

}

void WorkerThread::progress_stats() {
		if(get_thd_id() == 0) {
      uint64_t now_time = get_sys_clock();
      if (now_time - prog_time >= g_prog_timer) {
        prog_time = now_time;
        SET_STATS(get_thd_id(), tot_run_time, prog_time - run_starttime); 

        stats.print(true);
      }
		}

}

void WorkerThread::process(Message * msg) {
  RC rc __attribute__ ((unused));
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
			case RQRY_RSP:
        rc = process_rqry_rsp(msg);
				break;
			case RFIN: 
        rc = process_rfin(msg);
				break;
			case RACK:
        rc = process_rack(msg);
				break;
      case CL_QRY:
			case RTXN:
        rc = process_rtxn(msg);
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
}

void WorkerThread::check_if_done(RC rc, TxnManager * txn_man) {
  //TODO: get txn_id in non-hacky way, or change the way commit() is handled
  if(rc == Commit)
    commit(txn_man);
  if(rc == Abort)
    abort(txn_man);
}

void WorkerThread::commit(TxnManager * txn_man) {
  //TxnManager * txn_man = txn_table.get_transaction_manager(txn_id,0);
  //txn_man->release_locks(RCOK);
  //        txn_man->commit_stats();

  uint64_t timespan = get_sys_clock() - txn_man->get_start_timestamp();
  DEBUG("COMMIT %ld -- %f\n",txn_man->get_txn_id(),(double)timespan/ BILLION);

  // Send result back to client
  msg_queue.enqueue(Message::create_message(txn_man,CL_RSP),txn_man->client_id);
  simulation->inc_txn_cnt();
  // remove txn from pool

}

void WorkerThread::abort(TxnManager * txn_man) {

  //TxnManager * txn_man = txn_table.get_transaction_manager(txn_id,0);
  DEBUG("ABORT %ld -- %f\n",txn_man->get_txn_id(),(double)get_sys_clock() - run_starttime/ BILLION);
  txn_man->release_locks(Abort);
          /*
#if WORKLOAD == TPCC
        if(((TPCCQuery*)m_query)->rbk && m_query->rem_req_state == TPCC_FIN) {
          INC_STATS(get_thd_id(),txn_cnt,1);
			    INC_STATS(get_thd_id(), rbk_abort_cnt, 1);
		      timespan = get_sys_clock() - m_txn->starttime;
		      INC_STATS(get_thd_id(), run_time, timespan);
		      INC_STATS(get_thd_id(), latency, timespan);
          simulation->inc_txn_cnt();
        //ATOM_SUB(txn_table.inflight_cnt,1);
          msg_queue.enqueue(Message::create_message(m_query,CL_RSP),m_query->client_id);
          break;
        
        }
#endif
*/
  INC_STATS(get_thd_id(), abort_cnt, 1);

  ++txn_man->abort_cnt;
  txn_man->reset();


  abort_queue.enqueue(txn_man->get_txn_id(),txn_man->get_abort_cnt());

}

RC WorkerThread::run() {
  tsetup();

	uint64_t timespan;
  uint64_t thd_prof_start;

	while(!simulation->is_done()) {
    uint64_t thd_prof_start_thd1 = get_sys_clock();
    thd_prof_start = thd_prof_start_thd1;

    progress_stats();

    INC_STATS(_thd_id,thd_prof_thd1a,get_sys_clock() - thd_prof_start);
    thd_prof_start = get_sys_clock();
    Message * msg = work_queue.dequeue();
    INC_STATS(_thd_id,thd_prof_thd1,get_sys_clock() - thd_prof_start_thd1);
    thd_prof_start = get_sys_clock();

    if(!msg)
      continue;

		uint64_t txn_starttime = get_sys_clock();

    process(msg);

    uint64_t thd_prof_end = get_sys_clock();
    INC_STATS(_thd_id,thd_prof_thd2,thd_prof_end - thd_prof_start);
    if(IS_LOCAL(msg->txn_id) || msg->txn_id == UINT64_MAX) {
      INC_STATS(_thd_id,thd_prof_thd2_loc,thd_prof_end - thd_prof_start);
    } else {
      INC_STATS(_thd_id,thd_prof_thd2_rem,thd_prof_end - thd_prof_start);
    }
    INC_STATS(_thd_id,thd_prof_thd2_type[msg->rtype],thd_prof_end - thd_prof_start);
    thd_prof_start = get_sys_clock();

    work_queue.deactivate_txn_id(msg->txn_id);

		timespan = get_sys_clock() - txn_starttime;
    thd_prof_end = get_sys_clock();
		INC_STATS(get_thd_id(),time_work,timespan);
    INC_STATS(_thd_id,thd_prof_thd3,thd_prof_end - thd_prof_start);
    INC_STATS(_thd_id,thd_prof_thd3_type[msg->rtype],thd_prof_end - thd_prof_start);

	}
  printf("FINISH %ld:%ld\n",_node_id,_thd_id);
  fflush(stdout);
  return FINISH;
}

RC WorkerThread::process_rfin(Message * msg) {
        //uint64_t starttime = thd_prof_thd_rfin_start;

  TxnManager * txn_man = txn_table.get_transaction_manager(msg->get_txn_id(),0);
				DEBUG("%ld Received RFIN %ld\n",GET_PART_ID_FROM_IDX(_thd_id),msg->txn_id);
				INC_STATS(0,rfin,1);
        if(((FinishMessage*)msg)->rc == Abort) {
          INC_STATS(_thd_id,abort_rem_cnt,1);
          abort(txn_man);
          msg_queue.enqueue(Message::create_message(RACK),GET_NODE_ID(msg->get_txn_id()));
          return Abort;
        } 
        commit(txn_man);
        if(!txn_man->query->readonly() || CC_ALG == OCC)
          msg_queue.enqueue(Message::create_message(RACK),GET_NODE_ID(msg->get_txn_id()));

        return RCOK;
}

RC WorkerThread::process_rack(Message * msg) {

  //uint64_t starttime = get_sys_clock();
  INC_STATS(0,rack,1);

  RC rc = RCOK;

  TxnManager * txn_man = txn_table.get_transaction_manager(msg->get_txn_id(),0);
  int responses_left = txn_man->received_response(((AckMessage*)msg)->rc);
  assert(responses_left >=0);
  if(responses_left > 0) 
    return WAIT;

    // Done waiting 

    switch(msg->get_rtype()) {
      case RACK_PREP:
        // Validate
        rc  = txn_man->validate();
        txn_man->send_rfin_messages(rc);
        if(rc == Abort)
          abort(txn_man);
        break;
      case RACK_FIN:
        if(txn_man->get_rc() == RCOK) {
          commit(txn_man);
        } else {
          abort(txn_man);
        }
        break;
      default:
        assert(false);
  }
  return rc;
}

RC WorkerThread::process_rqry_rsp(Message * msg) {
  //uint64_t thd_prof_start = get_sys_clock();
  INC_STATS(0,rqry_rsp,1);

  TxnManager * txn_man = txn_table.get_transaction_manager(msg->get_txn_id(),0);
  RC rc = txn_man->run_txn();
  return rc;

}

RC WorkerThread::process_rqry(Message * msg) {
  //uint64_t thd_prof_start = get_sys_clock();
  INC_STATS(0,rqry,1);
  RC rc = RCOK;

  // Create new transaction table entry if one does not already exist
  TxnManager * txn_man = txn_table.get_transaction_manager(msg->get_txn_id(),0);
  msg->copy_to_txn(txn_man);

  rc = txn_man->run_txn();

  // Send response
  if(rc != WAIT) {
    msg_queue.enqueue(Message::create_message(txn_man,RQRY_RSP),msg->return_node_id);
  }
  return rc;

}

RC WorkerThread::process_rtxn_cont(Message * msg) {
  TxnManager * txn_man = txn_table.get_transaction_manager(msg->get_txn_id(),0);
  //txn_man->run_txn_post_wait();
  RC rc = txn_man->run_txn();
  check_if_done(rc,txn_man);
  return RCOK;
}

RC WorkerThread::process_rprepare(Message * msg) {
    //uint64_t starttime = get_sys_clock();
    INC_STATS(0,rprep,1);
    RC rc = RCOK;
  TxnManager * txn_man = txn_table.get_transaction_manager(msg->get_txn_id(),0);

    // Validate transaction
    if (!((FinishMessage*)msg)->is_abort()) {
      rc  = txn_man->validate();
    } 
    // FIXME: need to add RC into message
    msg_queue.enqueue(Message::create_message(RACK),msg->return_node_id);

    return rc;
}

uint64_t WorkerThread::get_next_txn_id() {
  uint64_t txn_id = ( get_node_id() + get_thd_id() * g_node_cnt) 
							+ (g_thread_cnt * g_node_cnt * _thd_txn_id);
  ++_thd_txn_id;
  return txn_id;
}

RC WorkerThread::process_rtxn(Message * msg) {
				INC_STATS(0,rtxn,1);
        RC rc = RCOK;
        TxnManager * txn_man;
        uint64_t txn_id = UINT64_MAX;

        if(msg->get_rtype() == CL_QRY) {
          // This is a new transaction

					// Only set new txn_id when txn first starts
          txn_id = get_next_txn_id();
          work_queue.activate_txn_id(txn_id);

					// Put txn in txn_table
          txn_man = txn_table.get_transaction_manager(txn_id,0);
					if (CC_ALG == WAIT_DIE || CC_ALG == VLL) {
            txn_man->set_timestamp(get_next_ts());
          }
          txn_man->txn_stats.starttime = get_sys_clock();

				} else {
          txn_man = txn_table.get_transaction_manager(msg->get_txn_id(),0);
        }

          // Get new timestamps
          if(is_cc_new_timestamp()) {
            txn_man->set_timestamp(get_next_ts());
					}

#if CC_ALG == OCC
          txn_man->set_start_timestamp(get_next_ts());
#endif
      DEBUG("START %ld %f %lu\n",txn_man->get_txn_id(),simulation->seconds_from_start(get_sys_clock()),txn_man->txn_stats.starttime);
    msg->copy_to_txn(txn_man);

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
  return RCOK;
}

RC WorkerThread::process_log_msg_rsp(Message * msg) {
  return RCOK;
}

bool WorkerThread::is_cc_new_timestamp() {
  return (CC_ALG == HSTORE && !HSTORE_LOCAL_TS)
							|| (CC_ALG == HSTORE_SPEC && !HSTORE_LOCAL_TS)
							|| CC_ALG == MVCC 
              || CC_ALG == VLL
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


