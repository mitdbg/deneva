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
#include "message.h"

void WorkerThread::send_init_done_to_all_nodes() {
		uint64_t total_nodes = g_node_cnt + g_client_node_cnt;
		for(uint64_t i = 0; i < total_nodes; i++) {
			if(i != g_node_id) {
        msg_queue.enqueue(Message::create_message(NULL,INIT_DONE),i);
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
		switch(msg->get_rtype()) {
			case RPASS:
        rc = process_rpass(msg);
				break;
			case RPREPARE: 
        rc = process_rprepare(msg);
				break;
			case RQRY:
        rc = process_rqry(msg);
				break;
			case RQRY_RSP:
        rc = process_rqry_rsp(txn_id, msg);
				break;
			case RFIN: 
        rc = process_rfin(msg);
				break;
			case RACK:
        rc = process_rack(msg);
				break;
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

void WorkerThread::commit() {
#if CC_ALG == HSTORE_SPEC
					if(m_txn->spec && m_txn->state != DONE)
						break;
					// Transaction is finished. Increment stats. Remove from txn pool
          if(m_txn->spec) {
            assert(m_query->part_num == 1);
            INC_STATS(0,spec_commit_cnt,1);
          }
#endif
#if MODE==QRY_ONLY_MODE
          // Release locks
          if(m_query->part_num > 1)
            rc = m_txn->finish(m_query->rc,m_query->part_to_access,1);
#endif
					assert(m_txn->get_rsp_cnt() == 0);
          //assert(MODE==TWOPC || MODE==QRY || m_query->part_num == 1 || ??);
          assert(MODE!=NORMAL_MODE || (m_query->part_num == 1 && (m_query->rtype == RTXN || m_query->rtype == RPASS)) || (m_query->part_num > 1 && m_query->rtype == RACK) || ((m_query->rtype == RQRY_RSP || m_query->rtype == RTXN) && m_query->ro));

					if(m_query->part_num == 1 && !m_txn->spec) {
						rc = m_txn->finish(rc,m_query->part_to_access,m_query->part_num);
					} else
						assert(m_txn->state == DONE || MODE!= NORMAL_MODE);


          m_txn->commit_stats(m_query);
          m_txn->update_stats();
          INC_STATS(get_thd_id(),part_cnt[m_query->part_touched_cnt-1],1);
          for(uint64_t i = 0 ; i < m_query->part_num; i++) {
            INC_STATS(get_thd_id(),part_acc[m_query->part_to_access[i]],1);
          }

					DEBUG("COMMIT %ld %ld %ld %f -- %f -- %d\n",m_txn->get_txn_id(),m_query->part_num,m_txn->abort_cnt,(double)m_txn->txn_time_wait/BILLION,(double)timespan/ BILLION,m_query->client_id);

					// Send result back to client
          assert(ISCLIENTN(m_query->client_id));
          msg_queue.enqueue(Message::create_message(m_query,CL_RSP),m_query->client_id);
          simulation->inc_txn_cnt();

}

void WorkerThread::abort() {
  DEBUG("ABORT %ld %f\n",m_txn->get_txn_id(),(float)(get_sys_clock() - run_starttime) / BILLION);
					assert(m_txn->get_rsp_cnt() == 0);
					assert(m_txn != NULL);
					assert(m_query->txn_id != UINT64_MAX);

#if MODE==QRY_ONLY_MODE
          // Release locks
					if(m_query->part_num > 1) 
            rc = m_txn->finish(m_query->rc,m_query->part_to_access,1);
#endif
					if(m_query->part_num == 1 && !m_txn->spec)
						rc = m_txn->finish(rc,m_query->part_to_access,m_query->part_num);
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
				INC_STATS(get_thd_id(), abort_cnt, 1);

        m_query->base_reset();
        m_query->reset();
        m_txn->abort_cnt++;
        m_txn->cc_wait_abrt_cnt += m_txn->cc_wait_cnt;
        m_txn->cc_wait_abrt_time += m_txn->cc_wait_time;
        m_txn->cc_hold_abrt_time += m_txn->cc_hold_time;
        m_txn->clear();

        m_txn->penalty_start = get_sys_clock();

#if CC_ALG == HSTORE_SPEC
          if(m_txn->spec) {
            INC_STATS(0,spec_abort_cnt,1);
            m_txn->spec = false;
            m_txn->spec_done = false;
					  //work_queue.add_query(_thd_id,m_query);
            work_queue.enqueue(get_thd_id(),m_query,false);
            break;
          }
#endif

          abort_queue.add_abort_query(_thd_id,m_query);


}

RC WorkerThread::run() {
  tsetup();

	RC rc = RCOK;
#if CC_ALG == HSTORE|| CC_ALG == HSTORE_SPEC
	RC rc2 = RCOK;
#endif
	TxnManager * m_txn = NULL;

	BaseQuery * tmp_query __attribute__ ((unused));
  tmp_query = NULL;
	uint64_t timespan;
	uint64_t prog_time = get_sys_clock();
  uint64_t thd_prof_start;
  BaseQuery * m_query;

	while(!simulation->is_done()) {
    m_query = NULL;
    m_txn = NULL;
    tmp_query = NULL;
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

		rc = RCOK;
		uint64_t txn_starttime = get_sys_clock();

    process(msg);

    uint64_t thd_prof_end = get_sys_clock();
    INC_STATS(_thd_id,thd_prof_thd2,thd_prof_end - thd_prof_start);
    if(IS_LOCAL(txn_id) || txn_id == UINT64_MAX) {
      INC_STATS(_thd_id,thd_prof_thd2_loc,thd_prof_end - thd_prof_start);
    } else {
      INC_STATS(_thd_id,thd_prof_thd2_rem,thd_prof_end - thd_prof_start);
    }
    INC_STATS(_thd_id,thd_prof_thd2_type[txn_type],thd_prof_end - thd_prof_start);
    thd_prof_start = get_sys_clock();

    work_queue.done(get_thd_id(),txn_id);

		timespan = get_sys_clock() - txn_starttime;
    thd_prof_end = get_sys_clock();
		INC_STATS(get_thd_id(),time_work,timespan);
    INC_STATS(_thd_id,thd_prof_thd3,thd_prof_end - thd_prof_start);
    INC_STATS(_thd_id,thd_prof_thd3_type[txn_type],thd_prof_end - thd_prof_start);

	}
  printf("FINISH %ld:%ld\n",_node_id,_thd_id);
  fflush(stdout);
  return FINISH;
}

RC WorkerThread::process_rfin(Message * msg) {
        uint64_t thd_prof_thd_rfin_start = get_sys_clock();
        uint64_t starttime = thd_prof_thd_rfin_start;
				DEBUG("%ld Received RFIN %ld\n",GET_PART_ID_FROM_IDX(_thd_id),m_query->txn_id);
				INC_STATS(0,rfin,1);
        if(msg->is_abort()) {
          INC_STATS(_thd_id,abort_rem_cnt,1);
          abort(msg->get_txn_id());
          msg_queue.enqueue(Message::create_message(RACK,msg->get_txn_id()),GET_HOME_NODE(msg->get_txn_id()));
          return Abort;
        } 
        finish_transaction();
        commit(msg->get_txn_id());
        if(!query->is_readonly() || CC_ALG == OCC)
          msg_queue.enqueue(Message::create_message(RACK,msg->get_txn_id()),GET_HOME_NODE(msg->get_txn_id()));

        return RCOK;
}

RC WorkerThread::process_rack(Message * msg) {

  uint64_t thd_prof_start = get_sys_clock();
  INC_STATS(0,rack,1);

  RC rc = RCOK;

  int responses_left = transaction_table->received_response(msg->get_txn_id());
  assert(responses_left >=0);
  if(responses_left > 0) 
    return;

  INC_STATS(_thd_id,thd_prof_thd_rack1,get_sys_clock() - thd_prof_thd_rack_start);
  thd_prof_thd_rack_start = get_sys_clock();

    // Done waiting 
    INC_STATS(get_thd_id(),time_wait_rem,get_sys_clock() - m_txn->wait_starttime);

    switch(msg->get_rtype()) {
      case RACK_PREP:
        // Validate
        rc  = m_txn->validate();
        send_rfin_messages(rc);
        if(rc == Abort)
          abort();
        //m_txn->finish(m_query,true);
        break;
      case RACK_FIN:
        uint64_t part_arr[1];
        part_arr[0] = m_query->part_to_access[0];
        rc = m_txn->finish(m_query->rc,part_arr,1);
        break;
      default:
        assert(false);
    }
  }
  return rc;
}

RC WorkerThread::process_rqry_rsp(Message * msg) {
  uint64_t thd_prof_start = get_sys_clock();
  INC_STATS(0,rqry_rsp,1);

  RC rc = m_txn->run_txn();
  return rc;

}

RC WorkerThread::process_rqry(Message * msg) {
  uint64_t thd_prof_start = get_sys_clock();
  INC_STATS(0,rqry,1);
  RC rc = RCOK;

  // Create new transaction table entry if one does not already exist
  Transaction * txn_man = transaction_table.get_transaction_manager(msg->get_txn_id());
  txn_man->set_timestamp(msg->get_timestamp());
  txn_man->set_start_timestamp(msg->get_start_timestamp());
  msg->copy_to_query(txn_man->query);

  rc = run_txn();

  // Send response
  if(rc != WAIT) {
    msg_queue.enqueue(Message::create_message(m_query,RQRY_RSP),m_query->return_id);
  }
  return rc;

}

RC WorkerThread::process_rtxn_cont() {
  run_txn_post_wait();
  run_txn();
}

RC WorkerThread::process_rprepare(Message * msg) {
    uint64_t starttime = get_sys_clock();
    uint64_t thd_prof_start = get_sys_clock();
    INC_STATS(0,rprep,1);

    // Validate transaction
    if (!msg->is_abort()) {
      rc  = m_txn->validate();
    } 
    // FIXME: need to add RC into message
    msg_queue.enqueue(Message::create_message(m_query,RACK),m_query->return_id);

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
        TransactionManager * txn_man;
        uint64_t txn_id = UINT64_MAX;

        if(msg->get_rtype() == CL_QRY) {
          // This is a new transaction

					// Only set new txn_id when txn first starts
          txn_id = get_next_txn_id();
          work_queue.update_hash(get_thd_id(),txn_id);

					// Put txn in txn_table
          transaction_table.create_entry(txn_id);
          txn_man = transaction_table.get_transaction_manager(txn_id);
					if (CC_ALG == WAIT_DIE || CC_ALG == VLL) {
            txn_man->set_timestamp(get_next_ts());
          }

				} else {
          txn_man = transaction_table.get_transaction_manager(msg->get_txn_id());
        }

          // Get new timestamps
          if(is_cc_new_timestamp())
            txn_man->set_timestamp(get_next_ts());
					}

#if CC_ALG == OCC
          txn_man->set_start_timestamp(get_next_ts());
#endif
      DEBUG("START %ld %f %lu\n",txn_man->get_txn_id(),simulation->seconds_from_start(get_sys_clock()),txn_man->get_timestamp());
    msg->copy_to_query(txn_man->query);

    rc = init_phase();
    if(rc != RCOK)
      return rc;

    // Execute transaction
    rc = txn_man->run_txn();
    return rc;
}

RC WorkerThread::init_phase() {
  RC rc = RCOK;
  //m_query->part_touched[m_query->part_touched_cnt++] = m_query->part_to_access[0];
  return rc;
}


RC WorkerThread::process_log_msg() {
  return RCOK;
}

RC WorkerThread::process_log_msg_rsp() {
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


