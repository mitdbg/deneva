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

void CalvinLockThread::setup() {
}

RC CalvinLockThread::run() {
  tsetup();

	BaseQuery * m_query = NULL;

	RC rc = RCOK;
	TxnManager * m_txn;
  uint64_t thd_prof_start;
  BaseQuery * tmp_query;

	while(!simulation->is_done()) {
    m_query = NULL;
    m_txn = NULL;

    bool got_qry = work_queue.dequeue(_thd_id, m_query);


		if(!got_qry)
			continue;
    thd_prof_start = get_sys_clock();

    assert(m_query->rtype == RTXN);
    assert(m_query->txn_id != UINT64_MAX);

    txn_table.get_txn(g_node_id,m_query->txn_id,m_query->batch_id,m_txn,tmp_query);
    if(m_txn==NULL) {
      txn_pool.get(m_txn);
      m_txn->set_txn_id(m_query->txn_id);
    } 
    txn_table.add_txn(g_node_id,m_txn,m_query);
    m_txn->register_thd(this);
    m_txn->batch_id = m_query->batch_id;
    // Acquire locks
    rc = m_txn->acquire_locks(m_query);

    INC_STATS(_thd_id,thd_prof_thd3,get_sys_clock() - thd_prof_start);


    if(rc == RCOK) {
      work_queue.enqueue(_thd_id,m_query,false);
    }

    work_queue.done(_thd_id,m_txn->get_txn_id());
  }
  printf("FINISH %ld:%ld\n",_node_id,_thd_id);
  fflush(stdout);
  return FINISH;
}

void CalvinThread::setup() {
	if( _thd_id == 0) {
		uint64_t total_nodes = g_node_cnt + g_client_node_cnt;
		for(uint64_t i = 0; i < total_nodes; i++) {
			if(i != g_node_id) {
        msg_queue.enqueue(Message::create_message(NULL,INIT_DONE),i);
			}
		}
  }
}

RC CalvinThread::run() {
  tsetup();

	BaseQuery * m_query = NULL;
	RC rc = RCOK;
	TxnManager * m_txn;

  BaseQuery * tmp_query;
	uint64_t starttime;
	uint64_t timespan;

	uint64_t run_starttime = get_sys_clock();
	uint64_t prog_time = run_starttime;
  uint64_t thd_prof_start;

	while(!simulation->is_done()) {
    m_query = NULL;
    tmp_query = NULL;
    m_txn = NULL;
    uint64_t thd_prof_start_thd1 = get_sys_clock();
    thd_prof_start = thd_prof_start_thd1;

		if(get_thd_id() == 0) {
      uint64_t now_time = get_sys_clock();
      if (now_time - prog_time >= g_prog_timer) {
        prog_time = now_time;
        SET_STATS(get_thd_id(), tot_run_time, prog_time - run_starttime); 
#if DEBUG_DISTR
        txn_table.dump();
#endif

        stats.print(true);
      }
		}

    INC_STATS(_thd_id,thd_prof_thd1a,get_sys_clock() - thd_prof_start);
    thd_prof_start = get_sys_clock();
    bool got_qry = work_queue.dequeue(_thd_id, m_query);

    INC_STATS(_thd_id,thd_prof_thd1c,get_sys_clock() - thd_prof_start);
    thd_prof_start = get_sys_clock();

    INC_STATS(_thd_id,thd_prof_thd1d,get_sys_clock() - thd_prof_start);
    INC_STATS(_thd_id,thd_prof_thd1,get_sys_clock() - thd_prof_start_thd1);
		if(!got_qry)
			continue;
    thd_prof_start = get_sys_clock();
    starttime = get_sys_clock();

    assert(m_query);

		rc = RCOK;
		txn_starttime = get_sys_clock();
    assert(m_query->rtype <= NO_MSG);
    uint64_t txn_type = (uint64_t)m_query->rtype;
    uint64_t tid = m_query->txn_id;
    uint64_t bid __attribute__((unused));
    bid = m_query->batch_id;
    bool txn_done = false;
    uint64_t rsp_cnt;

		switch(m_query->rtype) {
      case RFIN:
				DEBUG("%ld RFIN (%ld,%ld)\n",_thd_id,m_query->txn_id,m_query->batch_id);
				INC_STATS(0,rfin,1);
				txn_table.get_txn(g_node_id,m_query->txn_id,m_query->batch_id,m_txn,tmp_query);
        assert(m_txn != NULL);
        tmp_query = tmp_query->merge(m_query);
        if(m_query != tmp_query) {
          DEBUG_R("merge2 put 0x%lx\n",(uint64_t)m_query);
          qry_pool.put(m_query);
        }
        m_query = tmp_query;
        m_txn->register_thd(this);
				rsp_cnt = m_txn->incr_rsp2(1);
        assert(rsp_cnt >=0);
        if(m_txn->phase==6 && rsp_cnt == m_txn->active_cnt-1) {
          rc = m_query->rc;
          txn_done = true;
        } else {
          rc = WAIT;
        }
        break;
      case RFWD:
				INC_STATS(0,rfwd,1);
        DEBUG("%ld RFWD (%ld,%ld)\n",_thd_id,m_query->txn_id,m_query->batch_id);
				txn_table.get_txn(g_node_id,m_query->txn_id,m_query->batch_id,m_txn,tmp_query);
        if(m_txn == NULL) {
					rc = _wl->get_txn_man(m_txn);
					assert(rc == RCOK);
					m_txn->set_txn_id(m_query->txn_id);
          m_txn->batch_id = m_query->batch_id;
					txn_table.add_txn(g_node_id,m_txn,m_query);
        } else {
          tmp_query = tmp_query->merge(m_query);
          if(m_query != tmp_query) {
            DEBUG_R("merge2 put 0x%lx\n",(uint64_t)m_query);
            qry_pool.put(m_query);
          }
          m_query = tmp_query;
        }
        m_txn->register_thd(this);
        m_txn->set_query(m_query);
				rsp_cnt = m_txn->incr_rsp(1);
        if(m_txn->phase == 4 && rsp_cnt == m_txn->participant_cnt-1) {
          rc = m_txn->run_calvin_txn(m_query);
          if(m_txn->phase==6 && rc == RCOK)
            txn_done = true;
        } else {
          rc = WAIT;
        }
        break;
      case RQRY:
      case RTXN:
				INC_STATS(0,rtxn,1);

				txn_table.get_txn(g_node_id,m_query->txn_id,m_query->batch_id,m_txn,tmp_query);
        assert(m_txn != NULL);
        tmp_query = tmp_query->merge(m_query);
        if(m_query != tmp_query) {
          qry_pool.put(m_query);
        }
        m_query = tmp_query;
        assert(m_query->batch_id == simulation->get_worker_epoch() || m_query->batch_id == simulation->get_worker_epoch() - 1);
        m_txn->set_query(m_query);
        m_txn->register_thd(this);

				m_txn->abort_cnt = 0;
        //m_txn->set_txn_id(m_query->txn_id);
				m_txn->starttime = get_sys_clock();
#if DEBUG_TIMELINE
					printf("START %ld %ld\n",m_txn->get_txn_id(),m_txn->starttime);
#endif
        DEBUG("START %ld %ld\n",m_txn->get_txn_id(),m_txn->starttime);
        // Execute
				rc = m_txn->run_calvin_txn(m_query);
        if((m_txn->phase==6 && rc == RCOK) || m_txn->active_cnt == 0 || m_txn->participant_cnt == 1)
          txn_done = true;
				break;
		  default:
			  assert(false);
    }
    uint64_t thd_prof_end = get_sys_clock();
    INC_STATS(_thd_id,thd_prof_thd2,thd_prof_end - thd_prof_start);
    INC_STATS(_thd_id,thd_prof_thd2_type[txn_type],thd_prof_end - thd_prof_start);


    if(txn_done) {
      // Release locks
      //rc = m_txn->release_locks(m_query);
      m_txn->finish(rc,NULL,0);
			INC_STATS(get_thd_id(),txn_cnt,1);
			timespan = get_sys_clock() - m_txn->starttime;
			INC_STATS(get_thd_id(), run_time, timespan);
			INC_STATS(get_thd_id(), latency, timespan);
      // delete_txn does NOT free m_query for CALVIN
      txn_table.delete_txn(m_query->return_id,tid,m_query->batch_id);
      assert(m_query->batch_id == simulation->get_worker_epoch() || m_query->batch_id == simulation->get_worker_epoch() - 1);
      if(m_query->return_id == g_node_id) {
        DEBUG_R("RACK (%ld,%ld)  0x%lx\n",m_query->txn_id,m_query->batch_id,(uint64_t)m_query);
        //printf("%ld RACK (%ld,%ld)  0x%lx\n",_thd_id,m_query->txn_id,m_query->batch_id,(uint64_t)m_query);
        m_query->rtype = RACK;
        work_queue.enqueue(_thd_id,m_query,false);
      } else {
        msg_queue.enqueue(Message::create_message(m_query,RACK),m_query->return_id);
      }
    }
    work_queue.done(_thd_id,tid);

		timespan = get_sys_clock() - starttime;
		INC_STATS(get_thd_id(),time_work,timespan);

  }
  printf("FINISH %ld:%ld\n",_node_id,_thd_id);
  fflush(stdout);
  return FINISH;
}

void CalvinSequencerThread::setup() {
}
RC CalvinSequencerThread::run() {
	BaseQuery * m_query = NULL;

	uint64_t last_batchtime = run_starttime;

	while(!simulation->is_done()) {
    m_query = NULL;

    bool got_qry = work_queue.dequeue(_thd_id, m_query);

    if(get_sys_clock() - last_batchtime >= g_batch_time_limit) {
      simulation->next_sched_epoch();
      seq_man.send_next_batch(_thd_id);
      last_batchtime = get_sys_clock();
    }

		if(!got_qry)
			continue;
    uint64_t tid = m_query->txn_id;

    switch (m_query->rtype) {
      case RTXN:
        // Query from client
        seq_man.process_txn(m_query);
        break;
      case RACK:
        // Ack from server
        seq_man.process_ack(m_query,get_thd_id());
        break;
      default:
        assert(false);
    }
    DEBUG_R("seq_man %d put 0x%lx\n",m_query->rtype,(uint64_t)m_query);
    qry_pool.put(m_query);
    work_queue.done(_thd_id,tid);
  }
  printf("FINISH %ld:%ld\n",_node_id,_thd_id);
  fflush(stdout);
  return FINISH;

}


