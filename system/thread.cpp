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
  _thd_txn_id = 0;
}

uint64_t Thread::get_thd_id() { return _thd_id; }
uint64_t Thread::get_node_id() { return _node_id; }
uint64_t Thread::get_host_cid() {	return _host_cid; }
void Thread::set_host_cid(uint64_t cid) { _host_cid = cid; }
uint64_t Thread::get_cur_cid() { return _cur_cid; }
void Thread::set_cur_cid(uint64_t cid) {_cur_cid = cid; }

RC Thread::run_remote() {
	printf("Run_remote %ld:%ld\n",_node_id, _thd_id);
  fflush(stdout);
	if (warmup_finish) {
		mem_allocator.register_thread(_thd_id);
	}

	stats.init(get_thd_id());
	pthread_barrier_wait( &warmup_bar );
	run_starttime = get_sys_clock();

  while(!_wl->sim_init_done) {
    tport_man.recv_msg();
    /*
    if(get_sys_clock() - run_starttime >= g_done_timer)
      return FINISH;
      */
  }
  warmup_done = true;
	pthread_barrier_wait( &warmup_bar );
	printf("Run_remote %ld:%ld\n",_node_id, _thd_id);
  fflush(stdout);

	myrand rdm;
	rdm.init(get_thd_id());
	RC rc = RCOK;
	assert (rc == RCOK);

	run_starttime = get_sys_clock();
	ts_t rq_time = get_sys_clock();

  uint64_t thd_prof_start;
	while (true) {
    thd_prof_start = get_sys_clock();
    if(tport_man.recv_msg()) {
      rq_time = get_sys_clock();
      INC_STATS(_thd_id,rthd_prof_1,get_sys_clock() - thd_prof_start);
    } else {
      INC_STATS(_thd_id,rthd_prof_2,get_sys_clock() - thd_prof_start);
    }

		ts_t tend = get_sys_clock();
		if (warmup_finish 
        && ((tend - run_starttime >= g_done_timer) 
          || (_wl->sim_done && ((tend - rq_time) > MSG_TIMEOUT)))) {
			if( !ATOM_CAS(_wl->sim_timeout, false, true) ) {
				assert( _wl->sim_timeout);
      } else{
        printf("_wl->sim_timeout=%d\n",_wl->sim_timeout);
        fflush(stdout);
      }
		}

		//if ((_wl->sim_done && _wl->sim_timeout) || (tend - run_starttime >= g_done_timer)) {
		if (_wl->sim_done && _wl->sim_timeout) {

      printf("FINISH %ld:%ld\n",_node_id,_thd_id);
      fflush(stdout);
			return FINISH;
		}


	}
}
RC Thread::run_abort() {
	printf("Run_abort %ld:%ld\n",_node_id, _thd_id);
	pthread_barrier_wait( &warmup_bar );
	pthread_barrier_wait( &warmup_bar );
	printf("Run_abort %ld:%ld\n",_node_id, _thd_id);
	while (!_wl->sim_done) {
    abort_queue.process_aborts();
    /*
    if(abort_queue.poll_abort(get_thd_id())) {
      BaseQuery * tmp_query = abort_queue.get_next_abort_query(get_thd_id());
      if(tmp_query)
        work_queue.enqueue(0,tmp_query);
    } else {
      sleep(10);
    }
    */
  }
  return FINISH;
 
}

RC Thread::run_send() {
	printf("Run_send %ld:%ld\n",_node_id, _thd_id);
  fflush(stdout);
	if (warmup_finish) {
		mem_allocator.register_thread(_thd_id);
	}
	stats.init(get_thd_id());
	pthread_barrier_wait( &warmup_bar );

  MessageThread messager;
  messager.init(_thd_id);

  run_starttime = get_sys_clock();
	while (!_wl->sim_init_done) {
    messager.run();
    /*
    if(get_sys_clock() - run_starttime >= g_done_timer)
      return FINISH;
      */
  }

	pthread_barrier_wait( &warmup_bar );
	printf("Run_send %ld:%ld\n",_node_id, _thd_id);
  fflush(stdout);
  uint64_t starttime = get_sys_clock();

	while (true) {
    messager.run();
		if ((get_sys_clock() - starttime) >= g_done_timer || (_wl->sim_done && _wl->sim_timeout)) {
      printf("FINISH %ld:%ld\n",_node_id,_thd_id);
      fflush(stdout);
			return FINISH;
    }
  }
}

RC Thread::run() {
	printf("Run %ld:%ld\n",_node_id, _thd_id);
  fflush(stdout);
	if (warmup_finish) {
		mem_allocator.register_thread(_thd_id);
	}
	stats.init(get_thd_id());
	pthread_barrier_wait( &warmup_bar );
	BaseQuery * m_query = NULL;

	run_starttime = get_sys_clock();

	if( _thd_id == 0) {
#if WORKLOAD == YCSB
    m_query = new YCSBQuery;
#elif WORKLOAD == TPCC
    m_query = new TPCCQuery;
#endif
		uint64_t total_nodes = g_node_cnt + g_client_node_cnt;
    /*
#if CC_ALG == CALVIN
		total_nodes++;
#endif
*/
		for(uint64_t i = 0; i < total_nodes; i++) {
			if(i != g_node_id) {
        msg_queue.enqueue(NULL,INIT_DONE,i);
			}
		}
  }
    /*
    while(!_wl->sim_init_done) {
      while(!work_queue.dequeue(_thd_id,m_query)) { }
      if(m_query->rtype == INIT_DONE) {
        ATOM_SUB(_wl->rsp_cnt,1);
        printf("Processed INIT_DONE from %ld -- %ld\n",m_query->return_id,_wl->rsp_cnt);
        fflush(stdout);
        if(_wl->rsp_cnt ==0) {
			    if( !ATOM_CAS(_wl->sim_init_done, false, true) )
				    assert( _wl->sim_init_done);
        }
      } else {
        // Put other queries aside until all nodes are ready
        //work_queue.add_query(_thd_id,m_query);
        work_queue.enqueue(m_query);
      }
    if(get_sys_clock() - run_starttime >= g_done_timer)
      return FINISH;
	  }
  } else {
    while(!_wl->sim_init_done) {
      if(get_sys_clock() - run_starttime >= g_done_timer)
        return FINISH;
    }
  }
    */
	pthread_barrier_wait( &warmup_bar );
	printf("Run %ld:%ld\n",_node_id, _thd_id);
  fflush(stdout);

	myrand rdm;
	rdm.init(get_thd_id());
	RC rc = RCOK;
#if CC_ALG == HSTORE|| CC_ALG == HSTORE_SPEC
	RC rc2 = RCOK;
#endif
	TxnManager * m_txn = NULL;
	//rc = _wl->get_txn_man(m_txn, this);
  //txn_pool.get(m_txn);
	//glob_manager.set_TxnManager(m_txn);

	BaseQuery * tmp_query __attribute__ ((unused));
  tmp_query = NULL;
	uint64_t stoptime = 0;
	uint64_t timespan;
	run_starttime = get_sys_clock();
	uint64_t prog_time = run_starttime;
  uint64_t thd_prof_start;

	while(true) {
    m_query = NULL;
    m_txn = NULL;
    tmp_query = NULL;
    uint64_t thd_prof_start_thd1 = get_sys_clock();
    thd_prof_start = thd_prof_start_thd1;

		if(get_thd_id() == 0) {
      uint64_t now_time = get_sys_clock();
      if (now_time - prog_time >= g_prog_timer) {
        prog_time = now_time;
        SET_STATS(get_thd_id(), tot_run_time, prog_time - run_starttime); 

        stats.print(true);
      }
		}

    INC_STATS(_thd_id,thd_prof_thd1a,get_sys_clock() - thd_prof_start);
    thd_prof_start = get_sys_clock();
    /*
    if(((get_sys_clock() - run_starttime >= g_done_timer)
        //|| (_wl->txn_cnt >= g_max_txn_per_part 
        //  && txn_table.empty(get_node_id()))
          )
        && !_wl->sim_done) {
        if( !ATOM_CAS(_wl->sim_done, false, true) ) {
          assert( _wl->sim_done);
        } else {
          printf("_wl->sim_done=%d\n",_wl->sim_done);
          fflush(stdout);
        }
        stoptime = get_sys_clock();
        if(stats._stats[get_thd_id()]->finish_time == 0) {
          printf("Setting final finish time\n");
          SET_STATS(get_thd_id(), finish_time, stoptime - run_starttime);
        }
    }

    INC_STATS(_thd_id,thd_prof_thd1b,get_sys_clock() - thd_prof_start);
    thd_prof_start = get_sys_clock();
    */
    /*
		while(!work_queue.poll_next_query(get_thd_id())
                && !abort_queue.poll_abort(get_thd_id())
                && !_wl->sim_done && !_wl->sim_timeout) {
                */
    bool got_qry = false;
		while(!(got_qry = work_queue.dequeue(_thd_id, m_query))
                && !_wl->sim_done && !_wl->sim_timeout) {
#if CC_ALG == HSTORE_SPEC
      txn_table.spec_next(_thd_id);
#endif

      if (warmup_finish && 
          ((get_sys_clock() - run_starttime >= g_done_timer) 
           /*|| (_wl->txn_cnt >= g_max_txn_per_part
           && txn_table.empty(get_node_id()))*/)) {
        break;
      }
    }

    INC_STATS(_thd_id,thd_prof_thd1c,get_sys_clock() - thd_prof_start);
    thd_prof_start = get_sys_clock();
		// End conditions
    if((get_sys_clock() - run_starttime >= g_done_timer)
        ||(_wl->sim_done && _wl->sim_timeout)) { 
      if( !ATOM_CAS(_wl->sim_done, false, true) ) {
        assert( _wl->sim_done);
      } else {
        printf("_wl->sim_done=%d\n",_wl->sim_done);
        fflush(stdout);
      }
			uint64_t currtime = get_sys_clock();
      if(stoptime == 0)
        stoptime = currtime;
			if(currtime - stoptime > MSG_TIMEOUT) {
				SET_STATS(get_thd_id(), tot_run_time, currtime - run_starttime - MSG_TIMEOUT); 
			}
			else {
				SET_STATS(get_thd_id(), tot_run_time, stoptime - run_starttime); 
			}
      if(get_thd_id() == 0) {
        abort_queue.abort_finish(currtime);

        // Send EXP_DONE to all nodes
        /*
        uint64_t nnodes = g_node_cnt + g_client_node_cnt;
#if CC_ALG == CALVIN
        nnodes++;
#endif
#if WORKLOAD == YCSB
        m_query = new YCSBQuery;
#endif
        for(uint64_t i = 0; i < nnodes; i++) {
          if(i != g_node_id) {
            msg_queue.enqueue(NULL,EXP_DONE,i);
          }
        }
      */
      }

      printf("FINISH %ld:%ld\n",_node_id,_thd_id);
      fflush(stdout);
			return FINISH;
		}

    INC_STATS(_thd_id,thd_prof_thd1d,get_sys_clock() - thd_prof_start);
    INC_STATS(_thd_id,thd_prof_thd1,get_sys_clock() - thd_prof_start_thd1);
		if(!got_qry)
			continue;
    thd_prof_start = get_sys_clock();

    assert(m_query);
    uint64_t txn_id = m_query->txn_id;
    uint64_t txn_type = (uint64_t)m_query->rtype;

		rc = RCOK;
		txn_starttime = get_sys_clock();
    assert(m_query->rtype <= NO_MSG);

		switch(m_query->rtype) {
      case EXP_DONE:
        DEBUG("Received EXP_DONE from %ld\n",m_query->return_id)
        ATOM_SUB(_wl->done_cnt,1);
        if(_wl->done_cnt == 0) {
          if( !ATOM_CAS(_wl->sim_timeout, false, true) )
            assert( _wl->sim_timeout);
          if( !ATOM_CAS(_wl->sim_done, false, true) )
            assert( _wl->sim_done);
        }
        qry_pool.put(m_query);
        m_query = NULL;
        break;
			case RPASS:
        rc = process_rpass(m_query,m_txn);
				break;
			case RINIT:
        rc = process_rinit(m_query,m_txn);
				break;
			case RPREPARE: {
        rc = process_rprepare(m_query,m_txn);
				break;
			}
			case RQRY:
        rc = process_rqry(m_query,m_txn);
				break;
			case RQRY_RSP:
        rc = process_rqry_rsp(txn_id, m_query,m_txn);
				break;
			case RFIN: 
        rc = process_rfin(m_query,m_txn);
				break;
			case RACK:
        rc = process_rack(m_query,m_txn);
				break;
			case RTXN:
        rc = process_rtxn(m_query,m_txn);
				break;
			case LOG_MSG:
        rc = process_log_msg(m_query,m_txn);
				break;
			case LOG_MSG_RSP:
        rc = process_log_msg_rsp(m_query,m_txn);
				break;
			default:
        printf("Msg: %d\n",m_query->rtype);
        fflush(stdout);
				assert(false);
				break;
		}
    if(!(!m_query || m_query->txn_id == m_txn->get_txn_id())) {
      printf("FAIL2: %ld %lx %lx\n",txn_id,(uint64_t)m_txn,(uint64_t)m_query);
      fflush(stdout);
    }
    assert(!m_query || m_query->txn_id == m_txn->get_txn_id());

    uint64_t thd_prof_end = get_sys_clock();
    INC_STATS(_thd_id,thd_prof_thd2,thd_prof_end - thd_prof_start);
    if(IS_LOCAL(txn_id) || txn_id == UINT64_MAX) {
      INC_STATS(_thd_id,thd_prof_thd2_loc,thd_prof_end - thd_prof_start);
    } else {
      INC_STATS(_thd_id,thd_prof_thd2_rem,thd_prof_end - thd_prof_start);
    }
    INC_STATS(_thd_id,thd_prof_thd2_type[txn_type],thd_prof_end - thd_prof_start);
    thd_prof_start = get_sys_clock();

		// If m_query was freed just before this, m_query is NULL
		if(m_query != NULL && IS_LOCAL(m_query->txn_id)) {
      if(!ISCLIENTN(m_query->client_id)) {
        printf("FAIL0: %ld %ld\n",txn_id,txn_type);
        fflush(stdout);
      }
      assert((!m_txn && !m_query)|| m_query->txn_id == m_txn->get_txn_id());
      assert(ISCLIENTN(m_query->client_id));
			switch(rc) {
				case RCOK:
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
          msg_queue.enqueue(m_query,CL_RSP,m_query->client_id);
					ATOM_ADD(_wl->txn_cnt,1);

					break;

				case Abort:
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
					ATOM_ADD(_wl->txn_cnt,1);
        //ATOM_SUB(txn_table.inflight_cnt,1);
          msg_queue.enqueue(m_query,CL_RSP,m_query->client_id);
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

        DEBUG("ABORT %ld %f\n",m_txn->get_txn_id(),(float)(get_sys_clock() - run_starttime) / BILLION);
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

					break;
				case WAIT:
					assert(m_txn != NULL);
					break;
				case WAIT_REM:
					// Save transaction information for later
					assert(m_txn != NULL);
					m_txn->wait_starttime = get_sys_clock();
#if WORKLOAD == TPCC
					// WAIT_REM from finish
					if(m_query->rem_req_state == TPCC_FIN)
						break;
#elif WORKLOAD == YCSB
					if(m_query->rem_req_state == YCSB_FIN)
						break;
#endif
					assert((CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC) || m_query->dest_id != g_node_id);
#if CC_ALG != HSTORE && CC_ALG != HSTORE_SPEC
          uint64_t i;
          for(i = m_query->part_touched_cnt-1; i > 0;i--) {
            if(m_query->part_touched[i] == GET_PART_ID(0,m_query->dest_id))
              break;
          }
          if(i == 0)
            m_query->part_touched[m_query->part_touched_cnt++] = GET_PART_ID(0,m_query->dest_id);
#endif
          // Stat start for txn_time_net
          m_txn->txn_stat_starttime = get_sys_clock();

          msg_queue.enqueue(m_query,RQRY,m_query->dest_id);
					break;
				default:
					assert(false);
					break;
			}
		} 

    work_queue.done(get_thd_id(),txn_id);

		timespan = get_sys_clock() - txn_starttime;
    thd_prof_end = get_sys_clock();
		INC_STATS(get_thd_id(),time_work,timespan);
    INC_STATS(_thd_id,thd_prof_thd3,thd_prof_end - thd_prof_start);
    INC_STATS(_thd_id,thd_prof_thd3_type[txn_type],thd_prof_end - thd_prof_start);

	}
}

ts_t
Thread::get_next_ts() {
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

RC Thread::process_rfin(BaseQuery *& m_query,TxnManager *& m_txn) {
        uint64_t thd_prof_thd_rfin_start = get_sys_clock();
        uint64_t starttime = thd_prof_thd_rfin_start;
				DEBUG("%ld Received RFIN %ld\n",GET_PART_ID_FROM_IDX(_thd_id),m_query->txn_id);
				INC_STATS(0,rfin,1);
        /*
#if MODE==TWOPC
        // Immediate ack back
        msg_queue.enqueue(m_query,RACK,m_query->return_id);
        m_query = NULL;
        return RCOK;
#endif
*/
        if(m_query->rc == Abort) {
          INC_STATS(_thd_id,abort_rem_cnt,1);
        } 
				assert(CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC || IS_REMOTE(m_query->txn_id));
        assert(MODE == NORMAL_MODE || MODE == NOCC_MODE);

        // Retrieve transaction from transaction pool
        // TODO: move outside of process functions?
        BaseQuery * tmp_query;
				txn_table.get_txn(m_query->return_id, m_query->txn_id,0,m_txn,tmp_query);
				bool finish = true;
#if CC_ALG != HSTORE && CC_ALG != HSTORE_SPEC && CC_ALG != VLL
				if (m_txn == NULL) {
					assert(m_query->rc == Abort);
					finish = false;	
				}
#else
				assert( m_txn != NULL);
#endif

        INC_STATS(_thd_id,thd_prof_thd_rfin0,get_sys_clock() - thd_prof_thd_rfin_start);
        thd_prof_thd_rfin_start = get_sys_clock();

#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
        if(m_txn) {
          m_txn->register_thd(this);
          if(GET_NODE_ID(m_query->home_part) != g_node_id || GET_PART_ID_IDX(m_query->home_part) == _thd_id) {
            tmp_query = tmp_query->merge(m_query);
            tmp_query->readonly = m_query->readonly;
            if(m_query != tmp_query) {
              //YCSBQuery_FREE(m_query)
              qry_pool.put(m_query);
            }
            m_query = tmp_query;
            m_txn->set_query(m_query);
            assert(m_query->home_part != GET_PART_ID_FROM_IDX(_thd_id));
          } 
        }

				if (finish) {
          if(GET_NODE_ID(m_query->home_part) != g_node_id || GET_PART_ID_IDX(m_query->home_part) == _thd_id) {
					  m_txn->state = DONE;
					  m_txn->rem_fin_txn(m_query);
          } else {
					  m_txn->loc_fin_txn(m_query);
          }
        }
        if(GET_NODE_ID(m_query->home_part) != g_node_id) {
          msg_queue.enqueue(m_query,RACK,m_query->return_id);
        } else {
          m_query->local_rack_query();
        }

#else // NOT CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
        // Merge query information
        if(m_txn) {
          m_txn->register_thd(this);
          tmp_query = tmp_query->merge(m_query);
          tmp_query->ro = m_query->ro;
          if(m_query != tmp_query) {
            //YCSBQuery_FREE(m_query)
            qry_pool.put(m_query);
          }
          m_query = tmp_query;
          m_txn->set_query(m_query);
        }
    INC_STATS(_thd_id,prof_time_twopc,get_sys_clock() - starttime);
        // Clean up transaction
				if (finish) {
					m_txn->state = DONE;
					m_txn->rem_fin_txn(m_query);
        }
        INC_STATS(_thd_id,thd_prof_thd_rfin1,get_sys_clock() - thd_prof_thd_rfin_start);
        thd_prof_thd_rfin_start = get_sys_clock();
        starttime = thd_prof_thd_rfin_start;
        // Send ack back to home node
        //  m_query is used by send thread; do not free
        //  m_query will be freed by send thread
        if(m_query->rc == Abort || !m_query->ro || CC_ALG == OCC)
          msg_queue.enqueue(m_query,RACK,m_query->return_id);
#endif

        INC_STATS(_thd_id,thd_prof_thd_rfin2,get_sys_clock() - thd_prof_thd_rfin_start);
    INC_STATS(_thd_id,prof_time_twopc,get_sys_clock() - starttime);
        // TODO: change check from null m_query to qry type?
        //qry_pool.put(m_query);
				m_query = NULL;
        return RCOK;
}

RC Thread::process_rack(BaseQuery *& m_query,TxnManager *& m_txn) {

        uint64_t thd_prof_start = get_sys_clock();
        uint64_t thd_prof_thd_rack_start = get_sys_clock();
#if CC_ALG == VLL
        uint64_t thd_prof_thd_rack_vll = get_sys_clock();
#endif
				DEBUG("%ld Received RACK %ld\n",GET_PART_ID_FROM_IDX(_thd_id),m_query->txn_id);
				INC_STATS(0,rack,1);
				assert(IS_LOCAL(m_query->txn_id));
				assert(!(CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC) || GET_PART_ID_IDX(m_query->home_part) == _thd_id);

        RC rc = RCOK;

        // Retrieve this transaction
        BaseQuery * tmp_query;
				txn_table.get_txn(g_node_id, m_query->txn_id,0,m_txn,tmp_query);
				assert(m_txn != NULL);
        m_txn->register_thd(this);

        // Merge queries
        assert(ISCLIENTN(tmp_query->client_id));
        tmp_query = tmp_query->merge(m_query);
        if(m_query != tmp_query) {
          //YCSBQuery_FREE(m_query)
          qry_pool.put(m_query);
        }
        m_query = tmp_query;
        m_txn->set_query(m_query);
        assert(ISCLIENTN(m_query->client_id));

        INC_STATS(_thd_id,thd_prof_thd_rack0,get_sys_clock() - thd_prof_thd_rack_start);
        thd_prof_thd_rack_start = get_sys_clock();
        // Check if all responses have been received
        // returns the current response count for this transaction
				int rsp_cnt = m_txn->decr_rsp(1);
        assert(rsp_cnt >=0);
        if(rsp_cnt > 0) {
          //qry_pool.put(m_query);
          m_query = NULL;
          return RCOK;
        }

        INC_STATS(_thd_id,thd_prof_thd_rack1,get_sys_clock() - thd_prof_thd_rack_start);
        thd_prof_thd_rack_start = get_sys_clock();
				if(rsp_cnt == 0) {
					// Done waiting 
					INC_STATS(get_thd_id(),time_wait_rem,get_sys_clock() - m_txn->wait_starttime);
          // Reset status
					m_txn->rc = RCOK;

					// After RINIT
					switch(m_txn->state) {
						case INIT:
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
              
#if CC_ALG == HSTORE_SPEC
							txn_table.commit_spec_ex(RCOK,_thd_id);
							spec_man.clear();
#endif
							uint64_t part_arr_s[1];
							part_arr_s[0] = m_query->part_to_access[0];
							rc = part_lock_man.rem_lock(part_arr_s, 1, m_txn);
							m_query->update_rc(rc);
        INC_STATS(_thd_id,thd_prof_thd_rack2a,get_sys_clock() - thd_prof_thd_rack_start);
        thd_prof_thd_rack_start = get_sys_clock();
							if(rc == WAIT) {
								m_txn->rc = rc;
                //qry_pool.put(m_query);
					      m_query = NULL;
                return WAIT;
							}

							if(m_txn->ready_part > 0) {
                //qry_pool.put(m_query);
					      m_query = NULL;
                return WAIT;
              }
#endif
#if CC_ALG == VLL
              // This may return an Abort

              if(m_query->rc == Abort) {
                    m_txn->state = FIN;
                    // send rfin messages w/ abort
                    m_txn->finish(m_query,true);
                    //qry_pool.put(m_query);
                    m_query = NULL;
                    return Abort;
              }

              thd_prof_thd_rack_vll = get_sys_clock();
              rc = vll_man.beginTxn(m_txn,m_query);
              INC_STATS(_thd_id,txn_time_begintxn,get_sys_clock() - thd_prof_thd_rack_vll);
              if(rc == WAIT)
                return WAIT;
              if(rc == Abort) {
                    m_txn->state = FIN;
                    // send rfin messages w/ abort
                    m_txn->finish(m_query,true);
                    //qry_pool.put(m_query);
                    m_query = NULL;
                    return Abort;
              }
#endif
							m_txn->state = EXEC; 
							txn_table.restart_txn(m_txn->get_txn_id(),0);
              //qry_pool.put(m_query);
					    m_query = NULL;
              INC_STATS(_thd_id,prof_time_twopc,get_sys_clock() - thd_prof_start);
              break;
            case PREP:
							// Validate
							m_txn->spec = true;
              INC_STATS(_thd_id,prof_time_twopc,get_sys_clock() - thd_prof_start);
							rc  = m_txn->validate();
              thd_prof_start = get_sys_clock();
							m_txn->spec = false;
							if(rc == Abort)
								m_query->rc = rc;
#if CC_ALG == HSTORE_SPEC
							if(m_query->rc != Abort) {
								// allow speculative execution to start
								txn_table.start_spec_ex(_thd_id);
							}
#endif

							m_txn->state = FIN;
							// After RPREPARE, send rfin messages
							m_txn->finish(m_query,true);
              // Note: can't touch m_txn after this, since all other
              //  RACKs may have been received and FIN processed before this point
              //qry_pool.put(m_query);
					    m_query = NULL;
              INC_STATS(_thd_id,prof_time_twopc,get_sys_clock() - thd_prof_start);
							break;
						case FIN:
							// After RFIN
							m_txn->state = DONE;
#if CC_ALG == HSTORE_SPEC
							txn_table.commit_spec_ex(m_query->rc,_thd_id);
							spec_man.clear();
#endif
              INC_STATS(_thd_id,prof_time_twopc,get_sys_clock() - thd_prof_start);
							uint64_t part_arr[1];
							part_arr[0] = m_query->part_to_access[0];
							rc = m_txn->finish(m_query->rc,part_arr,1);
							break;
						default:
							assert(false);
					}
        }
        else {
              INC_STATS(_thd_id,prof_time_twopc,get_sys_clock() - thd_prof_start);
        }

        INC_STATS(_thd_id,thd_prof_thd_rack2,get_sys_clock() - thd_prof_thd_rack_start);
        return rc;
}

RC Thread::process_rqry_rsp(uint64_t tid, BaseQuery *& m_query,TxnManager *& m_txn) {
  uint64_t thd_prof_start = get_sys_clock();
				DEBUG("%ld Received RQRY_RSP %ld\n",GET_PART_ID_FROM_IDX(_thd_id),m_query->txn_id);
				INC_STATS(0,rqry_rsp,1);
				assert(IS_LOCAL(m_query->txn_id));

        // Retrieve transaction
        BaseQuery * tmp_query;
				txn_table.get_txn(g_node_id, m_query->txn_id,0,m_txn,tmp_query);
				assert(m_txn != NULL);
        m_txn->register_thd(this);

        // Collect Stats
				INC_STATS(get_thd_id(),time_wait_rem,get_sys_clock() - m_txn->wait_starttime);
        if(m_txn->txn_stat_starttime > 0) {
          m_txn->txn_time_net += get_sys_clock() - m_txn->txn_stat_starttime;
          m_txn->txn_stat_starttime = 0;
        }

        // Merge queries
        tmp_query = tmp_query->merge(m_query);
        if(m_query != tmp_query) {
          //YCSBQuery_FREE(m_query)
          qry_pool.put(m_query);
        }
        m_query = tmp_query;
        m_txn->set_query(m_query);

    INC_STATS(_thd_id,thd_prof_thd_rqry_rsp0,get_sys_clock() - thd_prof_start);
    thd_prof_start = get_sys_clock();
				// Execute from original txn; Note: txn may finish
    if(m_txn->get_rsp_cnt() != 0 || m_txn->get_txn_id() != m_query->txn_id) {
        printf("FAIL1: %ld %lx %lx\n",tid,(uint64_t)m_txn,(uint64_t)m_query);
        fflush(stdout);
      }
        assert(m_txn->get_txn_id() == m_query->txn_id);
        assert(m_query->txn_id == tid);
				assert(m_txn->get_rsp_cnt() == 0);
				RC rc = m_txn->run_txn(m_query);
    INC_STATS(_thd_id,thd_prof_thd_rqry_rsp1,get_sys_clock() - thd_prof_start);
        return rc;

}

RC Thread::process_rqry(BaseQuery *& m_query,TxnManager *& m_txn) {
  uint64_t thd_prof_start = get_sys_clock();
				DEBUG("%ld Received RQRY %ld\n",GET_PART_ID_FROM_IDX(_thd_id),m_query->txn_id);
				INC_STATS(0,rqry,1);
#if MODE == QRY_ONLY_MODE
        uint64_t max_access = m_query->max_access;
        assert(max_access > 0);
#endif
				assert(CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC || IS_REMOTE(m_query->txn_id));
        assert(CC_ALG != VLL || m_query->rc != Abort);
        RC rc = RCOK;

				// Theoretically, we could send multiple queries to one node,
				//    so m_txn could already be in txn_table
        BaseQuery * tmp_query;
				txn_table.get_txn(m_query->return_id, m_query->txn_id,0,m_txn,tmp_query);

        // If transaction not in pool, create one
				if (m_txn == NULL) {
					//rc = _wl->get_txn_man(m_txn, this);
          txn_pool.get(m_txn);
					m_txn->set_txn_id(m_query->txn_id);
					m_txn->set_ts(m_query->ts);
					m_txn->set_pid(m_query->pid);
					m_txn->state = INIT;
					txn_table.add_txn(m_query->return_id,m_txn,m_query);
          tmp_query = m_query;
          INC_STATS(_thd_id,txn_rem_cnt,1);
				}
				assert(m_txn != NULL);
        m_txn->register_thd(this);
				m_txn->set_ts(m_query->ts);
#if CC_ALG == OCC
				m_txn->set_start_ts(m_query->start_ts);
#endif

        // merge queries
        tmp_query = tmp_query->merge(m_query);
          if(m_query != tmp_query) {
            //YCSBQuery_FREE(m_query)
            qry_pool.put(m_query);
          }
        m_query = tmp_query;
        m_txn->set_query(m_query);

				m_txn->state = EXEC;

    INC_STATS(_thd_id,thd_prof_thd_rqry0,get_sys_clock() - thd_prof_start);
    thd_prof_start = get_sys_clock();

    // Execute transaction
    rc = m_txn->run_txn(m_query);

    INC_STATS(_thd_id,thd_prof_thd_rqry1,get_sys_clock() - thd_prof_start);
    thd_prof_start = get_sys_clock();
				assert(rc != WAIT_REM);

				uint64_t timespan = get_sys_clock() - txn_starttime;
				INC_STATS(get_thd_id(),time_rqry,timespan);

        // Send response
				if(rc != WAIT) {
#if MODE==QRY_ONLY_MODE
          // Release locks
#if CC_ALG == VLL
          if((uint64_t)m_txn->vll_row_cnt2 == max_access) {
#else
          if((uint64_t)m_txn->row_cnt == max_access) {
#endif
            m_query->max_done = true;
            m_txn->state = DONE;
            m_txn->rem_fin_txn(m_query);
          } else {
            m_query->max_done = false;
          }
#endif
          msg_queue.enqueue(m_query,RQRY_RSP,m_query->return_id);
        }
        //qry_pool.put(m_query);
        m_query = NULL;
    INC_STATS(_thd_id,thd_prof_thd_rqry2,get_sys_clock() - thd_prof_start);
        return rc;

}

RC Thread::process_rprepare(BaseQuery *& m_query,TxnManager *& m_txn) {
    uint64_t starttime = get_sys_clock();
    uint64_t thd_prof_start = get_sys_clock();
        DEBUG("%ld Received RPREPARE %ld\n",GET_PART_ID_FROM_IDX(_thd_id),m_query->txn_id);
				INC_STATS(0,rprep,1);
				assert(CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC || IS_REMOTE(m_query->txn_id));
        assert(MODE == NORMAL_MODE || MODE == NOCC_MODE);

        /*
#if MODE==TWOPC
        msg_queue.enqueue(m_query,RACK,m_query->return_id);
        m_query = NULL;
        return RCOK;
#endif
*/
        // Retrieve transaction
        BaseQuery * tmp_query;
				txn_table.get_txn(m_query->return_id, m_query->txn_id,0,m_txn,tmp_query);

				bool validate = true;
        RC rc = RCOK;

#if CC_ALG != HSTORE && CC_ALG != HSTORE_SPEC && CC_ALG != VLL
				if (m_txn == NULL) {
					validate = false;
				}
#else
				assert(m_txn != NULL);
#endif
        if(m_txn) {
          m_txn->register_thd(this);

          // Merge queries
          tmp_query = tmp_query->merge(m_query);
          if(m_query != tmp_query) {
            //YCSBQuery_FREE(m_query)
            qry_pool.put(m_query);
          }
          m_query = tmp_query;
          m_txn->set_query(m_query);
          assert(!(CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC) || m_query->home_part != GET_PART_ID_FROM_IDX(_thd_id));
        }

    INC_STATS(_thd_id,thd_prof_thd_rprep0,get_sys_clock() - thd_prof_start);
    thd_prof_start = get_sys_clock();

        INC_STATS(_thd_id,prof_time_twopc,get_sys_clock() - starttime);
			  // Validate transaction
				if (validate && m_query->rc == RCOK) {
					m_txn->state = PREP;
					rc  = m_txn->validate();
				} else {
          assert(m_query->rc == Abort);
          rc = m_query->rc;
        }
    INC_STATS(_thd_id,thd_prof_thd_rprep1,get_sys_clock() - thd_prof_start);
    thd_prof_start = get_sys_clock();
    starttime = thd_prof_start;
				// Send ACK w/ commit/abort
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
        if(GET_NODE_ID(m_query->active_part) != GET_NODE_ID(m_query->home_part)) {
          if(validate)
					  m_query->rc = rc;
          msg_queue.enqueue(m_query,RACK,m_query->return_id);
        } else {
          m_query->local_rack_query();
        }
#else
        if(validate)
				  m_query->rc = rc;
        // Send back ack
        msg_queue.enqueue(m_query,RACK,m_query->return_id);
#endif

    INC_STATS(_thd_id,thd_prof_thd_rprep2,get_sys_clock() - thd_prof_start);
        //qry_pool.put(m_query);
        m_query = NULL;
    INC_STATS(_thd_id,prof_time_twopc,get_sys_clock() - starttime);
        return rc;
}

RC Thread::process_rinit(BaseQuery *& m_query,TxnManager *& m_txn) {
    uint64_t thd_prof_start = get_sys_clock();
        DEBUG("%ld Received RINIT %ld\n",GET_PART_ID_FROM_IDX(_thd_id),m_query->txn_id)
				INC_STATS(0,rinit,1);
        assert(CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC || CC_ALG == VLL);
				assert( ((CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC) 
              && m_query->home_part != GET_PART_ID_FROM_IDX(_thd_id)) 
              || IS_REMOTE(m_query->txn_id));
        RC rc = RCOK;

				// Set up txn_table
        if((CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC) && IS_LOCAL(m_query->txn_id)) {
          BaseQuery * tmp_query;
          txn_table.get_txn(m_query->return_id, m_query->txn_id,0,m_txn,tmp_query);
          tmp_query->active_part = m_query->active_part;
          //m_query = tmp_query;
        } else {
				  //rc = _wl->get_txn_man(m_txn, this);
          txn_pool.get(m_txn);
          INC_STATS(_thd_id,txn_rem_cnt,1);
        }
				//assert(rc == RCOK);
        m_txn->register_thd(this);
				m_txn->set_txn_id(m_query->txn_id);
				m_txn->set_ts(m_query->ts);
				m_txn->set_pid(m_query->pid);
				m_txn->state = INIT;

#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
        m_txn->home_part = m_query->home_part;
        m_txn->active_part = m_query->active_part;
#endif
    INC_STATS(_thd_id,thd_prof_thd_rqry0,get_sys_clock() - thd_prof_start);
    thd_prof_start = get_sys_clock();

        if( !IS_LOCAL(m_query->txn_id)) {
				  txn_table.add_txn(m_query->return_id,m_txn,m_query);
        }

    INC_STATS(_thd_id,thd_prof_thd_rqry1,get_sys_clock() - thd_prof_start);
    thd_prof_start = get_sys_clock();
#if CC_ALG == VLL
        uint64_t debug3 = get_sys_clock();
        rc = vll_man.beginTxn(m_txn,m_query);
        INC_STATS(_thd_id,txn_time_begintxn,get_sys_clock() - debug3);
        if(rc == WAIT)
          return rc;
				// Send back ACK
        msg_queue.enqueue(m_query,RACK,m_query->return_id);
#endif
				// HStore: lock partitions at this node
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
				part_lock_man.rem_lock(m_query->parts, m_query->part_cnt, m_txn);
#endif
    INC_STATS(_thd_id,thd_prof_thd_rqry2,get_sys_clock() - thd_prof_start);
        //qry_pool.put(m_query);
        m_query = NULL;
        return rc;
}

RC Thread::process_rpass(BaseQuery *& m_query,TxnManager *& m_txn) {
        BaseQuery * tmp_query;
				txn_table.get_txn(m_query->return_id, m_query->txn_id,0,m_txn,tmp_query);
				assert(m_txn != NULL);
        m_txn->register_thd(this);
        return m_txn->rc;
}

RC Thread::process_rtxn(BaseQuery *& m_query,TxnManager *& m_txn) {
    uint64_t thd_prof_start = get_sys_clock();
    uint64_t thd_prof_start1 = thd_prof_start;
				INC_STATS(0,rtxn,1);
				assert(m_query->txn_id == UINT64_MAX || IS_LOCAL(m_query->txn_id) );
        RC rc = RCOK;
        bool restart = true;

				if(m_query->txn_id == UINT64_MAX) {
          restart = false;
          // This is a new transaction
          txn_pool.get(m_txn);
          INC_STATS(_thd_id,debug1, get_sys_clock()-thd_prof_start1);
          thd_prof_start1 = get_sys_clock();

					m_txn->abort_cnt = 0;
					if (CC_ALG == WAIT_DIE || CC_ALG == VLL) {
						m_txn->set_ts(get_next_ts());
						m_query->ts = m_txn->get_ts();
					}
					if (CC_ALG == MVCC) {
						m_query->thd_id = _thd_id;
					}

#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
          m_txn->home_part = m_query->home_part;
          m_txn->active_part = m_query->active_part;
#endif
					m_txn->set_pid(m_query->pid);

					// Only set new txn_id when txn first starts
					m_txn->set_txn_id( ( get_node_id() + get_thd_id() * g_node_cnt) 
							+ (g_thread_cnt * g_node_cnt * _thd_txn_id));
					_thd_txn_id ++;
					m_query->set_txn_id(m_txn->get_txn_id());
          INC_STATS(_thd_id,debug2, get_sys_clock()-thd_prof_start1);
          thd_prof_start1 = get_sys_clock();
          work_queue.update_hash(get_thd_id(),m_txn->get_txn_id());

					// Put txn in txn_table
          assert(ISCLIENTN(m_query->client_id));
					txn_table.add_txn(g_node_id,m_txn,m_query);
          INC_STATS(_thd_id,debug3, get_sys_clock()-thd_prof_start1);
          thd_prof_start1 = get_sys_clock();

          m_query->penalty = 0;
          m_query->abort_restart = false;

					m_txn->starttime = get_sys_clock();
          m_txn->txn_time_misc += m_query->time_q_work;
          m_txn->txn_time_misc += m_query->time_copy;
          m_txn->register_thd(this);
    INC_STATS(_thd_id,thd_prof_thd_rtxn1a,get_sys_clock() - thd_prof_start);
				}
				else {
					// Re-executing transaction
          BaseQuery * tmp_query;
					txn_table.get_txn(g_node_id,m_query->txn_id,0,m_txn,tmp_query);
					assert(m_txn != NULL);
          m_txn->register_thd(this);
    INC_STATS(_thd_id,thd_prof_thd_rtxn1b,get_sys_clock() - thd_prof_start);
				}
    thd_prof_start = get_sys_clock();

				if(m_txn->rc != WAIT && m_txn->state == START) {

          // Get new timestamps
					if ((CC_ALG == HSTORE && !HSTORE_LOCAL_TS)
							|| (CC_ALG == HSTORE_SPEC && !HSTORE_LOCAL_TS)
							|| CC_ALG == MVCC 
              || CC_ALG == VLL
							|| CC_ALG == TIMESTAMP) { 
						m_txn->set_ts(get_next_ts());
						m_query->ts = m_txn->get_ts();
					}

#if CC_ALG == OCC
					m_txn->start_ts = get_next_ts(); 
					m_query->start_ts = m_txn->get_start_ts();
#endif
    if(restart) {
					DEBUG("RESTART %ld %f %lu\n",m_txn->get_txn_id(),(double)(m_txn->starttime - run_starttime) / BILLION,m_txn->get_ts());
    } else {
					DEBUG("START %ld %f %lu\n",m_txn->get_txn_id(),(double)(m_txn->starttime - run_starttime) / BILLION,m_txn->get_ts());
    }

    INC_STATS(_thd_id,thd_prof_thd_rtxn2,get_sys_clock() - thd_prof_start);
    thd_prof_start = get_sys_clock();
		uint64_t ttime = get_sys_clock();

          rc = init_phase(m_query,m_txn);

    INC_STATS(_thd_id,thd_prof_thd_rtxn3,get_sys_clock() - thd_prof_start);
    thd_prof_start = get_sys_clock();
					INC_STATS(get_thd_id(),time_msg_sent,get_sys_clock() - ttime);
				} //if(m_txn->rc != WAIT) 

				if(m_query->rc == Abort && rc == WAIT) {
          //qry_pool.put(m_query);
					m_query = NULL;
          return rc;
				}

        // Execute transaction
				if(rc == RCOK) {
					assert(m_txn->get_rsp_cnt() == 0);
					m_txn->state = EXEC;
					rc = m_txn->run_txn(m_query);
        }
    INC_STATS(_thd_id,thd_prof_thd_rtxn4,get_sys_clock() - thd_prof_start);
          return rc;
}

RC Thread::init_phase(BaseQuery * m_query, TxnManager * m_txn) {
  RC rc = RCOK;
#if CC_ALG != HSTORE && CC_ALG != HSTORE_SPEC && CC_ALG != VLL
  /*
#if MODE==TWOPC
          // Only 2PC, touch all partitions
          for(uint64_t i = 0; i < m_query->part_num; i++) {
            m_query->part_touched[m_query->part_touched_cnt++] = m_query->part_to_access[i];
          }
#else
*/
          // Touch first partition
          m_query->part_touched[m_query->part_touched_cnt++] = m_query->part_to_access[0];
//#endif
#endif
#if (CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC || CC_ALG == VLL)
          /*
#if MODE != NORMAL_MODE
					for(uint64_t i = 0; i < m_query->part_num; i++) {
            m_query->part_touched[m_query->part_touched_cnt++] = m_query->part_to_access[i];
          }
#else
*/
					m_txn->state = INIT;
					for(uint64_t i = 0; i < m_query->part_num; i++) {
						uint64_t part_id = m_query->part_to_access[i];
						// Check for duplicate partitions so we don't send init twice
						uint64_t j;
						bool sent = false;
						for(j = 0; j < i; j++) {
							if(part_id == m_query->part_to_access[j]) {
								sent = true;
							}
						}
						if(sent)
							continue;
						//if(GET_NODE_ID(part_id) != g_node_id) {
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
						if(part_id != m_query->home_part) {
							m_txn->incr_rsp(1);
						}
#else
						if(GET_NODE_ID(part_id) != g_node_id) {
							m_txn->incr_rsp(1);
						}
#endif
					}

#if CC_ALG == HSTORE_SPEC
          if(m_query->part_num > 1) {
						txn_table.start_spec_ex(_thd_id);
          }
#endif

          if(CC_ALG != HSTORE_SPEC || !m_txn->spec) {

					// Send RINIT message to all involved nodes
					for(uint64_t i = 0; i < m_query->part_num; i++) {
						uint64_t part_id = m_query->part_to_access[i];
						// Check for duplicate partitions so we don't send init twice
						uint64_t j;
						bool sent = false;
						for(j = 0; j < i; j++) {
							if(part_id == m_query->part_to_access[j]) {
								sent = true;
							}
						}
						if(sent)
							continue;

#if CC_ALG == VLL
              if(GET_NODE_ID(part_id) != g_node_id) {
                //m_txn->incr_rsp(1);
                rc = WAIT;
                m_txn->rc = rc;
                m_query->dest_part = part_id;
                msg_queue.enqueue(m_query,RINIT,GET_NODE_ID(part_id));
                m_txn->wait_starttime = get_sys_clock();
                m_query->part_touched[m_query->part_touched_cnt++] = part_id;
              } else {
							// local init: MPQ Moved to after RINITs return
                if(m_query->part_num == 1) {
                  uint64_t debug3 = get_sys_clock();
                  rc = vll_man.beginTxn(m_txn,m_query);
                  INC_STATS(_thd_id,txn_time_begintxn,get_sys_clock() - debug3);
                  if(rc == WAIT) {
                    break;
                    //m_txn->rc = rc;
                  }
                }
              }
#elif CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
            assert(m_query->active_part == m_query->home_part);
						if(part_id != m_query->home_part) {
              if(GET_NODE_ID(part_id) != g_node_id) {
                //m_txn->incr_rsp(1);
                rc = WAIT;
                m_txn->rc = rc;
                m_query->dest_part = part_id;
                msg_queue.enqueue(m_query,RINIT,GET_NODE_ID(part_id));
                m_txn->wait_starttime = get_sys_clock();
                m_query->part_touched[m_query->part_touched_cnt++] = part_id;
              } else {
                assert(CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC);
                rc = WAIT;
                m_txn->rc = rc;
                m_query->part_touched[m_query->part_touched_cnt++] = part_id;
                // initialize new query and place on work queue
                m_query->local_rinit_query(part_id);
               

              }
						} else {
							// local init: MPQ Moved to after RINITs return
              if(m_query->part_num == 1) {
							uint64_t part_arr[1];
							part_arr[0] = part_id;
							rc2 = part_lock_man.rem_lock(part_arr, 1, m_txn);
							m_query->update_rc(rc2);
							if(rc2 == WAIT) {
								rc = WAIT;
								m_txn->rc = rc;
							}
              }
						}
#endif //CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
					} // for(uint64_t i = 0; i < m_query->part_num; i++) 
        }

//#endif
#endif // MODE<QRY && (CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC || CC_ALG == VLL)
          return rc;
}


RC Thread::process_log_msg(BaseQuery *& m_query,TxnManager *& m_txn) {
  return RCOK;
}

RC Thread::process_log_msg_rsp(BaseQuery *& m_query,TxnManager *& m_txn) {
  return RCOK;
}

RC Thread::run_calvin_lock() {
	printf("RunCalvinLock %ld:%ld\n",_node_id, _thd_id);
  fflush(stdout);
	if (warmup_finish) {
		mem_allocator.register_thread(_thd_id);
	}
	stats.init(get_thd_id());
	pthread_barrier_wait( &warmup_bar );
	BaseQuery * m_query = NULL;

	pthread_barrier_wait( &warmup_bar );
	printf("RunCalvinLock %ld:%ld\n",_node_id, _thd_id);
  fflush(stdout);

	myrand rdm;
	rdm.init(get_thd_id());
	RC rc = RCOK;
	TxnManager * m_txn;
  uint64_t thd_prof_start;
	uint64_t run_starttime = get_sys_clock();
  BaseQuery * tmp_query;

	while(true) {
    m_query = NULL;
    m_txn = NULL;

    bool got_qry = false;
		while(!(got_qry = work_queue.dequeue(_thd_id, m_query))
                && !_wl->sim_done && !_wl->sim_timeout
                && (get_sys_clock() - run_starttime < g_done_timer)) {
    }

		// End conditions
    if((get_sys_clock() - run_starttime >= g_done_timer)
        ||(_wl->sim_done && _wl->sim_timeout)) { 
      printf("FINISH %ld:%ld\n",_node_id,_thd_id);
      fflush(stdout);
			return FINISH;
		}

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
}

RC Thread::run_calvin() {
	printf("RunCalvin %ld:%ld\n",_node_id, _thd_id);
  fflush(stdout);
	if (warmup_finish) {
		mem_allocator.register_thread(_thd_id);
	}
	stats.init(get_thd_id());
	pthread_barrier_wait( &warmup_bar );
	BaseQuery * m_query = NULL;

	if( _thd_id == 0) {
#if WORKLOAD == YCSB
    m_query = new YCSBQuery;
#elif WORKLOAD == TPCC
    m_query = new TPCCQuery;
#endif
		uint64_t total_nodes = g_node_cnt + g_client_node_cnt;
    /*
#if CC_ALG == CALVIN
		total_nodes++;
#endif
*/
		for(uint64_t i = 0; i < total_nodes; i++) {
			if(i != g_node_id) {
        msg_queue.enqueue(NULL,INIT_DONE,i);
			}
		}
  }
	pthread_barrier_wait( &warmup_bar );
	printf("RunCalvin %ld:%ld\n",_node_id, _thd_id);
  fflush(stdout);

	myrand rdm;
	rdm.init(get_thd_id());
	RC rc = RCOK;
	TxnManager * m_txn;

  BaseQuery * tmp_query;
	uint64_t starttime;
	uint64_t stoptime = 0;
	uint64_t timespan;

	uint64_t run_starttime = get_sys_clock();
	uint64_t prog_time = run_starttime;
  uint64_t thd_prof_start;

	while(true) {
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
    bool got_qry = false;
		while(!(got_qry = work_queue.dequeue(_thd_id, m_query))
                && !_wl->sim_done && !_wl->sim_timeout) {
      if (warmup_finish && 
          ((get_sys_clock() - run_starttime >= g_done_timer) )) {
        break;
      }
    }

    INC_STATS(_thd_id,thd_prof_thd1c,get_sys_clock() - thd_prof_start);
    thd_prof_start = get_sys_clock();
		// End conditions
    if((get_sys_clock() - run_starttime >= g_done_timer)
        ||(_wl->sim_done && _wl->sim_timeout)) { 
      if( !ATOM_CAS(_wl->sim_done, false, true) ) {
        assert( _wl->sim_done);
      } else {
        printf("_wl->sim_done=%d\n",_wl->sim_done);
        fflush(stdout);
      }
			uint64_t currtime = get_sys_clock();
      if(stoptime == 0)
        stoptime = currtime;
			if(currtime - stoptime > MSG_TIMEOUT) {
				SET_STATS(get_thd_id(), tot_run_time, currtime - run_starttime - MSG_TIMEOUT); 
			}
			else {
				SET_STATS(get_thd_id(), tot_run_time, stoptime - run_starttime); 
			}
#if DEBUG_DISTR
        txn_table.dump();
#endif

      printf("FINISH %ld:%ld\n",_node_id,_thd_id);
      fflush(stdout);
			return FINISH;
		}

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
        /*
        if(m_txn != NULL)
          printf("rtxn %ld %d %d\n",m_query->txn_id,m_txn->lock_ready,m_txn->lock_ready_cnt);
        else
          printf("rtxn %ld -- -- \n",m_query->txn_id);
        fflush(stdout);
        */
        assert(m_txn != NULL);
        tmp_query = tmp_query->merge(m_query);
        if(m_query != tmp_query) {
          qry_pool.put(m_query);
        }
        m_query = tmp_query;
        assert(m_query->batch_id == _wl->curr_epoch || m_query->batch_id == _wl->curr_epoch - 1);
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
      //assert(_wl->epoch_txn_cnt > 1 || txn_table.get_cnt() == 0);
      assert(m_query->batch_id == _wl->curr_epoch || m_query->batch_id == _wl->curr_epoch - 1);
      if(m_query->return_id == g_node_id) {
        DEBUG_R("RACK (%ld,%ld)  0x%lx\n",m_query->txn_id,m_query->batch_id,(uint64_t)m_query);
        //printf("%ld RACK (%ld,%ld)  0x%lx\n",_thd_id,m_query->txn_id,m_query->batch_id,(uint64_t)m_query);
        m_query->rtype = RACK;
        work_queue.enqueue(_thd_id,m_query,false);
      } else {
        msg_queue.enqueue(m_query,RACK,m_query->return_id);
      }
      ATOM_SUB(_wl->epoch_txn_cnt,1);
    }
    work_queue.done(_thd_id,tid);

		timespan = get_sys_clock() - starttime;
		INC_STATS(get_thd_id(),time_work,timespan);

  }
}

RC Thread::run_calvin_seq() {
	printf("RunCalvinSequencer %ld:%ld\n",_node_id, _thd_id);
  fflush(stdout);
	if (warmup_finish) {
		mem_allocator.register_thread(_thd_id);
	}
	stats.init(get_thd_id());
	pthread_barrier_wait( &warmup_bar );
	BaseQuery * m_query = NULL;
  _wl->epoch = 0;

	pthread_barrier_wait( &warmup_bar );
	printf("RunCalvinSequencer %ld:%ld\n",_node_id, _thd_id);
  fflush(stdout);

	myrand rdm;
	rdm.init(get_thd_id());
  //uint64_t thd_prof_start;
	uint64_t run_starttime = get_sys_clock();
	uint64_t last_batchtime = run_starttime;

	while(true) {
    m_query = NULL;

    bool got_qry = false;
		while(!(got_qry = work_queue.dequeue(_thd_id, m_query))
                && (get_sys_clock() - last_batchtime < g_batch_time_limit) 
                && !_wl->sim_done && !_wl->sim_timeout
                && (get_sys_clock() - run_starttime < g_done_timer)) {
    }

		// End conditions
    if((get_sys_clock() - run_starttime >= g_done_timer)
        ||(_wl->sim_done && _wl->sim_timeout)) { 
      printf("FINISH %ld:%ld\n",_node_id,_thd_id);
      fflush(stdout);
			return FINISH;
		}


    if(get_sys_clock() - last_batchtime >= g_batch_time_limit) {
      ATOM_ADD(_wl->epoch,1);
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
}

RC Thread::run_logger() {
	printf("Run_logger %ld:%ld\n",_node_id, _thd_id);
	pthread_barrier_wait( &warmup_bar );
	pthread_barrier_wait( &warmup_bar );
	printf("Run_logger %ld:%ld\n",_node_id, _thd_id);
	while (!_wl->sim_done) {
    logger.processRecord();
    logger.flushBufferCheck();
  }
  return FINISH;
 
}


