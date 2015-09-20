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
#include "test.h"
#include "transport.h"
#include "remote_query.h"
#include "math.h"
#include "specex.h"
#include "helper.h"
#include "msg_thread.h"
#include "msg_queue.h"

void thread_t::init(uint64_t thd_id, uint64_t node_id, workload * workload) {
	_thd_id = thd_id;
	_node_id = node_id;
	_wl = workload;
}

uint64_t thread_t::get_thd_id() { return _thd_id; }
uint64_t thread_t::get_node_id() { return _node_id; }
uint64_t thread_t::get_host_cid() {	return _host_cid; }
void thread_t::set_host_cid(uint64_t cid) { _host_cid = cid; }
uint64_t thread_t::get_cur_cid() { return _cur_cid; }
void thread_t::set_cur_cid(uint64_t cid) {_cur_cid = cid; }

RC thread_t::run_remote() {
	printf("Run_remote %ld:%ld\n",_node_id, _thd_id);
  fflush(stdout);
	if (warmup_finish) {
		mem_allocator.register_thread(_thd_id);
	}

	pthread_barrier_wait( &warmup_bar );
	stats.init(get_thd_id());

  while(!_wl->sim_init_done) {
    tport_man.recv_msg();
  }
	pthread_barrier_wait( &warmup_bar );
	printf("Run_remote %ld:%ld\n",_node_id, _thd_id);
  fflush(stdout);

	myrand rdm;
	rdm.init(get_thd_id());
	RC rc = RCOK;
	assert (rc == RCOK);

	uint64_t run_starttime = get_sys_clock();
	ts_t rq_time = get_sys_clock();

	while (true) {
    if(tport_man.recv_msg()) {
      rq_time = get_sys_clock();
    }
		ts_t tend = get_sys_clock();
		if (warmup_finish && ((tend - run_starttime >= DONE_TIMER) || (_wl->sim_done && (  ((tend - rq_time) > MSG_TIMEOUT))))) {
			if( !ATOM_CAS(_wl->sim_timeout, false, true) )
				assert( _wl->sim_timeout);
		}

		if ((_wl->sim_done && _wl->sim_timeout) || (tend - run_starttime >= DONE_TIMER)) {

			return FINISH;
		}

	}

}

RC thread_t::run_send() {
	printf("Run_send %ld:%ld\n",_node_id, _thd_id);
  fflush(stdout);
	if (warmup_finish) {
		mem_allocator.register_thread(_thd_id);
	}
	pthread_barrier_wait( &warmup_bar );
	stats.init(get_thd_id());

  MessageThread messager;
  messager.init(_thd_id);
	while (!_wl->sim_init_done) {
    messager.run();
  }

	pthread_barrier_wait( &warmup_bar );
	printf("Run_send %ld:%ld\n",_node_id, _thd_id);
  fflush(stdout);

	while (true) {
    messager.run();
		if (_wl->sim_done && _wl->sim_timeout) {
			return FINISH;
    }
  }

}

RC thread_t::run() {
	printf("Run %ld:%ld\n",_node_id, _thd_id);
  fflush(stdout);
	if (warmup_finish) {
		mem_allocator.register_thread(_thd_id);
	}
	pthread_barrier_wait( &warmup_bar );
	stats.init(get_thd_id());
	base_query * m_query = NULL;

	if( _thd_id == 0) {
#if WORKLOAD == YCSB
    m_query = new ycsb_query;
#elif WORKLOAD == TPCC
    m_query = new tpcc_query;
#endif
		uint64_t total_nodes = g_node_cnt + g_client_node_cnt;
#if CC_ALG == CALVIN
		total_nodes++;
#endif
		for(uint64_t i = 0; i < total_nodes; i++) {
			if(i != g_node_id) {
        msg_queue.enqueue(NULL,INIT_DONE,i);
			}
		}
    while(!_wl->sim_init_done) {
      while((m_query = work_queue.get_next_query(get_thd_id())) == NULL) { }
      if(m_query->rtype == INIT_DONE) {
        ATOM_SUB(_wl->rsp_cnt,1);
        DEBUG("Processed INIT_DONE from %ld -- %ld\n",m_query->return_id,_wl->rsp_cnt);
        DEBUG_FLUSH();
        if(_wl->rsp_cnt ==0) {
			    if( !ATOM_CAS(_wl->sim_init_done, false, true) )
				    assert( _wl->sim_init_done);
        }
      } else {
        // Put other queries aside until all nodes are ready
        work_queue.add_query(_thd_id,m_query);
      }
	  }
  }
	pthread_barrier_wait( &warmup_bar );
	printf("Run %ld:%ld\n",_node_id, _thd_id);
  fflush(stdout);

	myrand rdm;
	rdm.init(get_thd_id());
	RC rc = RCOK;
#if CC_ALG == HSTORE|| CC_ALG == HSTORE_SPEC
	RC rc2 = RCOK;
#endif
	txn_man * m_txn;
	rc = _wl->get_txn_man(m_txn, this);
	assert (rc == RCOK);
	glob_manager.set_txn_man(m_txn);

	base_query * tmp_query = NULL;
	uint64_t txn_st_cnt = 0;
	uint64_t thd_txn_id = 0;
	uint64_t starttime;
	uint64_t stoptime = 0;
	uint64_t timespan;
	uint64_t ttime;
	uint64_t last_waittime = 0;
	uint64_t last_rwaittime = 0;
	uint64_t outstanding_waits = 0;
	uint64_t outstanding_rwaits = 0;
  uint64_t debug1;
  uint64_t rsp_cnt = 0;
  RemReqType debug2;
#if CC_ALG == VLL
  uint64_t debug3;
#endif

	uint64_t run_starttime = get_sys_clock();
	uint64_t prog_time = run_starttime;
    bool txns_completed = false;

	while(true) {

		if(get_thd_id() == 0) {
            uint64_t now_time = get_sys_clock();
            if (now_time - prog_time >= PROG_TIMER) {
			    prog_time = now_time;
			    SET_STATS(get_thd_id(), tot_run_time, prog_time - run_starttime); 

			    stats.print(true);
            }
            if (!txns_completed && _wl->txn_cnt >= MAX_TXN_PER_PART) {
                assert (stats._stats[get_thd_id()]->finish_time == 0);
                txns_completed = true;
                printf("Setting final finish time\n");
                SET_STATS(get_thd_id(), finish_time, now_time - run_starttime);
                fflush(stdout);
            }
		}

    if((get_sys_clock() - run_starttime >= DONE_TIMER)
        || (_wl->txn_cnt >= MAX_TXN_PER_PART 
          && txn_pool.empty(get_node_id()))) {
        if( !ATOM_CAS(_wl->sim_done, false, true) )
          assert( _wl->sim_done);
        stoptime = get_sys_clock();
    }

		while(!work_queue.poll_next_query(get_thd_id())
                && !abort_queue.poll_abort(get_thd_id())
                && !_wl->sim_done && !_wl->sim_timeout) {
#if CC_ALG == HSTORE_SPEC
      txn_pool.spec_next(_thd_id);
#endif
      if (warmup_finish && 
          ((get_sys_clock() - run_starttime >= DONE_TIMER) 
           || (_wl->txn_cnt >= MAX_TXN_PER_PART
           && txn_pool.empty(get_node_id())))) {
        break;
      }
    }

		// End conditions
    if((get_sys_clock() - run_starttime >= DONE_TIMER)
        ||(_wl->sim_done && _wl->sim_timeout)) { 
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
        work_queue.finish(currtime);
        abort_queue.abort_finish(currtime);

        // Send EXP_DONE to all nodes
        uint64_t nnodes = g_node_cnt + g_client_node_cnt;
#if CC_ALG == CALVIN
        nnodes++;
#endif
#if WORKLOAD == YCSB
        m_query = new ycsb_query;
#endif
        for(uint64_t i = 0; i < nnodes; i++) {
          if(i != g_node_id) {
            msg_queue.enqueue(NULL,EXP_DONE,i);
          }
        }
      }
			return FINISH;
		}

    // Move queries from abort queue to work queue when penalty is done
		if((m_query = abort_queue.get_next_abort_query(get_thd_id())) != NULL) {
      work_queue.add_query(_thd_id,m_query);
    }

    // Get next query from work queue
		if((m_query = work_queue.get_next_query(get_thd_id())) == NULL)
			continue;

    uint64_t txn_id = m_query->txn_id;

		rc = RCOK;
		starttime = get_sys_clock();
    debug1 = 0;
    debug2 = m_query->rtype;
    assert(debug2 <= NO_MSG);

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
        m_query = NULL;
        break;
			case RPASS:
				m_txn = txn_pool.get_txn(m_query->return_id, m_query->txn_id);
				assert(m_txn != NULL);
        m_txn->register_thd(this);
        rc = m_txn->rc;
				break;
			case RINIT:
        DEBUG("%ld Received RINIT %ld\n",GET_PART_ID_FROM_IDX(_thd_id),m_query->txn_id)
				INC_STATS(0,rinit,1);
        assert(CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC || CC_ALG == VLL);
				assert( ((CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC) 
              && m_query->home_part != GET_PART_ID_FROM_IDX(_thd_id)) 
              || IS_REMOTE(m_query->txn_id));

				// Set up txn_pool
        if((CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC) && m_query->txn_id % g_node_cnt == g_node_id) {
          m_txn = txn_pool.get_txn(m_query->return_id, m_query->txn_id);
          tmp_query = txn_pool.get_qry(m_query->return_id, m_query->txn_id);
          tmp_query->active_part = m_query->active_part;
          //m_query = tmp_query;
        } else {
				  rc = _wl->get_txn_man(m_txn, this);
        }
				assert(rc == RCOK);
				m_txn->set_txn_id(m_query->txn_id);
				m_txn->set_ts(m_query->ts);
				m_txn->set_pid(m_query->pid);
				m_txn->state = INIT;

#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
        m_txn->home_part = m_query->home_part;
        m_txn->active_part = m_query->active_part;
#endif
        //if((CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC) && m_query->txn_id % g_node_cnt == g_node_id) {
        if( m_query->txn_id % g_node_cnt != g_node_id) {
				  txn_pool.add_txn(m_query->return_id,m_txn,m_query);
        }

#if CC_ALG == VLL
        debug3 = get_sys_clock();
        rc = vll_man.beginTxn(m_txn,m_query);
        INC_STATS(_thd_id,txn_time_begintxn,get_sys_clock() - debug3);
        if(rc == WAIT)
          break;
				// Send back ACK
        msg_queue.enqueue(m_query,RACK,m_query->return_id);
#endif
				// HStore: lock partitions at this node
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
				part_lock_man.rem_lock(m_query->parts, m_query->part_cnt, m_txn);
#endif
        m_query = NULL;


				break;
			case RPREPARE: {
        DEBUG("%ld Received RPREPARE %ld\n",GET_PART_ID_FROM_IDX(_thd_id),m_query->txn_id);
				INC_STATS(0,rprep,1);
				assert(CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC || IS_REMOTE(m_query->txn_id));

				m_txn = txn_pool.get_txn(m_query->return_id, m_query->txn_id);
				bool validate = true;

#if CC_ALG != HSTORE && CC_ALG != HSTORE_SPEC && CC_ALG != VLL
				if (m_txn == NULL) {
					validate = false;
				}
#else
				assert(m_txn != NULL);
#endif
        if(m_txn) {
          m_txn->register_thd(this);
				  tmp_query = txn_pool.get_qry(m_query->return_id, m_query->txn_id);
          tmp_query = tmp_query->merge(m_query);
          if(m_query != tmp_query) {
            YCSB_QUERY_FREE(m_query)
          }
          m_query = tmp_query;
          assert(!(CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC) || m_query->home_part != GET_PART_ID_FROM_IDX(_thd_id));
        }

				if (validate) {
					m_txn->state = PREP;
					// Validate transaction
					rc  = m_txn->validate();
				}
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
        msg_queue.enqueue(m_query,RACK,m_query->return_id);
#endif

        m_query = NULL;
				break;
			}
			case RQRY:
				INC_STATS(0,rqry,1);
				assert(CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC || IS_REMOTE(m_query->txn_id));
        assert(CC_ALG != VLL || m_query->rc != Abort);

				// Theoretically, we could send multiple queries to one node,
				//    so m_txn could already be in txn_pool
				m_txn = txn_pool.get_txn(m_query->return_id, m_query->txn_id);

				if (m_txn == NULL) {
					rc = _wl->get_txn_man(m_txn, this);
					assert(rc == RCOK);
					m_txn->set_txn_id(m_query->txn_id);
					m_txn->set_ts(m_query->ts);
					m_txn->set_pid(m_query->pid);
					m_txn->state = INIT;
					txn_pool.add_txn(m_query->return_id,m_txn,m_query);
				}
				assert(m_txn != NULL);
        m_txn->register_thd(this);
				m_txn->set_ts(m_query->ts);
#if CC_ALG == OCC
				m_txn->set_start_ts(m_query->start_ts);
#endif
        //printf("RQRY %ld %f\n",m_txn->get_ts(),(float)(get_sys_clock() - m_txn->get_ts())/ BILLION);

        // FIXME: May be doing twice the work
				tmp_query = txn_pool.get_qry(m_query->return_id, m_query->txn_id);
        tmp_query = tmp_query->merge(m_query);
          if(m_query != tmp_query) {
            YCSB_QUERY_FREE(m_query)
          }
        m_query = tmp_query;

				if(m_txn->state != EXEC) {
					DEBUG("%ld Received RQRY %ld\n",GET_PART_ID_FROM_IDX(_thd_id),m_query->txn_id);
					m_txn->state = EXEC;
        } else {
					outstanding_waits--;
					if(outstanding_waits == 0) {
						INC_STATS(get_thd_id(),time_clock_wait,get_sys_clock() - last_waittime);
					}
				}

				rc = m_txn->run_txn(m_query);

				assert(rc != WAIT_REM);

				timespan = get_sys_clock() - starttime;
				INC_STATS(get_thd_id(),time_rqry,timespan);

				if(rc != WAIT) {
          msg_queue.enqueue(m_query,RQRY_RSP,m_query->return_id);
        }
        /*
#if QRY_ONLY
				txn_pool.delete_txn(m_query->return_id, m_query->txn_id);
#endif
*/
        m_query = NULL;
				break;
			case RQRY_RSP:
				DEBUG("%ld Received RQRY_RSP %ld\n",GET_PART_ID_FROM_IDX(_thd_id),m_query->txn_id);
				INC_STATS(0,rqry_rsp,1);
				assert(IS_LOCAL(m_query->txn_id));

				m_txn = txn_pool.get_txn(g_node_id, m_query->txn_id);
				assert(m_txn != NULL);
        m_txn->register_thd(this);
				INC_STATS(get_thd_id(),time_wait_rem,get_sys_clock() - m_txn->wait_starttime);
        if(m_txn->txn_stat_starttime > 0) {
          m_txn->txn_time_net += get_sys_clock() - m_txn->txn_stat_starttime;
          m_txn->txn_stat_starttime = 0;
        }

				tmp_query = txn_pool.get_qry(m_query->return_id, m_query->txn_id);
        tmp_query = tmp_query->merge(m_query);
          if(m_query != tmp_query) {
            YCSB_QUERY_FREE(m_query)
          }
        m_query = tmp_query;

				outstanding_rwaits--;
				if(outstanding_rwaits == 0) {
					INC_STATS(get_thd_id(),time_clock_rwait,get_sys_clock() - last_rwaittime);
				}

				// Execute from original txn; Note: txn may finish
				assert(m_txn->get_rsp_cnt() == 0);
				rc = m_txn->run_txn(m_query);

#if QRY_ONLY
        m_query->client_id =txn_pool.get_qry(g_node_id, m_query->txn_id)->client_id; 
#endif
				break;
			case RFIN: {
				DEBUG("%ld Received RFIN %ld\n",GET_PART_ID_FROM_IDX(_thd_id),m_query->txn_id);
				INC_STATS(0,rfin,1);
        if(m_query->rc == Abort) {
          INC_STATS(_thd_id,abort_rem_cnt,1);
        } else {
          INC_STATS(_thd_id,txn_rem_cnt,1);
        }
				assert(CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC || IS_REMOTE(m_query->txn_id));

				m_txn = txn_pool.get_txn(m_query->return_id, m_query->txn_id);
				bool finish = true;
#if CC_ALG != HSTORE && CC_ALG != HSTORE_SPEC && CC_ALG != VLL
				if (m_txn == NULL) {
					assert(m_query->rc == Abort);
					finish = false;	
				}
#else
				assert(m_txn != NULL);
#endif


#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
        if(m_txn) {
          m_txn->register_thd(this);
          if(GET_NODE_ID(m_query->home_part) != g_node_id || GET_PART_ID_IDX(m_query->home_part) == _thd_id) {
				    tmp_query = txn_pool.get_qry(m_query->return_id, m_query->txn_id);
          tmp_query = tmp_query->merge(m_query);
          if(m_query != tmp_query) {
            YCSB_QUERY_FREE(m_query)
          }
          m_query = tmp_query;
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

        /*
				if (finish) {
          if(GET_NODE_ID(m_query->home_part) != g_node_id || GET_PART_ID_IDX(m_query->home_part) == _thd_id) {
					  txn_pool.delete_txn(m_query->return_id, m_query->txn_id);
          } 
        }
        */
#else // NOT CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
        if(m_txn) {
          m_txn->register_thd(this);
				  tmp_query = txn_pool.get_qry(m_query->return_id, m_query->txn_id);
          tmp_query = tmp_query->merge(m_query);
          if(m_query != tmp_query) {
            YCSB_QUERY_FREE(m_query)
          }
          m_query = tmp_query;
        }
				if (finish) {
					m_txn->state = DONE;
					m_txn->rem_fin_txn(m_query);
        }
        // m_query is used by send thread; do not free
          msg_queue.enqueue(m_query,RACK,m_query->return_id);
          /*
				if (finish) {
					txn_pool.delete_txn(m_query->return_id, m_query->txn_id);
        }
        */
#endif

				m_query = NULL;
				break;
			}
			case RACK:
				DEBUG("%ld Received RACK %ld\n",GET_PART_ID_FROM_IDX(_thd_id),m_query->txn_id);
				INC_STATS(0,rack,1);
				assert(IS_LOCAL(m_query->txn_id));
				assert(!(CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC) || GET_PART_ID_IDX(m_query->home_part) == _thd_id);

				m_txn = txn_pool.get_txn(g_node_id, m_query->txn_id);
				assert(m_txn != NULL);
        m_txn->register_thd(this);

				tmp_query = txn_pool.get_qry(m_query->return_id, m_query->txn_id);
        tmp_query = tmp_query->merge(m_query);
          if(m_query != tmp_query) {
            YCSB_QUERY_FREE(m_query)
          }
        m_query = tmp_query;

        // returns the current response count for this transaction
				rsp_cnt = m_txn->decr_rsp(1);
        assert(rsp_cnt >=0);
        if(rsp_cnt > 0) {
          m_query = NULL;
          break;
        }

				if(rsp_cnt == 0) {
					// Done waiting 
					INC_STATS(get_thd_id(),time_wait_rem,get_sys_clock() - m_txn->wait_starttime);
          //FIXME: this was uncommented. should we always say ok?
					m_txn->rc = RCOK;

					// After RINIT
					switch(m_txn->state) {
						case INIT:
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
              
#if CC_ALG == HSTORE_SPEC
							txn_pool.commit_spec_ex(RCOK,_thd_id);
							spec_man.clear();
#endif
							uint64_t part_arr_s[1];
							part_arr_s[0] = m_query->part_to_access[0];
							rc = part_lock_man.rem_lock(part_arr_s, 1, m_txn);
							m_query->update_rc(rc);
							if(rc == WAIT) {
								m_txn->rc = rc;
					      m_query = NULL;
								break;
							}

							if(m_txn->ready_part > 0) {
					      m_query = NULL;
								break;
              }
#endif
#if CC_ALG == VLL
              // This may return an Abort

              if(m_query->rc == Abort) {
                    m_txn->state = FIN;
                    // send rfin messages w/ abort
                    m_txn->finish(m_query,true);
                    m_query = NULL;
                    break;
              }

              debug3 = get_sys_clock();
              rc = vll_man.beginTxn(m_txn,m_query);
              INC_STATS(_thd_id,txn_time_begintxn,get_sys_clock() - debug3);
              if(rc == WAIT)
                break;
              if(rc == Abort) {
                    m_txn->state = FIN;
                    // send rfin messages w/ abort
                    m_txn->finish(m_query,true);
                    m_query = NULL;
                    break;
              }
#endif
							m_txn->state = EXEC; 
							txn_pool.restart_txn(m_txn->get_txn_id());
					    m_query = NULL;
							break;
						case PREP:
							// Validate
              debug1 = 1;
							m_txn->spec = true;
							rc  = m_txn->validate();
							m_txn->spec = false;
							if(rc == Abort)
								m_query->rc = rc;
#if CC_ALG == HSTORE_SPEC
							if(m_query->rc != Abort) {
								// allow speculative execution to start
								txn_pool.start_spec_ex(_thd_id);
							}
#endif

							m_txn->state = FIN;
							// After RPREPARE, send rfin messages
							m_txn->finish(m_query,true);
              // Note: can't touch m_txn after this, since all other
              //  RACKs may have been received and FIN processed before this point
					    m_query = NULL;
							break;
						case FIN:
							// After RFIN
              debug1 = 2;
							m_txn->state = DONE;
#if CC_ALG == HSTORE_SPEC
							txn_pool.commit_spec_ex(m_query->rc,_thd_id);
							spec_man.clear();
#endif
							uint64_t part_arr[1];
							part_arr[0] = m_query->part_to_access[0];
							rc = m_txn->finish(m_query->rc,part_arr,1);
							break;
						default:
							assert(false);
					}
				} 
				break;
			case RTXN:
				INC_STATS(0,rtxn,1);
				assert(m_query->txn_id == UINT64_MAX || (m_query->txn_id % g_node_cnt == g_node_id));

				if(m_query->txn_id == UINT64_MAX) {
          // This is a new transaction
					ATOM_ADD(txn_st_cnt,1);
					rc = _wl->get_txn_man(m_txn, this);
					assert(rc == RCOK);

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
							+ (g_thread_cnt * g_node_cnt * thd_txn_id));
					thd_txn_id ++;
					m_query->set_txn_id(m_txn->get_txn_id());
          work_queue.update_hash(get_thd_id(),m_txn->get_txn_id());
          txn_id= m_txn->get_txn_id();

					// Put txn in txn_pool
					txn_pool.add_txn(g_node_id,m_txn,m_query);

          m_query->penalty = 0;
          m_query->abort_restart = false;

					m_txn->starttime = get_sys_clock();
          m_txn->txn_time_misc += m_query->time_q_work;
          m_txn->txn_time_misc += m_query->time_copy;
          m_txn->register_thd(this);
					DEBUG("START %ld %f %lu\n",m_txn->get_txn_id(),(double)(m_txn->starttime - run_starttime) / BILLION,m_txn->get_ts());
				}
				else {
					// Re-executing transaction
					m_txn = txn_pool.get_txn(g_node_id,m_query->txn_id);
					assert(m_txn != NULL);
          m_txn->register_thd(this);
				}

				if(m_txn->rc != WAIT && m_txn->state == START) {

          // Get new timestamps
					if ((CC_ALG == HSTORE && !HSTORE_LOCAL_TS)
							|| (CC_ALG == HSTORE_SPEC && !HSTORE_LOCAL_TS)
							|| CC_ALG == MVCC 
              || CC_ALG == VLL
							|| CC_ALG == TIMESTAMP) { 
						m_txn->set_ts(get_next_ts());
						m_query->ts = m_txn->get_ts();
					//printf("START %ld %f %lu\n",m_txn->get_txn_id(),(double)(m_txn->starttime - run_starttime) / BILLION,m_txn->get_ts());
					}

#if CC_ALG == OCC
					m_txn->start_ts = get_next_ts(); 
					m_query->start_ts = m_txn->get_start_ts();
#endif

					ttime = get_sys_clock();
#if CC_ALG != HSTORE && CC_ALG != HSTORE_SPEC && CC_ALG != VLL
          // Touch first partition
          m_query->part_touched[m_query->part_touched_cnt++] = m_query->part_to_access[0];
#endif
#if !QRY_ONLY && (CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC || CC_ALG == VLL)
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
						txn_pool.start_spec_ex(_thd_id);
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

#endif // !QRY_ONLY && (CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC || CC_ALG == VLL)
					INC_STATS(get_thd_id(),time_msg_sent,get_sys_clock() - ttime);
				} //if(m_txn->rc != WAIT) 

				if(m_query->rc == Abort && rc == WAIT) {
					m_query = NULL;
					break;
				}

				if(rc == RCOK) {
					assert(m_txn->get_rsp_cnt() == 0);
					if(m_txn->state != EXEC) {
						m_txn->state = EXEC;
					} else {
						outstanding_waits--;
						if(outstanding_waits == 0) {
							INC_STATS(get_thd_id(),time_clock_wait,get_sys_clock() 
									- last_waittime);
						}
					}
					rc = m_txn->run_txn(m_query);
				}

				break;
			default:
				assert(false);
				break;
		}

		// If m_query was freed just before this, m_query is NULL
		if(m_query != NULL && m_query->txn_id % g_node_cnt == g_node_id) {
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
					assert(m_txn->get_rsp_cnt() == 0);
          assert(m_query->part_num == 1 || debug1 == 2);
          assert((m_query->part_num == 1 && (m_query->rtype == RTXN || m_query->rtype == RPASS)) || (m_query->part_num > 1 && m_query->rtype == RACK));

					if(m_query->part_num == 1 && !m_txn->spec) {
						rc = m_txn->finish(rc,m_query->part_to_access,m_query->part_num);
					} else
						assert(m_txn->state == DONE || QRY_ONLY || TWOPC_ONLY);

					timespan = get_sys_clock() - m_txn->starttime;

					INC_STATS(get_thd_id(),txn_cnt,1);
					INC_STATS(get_thd_id(), run_time, timespan);
					INC_STATS(get_thd_id(), latency, timespan);
          INC_STATS_ARR(get_thd_id(),all_lat,timespan);
					if(m_txn->abort_cnt > 0) { 
						INC_STATS(get_thd_id(), txn_abort_cnt, 1);
						INC_STATS_ARR(get_thd_id(), all_abort, m_txn->abort_cnt);
					}
					if(m_query->part_num > 1) {
						INC_STATS(get_thd_id(),mpq_cnt,1);
					}
          if(m_txn->cflt) {
						INC_STATS(get_thd_id(),cflt_cnt_txn,1);
          }

          m_txn->txn_time_q_abrt += m_query->time_q_abrt;
          m_txn->txn_time_q_work += m_query->time_q_work;
          m_txn->txn_time_copy += m_query->time_copy;
          m_txn->txn_time_misc += timespan;
          m_txn->update_stats();

					DEBUG("COMMIT %ld %ld %ld %f -- %f\n",m_txn->get_txn_id(),m_query->part_num,m_txn->abort_cnt,(double)m_txn->txn_time_wait/BILLION,(double)timespan/ BILLION);

					// Send result back to client
          msg_queue.enqueue(m_query,CL_RSP,m_query->client_id);
					//txn_pool.delete_txn(g_node_id,m_query->txn_id);
					ATOM_ADD(_wl->txn_cnt,1);

					break;

				case Abort:
					assert(m_txn->get_rsp_cnt() == 0);
					assert(m_txn != NULL);
					assert(m_query->txn_id != UINT64_MAX);

					if(m_query->part_num == 1 && !m_txn->spec)
						rc = m_txn->finish(rc,m_query->part_to_access,m_query->part_num);
#if WORKLOAD == TPCC
        if(((tpcc_query*)m_query)->rbk && m_query->rem_req_state == TPCC_FIN) {
          INC_STATS(get_thd_id(),txn_cnt,1);
			    INC_STATS(get_thd_id(), rbk_abort_cnt, 1);
		      timespan = get_sys_clock() - m_txn->starttime;
		      INC_STATS(get_thd_id(), run_time, timespan);
		      INC_STATS(get_thd_id(), latency, timespan);
        //txn_pool.delete_txn(g_node_id,m_query->txn_id);
					ATOM_ADD(_wl->txn_cnt,1);
        //ATOM_SUB(txn_pool.inflight_cnt,1);
          msg_queue.enqueue(m_query,CL_RSP,m_query->client_id);
          break;
        
        }
#endif
				INC_STATS(get_thd_id(), abort_cnt, 1);

        m_txn->abort_cnt++;
        m_query->part_touched_cnt = 0;
        m_query->rtype = RTXN;
        m_query->rc = RCOK;
        m_query->spec = false;
        m_query->spec_done = false;
        m_query->abort_restart = true;
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
        m_query->active_part = m_query->home_part;
#endif
        m_query->reset();
        m_txn->state = START;
        m_txn->rc = RCOK;
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
					  work_queue.add_query(_thd_id,m_query);
            break;
          }
#endif

          /*
					if (CC_ALG == VLL) 
					  work_queue.add_query(_thd_id,m_query);
          else 
          */
            abort_queue.add_abort_query(_thd_id,m_query);

					break;
				case WAIT:
					assert(m_txn != NULL);
					if(m_txn->state != INIT) { 
						last_waittime = get_sys_clock();
						outstanding_waits++;
					}
					//m_txn->wait_starttime = get_sys_clock();
					break;
				case WAIT_REM:
					// Save transaction information for later
					assert(m_txn != NULL);
					m_txn->wait_starttime = get_sys_clock();
					last_rwaittime = get_sys_clock();
					outstanding_rwaits++;
					//txn_pool.add_txn(g_node_id,m_txn,m_query);
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

		timespan = get_sys_clock() - starttime;
		INC_STATS(get_thd_id(),time_work,timespan);

	}

}


ts_t
thread_t::get_next_ts() {
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

