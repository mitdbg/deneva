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
#if !NOGRAPHITE
	_thd_id = CarbonGetTileId();
#endif
	if (warmup_finish) {
		mem_allocator.register_thread(_thd_id);
	}

	base_query * m_query = NULL;

	pthread_barrier_wait( &warmup_bar );
	stats.init(get_thd_id());
  // Send start msg to all nodes; wait for rsp from all nodes before continuing.
  int rsp_cnt = g_node_cnt - 1;
  while(rsp_cnt > 0) {
		m_query = tport_man.recv_msg();
    if(m_query != NULL && m_query->rtype == INIT_DONE)
      rsp_cnt --;
    else if(m_query != NULL)
      work_queue.add_query(m_query);
  }
	pthread_barrier_wait( &warmup_bar );
	printf("Run_remote %ld:%ld\n",_node_id, _thd_id);
	
	myrand rdm;
	rdm.init(get_thd_id());
	RC rc = RCOK;
	assert (rc == RCOK);

#if !NOGRAPHITE
	if (warmup_finish) {
   		CarbonEnableModelsBarrier(&enable_barrier);
	}
#endif
	


  /*
#if WORKLOAD == TPCC
	m_query = (tpcc_query *) mem_allocator.alloc(sizeof(tpcc_query), get_thd_id());
#endif
*/
	ts_t rq_time = get_sys_clock();

	while (true) {
		m_query = tport_man.recv_msg();
		if( m_query != NULL ) { 
			rq_time = get_sys_clock();
      work_queue.add_query(m_query);
    }

		ts_t tend = get_sys_clock();
		if (warmup_finish && _wl->sim_done && ((tend - rq_time) > MSG_TIMEOUT)) {
	      if( !ATOM_CAS(_wl->sim_timeout, false, true) )
					assert( _wl->sim_timeout);
	  }

	  if (_wl->sim_done && _wl->sim_timeout) {
#if !NOGRAPHITE
   	CarbonDisableModelsBarrier(&enable_barrier);
#endif
   	   return FINISH;
   	}

  }

}

RC thread_t::run() {
	printf("Run %ld:%ld\n",_node_id, _thd_id);
#if !NOGRAPHITE
	_thd_id = CarbonGetTileId();
#endif
	if (warmup_finish) {
		mem_allocator.register_thread(_thd_id);
	}
	pthread_barrier_wait( &warmup_bar );
	stats.init(get_thd_id());

  for(uint64_t i = 0; i < g_node_cnt; i++) {
    if(i != g_node_id)
      rem_qry_man.send_init_done(i);
  }
	pthread_barrier_wait( &warmup_bar );
	//sleep(4);
	printf("Run %ld:%ld\n",_node_id, _thd_id);

	myrand rdm;
	rdm.init(get_thd_id());
	RC rc = RCOK;
#if CC_ALG == HSTORE
	RC rc2 = RCOK;
#endif
	txn_man * m_txn;
	rc = _wl->get_txn_man(m_txn, this);
	assert (rc == RCOK);
	glob_manager.set_txn_man(m_txn);

#if !NOGRAPHITE
	if (warmup_finish) {
   		CarbonEnableModelsBarrier(&enable_barrier);
	}
#endif

	base_query * m_query = NULL;
  //base_query * next_query = NULL;
  uint64_t txn_cnt = 0;
  uint64_t txn_st_cnt = 0;
	uint64_t thd_txn_id = 0;
  uint64_t starttime;
  uint64_t timespan;
  uint64_t prog_time = get_sys_clock();

  uint64_t run_starttime = get_sys_clock();
	
  while(true) {

    // FIXME: Susceptible to race conditions, but will hopefully even out eventually...
    // Put here so first time through, threads will populate the work queue with new txns.
     if(txn_pool.inflight_cnt < g_inflight_max && txn_st_cnt < MAX_TXN_PER_PART/g_thread_cnt) {
        // Fetch new txn from query queue and add to work queue
        ATOM_ADD(txn_st_cnt,1);
        ATOM_ADD(txn_pool.inflight_cnt,1);
			  m_query = query_queue.get_next_query( _thd_id );
        work_queue.add_query(m_query);
    }

    if(get_sys_clock() - prog_time >= PROG_TIMER) {
      prog_time = get_sys_clock();
      SET_STATS(get_thd_id(), tot_run_time, prog_time - run_starttime); 

      stats.print_prog(_thd_id);
    }

    while(!work_queue.poll_next_query() && !(_wl->sim_done && _wl->sim_timeout)) { }

    // End conditions
	  if (_wl->sim_done && _wl->sim_timeout) {
#if !NOGRAPHITE
   			CarbonDisableModelsBarrier(&enable_barrier);
#endif
        SET_STATS(get_thd_id(), tot_run_time, get_sys_clock() - run_starttime - MSG_TIMEOUT); 
   		    return FINISH;
   	}

    if((m_query = work_queue.get_next_query()) == NULL)
      continue;

    rc = RCOK;
    starttime = get_sys_clock();

		switch(m_query->rtype) {
        case RINIT:
          // This transaction is from a remote node
#if DEBUG_DISTR
          printf("Received RINIT %ld\n",m_query->txn_id);
#endif
          assert(m_query->txn_id % g_node_cnt != g_node_id);
          INC_STATS(0,rinit,1);

          // Set up txn_pool
          rc = _wl->get_txn_man(m_txn, this);
          assert(rc == RCOK);
          m_txn->set_txn_id(m_query->txn_id);
          m_txn->set_ts(m_query->ts);
          m_txn->set_pid(m_query->pid);
          m_txn->state = INIT;
          txn_pool.add_txn(m_query->return_id,m_txn,m_query);

#if CC_ALG == MVCC
          //glob_manager.add_ts(m_query->return_id, 0, m_query->ts);
          glob_manager.add_ts(m_query->return_id, m_query->thd_id, m_query->ts);
#endif
          // HStore: lock partitions at this node
#if CC_ALG == HSTORE
					part_lock_man.rem_lock(m_query->parts, m_query->part_cnt, m_txn);
#else
          // Send back ACK
          rem_qry_man.ack_response(m_query);
#endif


          break;
        case RPREPARE:
#if DEBUG_DISTR
          printf("Received RPREPARE %ld\n",m_query->txn_id);
#endif
          // This transaction is from a remote node
          assert(m_query->txn_id % g_node_cnt != g_node_id);
          INC_STATS(0,rprep,1);
          m_txn = txn_pool.get_txn(m_query->return_id, m_query->txn_id);
          assert(m_txn != NULL);
          m_txn->state = PREP;
          // Validate transaction
          rc  = m_txn->validate();
          m_query->rc = rc;
          // Send ACK w/ commit/abort
          rem_qry_man.ack_response(m_query);
          break;
				case RQRY:

          // This transaction is from a remote node
          assert(m_query->txn_id % g_node_cnt != g_node_id);
          INC_STATS(0,rqry,1);

          // Theoretically, we could send multiple queries to one node.
          //    m_txn could already be in txn_pool
          m_txn = txn_pool.get_txn(m_query->return_id, m_query->txn_id);
          assert(m_txn != NULL);
#if DEBUG_DISTR
          if(m_txn->state != EXEC)
            printf("Received RQRY %ld\n",m_query->txn_id);
#endif


          m_txn->set_ts(m_query->ts);
#if CC_ALG == OCC
          m_txn->set_start_ts(m_query->start_ts);
#endif

          if(m_txn->state != EXEC) {
            m_txn->state = EXEC;
            // In case of restart
            /*
            next_query = txn_pool.get_qry(m_query->return_id, m_query->txn_id);
            m_txn->merge
            next_query->rtype = RQRY; 
            */
          }
          
          rc = m_txn->run_txn(m_query);

          assert(rc != WAIT_REM);
          timespan = get_sys_clock() - starttime;
          INC_STATS(get_thd_id(),time_rqry,timespan);
          if(rc != WAIT)
            rem_qry_man.remote_rsp(m_query,m_txn);
					break;
				case RQRY_RSP:
#if DEBUG_DISTR
          printf("Received RQRY_RSP %ld\n",m_query->txn_id);
#endif
          // This transaction originated from this node
          assert(m_query->txn_id % g_node_cnt == g_node_id);
          INC_STATS(0,rqry_rsp,1);
		      m_txn = txn_pool.get_txn(g_node_id, m_query->txn_id);
          assert(m_txn != NULL);
          INC_STATS(get_thd_id(),time_wait_rem,get_sys_clock() - m_txn->wait_starttime);
          /*
		      next_query = txn_pool.get_qry(g_node_id, m_query->txn_id);
					m_txn->merge_txn_rsp(m_query,next_query);
          // free this m_query
#if WORKLOAD == TPCC
          mem_allocator.free(m_query, sizeof(tpcc_query));
          m_query = NULL;
#endif
          m_query = next_query;
          */
          // Execute from original txn; Note: txn may finish
          assert(m_txn->get_rsp_cnt() == 0);
          rc = m_txn->run_txn(m_query);

          /*
#if CC_ALG == HSTORE
          if(rc == RCOK || rc == Abort) {
            m_query->rc = rc;
		        rc = part_lock_man.unlock(m_query->part_to_access, m_query->part_num, m_txn);
          }
#endif
*/
					break;
        case RFIN:
#if DEBUG_DISTR
          printf("Received RFIN %ld\n",m_query->txn_id);
#endif
          // This transaction is from a remote node
          assert(m_query->txn_id % g_node_cnt != g_node_id);
          INC_STATS(0,rfin,1);
		      m_txn = txn_pool.get_txn(m_query->return_id, m_query->txn_id);
          // Transaction should ALWAYS be in the pool until node receives RFIN
          assert(m_txn != NULL);
          m_txn->state = DONE;

          m_txn->rem_fin_txn(m_query);
          txn_pool.delete_txn(m_query->return_id, m_query->txn_id);
          rem_qry_man.ack_response(m_query);
#if WORKLOAD == TPCC
          //mem_allocator.free(m_query, sizeof(tpcc_query));
          m_query = NULL;
#endif
          break;
        case RACK:
#if DEBUG_DISTR
          printf("Received RACK %ld\n",m_query->txn_id);
#endif
          // This transaction originated from this node
          assert(m_query->txn_id % g_node_cnt == g_node_id);
          INC_STATS(0,rack,1);
		      m_txn = txn_pool.get_txn(g_node_id, m_query->txn_id);
          assert(m_txn != NULL);
          /*
		      next_query = txn_pool.get_qry(g_node_id, m_query->txn_id);
          // If after RPREPARE or RINIT, update next_query->rc
          next_query->update_rc(m_query->rc);
          */

          m_txn->decr_rsp(1);
          // TODO: May be waiting for different things: RINIT, RPREPARE, RFIN
          if(m_txn->get_rsp_cnt() == 0) {
            // Done waiting 
            INC_STATS(get_thd_id(),time_wait_rem,get_sys_clock() - m_txn->wait_starttime);
            m_txn->rc = RCOK;

            // After RINIT
            switch(m_txn->state) {
              case INIT:
                m_txn->state = EXEC; 
#if CC_ALG == HSTORE
                if(m_txn->ready_part > 0)
                  break;
#endif
                txn_pool.restart_txn(m_txn->get_txn_id());
                break;
              case PREP:
                // Validate
                rc  = m_txn->validate();
                if(rc == Abort)
                  m_query->rc = rc;

                m_txn->state = FIN;
                // After RPREPARE, send rfin messages
                m_txn->finish(m_query,true);
                break;
              case FIN:
                // After RFIN
                m_txn->state = DONE;
                uint64_t part_arr[1];
                part_arr[0] = m_query->part_to_access[0];
                rc = m_txn->finish(m_query->rc,part_arr,1);
                break;
              default:
                assert(false);
            }


          }
          // free this m_query
          /*
#if WORKLOAD == TPCC
          mem_allocator.free(m_query, sizeof(tpcc_query));
          m_query = NULL;
#endif
            */
          if(m_txn->state != DONE)
            m_query = NULL;
          break;
        case RTXN:
          // This transaction is originating at this node
          assert(m_query->txn_id == UINT64_MAX || (m_query->txn_id % g_node_cnt == g_node_id));
          INC_STATS(0,rtxn,1);

          if(m_query->txn_id == UINT64_MAX) {
            // New transaction

	          rc = _wl->get_txn_man(m_txn, this);
            assert(rc == RCOK);

		        m_txn->abort_cnt = 0;
            if (CC_ALG == WAIT_DIE) {
              m_txn->set_ts(get_next_ts());
              m_query->ts = m_txn->get_ts();
            }
            if (CC_ALG == MVCC) {
              m_query->thd_id = _thd_id;
            }

            m_txn->set_pid(m_query->pid);

            // Only set new txn_id when txn first starts
			      m_txn->set_txn_id( ( get_node_id() + get_thd_id() * g_node_cnt) + (g_thread_cnt * g_node_cnt * thd_txn_id));
			      //m_txn->set_txn_id( (get_thd_id() + get_node_id() * g_thread_cnt) + (g_thread_cnt * g_node_cnt * thd_txn_id));
			      thd_txn_id ++;
            m_query->set_txn_id(m_txn->get_txn_id());

		        if(m_query->part_num > 1) {
			        INC_STATS(get_thd_id(),mpq_cnt,1);
              // Get txn in txn_pool
              //txn_pool.add_txn(g_node_id,m_txn,m_query);
            }
            // Put txn in txn_pool in case of WAIT
            //assert(txn_pool.get_txn(g_node_id,m_txn->get_txn_id()) == NULL);
            txn_pool.add_txn(g_node_id,m_txn,m_query);

            m_txn->starttime = get_sys_clock();
          }
          else {
            // Re-executing transaction
            m_txn = txn_pool.get_txn(g_node_id,m_query->txn_id);
            // 
            assert(m_txn != NULL);
          }

          if(m_txn->rc != WAIT && m_txn->state == START) {

			      if ((CC_ALG == HSTORE && !HSTORE_LOCAL_TS)
					    || CC_ALG == MVCC 
					    || CC_ALG == TIMESTAMP) { 
				      m_txn->set_ts(get_next_ts());
				      m_query->ts = m_txn->get_ts();
			      }

#if CC_ALG == MVCC
			      glob_manager.add_ts(get_thd_id(), m_txn->get_ts());
#elif CC_ALG == OCC
			      m_txn->start_ts = get_next_ts(); 
            m_query->start_ts = m_txn->get_start_ts();
#endif

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
              if(GET_NODE_ID(part_id) != g_node_id) {
                m_txn->incr_rsp(1);
              }
            }


            // Send init message to all involved nodes
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

              if(GET_NODE_ID(part_id) != g_node_id) {
                //m_txn->incr_rsp(1);
                rc = WAIT;
                m_txn->rc = rc;
                rem_qry_man.send_init(m_query,part_id);
                m_txn->wait_starttime = get_sys_clock();
              } else {
                // local init
#if CC_ALG == HSTORE
                uint64_t part_arr[1];
                part_arr[0] = part_id;
					      rc2 = part_lock_man.rem_lock(part_arr, 1, m_txn);
                m_query->update_rc(rc2);
                if(rc2 == WAIT) {
                  rc = WAIT;
                  m_txn->rc = rc;
                }
		            //rc = part_lock_man.lock(m_query->part_to_access, m_query->part_num, m_txn);
#endif
              }
            } // for(uint64_t i = 0; i < m_query->part_num; i++) 

          } //if(m_txn->rc != WAIT) 

          if(m_query->rc == Abort && rc == WAIT) {
            m_query = NULL;
            break;
          }

          if(rc == RCOK) {
            assert(m_txn->get_rsp_cnt() == 0);
            rc = m_txn->run_txn(m_query);
          }

          break;
				default:
          assert(false);
					break;
			}

    // If m_query was freed just before this, m_query is NULL
    if(m_query != NULL && GET_NODE_ID(m_query->pid) == g_node_id) {
    switch(rc) {
      case RCOK:
        // Transaction is finished. Increment stats. Remove from txn pool
        assert(m_txn->get_rsp_cnt() == 0);
        if(m_query->part_num == 1)
          rc = m_txn->finish(rc,m_query->part_to_access,m_query->part_num);
        else
          assert(m_txn->state == DONE);
        INC_STATS(get_thd_id(),txn_cnt,1);
		    if(m_txn->abort_cnt > 0) { 
			    INC_STATS(get_thd_id(), txn_abort_cnt, 1);
          INC_STATS_ARR(get_thd_id(), all_abort, m_txn->abort_cnt);
        }
		    timespan = get_sys_clock() - m_txn->starttime;
		    INC_STATS(get_thd_id(), run_time, timespan);
		    INC_STATS(get_thd_id(), latency, timespan);
        
        txn_pool.delete_txn(g_node_id,m_query->txn_id);
        txn_cnt++;
        ATOM_SUB(txn_pool.inflight_cnt,1);
        break;
      case Abort:
        // TODO: Add to abort list that includes txn_id and ts
        //    List will be checked periodically to restart transactions and put them back on the work queue
        //    This will be used to implement abort penalty
        // For now, just re-add query to work queue.
        /*
#if CC_ALG == HSTORE
				rc = part_lock_man.unlock(m_txn, m_query->part_to_access, m_query->part_num);
        if(rc != RCOK)
          break;
#endif
*/
        assert(m_txn->get_rsp_cnt() == 0);
        if(m_query->part_num == 1)
          rc = m_txn->finish(rc,m_query->part_to_access,m_query->part_num);
        else
          assert(m_txn->state == DONE);
        assert(m_txn != NULL);
        assert(m_query->txn_id != UINT64_MAX);
        //rc = m_txn->finish(rc);
        //txn_pool.add_txn(g_node_id,m_txn,m_query);
        m_query->rtype = RTXN;
        m_query->reset();
				INC_STATS(get_thd_id(), abort_cnt, 1);
        m_txn->abort_cnt++;
        m_txn->state = START;

        work_queue.add_query(m_query);
        break;
      case WAIT:
        assert(m_txn != NULL);
        //m_txn->wait_starttime = get_sys_clock();
        break;
      case WAIT_REM:
        // Save transaction information for later
        assert(m_txn != NULL);
        m_txn->wait_starttime = get_sys_clock();
        //txn_pool.add_txn(g_node_id,m_txn,m_query);
#if WORKLOAD == TPCC
        // WAIT_REM from finish
        if(m_query->rem_req_state == TPCC_FIN)
          break;
#endif
        assert(m_query->dest_id != g_node_id);
        rem_qry_man.remote_qry(m_query,m_query->rem_req_state,m_query->dest_id,m_txn);
        break;
      default:
        assert(false);
        break;
    }
    } 
    timespan = get_sys_clock() - starttime;
    INC_STATS(get_thd_id(),time_work,timespan);

		if (!warmup_finish && txn_st_cnt >= WARMUP / g_thread_cnt) 
		{
			stats.clear( get_thd_id() );
#if !NOGRAPHITE
   			CarbonDisableModelsBarrier(&enable_barrier);
#endif
			return FINISH;
		}

		if (warmup_finish && txn_st_cnt >= MAX_TXN_PER_PART/g_thread_cnt && txn_pool.empty(get_node_id())) {
			//assert(txn_cnt == MAX_TXN_PER_PART);
	      if( !ATOM_CAS(_wl->sim_done, false, true) )
				  assert( _wl->sim_done);
	    }


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

