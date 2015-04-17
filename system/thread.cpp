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
	pthread_barrier_wait( &warmup_bar );
	stats.init(get_thd_id());
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
	

	base_query * m_query = NULL;

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
	pthread_barrier_wait( &warmup_bar );
	//sleep(4);
	printf("Run %ld:%ld\n",_node_id, _thd_id);

	myrand rdm;
	rdm.init(get_thd_id());
	RC rc = RCOK;
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
  base_query * next_query = NULL;
  uint64_t txn_cnt = 0;
  uint64_t txn_st_cnt = 0;
	uint64_t thd_txn_id = 0;
	
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

    while(!work_queue.poll_next_query() && !(_wl->sim_done && _wl->sim_timeout)) { }

    // End conditions
	  if (_wl->sim_done && _wl->sim_timeout) {
#if !NOGRAPHITE
   			CarbonDisableModelsBarrier(&enable_barrier);
#endif
   		    return FINISH;
   	}

    if((m_query = work_queue.get_next_query()) == NULL)
      continue;


		switch(m_query->rtype) {
#if CC_ALG == HSTORE
				case RLK:
					part_lock_man.rem_lock(m_query->pid,m_query->ts, m_query->parts, m_query->part_cnt);
					break;
				case RULK:
					part_lock_man.rem_unlock(m_query->pid, m_query->parts, m_query->part_cnt,m_query->ts);
          // TODO: If this is last rulk we're waiting for, add waiting txn to query queue
					break;
				case RLK_RSP:
					part_lock_man.rem_lock_rsp(m_query->pid,m_query->rc,m_query->ts);
					break;
				case RULK_RSP:
					part_lock_man.rem_unlock_rsp(m_query->pid,m_query->rc,m_query->ts);
					break;
#endif
				case RQRY:
          // This transaction is from a remote node
          assert(m_query->txn_id % g_node_cnt != g_node_id);
          INC_STATS(0,rqry,1);
#if CC_ALG == MVCC
          //glob_manager.add_ts(m_query->return_id, 0, m_query->ts);
          glob_manager.add_ts(m_query->return_id, m_query->thd_id, m_query->ts);
#endif
          // Theoretically, we could send multiple queries to one node.
          //    m_txn could already be in txn_pool
          m_txn = txn_pool.get_txn(m_query->return_id, m_query->txn_id);
          if(m_txn == NULL) {
	          rc = _wl->get_txn_man(m_txn, this);
            assert(rc == RCOK);
            m_txn->set_txn_id(m_query->txn_id);
            txn_pool.add_txn(m_query->return_id,m_txn,m_query);
          } else {
            INC_STATS(get_thd_id(),time_wait_lock,get_sys_clock() - m_txn->wait_starttime);
          }
          m_txn->set_ts(m_query->ts);
#if CC_ALG == OCC
          m_txn->set_start_ts(m_query->start_ts);
#endif

          rc = m_txn->run_txn(m_query);

          assert(rc != WAIT_REM);
          if(rc != WAIT)
            rem_qry_man.remote_rsp(m_query,m_txn);
					break;
				case RQRY_RSP:
          // This transaction originated from this node
          assert(m_query->txn_id % g_node_cnt == g_node_id);
          INC_STATS(0,rqry_rsp,1);
		      m_txn = txn_pool.get_txn(g_node_id, m_query->txn_id);
          assert(m_txn != NULL);
          INC_STATS(get_thd_id(),time_wait_rem,get_sys_clock() - m_txn->wait_starttime);
		      next_query = txn_pool.get_qry(g_node_id, m_query->txn_id);
					m_txn->merge_txn_rsp(m_query,next_query);
          // free this m_query
#if WORKLOAD == TPCC
          mem_allocator.free(m_query, sizeof(tpcc_query));
#endif
          m_query = next_query;
          // Execute from original txn; Note: txn may finish
          /*
          if(m_query->rc == Abort) {
            rc = m_txn->finish(m_query);
          }
          else {
          */
          
            //assert(m_query->rc == RCOK || m_query->rc == WAIT_REM || m_query->rc == WAIT);
            rc = m_txn->run_txn(m_query);
          //}
					break;
        case RFIN:
          // This transaction is from a remote node
          assert(m_query->txn_id % g_node_cnt != g_node_id);
          INC_STATS(0,rfin,1);
		      m_txn = txn_pool.get_txn(m_query->return_id, m_query->txn_id);
          if(m_txn == NULL) {
            rem_qry_man.ack_response(m_query);
#if WORKLOAD == TPCC
            mem_allocator.free(m_query, sizeof(tpcc_query));
#endif
            break;
          }
          m_txn->rem_fin_txn(m_query);
          txn_pool.delete_txn(m_query->return_id, m_query->txn_id);
          rem_qry_man.ack_response(m_query);
#if WORKLOAD == TPCC
          mem_allocator.free(m_query, sizeof(tpcc_query));
#endif
          // TODO: If a waiting txn was affected by this txn finishing, add waiting txn to queue.
          break;
        case RACK:
          // This transaction originated from this node
          assert(m_query->txn_id % g_node_cnt == g_node_id);
          INC_STATS(0,rack,1);
		      m_txn = txn_pool.get_txn(g_node_id, m_query->txn_id);
          assert(m_txn != NULL);
          m_txn->decr_rsp(1);
          if(m_txn->get_rsp_cnt() == 0) {
            // TODO: Done waiting; 
            INC_STATS(get_thd_id(),time_wait_rem,get_sys_clock() - m_txn->wait_starttime);
		        next_query = txn_pool.get_qry(g_node_id, m_query->txn_id);
            rc = m_txn->finish(next_query->rc);
            //txn_pool.delete_txn(g_node_id, m_query->txn_id);
          }
          // free this m_query
#if WORKLOAD == TPCC
          mem_allocator.free(m_query, sizeof(tpcc_query));
#endif
          m_query = next_query;
          break;
        case RTXN:
          // This transaction is originating at this node
          assert(m_query->txn_id == UINT64_MAX || (m_query->txn_id % g_node_cnt == g_node_id));

          if(m_query->txn_id == UINT64_MAX) {
            // New transaction

	          rc = _wl->get_txn_man(m_txn, this);
            assert(rc == RCOK);

		        m_txn->abort_cnt = 0;
            if (CC_ALG == WAIT_DIE || CC_ALG == TIMESTAMP || CC_ALG == MVCC) {
              m_txn->set_ts(get_next_ts());
              m_query->ts = m_txn->get_ts();
            }
            if (CC_ALG == MVCC) {
              m_query->thd_id = _thd_id;
            }


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
            txn_pool.add_txn(g_node_id,m_txn,m_query);

            m_txn->starttime = get_sys_clock();
          }
          else {
            // Re-executing transaction
            m_txn = txn_pool.get_txn(g_node_id,m_query->txn_id);
            // 
            assert(m_txn != NULL);
          }

          if(m_query->rc != WAIT) {
			    if ((CC_ALG == HSTORE && !HSTORE_LOCAL_TS)
					  || CC_ALG == MVCC 
					  || CC_ALG == TIMESTAMP) { 
				    m_txn->set_ts(get_next_ts());
				    m_query->ts = m_txn->get_ts();
			    }

          // TODO: HSTORE plocks here
#if CC_ALG == MVCC
			glob_manager.add_ts(get_thd_id(), m_txn->get_ts());
#elif CC_ALG == OCC
			m_txn->start_ts = get_next_ts(); 
      m_query->start_ts = m_txn->get_start_ts();
#endif
          } else {
            INC_STATS(get_thd_id(),time_wait_lock,get_sys_clock() - m_txn->wait_starttime);
          }
          

          rc = m_txn->run_txn(m_query);

          break;
				default:
          assert(false);
					break;
			}

    if(GET_NODE_ID(m_query->pid) == g_node_id) {
    ts_t timespan;
    switch(rc) {
      case RCOK:
        // Transaction is finished. Increment stats. Remove from txn pool
        //rc = m_txn->finish(rc);
        INC_STATS(get_thd_id(),txn_cnt,1);
		    if(m_txn->abort_cnt > 0) 
			    INC_STATS(get_thd_id(), txn_abort_cnt, 1);
		    stats.add_abort_cnt(get_thd_id(), m_txn->abort_cnt);
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
        // TODO: Increment abort stats 
        // For now, just re-add query to work queue.
        assert(m_txn != NULL);
        assert(m_query->txn_id != UINT64_MAX);
        //rc = m_txn->finish(rc);
        //txn_pool.add_txn(g_node_id,m_txn,m_query);
        m_query->rtype = RTXN;
        m_query->reset();
				INC_STATS(get_thd_id(), abort_cnt, 1);
        m_txn->abort_cnt++;

        work_queue.add_query(m_query);
        break;
      case WAIT:
        assert(m_txn != NULL);
        m_txn->wait_starttime = get_sys_clock();
        break;
      case WAIT_REM:
        // Save transaction information for later
        // FIXME: Race conditions between when WAIT was issued to txn saved
        //    When > 1 thread
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
    } else {
      if(rc == WAIT) {
        m_txn->wait_starttime = get_sys_clock();
      }
    }

		if (!warmup_finish && txn_st_cnt >= WARMUP / g_thread_cnt) 
		{
			stats.clear( get_thd_id() );
#if !NOGRAPHITE
   			CarbonDisableModelsBarrier(&enable_barrier);
#endif
			return FINISH;
		}

		//if (warmup_finish && txn_st_cnt >= MAX_TXN_PER_PART) {
		if (warmup_finish && txn_st_cnt >= MAX_TXN_PER_PART/g_thread_cnt && txn_pool.empty(get_node_id())) {
			//assert(txn_cnt == MAX_TXN_PER_PART);
	      if( !ATOM_CAS(_wl->sim_done, false, true) )
				  assert( _wl->sim_done);
	    }


  }

}

RC thread_t::run_pool_txn() {
       printf("Run %ld:%ld\n",_node_id, _thd_id);
#if !NOGRAPHITE
       _thd_id = CarbonGetTileId();
#endif
       if (warmup_finish) {
               mem_allocator.register_thread(_thd_id);
       }
       pthread_barrier_wait( &warmup_bar );
       stats.init(get_thd_id());
       pthread_barrier_wait( &warmup_bar );
       //sleep(4);
       printf("Run %ld:%ld\n",_node_id, _thd_id);

       myrand rdm;
       rdm.init(get_thd_id());
       RC rc = RCOK;
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
  uint64_t txn_cnt = 0;
       uint64_t thd_txn_id = 0;
       
	while (true) {
		ts_t starttime = get_sys_clock();
		if (WORKLOAD != TEST)
			m_query = query_queue.get_next_query( _thd_id );
		ts_t t1 = get_sys_clock() - starttime;
		INC_STATS(_thd_id, time_query, t1);
		m_txn->abort_cnt = 0;
    if (CC_ALG == WAIT_DIE || CC_ALG == TIMESTAMP || CC_ALG == MVCC) {
      m_txn->set_ts(get_next_ts());
      m_query->ts = m_txn->get_ts();
    }
    if (CC_ALG == MVCC) {
      m_query->thd_id = _thd_id;
    }
//#if CC_ALG == VLL
//		_wl->get_txn_man(m_txn, this);
//#endif
		do {
			//ts_t t2 = get_sys_clock();
			m_txn->set_txn_id( (get_thd_id() + get_node_id() * g_thread_cnt) + (g_thread_cnt * g_node_cnt * thd_txn_id));
			thd_txn_id ++;
      m_query->set_txn_id(m_txn->get_txn_id());

			// for WAIT_DIE, the timestamp is not renewed after abort
			if ((CC_ALG == HSTORE && !HSTORE_LOCAL_TS)
					|| CC_ALG == MVCC 
					|| CC_ALG == TIMESTAMP) { 
				m_txn->set_ts(get_next_ts());
				m_query->ts = m_txn->get_ts();
			}

			rc = RCOK;

			if(m_query->part_num > 1)
				INC_TMP_STATS(_thd_id,mpq_cnt,1);

#if CC_ALG == HSTORE
			if (WORKLOAD == TEST) {
				uint64_t part_to_access[1] = {0};
				rc = part_lock_man.lock(m_txn, &part_to_access[0], 1);
			} else 
				rc = part_lock_man.lock(m_txn, m_query->part_to_access, m_query->part_num);
#elif CC_ALG == VLL
			vll_man.vllMainLoop(m_txn, m_query);
#elif CC_ALG == MVCC
			glob_manager.add_ts(get_thd_id(), m_txn->get_ts());
#elif CC_ALG == OCC
			// In the original OCC paper, start_ts only reads the current ts without advancing it.
			// But we advance the global ts here to simplify the implementation. However, the final
			// results should be the same.
			m_txn->start_ts = get_next_ts(); 
      m_query->start_ts = m_txn->get_start_ts();
			//glob_manager.get_ts( get_thd_id() ); 
#endif
			if (rc == RCOK) 
			{
#if CC_ALG != VLL
				if (WORKLOAD == TEST)
					rc = runTest(m_txn);
				else 
					rc = m_txn->run_txn(m_query);
#endif
#if CC_ALG == HSTORE
			if (WORKLOAD == TEST) {
				uint64_t part_to_access[1] = {0};
				part_lock_man.unlock(m_txn, &part_to_access[0], 1);
			} else 
				part_lock_man.unlock(m_txn, m_query->part_to_access, m_query->part_num);
#endif
			}
			if (rc == Abort) {
				uint64_t t = get_sys_clock();
                uint64_t penalty = 0;
				uint64_t tt;
                if (ABORT_PENALTY != 0) {
                    penalty = rdm.next() % ABORT_PENALTY;
					do {
						tt = get_sys_clock();
					} while (tt < t + penalty);
				}

				INC_STATS(_thd_id, time_abort, get_sys_clock() - t);
				//INC_STATS(_thd_id, time_abort, get_sys_clock() - t2);
				INC_STATS(get_thd_id(), abort_cnt, 1);

				stats.abort(get_thd_id());
				m_txn->abort_cnt ++;
//				printf("\n[Abort thd=%lld] %lld txns abort. ts=%lld", _thd_id, stats._stats[_thd_id]->txn_cnt, m_txn->get_ts());
			}
		} while (rc == Abort);

		ts_t endtime = get_sys_clock();
		uint64_t timespan = endtime - starttime;
		INC_STATS(get_thd_id(), run_time, timespan);
		INC_STATS(get_thd_id(), latency, timespan);
		if(m_txn->abort_cnt > 0) 
			INC_STATS(get_thd_id(), txn_abort_cnt, 1);

		stats.add_lat(get_thd_id(), timespan);
		stats.add_abort_cnt(get_thd_id(), m_txn->abort_cnt);
		if(m_query->part_num > 1)
			INC_STATS(_thd_id,mpq_cnt,1);

		INC_STATS(get_thd_id(), txn_cnt, 1);
		
		stats.commit(get_thd_id());

		txn_cnt ++;
		if (rc == FINISH)
			return rc;
		if (!warmup_finish && txn_cnt >= WARMUP / g_thread_cnt) 
		{
			stats.clear( get_thd_id() );
#if !NOGRAPHITE
   			CarbonDisableModelsBarrier(&enable_barrier);
#endif
			return FINISH;
		}

		if (warmup_finish && txn_cnt >= MAX_TXN_PER_PART) {
			assert(txn_cnt == MAX_TXN_PER_PART);
	        if( !ATOM_CAS(_wl->sim_done, false, true) )
				assert( _wl->sim_done);
	    }
	    if (_wl->sim_done) {
        if(g_rem_thread_cnt > 0)
				  while(!_wl->sim_timeout) {}
#if !NOGRAPHITE
   			CarbonDisableModelsBarrier(&enable_barrier);
#endif
   		    return FINISH;
   		}
//		printf("\n[Commit thd=%lld] %lld txns commits. ts=%lld", _thd_id, txn_cnt, m_txn->get_ts());
//#if CC_ALG != VLL
//		m_txn->release();
//		mem_allocator.free(m_txn, 0);
//#endif
	}
	assert(false);
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

