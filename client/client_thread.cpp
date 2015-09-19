#include "global.h"
#include "client_thread.h"
#include "query.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "client_query.h"
#include "mem_alloc.h"
#include "test.h"
#include "transport.h"
#include "client_txn.h"
#include "msg_thread.h"
#include "msg_queue.h"

void Client_thread_t::init(uint64_t thd_id, uint64_t node_id, workload * workload) {
	_thd_id = thd_id;
	_node_id = node_id;
	_wl = workload;
}

uint64_t Client_thread_t::get_thd_id() { return _thd_id; }
uint64_t Client_thread_t::get_node_id() { return _node_id; }
uint64_t Client_thread_t::get_host_cid() {	return _host_cid; }
void Client_thread_t::set_host_cid(uint64_t cid) { _host_cid = cid; }
uint64_t Client_thread_t::get_cur_cid() { return _cur_cid; }
void Client_thread_t::set_cur_cid(uint64_t cid) {_cur_cid = cid; }

RC Client_thread_t::run_remote() {
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
	int rsp_cnt = g_node_cnt + g_client_node_cnt - 1;
#if CC_ALG == CALVIN
	rsp_cnt++;	// Account for sequencer node
#endif
	int done_cnt = g_node_cnt + g_client_node_cnt - 1;
	int32_t inf;
  uint32_t return_node_offset;
	//int rsp_cnts[g_node_cnt];
	//memset(rsp_cnts, 0, g_node_cnt * sizeof(int));

  if(_thd_id == g_client_thread_cnt + g_client_rem_thread_cnt + g_client_send_thread_cnt - 1) {
    while(!_wl->sim_init_done) {
      tport_man.recv_msg();
      while((m_query = work_queue.get_next_query(get_thd_id())) != NULL) {
        assert(m_query->rtype == INIT_DONE);
        printf("Received INIT_DONE from node %ld\n",m_query->return_id);
        ATOM_SUB(rsp_cnt,1);
        if(rsp_cnt ==0) {
          if( !ATOM_CAS(_wl->sim_init_done, false, true) )
            assert( _wl->sim_init_done);
        }
      }
    }
  }
	int rsp_cnts[g_servers_per_client];
	memset(rsp_cnts, 0, g_servers_per_client * sizeof(int));
  /*
  if(_thd_id == g_client_thread_cnt) {
	while(rsp_cnt > 0) {
		m_query = (base_query *) tport_man.recv_msg();
		if (m_query != NULL) {
			switch(m_query->rtype) {
				case INIT_DONE:
					printf("Received INIT_DONE from node %u\n",m_query->return_id);
					rsp_cnt--;
					break;
				case CL_RSP:
#if CC_ALG == CALVIN
					assert(m_query->return_id == g_node_cnt + g_client_node_cnt);
					return_node_offset = 0;
#else
                    return_node_offset = m_query->return_id - g_server_start_node;
                    assert(return_node_offset < g_servers_per_client);
#endif
					rsp_cnts[return_node_offset]++;
					inf = client_man.dec_inflight(return_node_offset);
					break;
				default:
					assert(false);
			}
#if WORKLOAD == YCSB
      mem_allocator.free(m_query,sizeof(ycsb_query));
#elif WORKLOAD == TPCC
      mem_allocator.free(m_query,sizeof(tpcc_query));
#endif
		}
	}
  }
  */
	pthread_barrier_wait( &warmup_bar );
	printf("Run_remote %ld:%ld\n",_node_id, _thd_id);

	myrand rdm;
	rdm.init(get_thd_id());
	ts_t rq_time = get_sys_clock();
	uint64_t run_starttime = get_sys_clock();

	while (true) {
    if(get_sys_clock() - run_starttime >= DONE_TIMER) {
      return FINISH;
    }
		tport_man.recv_msg();
    while((m_query = work_queue.get_next_query(get_thd_id())) != NULL) {
			rq_time = get_sys_clock();
			assert(m_query->rtype == CL_RSP || m_query->rtype == EXP_DONE);
			assert(m_query->dest_id == g_node_id);
			switch (m_query->rtype) {
				case CL_RSP:
#if CC_ALG == CALVIN
					assert(m_query->return_id == g_node_cnt + g_client_node_cnt);
					return_node_offset = 0;
#else
          return_node_offset = m_query->return_id - g_server_start_node;
          assert(return_node_offset < g_servers_per_client);
#endif
		      rsp_cnts[return_node_offset]++;
					inf = client_man.dec_inflight(return_node_offset);
          assert(inf >=0);
					break;
        case EXP_DONE:
          done_cnt--;
          break;
				default:
					assert(false);
			}
#if WORKLOAD == YCSB
      mem_allocator.free(m_query,sizeof(ycsb_query));
#elif WORKLOAD == TPCC
      mem_allocator.free(m_query,sizeof(tpcc_query));
#endif
    }

      /*
		m_query = (base_query *) tport_man.recv_msg();
		if( m_query != NULL ) { 
			rq_time = get_sys_clock();
			assert(m_query->rtype == CL_RSP || m_query->rtype == EXP_DONE);
			assert(m_query->dest_id == g_node_id);
			//assert(m_query->return_id < g_node_id);
			//for (uint64_t l = 0; l < g_node_cnt; ++l)
			//    printf("Response count for %lu: %d\n", l, rsp_cnts[l]);
			switch (m_query->rtype) {
				case CL_RSP:
#if CC_ALG == CALVIN
					assert(m_query->return_id == g_node_cnt + g_client_node_cnt);
					return_node_offset = 0;
#else
          return_node_offset = m_query->return_id - g_server_start_node;
          assert(return_node_offset < g_servers_per_client);
#endif
		      rsp_cnts[return_node_offset]++;
					inf = client_man.dec_inflight(return_node_offset);
          assert(inf >=0);
					break;
        case EXP_DONE:
          done_cnt--;
          break;
				default:
					assert(false);
			}
#if WORKLOAD == YCSB
      mem_allocator.free(m_query,sizeof(ycsb_query));
#elif WORKLOAD == TPCC
      mem_allocator.free(m_query,sizeof(tpcc_query));
#endif
		}
    */
		ts_t tend = get_sys_clock(); 
		if (warmup_finish && _wl->sim_done && ((tend - rq_time) > MSG_TIMEOUT)) {
			if( !ATOM_CAS(_wl->sim_timeout, false, true) )
				assert( _wl->sim_timeout);
		}

    // If all other nodes are done, finish.
		if (warmup_finish && done_cnt == 0) {
			if( !ATOM_CAS(_wl->sim_done, false, true) )
				assert( _wl->sim_done);
			if( !ATOM_CAS(_wl->sim_timeout, false, true) )
				assert( _wl->sim_timeout);
		}

		if (_wl->sim_done && _wl->sim_timeout) {
			bool done = true;
			//for (uint32_t i = 0; i < g_node_cnt; ++i) {
			for (uint32_t i = 0; i < g_servers_per_client; ++i) {
				// Check if we're still waiting on any txns to finish
				inf = client_man.get_inflight(i);
				if (inf > 0 && done_cnt > 0) {
					done = false;
					break;
				}
			}


			if (!done)
				continue;
#if !NOGRAPHITE
			CarbonDisableModelsBarrier(&enable_barrier);
#endif
			return FINISH;
		}
	}
}

RC Client_thread_t::run_send() {
	printf("Run_send %ld:%ld\n",_node_id, _thd_id);
  fflush(stdout);
	if (warmup_finish) {
		mem_allocator.register_thread(_thd_id);
	}
	pthread_barrier_wait( &warmup_bar );
	stats.init(get_thd_id());

  MessageThread messager;
  messager.init(_thd_id);

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
RC Client_thread_t::run() {
	printf("Run %ld:%ld\n",_node_id, _thd_id);
#if !NOGRAPHITE
	_thd_id = CarbonGetTileId();
#endif
	if (warmup_finish) {
		mem_allocator.register_thread(_thd_id);
	}
	pthread_barrier_wait( &warmup_bar );
	stats.init(get_thd_id());
	base_client_query * m_query = NULL;

	if( _thd_id == 0) {
#if WORKLOAD == YCSB
    m_query = new ycsb_client_query;
#elif WORKLOAD == TPCC
    m_query = new tpcc_client_query;
#endif
		uint64_t nnodes = g_node_cnt + g_client_node_cnt;
#if CC_ALG == CALVIN
		nnodes++;
#endif
		for(uint64_t i = 0; i < nnodes; i++) {
			if(i != g_node_id) {
				//rem_qry_man.send_init_done(i);
        //m_query->txn_id = UINT64_MAX;
        msg_queue.enqueue(NULL,INIT_DONE,i);
			}
		}
	}
	pthread_barrier_wait( &warmup_bar );
	printf("Run %ld:%ld\n",_node_id, _thd_id);

	myrand rdm;
	rdm.init(get_thd_id());
	//base_query * m_query = NULL;

	uint64_t iters = 0;
	uint32_t num_txns_sent = 0;
	//int txns_sent[g_node_cnt];
	int txns_sent[g_servers_per_client];
    for (uint32_t i = 0; i < g_servers_per_client; ++i)
        txns_sent[i] = 0;
	//memset(txns_sent, 0, g_node_cnt * sizeof(int));

	uint64_t run_starttime = get_sys_clock();
	uint64_t prog_time = run_starttime;

	//while (num_txns_sent < g_node_cnt * MAX_TXN_PER_PART) {
	while (num_txns_sent < g_servers_per_client * MAX_TXN_PER_PART) {
		//uint32_t next_node = iters++ % g_node_cnt;
		if(get_sys_clock() - run_starttime >= DONE_TIMER) {
      break;
    }
#if NETWORK_DELAY > 0
        tport_man.check_delayed_messages();
#endif
		uint32_t next_node = (((iters++) * g_client_thread_cnt) + _thd_id )% g_servers_per_client;
		// Just in case...
		if (iters == UINT64_MAX)
			iters = 0;
		if (client_man.inc_inflight(next_node) < 0)
			continue;

		m_query = client_query_queue.get_next_query(next_node,_thd_id);
		if (m_query == NULL) {
			client_man.dec_inflight(next_node);
      if(client_query_queue.done()
              && (NETWORK_DELAY > 0 && !tport_man.delay_queue->poll_next_entry()))
        break;
			continue;
		}
#if DEBUG_DISTR
		DEBUG("Client: thread %lu sending query to node: %lu\n",
				_thd_id, GET_NODE_ID(m_query->pid));
		for (uint32_t k = 0; k < g_servers_per_client; ++k) {
			DEBUG("Node %u: txns in flight: %d\n", 
                    k + g_server_start_node, client_man.get_inflight(k));
		}
#endif

    /*
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
		m_query->client_query(m_query, m_query->pid);
#else
		m_query->client_query(m_query, GET_NODE_ID(m_query->pid));
#endif
*/
    msg_queue.enqueue((base_query*)((void*)m_query),RTXN,GET_NODE_ID(m_query->pid));
		num_txns_sent++;
		txns_sent[GET_NODE_ID(m_query->pid)-g_server_start_node]++;
    INC_STATS(get_thd_id(),txn_sent,1);

		if(get_sys_clock() - prog_time >= PROG_TIMER) {
			prog_time = get_sys_clock();
			SET_STATS(get_thd_id(), tot_run_time, prog_time - run_starttime); 
      if(get_thd_id() == 0)
        stats.print_client(true);
    }
		if(get_sys_clock() - run_starttime >= DONE_TIMER) {
      break;
    }
	}
	for (uint64_t l = 0; l < g_servers_per_client; ++l)
		printf("Txns sent to node %lu: %d\n", l+g_server_start_node, txns_sent[l]);
	if( !ATOM_CAS(_wl->sim_done, false, true) )
		assert( _wl->sim_done);

	prog_time = get_sys_clock();
	SET_STATS(get_thd_id(), tot_run_time, prog_time - run_starttime); 

	if( _thd_id == 0) {
      // Send EXP_DONE to all nodes
		uint64_t nnodes = g_node_cnt + g_client_node_cnt;
#if CC_ALG == CALVIN
		nnodes++;
#endif
#if WORKLOAD == YCSB
    m_query = new ycsb_client_query;
#endif
		for(uint64_t i = 0; i < nnodes; i++) {
			if(i != g_node_id) {
				//rem_qry_man.send_exp_done(i);
        //m_query->txn_id = UINT64_MAX;
        msg_queue.enqueue(NULL,EXP_DONE,i);
			}
		}
  }
	return FINISH;
}
