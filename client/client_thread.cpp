#include "global.h"
#include "client_thread.h"
#include "query.h"
#include "client_query.h"
#include "mem_alloc.h"
#include "test.h"
#include "transport.h"
#include "client_txn.h"

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
	int32_t inf;
	int rsp_cnts[g_node_cnt];
	memset(rsp_cnts, 0, g_node_cnt * sizeof(int));
	while(rsp_cnt > 0) {
		m_query = tport_man.recv_msg();
		if (m_query != NULL) {
			switch(m_query->rtype) {
				case INIT_DONE:
					rsp_cnt--;
					break;
				case CL_RSP:
					rsp_cnts[m_query->return_id]++;
					inf = client_man.dec_inflight(m_query->return_id);
					break;
				default:
					assert(false);
			}
		}
	}
	pthread_barrier_wait( &warmup_bar );
	printf("Run_remote %ld:%ld\n",_node_id, _thd_id);

	myrand rdm;
	rdm.init(get_thd_id());
	ts_t rq_time = get_sys_clock();

	while (true) {
		m_query = tport_man.recv_msg();
		if( m_query != NULL ) { 
			rq_time = get_sys_clock();
			assert(m_query->rtype == CL_RSP);
			assert(m_query->dest_id == g_node_id);
			assert(m_query->return_id < g_node_id);
			rsp_cnts[m_query->return_id]++;
#if DEBUG_DISTR
			printf("Received query response from %u\n", m_query->return_id);
#endif
			//for (uint64_t l = 0; l < g_node_cnt; ++l)
			//    printf("Response count for %lu: %d\n", l, rsp_cnts[l]);
			switch (m_query->rtype) {
				case CL_RSP:
					inf = client_man.dec_inflight(m_query->return_id);
					break;
				default:
					assert(false);
			}
		}
		ts_t tend = get_sys_clock(); 
		if (warmup_finish && _wl->sim_done && ((tend - rq_time) > MSG_TIMEOUT)) {
			if( !ATOM_CAS(_wl->sim_timeout, false, true) )
				assert( _wl->sim_timeout);
		}

		if (_wl->sim_done && _wl->sim_timeout) {
			bool done = true;
			for (uint32_t i = 0; i < g_node_cnt; ++i) {
				// Check if we're still waiting on any txns to finish
				inf = client_man.get_inflight(i);
#if DEBUG_DISTR
				//printf("Wrapping up... Node %u: inflight txns left: %d\n",i,inf);
#endif
				if (inf > 0) {
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

	if( _thd_id == 0) {
		for(uint64_t i = 0; i < g_node_cnt+g_client_node_cnt; i++) {
			if(i != g_node_id) {
				rem_qry_man.send_init_done(i);
			}
		}
	}
	pthread_barrier_wait( &warmup_bar );
	printf("Run %ld:%ld\n",_node_id, _thd_id);

	myrand rdm;
	rdm.init(get_thd_id());
	base_query * m_query = NULL;
	uint64_t iters = 0;
	uint32_t num_txns_sent = 0;
	int txns_sent[g_node_cnt];
	memset(txns_sent, 0, g_node_cnt * sizeof(int));

	uint64_t run_starttime = get_sys_clock();
	uint64_t prog_time = run_starttime;

	while (num_txns_sent < g_node_cnt * MAX_TXN_PER_PART) {
		uint32_t next_node = iters++ % g_node_cnt;
		// Just in case...
		if (iters == UINT64_MAX)
			iters = 0;
		if (client_man.inc_inflight(next_node) < 0)
			continue;
		// TODO: the query queue is not thread safe right now!!!
		m_query = client_query_queue.get_next_query(next_node);
		if (m_query == NULL) {
			client_man.dec_inflight(next_node);
			continue;
		}
#if DEBUG_DISTR
		printf("Client: thread %lu sending query to node: %lu\n",
				_thd_id, GET_NODE_ID(m_query->pid));
		for (uint32_t k = 0; k < g_node_id; ++k) {
			printf("Node %u: txns in flight: %d\n", k, client_man.get_inflight(k));
		}
#endif

		m_query->client_query(m_query, GET_NODE_ID(m_query->pid));
		num_txns_sent++;
		txns_sent[GET_NODE_ID(m_query->pid)]++;

		if(get_sys_clock() - prog_time >= PROG_TIMER) {
			prog_time = get_sys_clock();
      printf("[prog %ld] "
          "clock_time=%f"
          ",txns_sent=%d"
          ,_thd_id
          ,(float)(prog_time - run_starttime) / BILLION
          ,num_txns_sent
          );
		  for (uint32_t k = 0; k < g_node_id; ++k) {
        printf(",tif_node%u=%d"
            ,k,client_man.get_inflight(k)
            );
      }
      printf("\n");
      fflush(stdout);
    }
	}
//#if DEBUG_DISTR
	for (uint64_t l = 0; l < g_node_cnt; ++l)
		printf("Txns sent to node %lu: %d\n", l, txns_sent[l]);
//#endif
	if( !ATOM_CAS(_wl->sim_done, false, true) )
		assert( _wl->sim_done);

	return FINISH;
}
