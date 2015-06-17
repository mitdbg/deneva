#include "global.h"
#include "sequencer.h"
//#include "query.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "mem_alloc.h"
#include "transport.h"
#include "wl.h"
#include "helper.h"

void Sequencer::init(workload * wl) {
	fill_queue = new WorkQueue();
	batch_queue = new WorkQueue();
	fill_queue->init();
	batch_queue->init();
	wait_list = NULL;
	wait_list_size = 0;
	wait_txns_left = 0;
	pthread_mutex_init(&mtx, NULL);
	pthread_cond_init(&swap_cv, NULL);
	pthread_cond_init(&access_cv, NULL);
	swapping_queues = false;
	num_accessing_queues = 0;
	pthread_mutex_init(&batchts_mtx, NULL);
	sent_first_batch = false;
	batch_ts = UINT64_MAX;
	rsp_cnt = g_node_cnt + g_client_node_cnt;
	_wl = wl;
}

void Sequencer::process_txn_ack(base_client_query *query, uint64_t thd_id) {
	assert(wait_list != NULL);
	assert(wait_txns_left > 0);
	// TODO: fix this hack
	txnid_t txn_id = query->return_id;
	assert(wait_list[txn_id].server_ack_cnt > 0);

	// Decrement the number of acks needed for this txn
	uint32_t query_acks_left = ATOM_SUB_FETCH(wait_list[txn_id].server_ack_cnt, 1);
	uint64_t txns_left = UINT64_MAX;
	if (query_acks_left == 0) {
		txns_left = ATOM_SUB_FETCH(wait_txns_left, 1);
		ATOM_FETCH_ADD(total_txns_finished,1);
		INC_STATS(thd_id,txn_cnt,1);
	}

	// If we have all acks for this batch, send qry responses to all clients
	if (txns_left == 0) {
		for (uint32_t i = 0; i < wait_list_size; ++i) {
			qlite next = wait_list[i];
			rem_qry_man.send_client_rsp(i,RCOK,next.client_startts,next.client_id);	
		}
		free(wait_list);
		wait_list = NULL;
		prepare_next_batch(thd_id);
	}
}

void Sequencer::prepare_next_batch(uint64_t thd_id) {
	while (!fill_queue->poll_next_entry() && !_wl->sim_done) {}

	// Lock fill & batch queues for swap
	assert(!batch_queue->poll_next_entry());
	pthread_mutex_lock(&mtx);
	swapping_queues = true;
	while (num_accessing_queues > 0) {
		pthread_cond_wait(&access_cv, &mtx);
	}
	assert(num_accessing_queues == 0);
	pthread_mutex_unlock(&mtx);

	// Swap fill & batch queues
	WorkQueue *tmp = fill_queue;
	fill_queue = batch_queue;
	batch_queue = tmp;

	// Release queries and signal
	pthread_mutex_lock(&mtx);
	swapping_queues = false;
	pthread_cond_signal(&swap_cv);
	pthread_mutex_unlock(&mtx);

	// Create new wait list
	wait_list_size = batch_queue->size();
	wait_list = (qlite *) mem_allocator.alloc(sizeof(qlite) * wait_list_size, 0);
	wait_txns_left = wait_list_size;

	// Bookkeeping info for preparing and sending queries to nodes
	base_client_query * query;
	bool participating_nodes[g_node_cnt];
	reset_participating_nodes(participating_nodes);
#if WORKLOAD == YCSB
	ycsb_client_query node_queries[g_node_cnt];
	for (uint32_t i = 0; i < g_node_cnt; ++i) {
		node_queries[i].requests = (ycsb_request *) mem_allocator.alloc(sizeof(ycsb_request) * g_req_per_query, 0);
	}
#elif WORKLOAD == TPCC
	tpcc_client_query node_queries[g_node_cnt];
	printf("Add sequencer support for TPCC!\n");
	assert(false);
#endif
	// Set partitions to access for all node queries
	for (uint32_t i = 0; i < g_node_cnt; ++i) {
		node_queries[i].part_to_access = (uint64_t *) mem_allocator.alloc(1 * sizeof(uint64_t), 0);
		node_queries[i].part_to_access[0] = i;
		node_queries[i].part_num = 1;
	}

	// TODO: incrementing next_txn_id & next_batch_id is thread-safe
	// only if a single thread is doing the processing.
	next_txn_id = 0;
	uint64_t batch_id = next_batch_id++;
	for (uint32_t i = 0; i < wait_list_size; ++i) {
		query = (base_client_query *) batch_queue->get_next_entry();
		assert(query != NULL);
		txnid_t txn_id = next_txn_id++;
		uint32_t server_ack_cnt = 0;
#if WORKLOAD == YCSB
		// reset request counts
		for (uint64_t j = 0; j < g_node_cnt; ++j)
			node_queries[j].request_cnt = 0;
		ycsb_client_query * m_query = (ycsb_client_query *) query;
		ycsb_request req;

		// add individual query requests to each node's corresponding request list
		for (uint64_t j = 0; j < m_query->request_cnt; ++j) {
			req = m_query->requests[j];
			uint32_t req_node_id = req.key % g_node_cnt;	
			node_queries[req_node_id].requests[node_queries[req_node_id].request_cnt++] = req;		
			participating_nodes[req_node_id] = true;
		}
#elif WORKLOAD == TPCC
		printf("Add sequencer support for TPCC!\n");
		assert(false);
#endif
		for (uint64_t j = 0; j < g_node_cnt; ++j) {
			if (participating_nodes[j])
				server_ack_cnt++;
		}
		assert(server_ack_cnt > 0);
		wait_list[i].client_id = query->return_id;
		assert(query->return_id != g_node_id && query->return_id >= g_node_cnt);
		wait_list[i].client_startts = query->client_startts;
		wait_list[i].server_ack_cnt = server_ack_cnt;

		// update all node queries with necessary query information
		for (uint64_t j = 0; j < g_node_cnt; ++j) {
			if (participating_nodes[j]) {
				//node_queries[j].txn_id = txn_id;
				node_queries[j].rtype = RTXN;
				node_queries[j].pid = j;
#if DEBUG_DIST
				printf("sequencer: sending RTXN to %lu\n",j);
#endif
				query->client_query(&node_queries[j],j,batch_id,txn_id);
			}
		}

		reset_participating_nodes(participating_nodes);

	}
	// Cleanup
	for (uint64_t i = 0; i < g_node_cnt; ++i) {
		free(node_queries[i].part_to_access);
#if WORKLOAD == YCSB
		free(node_queries[i].requests);
#endif
	}
	INC_STATS(thd_id,batch_cnt,1);
}

void Sequencer::reset_participating_nodes(bool * part_nodes) {
	for (uint32_t i = 0; i < g_node_cnt; ++i)
		part_nodes[i] = false;
}

//void Sequencer::start_batch_timer() {
//	pthread_mutex_lock(&batchts_mtx);
//	printf("starting batch timer\n");
//	batch_ts = get_sys_clock();	
//	pthread_mutex_unlock(&batchts_mtx);
//}

void Sequencer::process_new_txn(base_client_query * query) {
	// Make sure we are not swapping queues
	pthread_mutex_lock(&mtx);
	while (swapping_queues) {
		pthread_cond_wait(&swap_cv, &mtx);
	}
	num_accessing_queues++;
	pthread_mutex_unlock(&mtx);

	// Add new txn to fill queue
	fill_queue->add_entry((void *)query);
	ATOM_FETCH_ADD(total_txns_received,1);
	//printf("txns received: %lu\n", total_txns_received);
	//printf("txns finished: %lu\n", total_txns_finished);
	//printf("waiting for %u txns\n", wait_txns_left);
	//printf("fill queue size: %lu\n",fill_queue->size());
	//printf("batch queue size: %lu\n",batch_queue->size());
	//printf("wait queue size: %lu\n",wait_list_size);
	//printf("next batch no: %lu\n",next_batch_id);

#if DEBUG_DIST
	printf("Sequencer: added new txn to fill queue\n");
#endif

	// Unlock and signal if no threads are accessing the queues
	pthread_mutex_lock(&mtx);
	num_accessing_queues--;
	if (num_accessing_queues == 0)
		pthread_cond_signal(&access_cv);
	pthread_mutex_unlock(&mtx);
}

void Sequencer::send_first_batch(uint64_t thd_id) {
	bool send_batch = false;
	pthread_mutex_lock(&batchts_mtx);
	if (!sent_first_batch && fill_queue->size() > 0) {
		send_batch = true;
		sent_first_batch = true;
	}
	pthread_mutex_unlock(&batchts_mtx);

	if (send_batch) {
		prepare_next_batch(thd_id);
	}

}

void Seq_thread_t::init(uint64_t thd_id, uint64_t node_id, workload * workload) {
	_thd_id = thd_id;
	_node_id = node_id;
	_wl = workload;
}

uint64_t Seq_thread_t::get_thd_id() { return _thd_id; }
uint64_t Seq_thread_t::get_node_id() { return _node_id; }

RC Seq_thread_t::run_remote() {
	printf("Run_remote %ld:%ld\n",_node_id, _thd_id);
#if !NOGRAPHITE
	_thd_id = CarbonGetTileId();
#endif
	if (warmup_finish) {
		mem_allocator.register_thread(_thd_id);
	}
	pthread_barrier_wait( &warmup_bar );
	stats.init(get_thd_id());

	// Send init messages
	if( _thd_id == 0) {
		uint64_t total_nodes = g_node_cnt + g_client_node_cnt + 1;

		for(uint64_t i = 0; i < total_nodes; i++) {
			if(i != g_node_id) {
				rem_qry_man.send_init_done(i);
			}
		}
	}

	base_client_query * m_query = NULL;

	// Send start msg to all nodes; wait for rsp from all nodes before continuing.
	//int rsp_cnt = g_node_cnt + g_client_node_cnt;
	while(seq_man.rsp_cnt > 0) {
		m_query = (base_client_query *) tport_man.recv_msg();
		if (m_query != NULL) {
			switch(m_query->rtype) {
				case INIT_DONE:
					ATOM_SUB(seq_man.rsp_cnt,1);
					break;
				case RTXN:
					// Query from client
					seq_man.process_new_txn(m_query);
					break;
				case RACK:
					// Ack from server
					seq_man.process_txn_ack(m_query,get_thd_id());
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

	//if (_thd_id == 0)
	//	seq_man.start_batch_timer();

	while (true) {
		if (!seq_man.sent_first_batch)
			seq_man.send_first_batch(get_thd_id());
		m_query = (base_client_query *) tport_man.recv_msg();
		if( m_query != NULL ) { 
			rq_time = get_sys_clock();
			//assert(m_query->dest_id == g_node_id);
			//assert(m_query->return_id < g_node_id);
#if DEBUG_DISTR
			printf("Received message from %lu, rtype = %d\n", m_query->return_id,
					(int)m_query->rtype);
#endif
			switch (m_query->rtype) {
				case RTXN:
					// Query from client
					seq_man.process_new_txn(m_query);
					break;
				case RACK:
					// Ack from server
					seq_man.process_txn_ack(m_query,get_thd_id());
					break;
				default:
					assert(false);
			}
		}
		uint64_t tend = get_sys_clock(); 
		if (warmup_finish && _wl->sim_done && ((tend - rq_time) > MSG_TIMEOUT)) {
			if( !ATOM_CAS(_wl->sim_timeout, false, true) )
				assert( _wl->sim_timeout);
		}

		// End conditions
//		if (_wl->sim_done && _wl->sim_timeout) {
//#if !NOGRAPHITE
//			CarbonDisableModelsBarrier(&enable_barrier);
//#endif
//			uint64_t currtime = get_sys_clock();
//			if(currtime - stoptime > MSG_TIMEOUT) {
//				SET_STATS(get_thd_id(), tot_run_time, currtime - run_starttime - MSG_TIMEOUT); 
//			}
//			else {
//				SET_STATS(get_thd_id(), tot_run_time, stoptime - run_starttime); 
//			}
//			return FINISH;
//		}

		uint64_t txns_fin = ATOM_FETCH_ADD(seq_man.total_txns_finished,0);
		uint64_t txns_rec = ATOM_FETCH_ADD(seq_man.total_txns_received,0);
		if (warmup_finish && txns_fin == txns_rec && 
				txns_fin >= MAX_TXN_PER_PART * g_node_cnt) {
			if( !ATOM_CAS(_wl->sim_done, false, true) )
				assert( _wl->sim_done);
			//stoptime = get_sys_clock();
		}
		if (_wl->sim_done && _wl->sim_timeout) {
#if !NOGRAPHITE
			CarbonDisableModelsBarrier(&enable_barrier);
#endif
			return FINISH;
		}
	}
}

