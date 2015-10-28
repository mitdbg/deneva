#include "global.h"
#include "sequencer.h"
//#include "query.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "mem_alloc.h"
#include "transport.h"
#include "wl.h"
#include "helper.h"
#include "msg_queue.h"
#include "msg_thread.h"
#include "query_work_queue.h"

void Sequencer::init(workload * wl) {
	wait_list = NULL;
  batch_size = 0;
  next_batch_size = 0;
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
  /*
#if WORKLOAD == YCSB
	node_queries = (ycsb_query*) mem_allocator.alloc(sizeof(ycsb_client_query)*g_node_cnt,0);
	for (uint32_t i = 0; i < g_node_cnt; ++i) {
		node_queries[i].requests = (ycsb_request *) mem_allocator.alloc(sizeof(ycsb_request) * g_req_per_query, 0);
	}
#elif WORKLOAD == TPCC
	node_queries = (tpcc_query*) mem_allocator.alloc(sizeof(tpcc_client_query)*g_node_cnt,0);
	printf("Add sequencer support for TPCC!\n");
	assert(false);
#endif
	for (uint32_t i = 0; i < g_node_cnt; ++i) {
		node_queries[i].part_to_access = (uint64_t *) mem_allocator.alloc(sizeof(uint64_t)*1, 0);
		node_queries[i].part_to_access[0] = i;
		node_queries[i].part_num = 1;
  }
  */
}

void Sequencer::process_txn_ack(base_query *query, uint64_t thd_id) {
	assert(wait_list != NULL);
	assert(wait_txns_left > 0);
	// TODO: fix this hack
	uint64_t id = query->txn_id;
	//uint64_t id = query->return_id;
	assert(wait_list[id].server_ack_cnt > 0);

	// Decrement the number of acks needed for this txn
	uint32_t query_acks_left = ATOM_SUB_FETCH(wait_list[id].server_ack_cnt, 1);
	uint64_t txns_left = UINT64_MAX;
	if (query_acks_left == 0) {
		txns_left = ATOM_SUB_FETCH(wait_txns_left, 1);
		ATOM_FETCH_ADD(total_txns_finished,1);
		INC_STATS(thd_id,txn_cnt,1);
	}

	// If we have all acks for this batch, send qry responses to all clients
	if (txns_left == 0) {
		for (uint32_t i = 0; i < batch_size; ++i) {
			qlite next = wait_list[i];
      base_query * m_query = (base_query *) mem_allocator.alloc(sizeof(base_query),0);
      m_query->client_id = next.client_id;
      m_query->client_startts = next.client_startts;
      msg_queue.enqueue(m_query,CL_RSP,m_query->client_id);
		}
		mem_allocator.free(wait_list,sizeof(qlite)*batch_size);
		wait_list = NULL;
		prepare_next_batch(thd_id);
	}
}

void Sequencer::prepare_next_batch(uint64_t thd_id) {
	while (fill_queue.size_approx() == 0 && !_wl->sim_done) {}

	// Lock fill & batch queues for swap
	pthread_mutex_lock(&mtx);
	swapping_queues = true;
	while (num_accessing_queues > 0) {
		pthread_cond_wait(&access_cv, &mtx);
	}
	assert(num_accessing_queues == 0);
	pthread_mutex_unlock(&mtx);

	// Swap fill & batch queues
  batch_size = next_batch_size;
  next_batch_size = 0;

	// Release queries and signal
	pthread_mutex_lock(&mtx);
	swapping_queues = false;
	pthread_cond_signal(&swap_cv);
	pthread_mutex_unlock(&mtx);

	// Create new wait list
	wait_list_size = batch_size;
	wait_list = (qlite *) mem_allocator.alloc(sizeof(qlite) * wait_list_size, 0);
	wait_txns_left = wait_list_size;

	// Bookkeeping info for preparing and sending queries to nodes
	base_query * query;
	bool * participating_nodes = (bool*)mem_allocator.alloc(sizeof(bool)*g_node_cnt,0);
	reset_participating_nodes(participating_nodes);


	// TODO: incrementing next_txn_id & next_batch_id is thread-safe
	// only if a single thread is doing the processing.
	next_txn_id = 0;
	//uint64_t batch_id = next_batch_id++;
	for (uint32_t i = 0; i < batch_size; ++i) {
		assert(fill_queue.try_dequeue(query));
		assert(query != NULL);
		txnid_t txn_id = next_txn_id++;
		uint32_t server_ack_cnt = 0;
#if WORKLOAD == YCSB
		// reset request counts
    /*
		for (uint64_t j = 0; j < g_node_cnt; ++j)
			node_queries[j].request_cnt = 0;
      */
    //FIXME
		ycsb_query * m_query = (ycsb_query *) query;
    server_ack_cnt = m_query->participants(participating_nodes,_wl);
    m_query->txn_id = txn_id;

#elif WORKLOAD == TPCC
		printf("Add sequencer support for TPCC!\n");
		assert(false);
#endif
		assert(server_ack_cnt > 0);
		wait_list[i].client_id = query->return_id;
		assert(query->return_id != g_node_id && query->return_id >= g_node_cnt);
		wait_list[i].client_startts = query->client_startts;
		wait_list[i].server_ack_cnt = server_ack_cnt;

		// update all node queries with necessary query information
		for (uint64_t j = 0; j < g_node_cnt; ++j) {
			if (participating_nodes[j]) {
        /*
				node_queries[j].rtype = RTXN;
				node_queries[j].pid = j;
        msg_queue.enqueue(&node_queries[j],RTXN,j);
        */
        msg_queue.enqueue(query,RTXN,j);
			}
		}

    mem_allocator.free(participating_nodes,sizeof(bool)*g_node_cnt);

	}
	// Cleanup
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

void Sequencer::process_new_txn(base_query * query) {
	// Make sure we are not swapping queues
	pthread_mutex_lock(&mtx);
	while (swapping_queues) {
		pthread_cond_wait(&swap_cv, &mtx);
	}
	num_accessing_queues++;
	pthread_mutex_unlock(&mtx);

	// Add new txn to fill queue
	fill_queue.enqueue(query);
	ATOM_ADD(next_batch_size,1);
	ATOM_ADD(total_txns_received,1);

  DEBUG("FILL rec %lu, fin %lu, wait %u; fill %u, nbatch %lu,batch %lu; nb# %lu\n"
      ,total_txns_received, total_txns_finished,wait_txns_left,(uint32_t)fill_queue.size_approx(),next_batch_size
      ,batch_size,next_batch_id);

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
	if (!sent_first_batch && fill_queue.size_approx() > 0) {
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

RC Seq_thread_t::run_recv() {
	printf("Run_recv %ld:%ld\n",_node_id, _thd_id);
  mem_allocator.register_thread(_thd_id);
	pthread_barrier_wait( &warmup_bar );
	stats.init(get_thd_id());
  while(!_wl->sim_init_done) {
    tport_man.recv_msg();
  }
	pthread_barrier_wait( &warmup_bar );
	myrand rdm;
	rdm.init(get_thd_id());
	RC rc = RCOK;
	assert (rc == RCOK);

	uint64_t run_starttime = get_sys_clock();
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

RC Seq_thread_t::run_send() {
	printf("Run_send %ld:%ld\n",_node_id, _thd_id);
  fflush(stdout);
  mem_allocator.register_thread(_thd_id);
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

RC Seq_thread_t::run_remote() {
	printf("Run_remote %ld:%ld\n",_node_id, _thd_id);
	if (warmup_finish) {
		mem_allocator.register_thread(_thd_id);
	}
	pthread_barrier_wait( &warmup_bar );
	stats.init(get_thd_id());
	base_query * m_query = NULL;
#if WORKLOAD == YCSB
    m_query = new ycsb_query;
#elif WORKLOAD == TPCC
    m_query = new tpcc_query;
#endif

	// Send init messages
	if( _thd_id == 0) {
		uint64_t total_nodes = g_node_cnt + g_client_node_cnt + 1;

		for(uint64_t i = 0; i < total_nodes; i++) {
			if(i != g_node_id) {
        msg_queue.enqueue(NULL,INIT_DONE,i);
			}
		}

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
    }
	}
	pthread_barrier_wait( &warmup_bar );
	printf("Run_remote %ld:%ld\n",_node_id, _thd_id);

	myrand rdm;
	rdm.init(get_thd_id());

	//if (_thd_id == 0)
	//	seq_man.start_batch_timer();
  bool got_qry;
  uint64_t thd_prof_start;
	uint64_t run_starttime = get_sys_clock();

	while (true) {

		if (!seq_man.sent_first_batch)
			seq_man.send_first_batch(get_thd_id());

    thd_prof_start = get_sys_clock();

		while(!(got_qry = work_queue.dequeue(_thd_id, m_query)) 
                && !_wl->sim_done && !_wl->sim_timeout) { }

    INC_STATS(_thd_id,thd_prof_thd1c,get_sys_clock() - thd_prof_start);
    thd_prof_start = get_sys_clock();

    if((get_sys_clock() - run_starttime >= g_done_timer)
        ||(_wl->sim_done && _wl->sim_timeout)) { 
          if( !ATOM_CAS(_wl->sim_done, false, true) ) {
        assert( _wl->sim_done);
      } else {
        printf("_wl->sim_done=%d\n",_wl->sim_done);
        fflush(stdout);
      }
      if(get_thd_id() == 0) {
        work_queue.finish(get_sys_clock());
      }
    }

    if(!got_qry)
      continue;

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
    uint64_t thd_prof_end = get_sys_clock();
    INC_STATS(_thd_id,thd_prof_thd2,thd_prof_end - thd_prof_start);
    INC_STATS(_thd_id,thd_prof_thd2_type[m_query->rtype],thd_prof_end - thd_prof_start);

	}
}

