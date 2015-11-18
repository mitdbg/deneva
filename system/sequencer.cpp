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
  next_txn_id = 0;
  batch_size = 0;
  next_batch_size = 0;
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
  send_batch = true;
  last_time_batch = 0;
  /*
  fill_queue = (moodycamel::ConcurrentQueue<base_query*,moodycamel::ConcurrentQueueDefaultTraits> *)
                  mem_allocator.alloc(sizeof(moodycamel::ConcurrentQueue<base_query*,moodycamel::ConcurrentQueueDefaultTraits>) * g_node_cnt, 0);
  for(uint64_t i = 0; i < g_node_cnt; i++) {
    fill_queue[i] = new moodycamel::ConcurrentQueue<base_query*,moodycamel::ConcurrentQueueDefaultTraits>;
  }
  */
  fill_queue = new moodycamel::ConcurrentQueue<base_query*,moodycamel::ConcurrentQueueDefaultTraits>[g_node_cnt];
  wl_head = NULL;
  wl_tail = NULL;
}

// FIXME: Assumes 1 thread does sequencer work
void Sequencer::process_ack(base_query *query, uint64_t thd_id) {
  qlite_ll * en = wl_head;
  while(en != NULL && en->epoch != query->batch_id) {
    en = en->next;
  }
  assert(en);
  qlite * wait_list = en->list;
	assert(wait_list != NULL);
	assert(en->txns_left > 0);

	uint64_t id = query->txn_id / g_node_cnt;
	uint64_t rid = query->return_id;
  uint64_t prof_stat = get_sys_clock();
	//uint64_t id = query->return_id;
	assert(wait_list[id].server_ack_cnt > 0);

	// Decrement the number of acks needed for this txn
	uint32_t query_acks_left = ATOM_SUB_FETCH(wait_list[id].server_ack_cnt, 1);

  //if(query->return_id != g_node_id)
  //  qry_pool.put(query);

	if (query_acks_left == 0) {
    en->txns_left--;
		ATOM_FETCH_ADD(total_txns_finished,1);
		INC_STATS(thd_id,seq_txn_cnt,1);

    base_query * m_query;// = wait_list[id].qry;
    qry_pool.get(m_query);
    DEBUG_R("^^got seq cl_rsp 0x%lx\n",(uint64_t)m_query);
    m_query->client_id = wait_list[id].client_id;
    m_query->client_startts = wait_list[id].client_startts;
    msg_queue.enqueue(m_query,CL_RSP,m_query->client_id);

	}
  DEBUG("ACK %ld (%ld,%ld) from %ld: %d %d 0x%lx\n",id,query->txn_id,en->epoch,rid,query_acks_left,en->txns_left,(uint64_t)query);


	// If we have all acks for this batch, send qry responses to all clients
	if (en->txns_left == 0) {
    DEBUG("FINISHED BATCH %ld\n",en->epoch);
    LIST_REMOVE_HT(en,wl_head,wl_tail);
    mem_allocator.free(en->list,sizeof(qlite) * en->max_size);
    mem_allocator.free(en,sizeof(qlite_ll));

	}
  INC_STATS(thd_id,time_seq_ack,get_sys_clock() - prof_stat);
}

// FIXME: Assumes 1 thread does sequencer work
void Sequencer::process_txn(base_query * query) {

  qlite_ll * en = wl_tail;

  if(!en || en->epoch != _wl->epoch+1) {
    // First txn of new wait list
    en = (qlite_ll *) mem_allocator.alloc(sizeof(qlite_ll), 0);
    en->epoch = _wl->epoch+1;
    en->max_size = 1000;
    en->size = 0;
    en->txns_left = 0;
    en->list = (qlite *) mem_allocator.alloc(sizeof(qlite) * en->max_size, 0);
    LIST_PUT_TAIL(wl_head,wl_tail,en)
  }
  if(en->size == en->max_size) {
    en->max_size *= 2;
    en->list = (qlite *) mem_allocator.realloc(en->list,sizeof(qlite) * en->max_size, 0);
  }

		txnid_t txn_id = g_node_id + g_node_cnt * next_txn_id;
    next_txn_id++;
    uint64_t id = txn_id / g_node_cnt;
    query->batch_id = en->epoch;
    query->txn_id = txn_id;
    assert(txn_id != UINT64_MAX);
#if WORKLOAD == YCSB
		ycsb_query * m_query = (ycsb_query *) query;
#elif WORKLOAD == TPCC
		tpcc_query * m_query = (tpcc_query *) query;
#endif
    bool * participating_nodes = (bool*)mem_allocator.alloc(sizeof(bool)*g_node_cnt,0);
    reset_participating_nodes(participating_nodes);
		uint32_t server_ack_cnt = m_query->participants(participating_nodes,_wl);
		assert(server_ack_cnt > 0);
		assert(ISCLIENTN(query->return_id));
		en->list[id].client_id = query->return_id;
		en->list[id].client_startts = query->client_startts;
		en->list[id].server_ack_cnt = server_ack_cnt;
		//en->list[id].qry = query;
    en->size++;
    en->txns_left++;
    query->return_id = g_node_id;
    assert(en->size == en->txns_left);
    assert(en->size <= ((uint64_t)g_inflight_max * g_node_cnt));

    // Add new txn to fill queue
		for (uint64_t j = 0; j < g_node_cnt; ++j) {
      // Make several deep copies of this query
      base_query * m_query;
      qry_pool.get(m_query);
      m_query->deep_copy(query);
      DEBUG_R("^^got seq rtxn (%ld,%ld) %d 0x%lx\n",m_query->txn_id,m_query->batch_id,m_query->rtype,(uint64_t)m_query);
			if (participating_nodes[j]) {
        fill_queue[j].enqueue(m_query);
			}
		}
  mem_allocator.free(participating_nodes,sizeof(bool)*g_node_cnt);

	ATOM_ADD(next_batch_size,1);
	ATOM_ADD(total_txns_received,1);
  DEBUG("FILL %ld %ld, %ld %ld\n",en->epoch,query->txn_id,total_txns_received,total_txns_finished)

}


// FIXME: Assumes 1 thread does sequencer work
void Sequencer::send_next_batch(uint64_t thd_id) {
  uint64_t prof_stat = get_sys_clock();
  qlite_ll * en = wl_tail;
  if(en && en->epoch == _wl->epoch) {
    DEBUG("SEND NEXT BATCH %ld [%ld,%ld] %ld\n",thd_id,_wl->epoch,en->epoch,en->size);
  }

  base_query * query;
  for(uint64_t j = 0; j < g_node_cnt; j++) {
    while(fill_queue[j].try_dequeue(query)) {
      assert(query->rtype == RTXN);
      if(j == g_node_id) {
        work_queue.enqueue(thd_id,query,false);
      } else {
        msg_queue.enqueue(query,RTXN,j);
      }
    }
    qry_pool.get(query);
    query->rtype = RDONE;
    query->return_id = g_node_id;
    query->batch_id = _wl->epoch;
    if(j == g_node_id)
      work_queue.enqueue(thd_id,query,false);
    else
      msg_queue.enqueue(query,RDONE,j);
  }

  if(last_time_batch > 0) {
    INC_STATS(thd_id,time_seq_batch,get_sys_clock() - last_time_batch);
  }
  last_time_batch = get_sys_clock();

	INC_STATS(thd_id,seq_batch_cnt,1);
  INC_STATS(thd_id,time_seq_prep,get_sys_clock() - prof_stat);
  next_txn_id = 0;
}

void Sequencer::reset_participating_nodes(bool * part_nodes) {
	for (uint32_t i = 0; i < g_node_cnt; ++i)
		part_nodes[i] = false;
}

/*
void Sequencer::process_txn_ack(base_query *query, uint64_t thd_id) {
  qlite_ll * en = wl_head;
  while(en != NULL && en->epoch != query->batch_id)
    en = en->next;
  assert(en != NULL);
	assert(en->list != NULL);
	assert(en->txns_left > 0);
	// TODO: fix this hack
	uint64_t id = query->txn_id - start_txn_id;
	//uint64_t id = query->return_id;
	assert(en->list[id].server_ack_cnt > 0);

	// Decrement the number of acks needed for this txn
	uint32_t query_acks_left = en->list[id].server_ack_cnt--;
	uint64_t txns_left = UINT64_MAX;
	if (query_acks_left == 0) {
		txns_left = ATOM_SUB_FETCH(wait_txns_left, 1);
		ATOM_FETCH_ADD(total_txns_finished,1);
		INC_STATS(thd_id,txn_cnt,1);
	}
  DEBUG("ACK %ld: %d %ld\n",id,query_acks_left,txns_left);

	// If we have all acks for this batch, send qry responses to all clients
	if (txns_left == 0) {
    uint64_t prof_stat = get_sys_clock();
    DEBUG("FINISH BATCH %ld %ld\n",thd_id,batch_size);
		for (uint32_t i = 0; i < batch_size; ++i) {
			qlite next = wait_list[i];
      base_query * m_query = (base_query *) mem_allocator.alloc(sizeof(base_query),0);
      m_query->client_id = next.client_id;
      m_query->client_startts = next.client_startts;
      msg_queue.enqueue(m_query,CL_RSP,m_query->client_id);
		}
		mem_allocator.free(wait_list,sizeof(qlite)*batch_size);
		wait_list = NULL;

    INC_STATS(thd_id,time_seq_ack,get_sys_clock() - prof_stat);
    ATOM_CAS(send_batch,false,true);
		//prepare_next_batch(thd_id);
	}
}

void Sequencer::prepare_next_batch(uint64_t thd_id) {
	//while (fill_queue.size_approx() == 0 && !_wl->sim_done) {}
  if(next_batch_size == 0)
    return;
  if(!ATOM_CAS(send_batch,true,false))
    return;
  // Only 1 thread should be here
  uint64_t prof_stat = get_sys_clock();

  assert(next_batch_size > 0);
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
  DEBUG("PREPARE NEXT BATCH %ld %ld \n",thd_id,batch_size);
  if(last_time_batch > 0) {
    INC_STATS(thd_id,time_batch,get_sys_clock() - last_time_batch);
  }
  last_time_batch = get_sys_clock();

	// Release queries and signal
	pthread_mutex_lock(&mtx);
	swapping_queues = false;
	pthread_cond_signal(&swap_cv);
	pthread_mutex_unlock(&mtx);

	// Create new wait list
	wait_list_size = batch_size;
	wait_list = (qlite *) mem_allocator.alloc(sizeof(qlite) * wait_list_size, 0);
  //printf("New batch: %ld -- %ld %lx\n",wait_list_size,sizeof(qlite) * wait_list_size,(uint64_t)wait_list);
	wait_txns_left = wait_list_size;

	// Bookkeeping info for preparing and sending queries to nodes
	base_query * query;
	bool * participating_nodes = (bool*)mem_allocator.alloc(sizeof(bool)*g_node_cnt,0);
	reset_participating_nodes(participating_nodes);


	// TODO: incrementing next_txn_id & next_batch_id is thread-safe
	// only if a single thread is doing the processing.
	//next_txn_id = 0;
  start_txn_id = next_txn_id;
	uint64_t batch_id = next_batch_id++;
	for (uint32_t i = 0; i < batch_size; ++i) {
		assert(fill_queue.try_dequeue(query));
		assert(query != NULL);
		txnid_t txn_id = next_txn_id++;
    query->batch_id = batch_id;
    query->txn_id = txn_id;
		uint32_t server_ack_cnt = 0;
#if WORKLOAD == YCSB
		ycsb_query * m_query = (ycsb_query *) query;

#elif WORKLOAD == TPCC
		tpcc_query * m_query = (tpcc_query *) query;
#endif
    server_ack_cnt = m_query->participants(participating_nodes,_wl);
		assert(server_ack_cnt > 0);
		wait_list[i].client_id = query->return_id;
		assert(query->return_id != g_node_id && query->return_id >= g_node_cnt);
		wait_list[i].client_startts = query->client_startts;
		wait_list[i].server_ack_cnt = server_ack_cnt;

		// update all node queries with necessary query information
		for (uint64_t j = 0; j < g_node_cnt; ++j) {
			if (participating_nodes[j]) {
				node_queries[j].pid = j;
        msg_queue.enqueue(query,RTXN,j);
			}
		}


	}
	// Cleanup
  mem_allocator.free(participating_nodes,sizeof(bool)*g_node_cnt);
	INC_STATS(thd_id,batch_cnt,1);
  INC_STATS(thd_id,time_seq_prep,get_sys_clock() - prof_stat);
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
      ,batch_size,seq_man.next_batch_id);

	// Unlock and signal if no threads are accessing the queues
	pthread_mutex_lock(&mtx);
	num_accessing_queues--;
	if (num_accessing_queues == 0)
		pthread_cond_signal(&access_cv);
	pthread_mutex_unlock(&mtx);
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
  warmup_done = true;
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
	uint64_t prog_time = run_starttime;

	while (true) {


    thd_prof_start = get_sys_clock();

		if(get_thd_id() == 0) {
      uint64_t now_time = get_sys_clock();
      if (now_time - prog_time >= g_prog_timer) {
        prog_time = now_time;
        SET_STATS(get_thd_id(), tot_run_time, prog_time - run_starttime); 

        stats.print_sequencer(true);
      }
		}


		while(!(got_qry = work_queue.dequeue(_thd_id, m_query)) 
                && !seq_man.send_batch
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
      printf("FINISH %ld:%ld\n",_node_id,_thd_id);
      fflush(stdout);
			return FINISH;
    }

    if(seq_man.send_batch)
      seq_man.prepare_next_batch(_thd_id);

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

*/
