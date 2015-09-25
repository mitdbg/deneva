#include "global.h"
#include "manager.h"
#include "calvin_thread.h"
#include "txn.h"
#include "wl.h"
#include "query.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "mem_alloc.h"
#include "test.h"
#include "transport.h"
#include "remote_query.h"
#include "math.h"
#include "msg_queue.h"

void calvin_thread_t::init(uint64_t thd_id, uint64_t node_id, workload * workload) {
	_thd_id = thd_id;
	_node_id = node_id;
	_wl = workload;
  current_batch_id = 0;
}

uint64_t calvin_thread_t::get_thd_id() { return _thd_id; }
uint64_t calvin_thread_t::get_node_id() { return _node_id; }
uint64_t calvin_thread_t::get_host_cid() {	return _host_cid; }
void calvin_thread_t::set_host_cid(uint64_t cid) { _host_cid = cid; }
uint64_t calvin_thread_t::get_cur_cid() { return _cur_cid; }
void calvin_thread_t::set_cur_cid(uint64_t cid) {_cur_cid = cid; }

RC calvin_thread_t::run_remote() {
	printf("Run_remote (calvin_thread) %ld:%ld\n",_node_id, _thd_id);
	if (warmup_finish) {
		mem_allocator.register_thread(_thd_id);
	}

	base_query * m_query = NULL;

	pthread_barrier_wait( &warmup_bar );
	stats.init(get_thd_id());
	// Send start msg to all nodes; wait for rsp from all nodes (incl. sequencer) before continuing.
	int rsp_cnt = g_node_cnt + g_client_node_cnt;
	while(rsp_cnt > 0) {
		m_query = (base_query *) tport_man.recv_msg();
		if(m_query != NULL && m_query->rtype == INIT_DONE) {
			rsp_cnt --;
		} else if(m_query != NULL) {
      assert(m_query->part_cnt > 0 || m_query->part_num > 0);
      if(m_query->part_num > 0)
			  work_queue.add_query(m_query->part_to_access[0]/g_node_cnt,m_query);
      if(m_query->part_cnt > 0)
			  work_queue.add_query(m_query->parts[0]/g_node_cnt,m_query);
		}
	}
	pthread_barrier_wait( &warmup_bar );
	printf("Run_remote (calvin_thread) %ld:%ld\n",_node_id, _thd_id);

	myrand rdm;
	rdm.init(get_thd_id());
	RC rc = RCOK;
	assert (rc == RCOK);

	ts_t rq_time = get_sys_clock();

	while (true) {
		m_query = (base_query *) tport_man.recv_msg();
		if( m_query != NULL ) { 
			rq_time = get_sys_clock();
      assert(m_query->part_cnt > 0 || m_query->part_num > 0);
      if(m_query->part_num > 0)
			  work_queue.add_query(m_query->part_to_access[0]/g_node_cnt,m_query);
      if(m_query->part_cnt > 0)
			  work_queue.add_query(m_query->parts[0]/g_node_cnt,m_query);
		}

		ts_t tend = get_sys_clock();
		if (warmup_finish && _wl->sim_done && ((tend - rq_time) > MSG_TIMEOUT)) {
			if( !ATOM_CAS(_wl->sim_timeout, false, true) )
				assert( _wl->sim_timeout);
		}

		if (_wl->sim_done && _wl->sim_timeout) {
			return FINISH;
		}

	}

}


RC calvin_thread_t::run() {
	printf("Run %ld:%ld (calvin_thread)\n",_node_id, _thd_id);
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

	if( _thd_id == 0) {
	  uint64_t rsp_cnt = g_node_cnt + g_client_node_cnt + 1;
		for(uint64_t i = 0; i < rsp_cnt; i++) {
			if(i != g_node_id) {
				//rem_qry_man.send_init_done(i);
        msg_queue.enqueue(m_query,INIT_DONE,i);
			}
		}
	}
	pthread_barrier_wait( &warmup_bar );
	//sleep(4);
	printf("Run %ld:%ld (calvin_thread)\n",_node_id, _thd_id);

	myrand rdm;
	rdm.init(get_thd_id());
	RC rc = RCOK;

	txn_man * m_txn;
	rc = _wl->get_txn_man(m_txn);
	assert (rc == RCOK);
	glob_manager.set_txn_man(m_txn);


	uint64_t txn_cnt = 0;
	uint64_t txn_st_cnt = 0;

	uint64_t starttime;
	uint64_t stoptime = 0;
	uint64_t timespan;

	uint64_t run_starttime = get_sys_clock();
	uint64_t prog_time = run_starttime;

	while(true) {

		if(get_sys_clock() - prog_time >= PROG_TIMER) {
			prog_time = get_sys_clock();
			SET_STATS(get_thd_id(), tot_run_time, prog_time - run_starttime); 

			stats.print(true);
		}
		while(!work_queue.poll_next_query(_thd_id) && !(_wl->sim_done && _wl->sim_timeout)) { }

    // End conditions
		if (_wl->sim_done && _wl->sim_timeout) {
			uint64_t currtime = get_sys_clock();
			if(currtime - stoptime > MSG_TIMEOUT) {
				SET_STATS(get_thd_id(), tot_run_time, currtime - run_starttime - MSG_TIMEOUT); 
			}
			else {
				SET_STATS(get_thd_id(), tot_run_time, stoptime - run_starttime); 
			}
			return FINISH;
		}

		if((m_query = work_queue.get_next_query(_thd_id)) == NULL)
			continue;

		rc = RCOK;
		starttime = get_sys_clock();

		switch(m_query->rtype) {
      case RTXN:
				INC_STATS(0,rtxn,1);

        base_query * tmp_query;
				txn_table.get_txn(g_node_id,m_query->txn_id,m_txn,tmp_query);
        if(m_txn == NULL) {
					ATOM_ADD(txn_st_cnt,1);
					rc = _wl->get_txn_man(m_txn);
					assert(rc == RCOK);
					m_txn->set_txn_id(m_query->txn_id);
					txn_table.add_txn(g_node_id,m_txn,m_query);
        }
				m_txn->abort_cnt = 0;
        //m_txn->set_txn_id(m_query->txn_id);
				m_txn->starttime = get_sys_clock();
#if DEBUG_TIMELINE
					printf("START %ld %ld\n",m_txn->get_txn_id(),m_txn->starttime);
#endif
        // Acquire locks
        rc = m_txn->acquire_locks(m_query);
        // Execute
				rc = m_txn->run_txn(m_query);
        // Release locks
        //rc = m_txn->release_locks(m_query);
        assert(rc == RCOK);
				rc = m_txn->finish(rc,m_query->part_to_access,m_query->part_num);
				break;
		  default:
			  assert(false);
    }

    if(rc == RCOK) {
			INC_STATS(get_thd_id(),txn_cnt,1);
			timespan = get_sys_clock() - m_txn->starttime;
			INC_STATS(get_thd_id(), run_time, timespan);
			INC_STATS(get_thd_id(), latency, timespan);
			txn_table.delete_txn(g_node_id,m_query->txn_id);
			txn_cnt++;
      // FIXME
			//rem_qry_man.send_client_rsp(m_query);
			//rem_qry_man.ack_response(m_query);
      msg_queue.enqueue(m_query,RACK,m_query->return_id);
    }

		timespan = get_sys_clock() - starttime;
		INC_STATS(get_thd_id(),time_work,timespan);

		if (!warmup_finish && txn_st_cnt >= WARMUP / g_thread_cnt) 
		{
			stats.clear( get_thd_id() );
			return FINISH;
		}

		if (warmup_finish && txn_st_cnt >= MAX_TXN_PER_PART/g_thread_cnt && txn_table.empty(get_node_id())) {
			//assert(txn_cnt == MAX_TXN_PER_PART);
			if( !ATOM_CAS(_wl->sim_done, false, true) )
				assert( _wl->sim_done);
			stoptime = get_sys_clock();
		}


  }
}
