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
#include "io_thread.h"
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
#include "client_txn.h"
#include "client_query.h"

void InputThread::setup() {

  while(!simulation->is_setup_done()) {
    tport_man.recv_msg();
    if(ISCLIENT) {
      check_for_init_done();
    }
  }
}

RC InputThread::run() {
  tsetup();

  if(ISCLIENT) {
    client_recv_loop();
  } else {
    server_recv_loop();
  }

  return FINISH;

}

RC InputThread::client_recv_loop() {
	int rsp_cnts[g_servers_per_client];
	memset(rsp_cnts, 0, g_servers_per_client * sizeof(int));

	run_starttime = get_sys_clock();
  uint64_t return_node_offset;
  uint64_t inf;
  BaseQuery * m_query = NULL;

	while (!simulation->is_done()) {
		tport_man.recv_msg();
    //while((m_query = work_queue.get_next_query(get_thd_id())) != NULL) {
    while(work_queue.dequeue(0,m_query)) { 
			assert(m_query->rtype == CL_RSP);
			assert(m_query->dest_id == g_node_id);
			switch (m_query->rtype) {
				case CL_RSP:
          return_node_offset = m_query->return_id - g_server_start_node;
          assert(return_node_offset < g_servers_per_client);
		      rsp_cnts[return_node_offset]++;
					inf = client_man.dec_inflight(return_node_offset);
          assert(inf >=0);
					break;
				default:
					assert(false);
			}
      qry_pool.put(m_query);
    }

	}

  printf("FINISH %ld:%ld\n",_node_id,_thd_id);
  fflush(stdout);
  return FINISH;
}

RC InputThread::server_recv_loop() {

	myrand rdm;
	rdm.init(get_thd_id());
	RC rc = RCOK;
	assert (rc == RCOK);

  uint64_t thd_prof_start;
	while (!simulation->is_done()) {
    thd_prof_start = get_sys_clock();
    if(tport_man.recv_msg()) {
      INC_STATS(_thd_id,rthd_prof_1,get_sys_clock() - thd_prof_start);
    } else {
      INC_STATS(_thd_id,rthd_prof_2,get_sys_clock() - thd_prof_start);
    }

	}
  printf("FINISH %ld:%ld\n",_node_id,_thd_id);
  fflush(stdout);
  return FINISH;
}

void InputThread::check_for_init_done() {
  BaseQuery * m_query = NULL;
    while(work_queue.dequeue(0,m_query)) { 
      assert(m_query->rtype == INIT_DONE);
      printf("Received INIT_DONE from node %ld\n",m_query->return_id);
      fflush(stdout);
      simulation->process_setup_msg();
    }
}

void OutputThread::setup() {
  messager->init(_thd_id);
	while (!simulation->is_setup_done()) {
    messager->run();
  }
}

RC OutputThread::run() {

  tsetup();

	while (!simulation->is_done()) {
    messager->run();
  }

  printf("FINISH %ld:%ld\n",_node_id,_thd_id);
  fflush(stdout);
  return FINISH;
}


