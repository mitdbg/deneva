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
#include "thread.h"
#include "client_thread.h"
#include "query.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "client_query.h"
#include "mem_alloc.h"
#include "transport.h"
#include "client_txn.h"
#include "msg_thread.h"
#include "msg_queue.h"
#include "wl.h"
#include "message.h"

void ClientThread::setup() {
	if( _thd_id == 0) {
		uint64_t nnodes = g_node_cnt + g_client_node_cnt + g_node_cnt*g_repl_cnt;
		for(uint64_t i = 0; i < nnodes; i++) {
			if(i != g_node_id) {
        msg_queue.enqueue(Message::create_message(INIT_DONE),i);
			}
		}
  }
#if LOAD_METHOD == LOAD_RATE
  assert(g_load_per_server > 0);
  // send ~twice as frequently due to delays in context switching
  send_interval = (g_client_thread_cnt * BILLION) / g_load_per_server / 1.8;
  printf("Client interval: %ld\n",send_interval);
#endif

}

RC ClientThread::run() {

  tsetup();
  BaseQuery * m_query;
	uint64_t iters = 0;
	uint32_t num_txns_sent = 0;
	int txns_sent[g_servers_per_client];
  for (uint32_t i = 0; i < g_servers_per_client; ++i)
      txns_sent[i] = 0;

	run_starttime = get_sys_clock();
	uint64_t prog_time = run_starttime;

  while(!simulation->is_done()) {
		//uint32_t next_node = iters++ % g_node_cnt;
		if(get_sys_clock() - run_starttime >= g_done_timer) {
      break;
    }
    int32_t inf_cnt;
		uint32_t next_node = (((iters++) * g_client_thread_cnt) + _thd_id )% g_servers_per_client;
		uint32_t next_node_id = next_node + g_server_start_node;
		// Just in case...
		if (iters == UINT64_MAX)
			iters = 0;
#if LOAD_METHOD == LOAD_MAX
		if ((inf_cnt = client_man.inc_inflight(next_node)) < 0)
			continue;

		m_query = client_query_queue.get_next_query(next_node,_thd_id);
    if(last_send_time > 0) {
      INC_STATS(get_thd_id(),cl_send_intv,get_sys_clock() - last_send_time);
    }
    last_send_time = get_sys_clock();
#elif LOAD_METHOD == LOAD_RATE
		if ((inf_cnt = client_man.inc_inflight(next_node)) < 0)
			continue;
    uint64_t gate_time;
    while((gate_time = get_sys_clock()) - last_send_time < send_interval) { }
    if(last_send_time > 0) {
      INC_STATS(get_thd_id(),cl_send_intv,gate_time - last_send_time);
    }
    last_send_time = gate_time;
		m_query = client_query_queue.get_next_query(next_node,_thd_id);
#else
    assert(false);
#endif
    assert(m_query);

		DEBUG("Client: thread %lu sending query to node: %u, %d\n",
				_thd_id, next_node_id,inf_cnt);

    Message * msg = Message::create_message((BaseQuery*)m_query,CL_QRY);
    ((ClientQueryMessage*)msg)->client_startts = get_sys_clock();
    msg_queue.enqueue(msg,next_node_id);
		num_txns_sent++;
		txns_sent[next_node]++;
    INC_STATS(get_thd_id(),txn_sent_cnt,1);

		if(get_sys_clock() - prog_time >= g_prog_timer) {
			prog_time = get_sys_clock();
			SET_STATS(get_thd_id(), total_runtime, prog_time - run_starttime); 
      if(get_thd_id() == 0)
        stats.print_client(true);
    }
		if(get_sys_clock() - run_starttime >= g_done_timer) {
      break;
    }
	}


	for (uint64_t l = 0; l < g_servers_per_client; ++l)
		printf("Txns sent to node %lu: %d\n", l+g_server_start_node, txns_sent[l]);

  prog_time = get_sys_clock();
  SET_STATS(get_thd_id(), total_runtime, prog_time - run_starttime); 

  printf("FINISH %ld:%ld\n",_node_id,_thd_id);
  fflush(stdout);
	return FINISH;
}
