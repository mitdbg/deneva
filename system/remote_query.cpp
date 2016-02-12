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

#include "remote_query.h"
#include "mem_alloc.h"
#include "tpcc.h"
#include "ycsb.h"
#include "tpcc_query.h"
#include "ycsb_query.h"
#include "query.h"
#include "transport.h"
#include "plock.h"
#include "msg_queue.h"
#include "message.h"

void Remote_query::init(uint64_t node_id, Workload * wl) {
	q_idx = 0;
	_node_id = node_id;
	_wl = wl;
  pthread_mutex_init(&mtx,NULL);
}

TxnManager * Remote_query::get_txn_man(uint64_t thd_id, uint64_t node_id, uint64_t txn_id) {

  TxnManager * next_txn = NULL;

  return next_txn;
}

void Remote_query::unmarshall(void * d, uint64_t len) {
    BaseQuery * query;
	char * data = (char *) d;
	uint64_t ptr = 0;
  uint32_t dest_id = UINT32_MAX;
  uint32_t return_id = UINT32_MAX;
  uint32_t tot_txn_cnt __attribute__((unused));
  uint32_t txn_cnt = 0;
  uint64_t starttime = get_sys_clock();
  assert(len > sizeof(uint32_t) * 3);

  COPY_VAL(dest_id,data,ptr);
  COPY_VAL(return_id,data,ptr);
  COPY_VAL(txn_cnt,data,ptr);
  assert(dest_id == g_node_id);
  assert(ISCLIENTN(return_id) || ISSERVERN(return_id));
  //DEBUG("Received batch %d txns from %d\n",txn_cnt,return_id);

  tot_txn_cnt = txn_cnt;
  while(txn_cnt > 0) { 
    qry_pool.get(query);
    assert(query);
    Message * msg = Message::create_message(&data[ptr]);
    ptr += msg->get_size();
    msg->copy_to_query(query);
    query->return_id = return_id;


    if(query->rtype == INIT_DONE) {
      printf("Processed INIT_DONE from %ld\n",query->return_id);
      fflush(stdout);
      assert(query->return_id < g_node_cnt + g_client_node_cnt);
      simulation->process_setup_msg();
    }
    DEBUG_R("^^got (%ld,%ld) %d 0x%lx\n",query->txn_id,query->batch_id,query->rtype,(uint64_t)query);
#if MODE==SIMPLE_MODE
    if(query->rtype == RTXN) {
      query->txn_id = g_node_id;
      msg_queue.enqueue(Message::create_message(query,CL_RSP),query->client_id);
		  INC_STATS(0,txn_cnt,1);
      ATOM_ADD(_wl->txn_cnt,1);
    } else {
      work_queue.enqueue(g_thread_cnt,query,false);
    }
#else
    if(query->rtype != INIT_DONE) {
      if(ISCLIENT) {
        work_queue.enqueue(g_client_thread_cnt,query,false);
      } else {
        work_queue.enqueue(g_thread_cnt,query,false);
      }
    }
#endif
    txn_cnt--;
    query->time_copy += get_sys_clock() - starttime;
  }
}
