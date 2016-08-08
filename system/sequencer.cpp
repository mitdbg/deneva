/*
   Copyright 2016 Massachusetts Institute of Technology

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
#include "work_queue.h"
#include "message.h"
#include <boost/lockfree/queue.hpp>

void Sequencer::init(Workload * wl) {
  next_txn_id = 0;
	rsp_cnt = g_node_cnt + g_client_node_cnt;
	_wl = wl;
  last_time_batch = 0;
  wl_head = NULL;
  wl_tail = NULL;
  fill_queue = new boost::lockfree::queue<Message*, boost::lockfree::capacity<65526> > [g_node_cnt];
}

// FIXME: Assumes 1 thread does sequencer work
void Sequencer::process_ack(Message * msg, uint64_t thd_id) {
  qlite_ll * en = wl_head;
  while(en != NULL && en->epoch != msg->get_batch_id()) {
    en = en->next;
  }
  assert(en);
  qlite * wait_list = en->list;
	assert(wait_list != NULL);
	assert(en->txns_left > 0);

	uint64_t id = msg->get_txn_id() / g_node_cnt;
  uint64_t prof_stat = get_sys_clock();
	assert(wait_list[id].server_ack_cnt > 0);

	// Decrement the number of acks needed for this txn
	uint32_t query_acks_left = ATOM_SUB_FETCH(wait_list[id].server_ack_cnt, 1);

	if (query_acks_left == 0) {
    en->txns_left--;
		ATOM_FETCH_ADD(total_txns_finished,1);
		INC_STATS(thd_id,seq_txn_cnt,1);
    // free msg, queries
#if WORKLOAD == YCSB
  YCSBClientQueryMessage* cl_msg = (YCSBClientQueryMessage*)wait_list[id].msg;
  for(uint64_t i = 0; i < cl_msg->requests.size(); i++) {
    DEBUG_M("Sequencer::process_ack() ycsb_request free\n");
    mem_allocator.free(cl_msg->requests[i],sizeof(ycsb_request));
  }
#elif WORKLOAD == TPCC
  TPCCClientQueryMessage* cl_msg = (TPCCClientQueryMessage*)wait_list[id].msg;
  if(cl_msg->txn_type == TPCC_NEW_ORDER) {
    for(uint64_t i = 0; i < cl_msg->items.size(); i++) {
      DEBUG_M("Sequencer::process_ack() items free\n");
      mem_allocator.free(cl_msg->items[i],sizeof(Item_no));
    }
  }
#endif
    cl_msg->release();

    ClientResponseMessage * rsp_msg = (ClientResponseMessage*)Message::create_message(msg->get_txn_id(),CL_RSP);
    rsp_msg->client_startts = wait_list[id].client_startts;
    msg_queue.enqueue(thd_id,rsp_msg,wait_list[id].client_id);
    INC_STATS(thd_id,seq_complete_cnt,1);

	}

	// If we have all acks for this batch, send qry responses to all clients
	if (en->txns_left == 0) {
    DEBUG("FINISHED BATCH %ld\n",en->epoch);
    LIST_REMOVE_HT(en,wl_head,wl_tail);
    mem_allocator.free(en->list,sizeof(qlite) * en->max_size);
    mem_allocator.free(en,sizeof(qlite_ll));

	}
  INC_STATS(thd_id,seq_ack_time,get_sys_clock() - prof_stat);
}

// FIXME: Assumes 1 thread does sequencer work
void Sequencer::process_txn( Message * msg,uint64_t thd_id) {

  uint64_t starttime = get_sys_clock();
  DEBUG("SEQ Processing msg\n");
  qlite_ll * en = wl_tail;

  // FIXME: LL is potentially a bottleneck here
  if(!en || en->epoch != simulation->get_seq_epoch()+1) {
    DEBUG("SEQ new wait list for epoch %ld\n",simulation->get_seq_epoch()+1);
    // First txn of new wait list
    en = (qlite_ll *) mem_allocator.alloc(sizeof(qlite_ll));
    en->epoch = simulation->get_seq_epoch()+1;
    en->max_size = 1000;
    en->size = 0;
    en->txns_left = 0;
    en->list = (qlite *) mem_allocator.alloc(sizeof(qlite) * en->max_size);
    LIST_PUT_TAIL(wl_head,wl_tail,en)
  }
  if(en->size == en->max_size) {
    en->max_size *= 2;
    en->list = (qlite *) mem_allocator.realloc(en->list,sizeof(qlite) * en->max_size);
  }

		txnid_t txn_id = g_node_id + g_node_cnt * next_txn_id;
    next_txn_id++;
    uint64_t id = txn_id / g_node_cnt;
    msg->batch_id = en->epoch;
    msg->txn_id = txn_id;
    assert(txn_id != UINT64_MAX);

#if WORKLOAD == YCSB
    std::set<uint64_t> participants = YCSBQuery::participants(msg,_wl);
#elif WORKLOAD == TPCC
    std::set<uint64_t> participants = TPCCQuery::participants(msg,_wl);
#endif
		uint32_t server_ack_cnt = participants.size();
		assert(server_ack_cnt > 0);
		assert(ISCLIENTN(msg->get_return_id()));
		en->list[id].client_id = msg->get_return_id();
		en->list[id].client_startts = ((ClientQueryMessage*)msg)->client_startts;
		en->list[id].server_ack_cnt = server_ack_cnt;
		en->list[id].msg = msg;
    en->size++;
    en->txns_left++;
    // Note: Modifying msg!
    msg->return_node_id = g_node_id;
    assert(en->size == en->txns_left);
    assert(en->size <= ((uint64_t)g_inflight_max * g_node_cnt));

    // Add new txn to fill queue
    for(auto participant = participants.begin(); participant != participants.end(); participant++) {
      DEBUG("SEQ adding (%ld,%ld) to fill queue\n",msg->get_txn_id(),msg->get_batch_id());
      while(!fill_queue[*participant].push(msg) && !simulation->is_done()) {}
    }

	INC_STATS(thd_id,seq_process_cnt,1);
	INC_STATS(thd_id,seq_process_time,get_sys_clock() - starttime);
	ATOM_ADD(total_txns_received,1);

}


// FIXME: Assumes 1 thread does sequencer work
void Sequencer::send_next_batch(uint64_t thd_id) {
  uint64_t prof_stat = get_sys_clock();
  qlite_ll * en = wl_tail;
  bool empty = true;
  if(en && en->epoch == simulation->get_seq_epoch()) {
    DEBUG("SEND NEXT BATCH %ld [%ld,%ld] %ld\n",thd_id,simulation->get_seq_epoch(),en->epoch,en->size);
    empty = false;
  }

  Message * msg;
  for(uint64_t j = 0; j < g_node_cnt; j++) {
    while(fill_queue[j].pop(msg)) {
      if(j == g_node_id) {
        work_queue.sched_enqueue(thd_id,msg);
      } else {
        msg_queue.enqueue(thd_id,msg,j);
      }
    }
    if(!empty) {
      DEBUG("Seq RDONE %ld\n",simulation->get_seq_epoch())
    }
    msg = Message::create_message(RDONE);
    msg->batch_id = simulation->get_seq_epoch(); 
    if(j == g_node_id) {
      work_queue.sched_enqueue(thd_id,msg);
    } else {
      msg_queue.enqueue(thd_id,msg,j);
    }
  }

  if(last_time_batch > 0) {
    INC_STATS(thd_id,seq_batch_time,get_sys_clock() - last_time_batch);
  }
  last_time_batch = get_sys_clock();

	INC_STATS(thd_id,seq_batch_cnt,1);
  if(!empty) {
    INC_STATS(thd_id,seq_full_batch_cnt,1);
  }
  INC_STATS(thd_id,seq_prep_time,get_sys_clock() - prof_stat);
  next_txn_id = 0;
}

