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

void Sequencer::init(Workload * wl) {
  next_txn_id = 0;
	rsp_cnt = g_node_cnt + g_client_node_cnt;
	_wl = wl;
  last_time_batch = 0;
  fill_queue = new moodycamel::ConcurrentQueue<BaseQuery*,moodycamel::ConcurrentQueueDefaultTraits>[g_node_cnt];
  wl_head = NULL;
  wl_tail = NULL;
}

// FIXME: Assumes 1 thread does sequencer work
void Sequencer::process_ack(BaseQuery *query, uint64_t thd_id) {
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

    BaseQuery * m_query;// = wait_list[id].qry;
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
void Sequencer::process_txn(BaseQuery * query) {

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
		YCSBQuery * m_query = (YCSBQuery *) query;
#elif WORKLOAD == TPCC
		TPCCQuery * m_query = (TPCCQuery *) query;
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
      BaseQuery * m_query;
      qry_pool.get(m_query);
      m_query->deep_copy(query);
      DEBUG_R("^^got seq rtxn (%ld,%ld) %d 0x%lx\n",m_query->txn_id,m_query->batch_id,m_query->rtype,(uint64_t)m_query);
			if (participating_nodes[j]) {
        fill_queue[j].enqueue(m_query);
			}
		}
  mem_allocator.free(participating_nodes,sizeof(bool)*g_node_cnt);

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

  BaseQuery * query;
  for(uint64_t j = 0; j < g_node_cnt; j++) {
    while(fill_queue[j].try_dequeue(query)) {
      assert(query->rtype == RTXN);
      if(j == g_node_id) {
        work_queue.enqueue(thd_id,query,false);
      } else {
        msg_queue.enqueue(query,RTXN,j);
      }
    }
    DEBUG("Seq RDONE %ld\n",_wl->epoch)
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

