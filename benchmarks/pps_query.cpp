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

#include "query.h"
#include "pps_query.h"
#include "pps.h"
#include "pps_helper.h"
#include "mem_alloc.h"
#include "wl.h"
#include "table.h"
#include "message.h"

BaseQuery * PPSQueryGenerator::create_query(Workload * h_wl,uint64_t home_partition_id) {
  double x = (double)(rand() % 100) / 100.0;
	if (x < g_perc_parts)
		return gen_parts(home_partition_id);

}

void PPSQuery::init(uint64_t thd_id, Workload * h_wl) {
  BaseQuery::init();
}

void PPSQuery::init() {
  BaseQuery::init();
}

void PPSQuery::print() {
  
}

std::set<uint64_t> PPSQuery::participants(Message * msg, Workload * wl) {
  std::set<uint64_t> participant_set;
  PPSClientQueryMessage* pps_msg = ((PPSClientQueryMessage*)msg);
  uint64_t id;

  id = GET_NODE_ID(wh_to_part(pps_msg->w_id));
  participant_set.insert(id);

  switch(pps_msg->txn_type) {
    case PPS_PAYMENT:
      id = GET_NODE_ID(wh_to_part(pps_msg->c_w_id));
      participant_set.insert(id);
      break;
    case PPS_NEW_ORDER: 
      break;
    default: assert(false);
  }

  return participant_set;
}

uint64_t PPSQuery::participants(bool *& pps,Workload * wl) {
  int n = 0;
  for(uint64_t i = 0; i < g_node_cnt; i++)
    pps[i] = false;
  uint64_t id;

  switch(txn_type) {
    case PPS_PAYMENT:
      break;
    case PPS_NEW_ORDER: 
      break;
    default: assert(false);
  }

  return n;
}

bool PPSQuery::readonly() {
  return false;
}

BaseQuery * PPSQueryGenerator::gen_parts(uint64_t home_partition) {
  PPSQuery * query = new PPSQuery;
	set<uint64_t> partitions_accessed;

	query->txn_type = PPS_PARTS;
  uint64_t home_warehouse;
	if (FIRST_PART_LOCAL) {
    while(wh_to_part(home_warehouse = URand(1, g_num_wh)) != home_partition) {}
  }
	else
		home_warehouse = URand(1, g_num_wh);

  partitions_accessed.insert(wh_to_part(query->w_id));

  query->partitions.init(partitions_accessed.size());
  for(auto it = partitions_accessed.begin(); it != partitions_accessed.end(); ++it) {
    query->partitions.add(*it);
  }
  return query;
}

uint64_t PPSQuery::get_participants(Workload * wl) {
   uint64_t participant_cnt = 0;
   uint64_t active_cnt = 0;
  assert(participant_nodes.size()==0);
  assert(active_nodes.size()==0);
  for(uint64_t i = 0; i < g_node_cnt; i++) {
      participant_nodes.add(0);
      active_nodes.add(0);
  }
  assert(participant_nodes.size()==g_node_cnt);
  assert(active_nodes.size()==g_node_cnt);

  uint64_t home_wh_node;
  home_wh_node = GET_NODE_ID(wh_to_part(w_id));
  participant_nodes.set(home_wh_node,1);
  active_nodes.set(home_wh_node,1);
  participant_cnt++;
  active_cnt++;
  if(txn_type == PPS_PAYMENT) {
      uint64_t req_nid = GET_NODE_ID(wh_to_part(c_w_id));
      if(participant_nodes[req_nid] == 0) {
        participant_cnt++;
        participant_nodes.set(req_nid,1);
        active_cnt++;
        active_nodes.set(req_nid,1);
      }

  } else if (txn_type == PPS_NEW_ORDER) {
    for(uint64_t i = 0; i < ol_cnt; i++) {
      uint64_t req_nid = GET_NODE_ID(wh_to_part(items[i]->ol_supply_w_id));
      if(participant_nodes[req_nid] == 0) {
        participant_cnt++;
        participant_nodes.set(req_nid,1);
        active_cnt++;
        active_nodes.set(req_nid,1);
      }
    }
  }
  return participant_cnt;
}

void PPSQuery::reset() {
  BaseQuery::clear();
}

void PPSQuery::release() {
  BaseQuery::release();
  DEBUG_M("PPSQuery::release() free\n");
}

void PPSQuery::release_items() {
  // A bit of a hack to ensure that original requests in client query queue aren't freed
  if(SERVER_GENERATE_QUERIES)
    return;
}
