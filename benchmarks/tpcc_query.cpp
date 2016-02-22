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

#include "query.h"
#include "tpcc_query.h"
#include "tpcc.h"
#include "tpcc_helper.h"
#include "mem_alloc.h"
#include "wl.h"
#include "table.h"

BaseQuery * TPCCQueryGenerator::create_query(Workload * h_wl,uint64_t home_partition_id) {
  double x = (double)(rand() % 100) / 100.0;
	if (x < g_perc_payment)
		return gen_payment(home_partition_id);
	else 
		return gen_new_order(home_partition_id);

}

uint64_t TPCCQuery::participants(bool *& pps,Workload * wl) {
  int n = 0;
  for(uint64_t i = 0; i < g_node_cnt; i++)
    pps[i] = false;
  uint64_t id;

  switch(txn_type) {
    case TPCC_PAYMENT:
      id = GET_NODE_ID(wh_to_part(w_id));
      if(!pps[id]) {
        pps[id] = true;
        n++;
      }
      id = GET_NODE_ID(wh_to_part(c_w_id));
      if(!pps[id]) {
        pps[id] = true;
        n++;
      }
      break;
    case TPCC_NEW_ORDER: 
      id = GET_NODE_ID(wh_to_part(w_id));
      if(!pps[id]) {
        pps[id] = true;
        n++;
      }
      /*
      id = GET_NODE_ID(wh_to_part(c_w_id));
      if(!pps[id]) {
        pps[id] = true;
        n++;
      }
      id = GET_NODE_ID(wh_to_part(d_w_id));
      if(!pps[id]) {
        pps[id] = true;
        n++;
      }
      */
      for(uint64_t i = 0; i < ol_cnt; i++) {
        uint64_t req_nid = GET_NODE_ID(wh_to_part(items[i]->ol_supply_w_id));
        if(!pps[req_nid]) {
          pps[req_nid] = true;
          n++;
        }
      }
      break;
    default: assert(false);
  }

  return n;
}

bool TPCCQuery::readonly() {
  return false;
}

BaseQuery * TPCCQueryGenerator::gen_payment(uint64_t home_partition) {
  TPCCQuery * query = new TPCCQuery;
	set<uint64_t> partitions_accessed;

	query->txn_type = TPCC_PAYMENT;
  uint64_t home_warehouse;
	if (FIRST_PART_LOCAL) {
    while(wh_to_part(home_warehouse = URand(1, g_num_wh)) != home_partition) {}
  }
	else
		home_warehouse = URand(1, g_num_wh);
  query->w_id =  home_warehouse;
	query->d_w_id = home_warehouse;

  partitions_accessed.insert(wh_to_part(query->w_id));

	query->d_id = URand(1, g_dist_per_wh);
	query->h_amount = URand(1, 5000);
  query->rbk = false;
	double x = (double)(rand() % 10000) / 10000;
	int y = URand(1, 100);

	//if(x > g_mpr) { 
	if(x > 0.15) { 
		// home warehouse
		query->c_d_id = query->d_id;
		query->c_w_id = query->w_id;
	} else {	
		// remote warehouse
		query->c_d_id = URand(1, g_dist_per_wh);
		if(g_num_wh > 1) {
			while((query->c_w_id = URand(1, g_num_wh)) == query->w_id) {}
			if (wh_to_part(query->w_id) != wh_to_part(query->c_w_id)) {
        partitions_accessed.insert(wh_to_part(query->c_w_id));
			}
		} else 
      query->c_w_id = query->w_id;
	}
	if(y <= 60) {
		// by last name
		query->by_last_name = true;
		Lastname(NURand(255,0,999),query->c_last);
	} else {
		// by cust id
		query->by_last_name = false;
		query->c_id = NURand(1023, 1, g_cust_per_dist);
	}

  return query;
}

BaseQuery * TPCCQueryGenerator::gen_new_order(uint64_t home_partition) {
  TPCCQuery * query = new TPCCQuery;
	set<uint64_t> partitions_accessed;

	query->txn_type = TPCC_NEW_ORDER;
	if (FIRST_PART_LOCAL) {
    while(wh_to_part(query->w_id = URand(1, g_num_wh)) != home_partition) {}
  }
	else
		query->w_id = URand(1, g_num_wh);

	query->d_id = URand(1, g_dist_per_wh);
	query->c_id = NURand(1023, 1, g_cust_per_dist);
  // FIXME: No rollback
	//rbk = URand(1, 100) == 1 ? true : false;
	query->rbk = false;
	query->ol_cnt = URand(5, g_max_items_per_txn);
	query->o_entry_d = 2013;

  partitions_accessed.insert(wh_to_part(query->w_id));

  double r_mpr = (double)(rand() % 10000) / 10000;
  uint64_t part_limit;
  if(r_mpr < g_mpr)
    part_limit = g_part_per_txn;
  else
    part_limit = 1;

  std::set<uint64_t> ol_i_ids;
  while(query->items.size() < query->ol_cnt) {
      Item_no * item = new Item_no;

    while(ol_i_ids.count( item->ol_i_id = NURand(8191, 1, g_max_items)) > 0) {}
    ol_i_ids.insert(item->ol_i_id);
    item->ol_quantity = URand(1, 10);
    double r_rem = (double)(rand() % 100000) / 100000;
		if (r_rem > 0.01 || r_mpr > g_mpr || g_num_wh == 1) {
			// home warehouse
			item->ol_supply_w_id = query->w_id;
    } else {
      if(partitions_accessed.size() < part_limit) {
        item->ol_supply_w_id = URand(1, g_num_wh);
        partitions_accessed.insert(wh_to_part(item->ol_supply_w_id));
      } else {
        // select warehouse from among those already selected
        while( partitions_accessed.count(wh_to_part(item->ol_supply_w_id = URand(1, g_num_wh))) > 0) {}
      }
    }

    query->items.push_back(item);
  }

  return query;

}

