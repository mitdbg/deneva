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
#include "ycsb_query.h"
#include "mem_alloc.h"
#include "wl.h"
#include "ycsb.h"
#include "table.h"
#include "helper.h"

uint64_t YCSBQueryGenerator::the_n = 0;
double YCSBQueryGenerator::denom = 0;

BaseQuery * YCSBQueryGenerator::create_query(Workload * h_wl, uint64_t home_partition_id) {
	mrand = (myrand *) mem_allocator.alloc(sizeof(myrand));
	mrand->init(get_sys_clock());
	zeta_2_theta = zeta(2, g_zipf_theta);
  BaseQuery * query;
  if (SKEW_METHOD == HOT) {
    query = gen_requests_hot(home_partition_id, h_wl);
  } else if (SKEW_METHOD == ZIPF){
    query = gen_requests_zipf(home_partition_id, h_wl);
  }

  return query;
}

void YCSBQuery::print() {
  
    printf("YCSBQuery: %ld %ld %ld %ld %ld %ld %ld %ld %ld %ld\n"
        ,GET_NODE_ID(requests[0]->key)
        ,GET_NODE_ID(requests[1]->key)
        ,GET_NODE_ID(requests[2]->key)
        ,GET_NODE_ID(requests[3]->key)
        ,GET_NODE_ID(requests[4]->key)
        ,GET_NODE_ID(requests[5]->key)
        ,GET_NODE_ID(requests[6]->key)
        ,GET_NODE_ID(requests[7]->key)
        ,GET_NODE_ID(requests[8]->key)
        ,GET_NODE_ID(requests[9]->key)
        );
}

void YCSBQuery::init() {
  requests.init(g_req_per_query);
  BaseQuery::init();
}
void YCSBQuery::release() {
  BaseQuery::release();
  DEBUG_M("YCSBQuery::release() free\n");
  for(uint64_t i = 0; i < requests.size(); i++) {
    DEBUG_M("YCSBQuery::release() ycsb_request free\n");
    mem_allocator.free(requests[i],sizeof(ycsb_request));
  }
  requests.release();
}

uint64_t YCSBQuery::participants(bool *& pps,Workload * wl) {
  int n = 0;
  for(uint64_t i = 0; i < g_node_cnt; i++)
    pps[i] = false;

  for(uint64_t i = 0; i < requests.size(); i++) {
    uint64_t req_nid = GET_NODE_ID(((YCSBWorkload*)wl)->key_to_part(requests[i]->key));
    if(!pps[req_nid])
      n++;
    pps[req_nid] = true;
  }
  return n;
}

bool YCSBQuery::readonly() {
  for(uint64_t i = 0; i < requests.size(); i++) {
    if(requests[i]->acctype == WR) {
      return false;
    }
  }
  return true;
}

// The following algorithm comes from the paper:
// Quickly generating billion-record synthetic databases
// However, it seems there is a small bug. 
// The original paper says zeta(theta, 2.0). But I guess it should be 
// zeta(2.0, theta).
double YCSBQueryGenerator::zeta(uint64_t n, double theta) {
	double sum = 0;
	for (uint64_t i = 1; i <= n; i++) 
		sum += pow(1.0 / i, theta);
	return sum;
}

uint64_t YCSBQueryGenerator::zipf(uint64_t n, double theta) {
	assert(this->the_n == n);
	assert(theta == g_zipf_theta);
	double alpha = 1 / (1 - theta);
	double zetan = denom;
	double eta = (1 - pow(2.0 / n, 1 - theta)) / 
		(1 - zeta_2_theta / zetan);
//	double eta = (1 - pow(2.0 / n, 1 - theta)) / 
//		(1 - zeta_2_theta / zetan);
	double u = (double)(mrand->next() % 10000000) / 10000000;
	double uz = u * zetan;
	if (uz < 1) return 1;
	if (uz < 1 + pow(0.5, theta)) return 2;
	return 1 + (uint64_t)(n * pow(eta*u -eta + 1, alpha));
}


BaseQuery * YCSBQueryGenerator::gen_requests_hot(uint64_t home_partition_id, Workload * h_wl) {
  YCSBQuery * query = (YCSBQuery*) mem_allocator.alloc(sizeof(YCSBQuery));
  query->requests.init(g_req_per_query);

	uint64_t access_cnt = 0;
	set<uint64_t> all_keys;
	set<uint64_t> partitions_accessed;
  double r_mpt = (double)(mrand->next() % 10000) / 10000;		
  uint64_t part_limit;
  if(r_mpt < g_mpr)
    part_limit = g_part_per_txn;
  else
    part_limit = 1;
  //uint64_t hot_key_max = g_synth_table_size * g_data_perc;
  uint64_t hot_key_max = (uint64_t)g_data_perc;
  double r_twr = (double)(mrand->next() % 10000) / 10000;		

	int rid = 0;
	for (UInt32 i = 0; i < g_req_per_query; i ++) {		
		double r = (double)(mrand->next() % 10000) / 10000;		
    double hot =  (double)(mrand->next() % 10000) / 10000;
    uint64_t partition_id;
		ycsb_request * req = (ycsb_request*) mem_allocator.alloc(sizeof(ycsb_request));
		if (r_twr < g_txn_read_perc || r < g_tup_read_perc) 
			req->acctype = RD;
		else
			req->acctype = WR;

    uint64_t row_id = 0; 
    if ( FIRST_PART_LOCAL && rid == 0) {
      if(hot < g_access_perc) {
        row_id = (uint64_t)(mrand->next() % (hot_key_max/g_part_cnt)) * g_part_cnt + home_partition_id;
      } else {
        uint64_t nrand = (uint64_t)mrand->next();
        row_id = ((nrand % (g_synth_table_size/g_part_cnt - (hot_key_max/g_part_cnt))) + hot_key_max/g_part_cnt) * g_part_cnt + home_partition_id;
      }

      partition_id = row_id % g_part_cnt;
      assert(row_id % g_part_cnt == home_partition_id);
    }
    else {
      while(1) {
        if(hot < g_access_perc) {
          row_id = (uint64_t)(mrand->next() % hot_key_max);
        } else {
          row_id = ((uint64_t)(mrand->next() % (g_synth_table_size - hot_key_max))) + hot_key_max;
        }
        partition_id = row_id % g_part_cnt;

        if(partitions_accessed.count(partition_id) > 0)
          break;
        else {
          if(g_strict_ppt && partitions_accessed.size() < part_limit && (partitions_accessed.size() + (g_req_per_query - rid) >= part_limit))
            continue;
          break;
        }
      }
    }
    partitions_accessed.insert(partition_id);
		assert(row_id < g_synth_table_size);
		uint64_t primary_key = row_id;
		//uint64_t part_id = row_id % g_part_cnt;
		req->key = primary_key;
		req->value = mrand->next() % (1<<8);
		// Make sure a single row is not accessed twice
		if (all_keys.find(req->key) == all_keys.end()) {
			all_keys.insert(req->key);
			access_cnt ++;
		} else {
      // Need to have the full g_req_per_query amount
      i--;
      continue;
    }
		rid ++;

    //query->requests.push_back(*req);
    query->requests.add(req);
	}
  assert(query->requests.size() == g_req_per_query);
	// Sort the requests in key order.
	if (g_key_order) {
    for(uint64_t i = 0; i < query->requests.size(); i++) {
      for(uint64_t j = query->requests.size() - 1; j > i ; j--) {
        if(query->requests[j]->key < query->requests[j-1]->key) {
          query->requests.swap(j,j-1);
        }
      }
    }
    //std::sort(query->requests.begin(),query->requests.end(),[](ycsb_request lhs, ycsb_request rhs) { return lhs.key < rhs.key;});
	}
  query->partitions.init(partitions_accessed.size());
  for(auto it = partitions_accessed.begin(); it != partitions_accessed.end(); ++it) {
    query->partitions.add(*it);
  }

  return query;

}

BaseQuery * YCSBQueryGenerator::gen_requests_zipf(uint64_t home_partition_id, Workload * h_wl) {
  YCSBQuery * query = (YCSBQuery*) mem_allocator.alloc(sizeof(YCSBQuery));
  query->requests.init(g_req_per_query);

	uint64_t access_cnt = 0;
	set<uint64_t> all_keys;
	set<uint64_t> partitions_accessed;
  /*
  double r_mpt = (double)(mrand->next() % 10000) / 10000;		
  uint64_t part_limit;
  if(r_mpt < g_mpr)
    part_limit = g_part_per_txn;
  else
    part_limit = 1;
    */

  double r_twr = (double)(mrand->next() % 10000) / 10000;		

	int rid = 0;
	for (UInt32 i = 0; i < g_req_per_query; i ++) {		
		double r = (double)(mrand->next() % 10000) / 10000;		
    uint64_t partition_id;
		ycsb_request * req = (ycsb_request*) mem_allocator.alloc(sizeof(ycsb_request));
		if (r_twr < g_txn_read_perc || r < g_tup_read_perc) 
			req->acctype = RD;
		else
			req->acctype = WR;
    uint64_t row_id = 0; 

    partitions_accessed.insert(partition_id);
		assert(row_id < g_synth_table_size);
		uint64_t primary_key = row_id;
		//uint64_t part_id = row_id % g_part_cnt;
		req->key = primary_key;
		req->value = mrand->next() % (1<<8);
		// Make sure a single row is not accessed twice
		if (all_keys.find(req->key) == all_keys.end()) {
			all_keys.insert(req->key);
			access_cnt ++;
		} else {
      // Need to have the full g_req_per_query amount
      i--;
      continue;
    }
		rid ++;

    //query->requests.push_back(*req);
    query->requests.add(req);
	}
  assert(query->requests.size() == g_req_per_query);
	// Sort the requests in key order.
	if (g_key_order) {
    for(uint64_t i = 0; i < query->requests.size(); i++) {
      for(uint64_t j = query->requests.size() - 1; j > i ; j--) {
        if(query->requests[j]->key < query->requests[j-1]->key) {
          query->requests.swap(j,j-1);
        }
      }
    }
    //std::sort(query->requests.begin(),query->requests.end(),[](ycsb_request lhs, ycsb_request rhs) { return lhs.key < rhs.key;});
	}
  query->partitions.init(partitions_accessed.size());
  for(auto it = partitions_accessed.begin(); it != partitions_accessed.end(); ++it) {
    query->partitions.add(*it);
  }

  return query;

}
