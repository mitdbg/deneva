#include "query.h"
#include "ycsb_query.h"
#include "mem_alloc.h"
#include "wl.h"
#include "ycsb.h"
#include "table.h"
#include "helper.h"

uint64_t ycsb_client_query::the_n = 0;
double ycsb_client_query::denom = 0;

void ycsb_query::init(uint64_t thd_id, workload * h_wl) {
    init();
}

void ycsb_client_query::client_init(uint64_t thd_id, workload * h_wl, uint64_t node_id) {
	mrand = (myrand *) mem_allocator.alloc(sizeof(myrand), thd_id);
	mrand->init(get_sys_clock());
  // FIXME for HSTORE/SPEC, need to generate queries to more than 1 part per node
	pid = GET_PART_ID(0,node_id);
#if CC_ALG==HSTORE || CC_ALG==HSTORE_SPEC
	UInt32 r = mrand->next() % (g_part_cnt/g_node_cnt);
  pid = node_id + r * g_node_cnt;
#endif
	requests = (ycsb_request *) 
		mem_allocator.alloc(sizeof(ycsb_request) * g_req_per_query, thd_id);
	part_to_access = (uint64_t *) 
		mem_allocator.alloc(sizeof(uint64_t) * g_part_per_txn, thd_id);
	zeta_2_theta = zeta(2, g_zipf_theta);
#if GEN_BY_MPR
	if (the_n == 0) {
		uint64_t table_size = g_synth_table_size / g_part_cnt;
		//uint64_t table_size = g_synth_table_size / g_virtual_part_cnt;
		the_n = table_size - 1;
		denom = zeta(the_n, g_zipf_theta);
	}
	gen_requests(pid, h_wl);
#else
  if(SKEW_METHOD == ZIPF) {
    if (the_n == 0) {
      //uint64_t table_size = g_synth_table_size / g_part_cnt;
      uint64_t table_size = g_synth_table_size;
      the_n = table_size - 1;
      denom = zeta(the_n, g_zipf_theta);
    }
    gen_requests2(pid, h_wl);
  } else if (SKEW_METHOD == HOT) {
    gen_requests3(pid, h_wl);
  } else
    assert(false);
#endif

}

void ycsb_client_query::client_init() {
	requests = (ycsb_request *) 
		mem_allocator.alloc(sizeof(ycsb_request) * g_req_per_query, 0);
	part_to_access = (uint64_t *) 
		mem_allocator.alloc(sizeof(uint64_t) * g_part_per_txn, 0);
}

void ycsb_query::init() {
  time_q_abrt = 0;
  time_q_work = 0;
  time_copy = 0;
}

void ycsb_query::reset() {
  txn_rtype = YCSB_0;
  rid = 0;
  req = requests[0];
}

uint64_t ycsb_query::participants(bool *& pps,workload * wl) {
  int n = 0;
  for(uint64_t i = 0; i < g_node_cnt; i++)
    pps[i] = false;

  for(uint64_t i = 0; i < request_cnt; i++) {
    uint64_t req_nid = GET_NODE_ID(((ycsb_wl*)wl)->key_to_part(requests[i].key));
    if(!pps[req_nid])
      n++;
    pps[req_nid] = true;
  }
  return n;
}

base_query * ycsb_query::merge(base_query * query) {
	ycsb_query * m_query = (ycsb_query *) query;
  this->rtype = m_query->rtype;
  switch(m_query->rtype) {
    case RINIT:
      assert(false);
      break;
    case RPREPARE:
      if(m_query->rc == Abort)
        this->rc = Abort;
      break;
    case RQRY:
      assert(m_query->pid == this->pid);
      //assert(m_query->ts == this->ts);
#if CC_ALG == MVCC 
      this->thd_id = m_query->thd_id;
#endif
#if CC_ALG == OCC 
      this->start_ts = m_query->start_ts;
#endif
      this->req = m_query->req;
      this->txn_rtype = m_query->txn_rtype;
      break;
    case RQRY_RSP:
      //if(m_query->rc == Abort || this->rc == WAIT || this->rc == WAIT_REM) {
      if(m_query->rc == Abort) {
        this->rc = m_query->rc;
        this->txn_rtype = YCSB_FIN;
      }
      break;
    case RFIN:
      assert(this->pid == m_query->pid);
      this->rc = m_query->rc;
      if(this->part_cnt == 0) {
        this->part_cnt = m_query->part_cnt;
        this->parts = m_query->parts;
      }
      break;
    case RACK:
      if(m_query->rc == Abort || this->rc == WAIT || this->rc == WAIT_REM) {
        this->rc = m_query->rc;
      }
    case RTXN:
      break;
    default:
      assert(false);
      break;
  }
  return this;
}

// The following algorithm comes from the paper:
// Quickly generating billion-record synthetic databases
// However, it seems there is a small bug. 
// The original paper says zeta(theta, 2.0). But I guess it should be 
// zeta(2.0, theta).
double ycsb_client_query::zeta(uint64_t n, double theta) {
	double sum = 0;
	for (uint64_t i = 1; i <= n; i++) 
		sum += pow(1.0 / i, theta);
	return sum;
}

uint64_t ycsb_client_query::zipf(uint64_t n, double theta) {
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


void ycsb_client_query::gen_requests(uint64_t thd_id, workload * h_wl) {
  /*
#if CC_ALG == HSTORE
	assert(g_virtual_part_cnt == g_part_cnt);
#endif
*/
	uint64_t access_cnt = 0;
	set<uint64_t> all_keys;
	part_num = 0;
	double r = (double)(mrand->next() % 10000) / 10000;
	//UInt32 r = mrand->next() % 100;
	//if (r < g_perc_multi_part) {
	if (r < g_mpr && g_part_cnt > 1) {
    	//bool rem = false;
		//for (UInt32 i = 0; i < g_part_per_txn; i++) {
		while (part_num < g_part_per_txn) {
			//if (i == 0 && FIRST_PART_LOCAL)
			if (part_num == 0 && FIRST_PART_LOCAL)
				part_to_access[part_num] = thd_id % g_part_cnt;
				//part_to_access[part_num] = thd_id % g_virtual_part_cnt;
			else {
//part_to_access[part_num] = part_to_access[0] + g_node_cnt % g_part_cnt;
				  	while((part_to_access[part_num] = mrand->next() % g_part_cnt) == thd_id % g_part_cnt) {}
			}
			UInt32 j;
			for (j = 0; j < part_num; j++) 
				if ( part_to_access[part_num] == part_to_access[j] )
					break;
			if (j == part_num) {
				part_num ++;
			}
		}
	} else {
		part_num = 1;
		if (FIRST_PART_LOCAL)
			part_to_access[0] = thd_id % g_part_cnt;
		else
			part_to_access[0] = mrand->next() % g_part_cnt;
	}

	int rid = 0;
  double r_twr = (double)(mrand->next() % 10000) / 10000;		
	for (UInt32 tmp = 0; tmp < g_req_per_query; tmp ++) {		
		double r = (double)(mrand->next() % 10000) / 10000;		
		ycsb_request * req = &requests[rid];
		//req->table_name = "SYNTH_TABLE";
		if (r_twr < g_txn_read_perc || r < g_tup_read_perc) {
			req->acctype = RD;
		} else if (r >= g_tup_read_perc && r <= g_tup_write_perc + g_tup_read_perc) {
			req->acctype = WR;
		} else {
			req->acctype = SCAN;
			req->scan_len = SCAN_LEN;
		}

		// the request will access part_id.
		uint64_t ith = tmp * part_num / g_req_per_query;
		uint64_t part_id = 
			part_to_access[ ith ];
		uint64_t table_size = g_synth_table_size / g_part_cnt;
		//uint64_t table_size = g_synth_table_size / g_virtual_part_cnt;
		uint64_t row_id = zipf(table_size - 1, g_zipf_theta);
		assert(row_id < table_size);
		uint64_t primary_key = row_id * g_part_cnt + part_id;
		//uint64_t primary_key = row_id * g_virtual_part_cnt + part_id;
		assert(primary_key % g_part_cnt == part_id);
		req->key = primary_key;
		req->value = mrand->next() % (1<<8);
		// Make sure a single row is not accessed twice
		if (req->acctype == RD || req->acctype == WR) {
			if (all_keys.find(req->key) == all_keys.end()) {
				all_keys.insert(req->key);
				access_cnt ++;
			} else {
        // Need to have the full g_req_per_query amount
        tmp--;
        continue;
      }
		} else {
			bool conflict = false;
			for (UInt32 i = 0; i < req->scan_len; i++) {
				primary_key = (row_id + i) * g_part_cnt + part_id;
				if (all_keys.find( primary_key )
					!= all_keys.end())
					conflict = true;
			}
			if (conflict) continue;
			else {
				for (UInt32 i = 0; i < req->scan_len; i++)
					all_keys.insert( (row_id + i) * g_part_cnt + part_id);
				access_cnt += SCAN_LEN;
			}
		}
		rid ++;
	}
	request_cnt = rid;
  for (int i = request_cnt - 1; i > 0; i--) 
    for (int j = 0; j < i; j ++)
      assert(requests[i].key != requests[j].key);

	// Sort the requests in key order.
	if (g_key_order) {
		for (int i = request_cnt - 1; i > 0; i--) 
			for (int j = 0; j < i; j ++)
				if (requests[j].key > requests[j + 1].key) {
					ycsb_request tmp = requests[j];
					requests[j] = requests[j + 1];
					requests[j + 1] = tmp;
				}
		for (UInt32 i = 0; i < request_cnt - 1; i++)
			assert(requests[i].key < requests[i + 1].key);
	}

}

void ycsb_client_query::gen_requests2(uint64_t thd_id, workload * h_wl) {
  /*
#if CC_ALG == HSTORE
	assert(g_virtual_part_cnt == g_part_cnt);
#endif
*/
	uint64_t access_cnt = 0;
	set<uint64_t> all_keys;
	part_num = 0;

	int rid = 0;
  double r_twr = (double)(mrand->next() % 10000) / 10000;		
	for (UInt32 i = 0; i < g_req_per_query; i ++) {		
		double r = (double)(mrand->next() % 10000) / 10000;		
		ycsb_request * req = &requests[rid];
		if (r_twr < g_txn_read_perc ||r < g_tup_read_perc) 
			req->acctype = RD;
		else
			req->acctype = WR;

    uint64_t row_id;
		uint64_t table_size = g_synth_table_size;
    if ( FIRST_PART_LOCAL && rid == 0) {
      while( (row_id = zipf(table_size - 1, g_zipf_theta)) % g_part_cnt != thd_id % g_part_cnt) {}
      part_to_access[part_num++] = row_id % g_part_cnt;
      assert(row_id % g_part_cnt == thd_id);
    }
    else {
      while(1) {
		    row_id = zipf(table_size - 1, g_zipf_theta);
        uint32_t j;
        for(j = 0; j < part_num; j++) {
          if(part_to_access[j] == row_id % g_part_cnt)
            break;
        }
        if( j < part_num)
          break;
        if( part_num < g_part_per_txn ) {
          part_to_access[part_num++] = row_id % g_part_cnt;
          break;
        }
        else 
          continue;
      }
    }
		assert(row_id < table_size);
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
	}
	request_cnt = rid;
	// Sort the requests in key order.
	if (g_key_order) {
		for (int i = request_cnt - 1; i > 0; i--) 
			for (int j = 0; j < i; j ++)
				if (requests[j].key > requests[j + 1].key) {
					ycsb_request tmp = requests[j];
					requests[j] = requests[j + 1];
					requests[j + 1] = tmp;
				}
		for (UInt32 i = 0; i < request_cnt - 1; i++)
			assert(requests[i].key < requests[i + 1].key);
	}

}

void ycsb_client_query::gen_requests3(uint64_t thd_id, workload * h_wl) {
	uint64_t access_cnt = 0;
	set<uint64_t> all_keys;
	part_num = 0;
  double r_mpt = (double)(mrand->next() % 10000) / 10000;		
  uint64_t part_limit;
  if(r_mpt < g_mpr)
    part_limit = g_part_per_txn;
  else
    part_limit = 1;
  //uint64_t hot_key_max = g_synth_table_size * g_data_perc;
  uint64_t hot_key_max = (uint64_t)g_data_perc;
  double r_twr = (double)(mrand->next() % 10000) / 10000;		
  //printf("%ld %f\n",hot_key_max,((float)hot_key_max)/g_synth_table_size);

	int rid = 0;
	for (UInt32 i = 0; i < g_req_per_query; i ++) {		
		double r = (double)(mrand->next() % 10000) / 10000;		
    double hot =  (double)(mrand->next() % 10000) / 10000;
		ycsb_request * req = &requests[rid];
		if (r_twr < g_txn_read_perc || r < g_tup_read_perc) 
			req->acctype = RD;
		else
			req->acctype = WR;

    uint64_t row_id;
		uint64_t table_size = g_synth_table_size;
    if ( FIRST_PART_LOCAL && rid == 0) {
      if(hot < g_access_perc) {
        row_id = (uint64_t)(mrand->next() % (hot_key_max/g_part_cnt)) * g_part_cnt + thd_id;
      } else {
        uint64_t nrand = (uint64_t)mrand->next();
        //row_id = (((uint64_t)(mrand->next() % (g_synth_table_size/g_part_cnt - (hot_key_max/g_part_cnt)))) + hot_key_max/g_part_cnt) * thd_id;
        row_id = ((nrand % (g_synth_table_size/g_part_cnt - (hot_key_max/g_part_cnt))) + hot_key_max/g_part_cnt) * g_part_cnt + thd_id;
      }
      part_to_access[part_num++] = row_id % g_part_cnt;
      assert(row_id % g_part_cnt == thd_id);
    }
    else {
      while(1) {
        if(hot < g_access_perc) {
          row_id = (uint64_t)(mrand->next() % hot_key_max);
        } else {
          row_id = ((uint64_t)(mrand->next() % (g_synth_table_size - hot_key_max))) + hot_key_max;
        }
        uint32_t j;
        for(j = 0; j < part_num; j++) {
          if(part_to_access[j] == row_id % g_part_cnt)
            break;
        }
        if( j < part_num) {
          if(g_strict_ppt && part_num < part_limit && (part_num + (g_req_per_query - rid) > part_limit))
            continue;
          else
            break;
        }
        if( part_num < part_limit ) {
          part_to_access[part_num++] = row_id % g_part_cnt;
          break;
        }
        else 
          continue;
      }
    }
    /*
    if(hot < g_access_perc) {
        printf("H %ld, ",row_id);
      assert(row_id < hot_key_max);
    } else {
        printf("N %ld, ",row_id);
      assert(row_id >= hot_key_max);
    }
    */
		assert(row_id < table_size);
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
	}
	request_cnt = rid;
	// Sort the requests in key order.
	if (g_key_order) {
		for (int i = request_cnt - 1; i > 0; i--) 
			for (int j = 0; j < i; j ++)
				if (requests[j].key > requests[j + 1].key) {
					ycsb_request tmp = requests[j];
					requests[j] = requests[j + 1];
					requests[j + 1] = tmp;
				}
		for (UInt32 i = 0; i < request_cnt - 1; i++)
			assert(requests[i].key < requests[i + 1].key);
	}

}
