#include "query.h"
#include "ycsb_query.h"
#include "mem_alloc.h"
#include "wl.h"
#include "ycsb.h"
#include "table.h"

uint64_t ycsb_client_query::the_n = 0;
double ycsb_client_query::denom = 0;

uint64_t ycsb_query::the_n = 0;
double ycsb_query::denom = 0;

void ycsb_query::init(uint64_t thd_id, workload * h_wl) {
    init(thd_id, h_wl, g_node_id);
}

void ycsb_client_query::init(uint64_t thd_id, workload * h_wl, uint64_t node_id) {
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
		//uint64_t table_size = g_synth_table_size / g_part_cnt;
		uint64_t table_size = g_synth_table_size / g_virtual_part_cnt;
		the_n = table_size - 1;
		denom = zeta(the_n, g_zipf_theta);
	}
	gen_requests(pid, h_wl);
#else
	if (the_n == 0) {
		//uint64_t table_size = g_synth_table_size / g_part_cnt;
		uint64_t table_size = g_synth_table_size;
		the_n = table_size - 1;
		denom = zeta(the_n, g_zipf_theta);
	}
	gen_requests2(pid, h_wl);
#endif

}

void ycsb_client_query::init() {
	requests = (ycsb_request *) 
		mem_allocator.alloc(sizeof(ycsb_request) * g_req_per_query, 0);
	part_to_access = (uint64_t *) 
		mem_allocator.alloc(sizeof(uint64_t) * g_part_per_txn, 0);
}

void ycsb_query::init(uint64_t thd_id, workload * h_wl, uint64_t node_id) {
  time_q_abrt = 0;
  time_q_work = 0;
  time_copy = 0;
}

void ycsb_query::reset() {
  txn_rtype = YCSB_0;
  rid = 0;
  req = requests[0];
}

void ycsb_query::unpack_rsp(base_query * query, void * d) {
	char * data = (char *) d;
  RC rc;

	ycsb_query * m_query = (ycsb_query *) query;
	uint64_t ptr = HEADER_SIZE + sizeof(txnid_t) + sizeof(RemReqType);
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
  ptr += 2*sizeof(uint64_t);
#endif
	//memcpy(&m_query->txn_rtype,&data[ptr],sizeof(YCSBRemTxnType));
	//ptr += sizeof(YCSBRemTxnType);
	memcpy(&rc,&data[ptr],sizeof(RC));
	ptr += sizeof(RC);
	memcpy(&m_query->pid,&data[ptr],sizeof(uint64_t));
	ptr += sizeof(uint64_t);

  m_query->rc = rc;
  /*
  if(rc == Abort || m_query->rc == WAIT || m_query->rc == WAIT_REM) {
    m_query->rc = rc;
    m_query->txn_rtype = YCSB_FIN;
  }
  */
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
      break;
    case RQRY_RSP:
      if(m_query->rc == Abort || this->rc == WAIT || this->rc == WAIT_REM) {
        this->rc = rc;
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

void ycsb_query::unpack(base_query * query, void * d) {
	ycsb_query * m_query = (ycsb_query *) query;
	char * data = (char *) d;
	uint64_t ptr = HEADER_SIZE + sizeof(txnid_t) + sizeof(RemReqType);
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
  ptr += 2*sizeof(uint64_t);
#endif
	memcpy(&m_query->txn_rtype,&data[ptr],sizeof(YCSBRemTxnType));
	ptr += sizeof(YCSBRemTxnType);
	memcpy(&m_query->pid,&data[ptr],sizeof(uint64_t));
	ptr += sizeof(uint64_t);
#if CC_ALG == WAIT_DIE || CC_ALG == TIMESTAMP || CC_ALG == MVCC
	memcpy(&m_query->ts,&data[ptr],sizeof(uint64_t));
	ptr += sizeof(uint64_t);
#endif
#if CC_ALG == MVCC 
  memcpy(&m_query->thd_id,&data[ptr], sizeof(uint64_t));
  ptr += sizeof(uint64_t);
#endif
#if CC_ALG == OCC
  memcpy(&m_query->start_ts, &data[ptr], sizeof(uint64_t));
  ptr += sizeof(uint64_t);
#endif
	memcpy(&m_query->req,&data[ptr],sizeof(ycsb_request));
	ptr += sizeof(ycsb_request);

}

void ycsb_query::remote_qry(base_query * query, int type, int dest_id) {
#if DEBUG_DISTR
  printf("Sending RQRY %ld\n",query->txn_id);
#endif
	ycsb_query * m_query = (ycsb_query *) query;
	YCSBRemTxnType t = (YCSBRemTxnType) type;
  assert(m_query->txn_rtype != YCSB_FIN);

	// Maximum number of parameters
	// NOTE: Adjust if parameters sent is changed
	int total = 5;

#if CC_ALG == WAIT_DIE | CC_ALG == TIMESTAMP || CC_ALG == MVCC
  total ++;   // For timestamp
#endif
#if CC_ALG == MVCC
  total ++;
#endif
#if CC_ALG == OCC
  total ++; // For start_ts
#endif
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
  total ++; // For destination partition
  total ++; // For home partition
#endif

	void ** data = new void *[total];
	int * sizes = new int [total];
	int num = 0;
	RemReqType rtype = RQRY;
	uint64_t _pid = m_query->pid;

	data[num] = &m_query->txn_id;
	sizes[num++] = sizeof(txnid_t);

	data[num] = &rtype;
	sizes[num++] = sizeof(RemReqType);

#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
	data[num] = &m_query->dest_part;
	sizes[num++] = sizeof(uint64_t);
	data[num] = &m_query->home_part;
	sizes[num++] = sizeof(uint64_t);
#endif

	data[num] = &t;
	sizes[num++] = sizeof(YCSBRemTxnType); 
	// The requester's PID
	data[num] = &_pid;
	sizes[num++] = sizeof(uint64_t); 

#if CC_ALG == WAIT_DIE || CC_ALG == TIMESTAMP || CC_ALG == MVCC
  data[num] = &m_query->ts;
  sizes[num++] = sizeof(uint64_t);   // sizeof ts_t
#endif
#if CC_ALG == MVCC 
  data[num] = &m_query->thd_id;
  sizes[num++] = sizeof(uint64_t);
#endif
#if CC_ALG == OCC
  data[num] = &m_query->start_ts;
  sizes[num++] = sizeof(uint64_t);   // sizeof ts_t
#endif

  //YCSB: Send req  
  data[num] = &m_query->req; 
  sizes[num++] = sizeof(ycsb_request);

	rem_qry_man.send_remote_query(dest_id, data, sizes, num);
}

void ycsb_query::remote_rsp(base_query * query) {
	ycsb_query * m_query = (ycsb_query *) query;

	// Maximum number of parameters
	// NOTE: Adjust if parameters sent is changed
#if DEBUG_DISTR
  printf("Sending RQRY_RSP %ld\n",query->txn_id);
#endif
	int total = 4;
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
  total++; // For dest partition id (same as home)
  total++; // For home partition id
#endif

	void ** data = new void *[total];
	int * sizes = new int [total];

	int num = 0;
	uint64_t _pid = m_query->pid;
	RemReqType rtype = RQRY_RSP;

	data[num] = &m_query->txn_id;
	sizes[num++] = sizeof(txnid_t);

	data[num] = &rtype;
	sizes[num++] = sizeof(RemReqType);
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
	data[num] = &m_query->home_part;
	sizes[num++] = sizeof(uint64_t);
	data[num] = &m_query->home_part;
	sizes[num++] = sizeof(uint64_t);
#endif

	data[num] = &m_query->rc;
	sizes[num++] = sizeof(RC);
	// The original requester's pid
	data[num] = &_pid;
	sizes[num++] = sizeof(uint64_t);
	rem_qry_man.send_remote_rsp(m_query->return_id, data, sizes, num);
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

void ycsb_client_query::unpack_client(base_client_query * query, void * d) {
	ycsb_client_query * m_query = (ycsb_client_query *) query;
	char * data = (char *) d;
	uint64_t ptr = HEADER_SIZE + sizeof(txnid_t) + sizeof(RemReqType);
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
  ptr += sizeof(uint64_t);
#endif
  memcpy(&m_query->pid, &data[ptr], sizeof(uint64_t));
  ptr += sizeof(uint64_t);

  memcpy(&m_query->client_startts, &data[ptr], sizeof(uint64_t));
  ptr += sizeof(uint64_t);

#if CC_ALG == CALVIN
	uint64_t batch_num __attribute__ ((unused));
	memcpy(&batch_num, &data[ptr],sizeof(uint64_t));
	ptr += sizeof(uint64_t);
#endif

  memcpy(&m_query->part_num, &data[ptr], sizeof(uint64_t));
  ptr += sizeof(uint64_t);

 	m_query->part_to_access = (uint64_t *)
            	mem_allocator.alloc(sizeof(uint64_t) * m_query->part_num, 0);

  for (uint64_t i = 0; i < m_query->part_num; ++i) {
     	memcpy(&m_query->part_to_access[i], &data[ptr], sizeof(uint64_t));
     	ptr += sizeof(uint64_t);
  }

   memcpy(&m_query->request_cnt, &data[ptr], sizeof(uint64_t));
   ptr += sizeof(uint64_t);

	m_query->requests = (ycsb_request *) 
		          mem_allocator.alloc(sizeof(ycsb_request) * m_query->request_cnt, 0);

   for (uint64_t i = 0; i < m_query->request_cnt; ++i) {
      	memcpy(&m_query->requests[i], &data[ptr], sizeof(ycsb_request));
      	ptr += sizeof(ycsb_request);
   }

}

void ycsb_client_query::client_query(base_client_query * query, uint64_t dest_id) {
  client_query(query, dest_id, 0, UINT64_MAX);
}

void ycsb_client_query::client_query(base_client_query * query, uint64_t dest_id,
		uint64_t batch_num, txnid_t txn_id) {
	ycsb_client_query * m_query = (ycsb_client_query *) query;
#if DEBUG_DISTR
    	printf("Client: sending RTXN to %ld -- %ld\n",dest_id,m_query->part_to_access[0]);
#endif

	// Maximum number of parameters
	// NOTE: Adjust if parameters sent is changed
	int total = 7 + m_query->part_num + m_query->request_cnt;
#if CC_ALG == CALVIN
  total++;
#endif
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
  total++; // For destination partition id
#endif

	void ** data = new void *[total];
	int * sizes = new int [total];
	int num = 0;
	RemReqType rtype = RTXN;
	uint64_t _pid = m_query->pid;
  uint64_t ts = get_sys_clock();
#if CC_ALG == CALVIN
  //uint64_t batch_num = 0; // FIXME
#endif

	data[num] = &txn_id;
	sizes[num++] = sizeof(txnid_t);
	data[num] = &rtype;
	sizes[num++] = sizeof(RemReqType);

#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
  data[num] = &m_query->part_to_access[0]; // Is this the same as _pid?
  sizes[num++] = sizeof(uint64_t);
#endif

	data[num] = &_pid;
	sizes[num++] = sizeof(uint64_t);

	data[num] = &ts;
	sizes[num++] = sizeof(uint64_t);

#if CC_ALG == CALVIN
	data[num] = &batch_num;
	sizes[num++] = sizeof(uint64_t);
#endif
    	data[num] = &m_query->part_num;
    	sizes[num++] = sizeof(uint64_t);
    	for (uint64_t i = 0; i < m_query->part_num; ++i) {
        	data[num] = &m_query->part_to_access[i];
        	sizes[num++] = sizeof(uint64_t);
    	}
      /*
    	data[num] = &m_query->access_cnt;
    	sizes[num++] = sizeof(uint64_t);
      */
    	data[num] = &m_query->request_cnt;
    	sizes[num++] = sizeof(uint64_t);

    	for (uint64_t i = 0; i < m_query->request_cnt; ++i) {
        	data[num] = &m_query->requests[i];
        	sizes[num++] = sizeof(ycsb_request);
    	}
	rem_qry_man.send_remote_query(GET_NODE_ID(m_query->pid), data, sizes, num);
}

void ycsb_query::unpack_client(base_query * query, void * d) {
	ycsb_query * m_query = (ycsb_query *) query;
	char * data = (char *) d;
	uint64_t ptr = HEADER_SIZE + sizeof(txnid_t) + sizeof(RemReqType);
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
  ptr += sizeof(uint64_t);
#endif
  /*
    	m_query->part_to_access = (uint64_t *)
            	mem_allocator.alloc(sizeof(uint64_t) * g_part_cnt, thd_id);
	m_query->requests = (ycsb_request *) 
		mem_allocator.alloc(sizeof(ycsb_request) * g_req_per_query, thd_id);
    */
    	m_query->client_id = m_query->return_id;
    	memcpy(&m_query->pid, &data[ptr], sizeof(uint64_t));
    	ptr += sizeof(uint64_t);

    	memcpy(&m_query->client_startts, &data[ptr], sizeof(uint64_t));
    	ptr += sizeof(uint64_t);

#if CC_ALG == CALVIN
    	memcpy(&m_query->batch_num, &data[ptr], sizeof(uint64_t));
    	ptr += sizeof(uint64_t);
#endif

    	memcpy(&m_query->part_num, &data[ptr], sizeof(uint64_t));
    	ptr += sizeof(uint64_t);

 	m_query->part_to_access = (uint64_t *)
            	mem_allocator.alloc(sizeof(uint64_t) * m_query->part_num, thd_id);

    	for (uint64_t i = 0; i < m_query->part_num; ++i) {
        	memcpy(&m_query->part_to_access[i], &data[ptr], sizeof(uint64_t));
        	ptr += sizeof(uint64_t);
    	}

      /*
    	memcpy(&m_query->access_cnt, &data[ptr], sizeof(uint64_t));
    	ptr += sizeof(uint64_t);
      */
    	memcpy(&m_query->request_cnt, &data[ptr], sizeof(uint64_t));
    	ptr += sizeof(uint64_t);

	m_query->requests = (ycsb_request *) 
		          mem_allocator.alloc(sizeof(ycsb_request) * m_query->request_cnt, thd_id);

    	for (uint64_t i = 0; i < m_query->request_cnt; ++i) {
        	memcpy(&m_query->requests[i], &data[ptr], sizeof(ycsb_request));
        	ptr += sizeof(ycsb_request);
    	}
    	m_query->reset();
	m_query->req = m_query->requests[0];

}

void ycsb_client_query::gen_requests(uint64_t thd_id, workload * h_wl) {
#if CC_ALG == HSTORE
	assert(g_virtual_part_cnt == g_part_cnt);
#endif
	uint64_t access_cnt = 0;
	set<uint64_t> all_keys;
	part_num = 0;
	double r = (double)(mrand->next() % 1000000) / 10000;
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
	for (UInt32 tmp = 0; tmp < g_req_per_query; tmp ++) {		
		double r = (double)(mrand->next() % 10000) / 10000;		
		ycsb_request * req = &requests[rid];
		//req->table_name = "SYNTH_TABLE";
		if (r < g_read_perc) {
			req->acctype = RD;
		} else if (r >= g_read_perc && r <= g_write_perc + g_read_perc) {
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
#if CC_ALG == HSTORE
	assert(g_virtual_part_cnt == g_part_cnt);
#endif
	uint64_t access_cnt = 0;
	set<uint64_t> all_keys;
	part_num = 0;

	int rid = 0;
	for (UInt32 i = 0; i < g_req_per_query; i ++) {		
		double r = (double)(mrand->next() % 10000) / 10000;		
		ycsb_request * req = &requests[rid];
		if (r < g_read_perc) 
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
        if( part_num < PART_PER_TXN ) {
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
