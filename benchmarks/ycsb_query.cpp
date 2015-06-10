#include "query.h"
#include "ycsb_query.h"
#include "mem_alloc.h"
#include "wl.h"
#include "ycsb.h"
#include "table.h"

uint64_t ycsb_query::the_n = 0;
double ycsb_query::denom = 0;

void ycsb_query::init(uint64_t thd_id, workload * h_wl) {
//	mrand = (myrand *) mem_allocator.alloc(sizeof(myrand), thd_id);
//	mrand->init(thd_id);
//	cout << g_req_per_query << endl;
	//pid = GET_PART_ID(0,g_node_id);
	//requests = (ycsb_request *) 
	//	mem_allocator.alloc(sizeof(ycsb_request) * g_req_per_query, thd_id);
	//part_to_access = (uint64_t *) 
	//	mem_allocator.alloc(sizeof(uint64_t) * g_part_per_txn, thd_id);
	//zeta_2_theta = zeta(2, g_zipf_theta);
	//if (the_n == 0) {
	//	//uint64_t table_size = g_synth_table_size / g_part_cnt;
	//	uint64_t table_size = g_synth_table_size / g_virtual_part_cnt;
	//	the_n = table_size - 1;
	//	denom = zeta(the_n, g_zipf_theta);
	//}
	//gen_requests(pid, h_wl);
    init(thd_id, h_wl, g_node_id);
}

void ycsb_query::init(uint64_t thd_id, workload * h_wl, uint64_t node_id) {
	mrand = (myrand *) mem_allocator.alloc(sizeof(myrand), thd_id);
	mrand->init(get_sys_clock());
	pid = GET_PART_ID(0,node_id);
	requests = (ycsb_request *) 
		mem_allocator.alloc(sizeof(ycsb_request) * g_req_per_query, thd_id);
	part_to_access = (uint64_t *) 
		mem_allocator.alloc(sizeof(uint64_t) * g_part_per_txn, thd_id);
	zeta_2_theta = zeta(2, g_zipf_theta);
	if (the_n == 0) {
		//uint64_t table_size = g_synth_table_size / g_part_cnt;
		uint64_t table_size = g_synth_table_size / g_virtual_part_cnt;
		the_n = table_size - 1;
		denom = zeta(the_n, g_zipf_theta);
	}
	gen_requests(pid, h_wl);
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
	//memcpy(&m_query->txn_rtype,&data[ptr],sizeof(YCSBRemTxnType));
	//ptr += sizeof(YCSBRemTxnType);
	memcpy(&rc,&data[ptr],sizeof(RC));
	ptr += sizeof(RC);
	memcpy(&m_query->pid,&data[ptr],sizeof(uint64_t));
	ptr += sizeof(uint64_t);

  if(rc == Abort || m_query->rc == WAIT || m_query->rc == WAIT_REM) {
    m_query->rc = rc;
    m_query->txn_rtype = YCSB_FIN;
  }
}

void ycsb_query::unpack(base_query * query, void * d) {
	ycsb_query * m_query = (ycsb_query *) query;
	char * data = (char *) d;
	uint64_t ptr = HEADER_SIZE + sizeof(txnid_t) + sizeof(RemReqType);
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

	void ** data = new void *[total];
	int * sizes = new int [total];
	int num = 0;
	RemReqType rtype = RQRY;
	uint64_t _pid = m_query->pid;

	data[num] = &m_query->txn_id;
	sizes[num++] = sizeof(txnid_t);

	data[num] = &rtype;
	sizes[num++] = sizeof(RemReqType);
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

	void ** data = new void *[total];
	int * sizes = new int [total];

	int num = 0;
	uint64_t _pid = m_query->pid;
	RemReqType rtype = RQRY_RSP;

	data[num] = &m_query->txn_id;
	sizes[num++] = sizeof(txnid_t);

	data[num] = &rtype;
	sizes[num++] = sizeof(RemReqType);
	//data[num] = &m_query->txn_rtype;
	//sizes[num++] = sizeof(m_query->txn_rtype);
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
double ycsb_query::zeta(uint64_t n, double theta) {
	double sum = 0;
	for (uint64_t i = 1; i <= n; i++) 
		sum += pow(1.0 / i, theta);
	return sum;
}

uint64_t ycsb_query::zipf(uint64_t n, double theta) {
	assert(this->the_n == n);
	assert(theta == g_zipf_theta);
	double alpha = 1 / (1 - theta);
	double zetan = denom;
	double eta = (1 - pow(2.0 / n, 1 - theta)) / 
		(1 - zeta_2_theta / zetan);
	double u = (double)(mrand->next() % 10000000) / 10000000;
	double uz = u * zetan;
	if (uz < 1) return 1;
	if (uz < 1 + pow(0.5, theta)) return 2;
	return 1 + (uint64_t)(n * pow(eta*u -eta + 1, alpha));
}

void ycsb_query::client_query(base_query * query, uint64_t dest_id) {
#if DEBUG_DISTR
    	printf("Sending RTXN %ld\n",query->txn_id);
#endif
	ycsb_query * m_query = (ycsb_query *) query;

	// Maximum number of parameters
	// NOTE: Adjust if parameters sent is changed
	int total = 6 + m_query->part_num + m_query->request_cnt;

	void ** data = new void *[total];
	int * sizes = new int [total];
	int num = 0;
	RemReqType rtype = RTXN;
	uint64_t _pid = m_query->pid;

	data[num] = &m_query->txn_id;
	sizes[num++] = sizeof(txnid_t);
	data[num] = &rtype;
	sizes[num++] = sizeof(RemReqType);

	data[num] = &_pid;
	sizes[num++] = sizeof(uint64_t);

    	data[num] = &m_query->part_num;
    	sizes[num++] = sizeof(uint64_t);
    	for (uint64_t i = 0; i < m_query->part_num; ++i) {
        	data[num] = &m_query->part_to_access[i];
        	sizes[num++] = sizeof(uint64_t);
    	}
    	data[num] = &m_query->access_cnt;
    	sizes[num++] = sizeof(uint64_t);
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
    	m_query->part_to_access = (uint64_t *)
            	mem_allocator.alloc(sizeof(uint64_t) * g_part_cnt, thd_id);
	m_query->requests = (ycsb_request *) 
		mem_allocator.alloc(sizeof(ycsb_request) * g_req_per_query, thd_id);
    	m_query->reset();
    	m_query->client_id = m_query->return_id;
    	memcpy(&m_query->pid, &data[ptr], sizeof(uint64_t));
    	ptr += sizeof(uint64_t);

    	memcpy(&m_query->part_num, &data[ptr], sizeof(uint64_t));
    	ptr += sizeof(uint64_t);
    	for (uint64_t i = 0; i < m_query->part_num; ++i) {
        	memcpy(&m_query->part_to_access[i], &data[ptr], sizeof(uint64_t));
        	ptr += sizeof(uint64_t);
    	}

    	memcpy(&m_query->access_cnt, &data[ptr], sizeof(uint64_t));
    	ptr += sizeof(uint64_t);
    	memcpy(&m_query->request_cnt, &data[ptr], sizeof(uint64_t));
    	ptr += sizeof(uint64_t);

    	for (uint64_t i = 0; i < m_query->request_cnt; ++i) {
        	memcpy(&m_query->requests[i], &data[ptr], sizeof(ycsb_request));
        	ptr += sizeof(ycsb_request);
    	}
	m_query->req = m_query->requests[0];

}

void ycsb_query::gen_requests(uint64_t thd_id, workload * h_wl) {
#if CC_ALG == HSTORE
	assert(g_virtual_part_cnt == g_part_cnt);
#endif
	access_cnt = 0;
	set<uint64_t> all_keys;
	part_num = 0;
	double r = (double)(mrand->next() % 1000000) / 10000;
	//UInt32 r = mrand->next() % 100;
	//if (r < g_perc_multi_part) {
	if (r < g_mpr) {
    	//bool rem = false;
		//for (UInt32 i = 0; i < g_part_per_txn; i++) {
		while (part_num < g_part_per_txn) {
			//if (i == 0 && FIRST_PART_LOCAL)
			if (part_num == 0 && FIRST_PART_LOCAL)
				part_to_access[part_num] = thd_id % g_part_cnt;
				//part_to_access[part_num] = thd_id % g_virtual_part_cnt;
			else {
        			//if(!rem) {
				  	while((part_to_access[part_num] = mrand->next() % g_part_cnt) == thd_id % g_part_cnt) {}
					//rem = true;
        			//}
        			//else
						//part_to_access[part_num] = mrand->next() % g_part_cnt;
						//part_to_access[part_num] = thd_id % g_part_cnt;
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
			} else continue;
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


