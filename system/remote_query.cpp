#include "remote_query.h"
#include "mem_alloc.h"
#include "tpcc.h"
#include "ycsb.h"
#include "tpcc_query.h"
#include "ycsb_query.h"
#include "query.h"
#include "transport.h"
#include "plock.h"

void Remote_query::init(uint64_t node_id, workload * wl) {
	q_idx = 0;
	_node_id = node_id;
	_wl = wl;
  pthread_mutex_init(&mtx,NULL);
}

txn_man * Remote_query::get_txn_man(uint64_t thd_id, uint64_t node_id, uint64_t txn_id) {

  txn_man * next_txn = NULL;

  return next_txn;
}

void Remote_query::remote_qry(base_query * query, int type, int dest_id, txn_man * txn) {
#if WORKLOAD == TPCC
	tpcc_query * m_query = (tpcc_query *) query;
	m_query->remote_qry(query,type,dest_id);
#elif WORKLOAD == YCSB
	ycsb_query * m_query = (ycsb_query *) query;
	m_query->remote_qry(query,type,dest_id);
#endif
}

void Remote_query::ack_response(RC rc, txn_man * txn) {

	// Maximum number of parameters
	// NOTE: Adjust if parameters sent is changed
#if DEBUG_DISTR
  printf("Sending RACK-1 %ld\n",txn->get_txn_id());
#endif
  uint64_t total = 3;
	void ** data = new void *[total];
	int * sizes = new int [total];
	int num = 0;

  txnid_t txn_id = txn->get_txn_id();
  RC _rc =rc;
	RemReqType rtype = RACK;

	data[num] = &txn_id;
	sizes[num++] = sizeof(txnid_t);

	data[num] = &rtype;
	sizes[num++] = sizeof(RemReqType);

  data[num] = &_rc;
	sizes[num++] = sizeof(RC);

  send_remote_query(GET_NODE_ID(txn->get_pid()),data,sizes,num);
}
void Remote_query::ack_response(base_query * query) {

	// Maximum number of parameters
	// NOTE: Adjust if parameters sent is changed
#if DEBUG_DISTR
  printf("Sending RACK-2 %ld\n",query->txn_id);
#endif
  uint64_t total = 3;
	void ** data = new void *[total];
	int * sizes = new int [total];
	int num = 0;

  txnid_t txn_id = query->txn_id;
	RemReqType rtype = RACK;

	data[num] = &txn_id;
	sizes[num++] = sizeof(txnid_t);

	data[num] = &rtype;
	sizes[num++] = sizeof(RemReqType);

  data[num] = &query->rc;
	sizes[num++] = sizeof(RC);

  send_remote_query(query->return_id,data,sizes,num);
}

void Remote_query::send_client_rsp(base_query * query) {
#if 0
	// Maximum number of parameters
	// NOTE: Adjust if parameters sent is changed
#if DEBUG_DISTR
    	printf("Sending client response (CL_RSP %lu)\n",query->txn_id);
#endif
#endif
    	query->return_id = query->client_id;
    	query->dest_id = g_node_id;
#if 0
    	uint64_t total = 5;
	void ** data = new void *[total];
	int * sizes = new int [total];
	int num = 0;

    	txnid_t txn_id = query->txn_id;
	RemReqType rtype = CL_RSP;

	data[num] = &txn_id;
	sizes[num++] = sizeof(txnid_t);

	data[num] = &rtype;
	sizes[num++] = sizeof(RemReqType);

    	data[num] = &query->rc;
	sizes[num++] = sizeof(RC);

    	data[num] = &query->client_startts;
	sizes[num++] = sizeof(uint64_t);

    	send_remote_query(query->return_id,data,sizes,num);
#endif
	send_client_rsp(query->txn_id, query->rc, query->client_startts, query->return_id);
}

void Remote_query::send_client_rsp(txnid_t txn_id, RC rc, uint64_t client_startts,
		uint32_t client_node_id) {
	// Maximum number of parameters
	// NOTE: Adjust if parameters sent is changed
#if DEBUG_DISTR
    	printf("Sending client response (CL_RSP)\n");
#endif
    //query->return_id = query->client_id;
    //query->dest_id = g_node_id;
    uint64_t total = 5;
	void ** data = new void *[total];
	int * sizes = new int [total];
	int num = 0;

    //txnid_t txn_id = query->txn_id;
	RemReqType rtype = CL_RSP;

	data[num] = &txn_id;
	sizes[num++] = sizeof(txnid_t);

	data[num] = &rtype;
	sizes[num++] = sizeof(RemReqType);

    data[num] = &rc;
	sizes[num++] = sizeof(RC);

    data[num] = &client_startts;
	sizes[num++] = sizeof(uint64_t);

    send_remote_query(client_node_id,data,sizes,num);
}

void Remote_query::send_init_done(uint64_t dest_id) {
  uint64_t total = 2;
	void ** data = new void *[total];
	int * sizes = new int [total];
  int num = 0;

  txnid_t txn_id = 0;
	RemReqType rtype = INIT_DONE;

	data[num] = &txn_id;
	sizes[num++] = sizeof(txnid_t);
	data[num] = &rtype;
	sizes[num++] = sizeof(RemReqType);
  send_remote_query(dest_id,data,sizes,num);
}

// FIXME: What if in HStore we want to lock multiple partitions at same node?
void Remote_query::send_init(base_query * query,uint64_t dest_part_id) {

	// Maximum number of parameters
	// NOTE: Adjust if parameters sent is changed
#if DEBUG_DISTR
  printf("Sending RINIT %ld\n",query->txn_id);
#endif

#if CC_ALG == VLL || CC_ALG == AVOID
#if WORKLOAD == TPCC
  tpcc_query * m_query = (tpcc_query *)query;
#elif WORKLOAD == YCSB
  ycsb_query * m_query = (ycsb_query *)query;
#endif
#endif

  uint64_t total = 3;
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
  total += 3;
#endif
#if CC_ALG == VLL
  total++; // m_query->request_cnt
  total++; // m_query->requests
#endif


#if CC_ALG == AVOID
  uint64_t * keys;
  uint64_t k = m_query->get_keys(&keys,&k);
  total += k + 2;
#endif
	void ** data = new void *[total];
	int * sizes = new int [total];
	int num = 0;

  txnid_t txn_id = query->txn_id;
	RemReqType rtype = RINIT;

	data[num] = &txn_id;
	sizes[num++] = sizeof(txnid_t);

	data[num] = &rtype;
	sizes[num++] = sizeof(RemReqType);

	data[num] = &query->ts;
	sizes[num++] = sizeof(ts_t);

#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
  uint64_t _dest_part_id = dest_part_id;
  uint64_t _part_id = GET_PART_ID(0,g_node_id);
  uint64_t _part_cnt = 1; // FIXME
	data[num] = &_part_id;
	sizes[num++] = sizeof(uint64_t);
	data[num] = &_part_cnt;
	sizes[num++] = sizeof(uint64_t);
	data[num] = &_dest_part_id;
	sizes[num++] = sizeof(uint64_t);
#endif

#if CC_ALG == VLL
#if WORKLOAD == YCSB

  data[num] = &m_query->request_cnt;
  sizes[num++] = sizeof(uint64_t);

  data[num] = m_query->requests;
  sizes[num++] = sizeof(ycsb_request) * m_query->request_cnt;

#endif
#endif

#if CC_ALG == AVOID
#if WORKLOAD == TPCC
  TPCCTxnType type = m_query->txn_type;
  data[num] = &type;
  sizes[num++] = sizeof(TPCCTxnType);
#elif WORKLOAD == YCSB
  YCSBTxnType type = m_query->txn_type;
  data[num] = &type;
  sizes[num++] = sizeof(YCSBTxnType);
#endif
  data[num] = &k;
  sizes[num++] = sizeof(uint64_t);
  for(int i;i<k; i++) {
    data[num] = keys[i];
    sizes[num++] = sizeof(uint64_t);
  }
#endif

  send_remote_query(GET_NODE_ID(dest_part_id),data,sizes,num);
}

void Remote_query::send_remote_query(uint64_t dest_id, void ** data, int * sizes, int num) {
	tport_man.send_msg(dest_id, data, sizes, num);
}

void Remote_query::remote_rsp(base_query * query, txn_man * txn) {

#if WORKLOAD == TPCC
    tpcc_query * m_query = (tpcc_query *) query;
    m_query->remote_rsp(query);
#elif WORKLOAD == YCSB
    ycsb_query * m_query = (ycsb_query *) query;
    m_query->remote_rsp(query);
#endif

}

void Remote_query::send_remote_rsp(uint64_t dest_id, void ** data, int * sizes, int num) {
	tport_man.send_msg(dest_id, data, sizes, num);
}

base_client_query * Remote_query::unpack_client_query(void * d, int len) {
  base_client_query * query;
	char * data = (char *) d;
	uint64_t ptr = 0;
  
	uint32_t dest_id;
	uint32_t return_id;
	txnid_t txn_id;
	RemReqType rtype;
  //RC rc;

	memcpy(&dest_id,&data[ptr],sizeof(uint32_t));
	ptr += sizeof(uint32_t);
	memcpy(&return_id,&data[ptr],sizeof(uint32_t));
	ptr += sizeof(uint32_t);
	memcpy(&txn_id,&data[ptr],sizeof(txnid_t));
	ptr += sizeof(txnid_t);
	memcpy(&rtype,&data[ptr],sizeof(RemReqType));
	ptr += sizeof(RemReqType);

#if WORKLOAD == TPCC
	      query = new tpcc_client_query();
#elif WORKLOAD == YCSB
        query = new ycsb_client_query();
#endif

    //query->dest_id = dest_id;
    query->return_id = return_id;
    //query->txn_id = txn_id;
    query->rtype = rtype;

	switch(rtype) {
        case RACK:
			// TODO: fix this hack
			query->return_id = txn_id;
			break;
        case RTXN: 
			query->unpack_client(query, data);
			break;
		case INIT_DONE:
			break;
        default:
			assert(false);
  }

  return query;
}


base_query * Remote_query::unpack(void * d, int len) {
    base_query * query;
	char * data = (char *) d;
	uint64_t ptr = 0;
  
    uint32_t dest_id;
    uint32_t return_id;
    txnid_t txn_id;
	RemReqType rtype;
    RC rc;

	memcpy(&dest_id,&data[ptr],sizeof(uint32_t));
	ptr += sizeof(uint32_t);
	memcpy(&return_id,&data[ptr],sizeof(uint32_t));
	ptr += sizeof(uint32_t);
	memcpy(&txn_id,&data[ptr],sizeof(txnid_t));
	ptr += sizeof(txnid_t);
	memcpy(&rtype,&data[ptr],sizeof(query->rtype));
	ptr += sizeof(query->rtype);

	bool handle_init = false;
#if CC_ALG != HSTORE && CC_ALG != HSTORE_SPEC && CC_ALG != VLL
	// The remaining algos do not use 2PC init so we must check if we've seen
	// this query before if the remote type is RPREPARE or RQRY or RFIN 
	if (rtype == RQRY || rtype == RPREPARE || rtype == RFIN) {
		query = txn_pool.get_qry(g_node_id,txn_id);
		if (query == NULL)
			handle_init = true;
	}
#endif

    if(rtype == RINIT || rtype == INIT_DONE || rtype == RTXN || rtype == CL_RSP || (QRY_ONLY && rtype == RQRY) || handle_init) {
#if WORKLOAD == TPCC
	    //query = (tpcc_query *) mem_allocator.alloc(sizeof(tpcc_query), 0);
	      query = new tpcc_query();
#elif WORKLOAD == YCSB
	    //query = (ycsb_query *) mem_allocator.alloc(sizeof(ycsb_query), 0);
        query = new ycsb_query();
#endif
        query->clear();
    } else {
        query = txn_pool.get_qry(g_node_id,txn_id);
    }
	assert(query != NULL);

    query->dest_id = dest_id;
    query->return_id = return_id;
    query->txn_id = txn_id;
    query->rtype = rtype;

  /*
	memcpy(&query->dest_id,&data[ptr],sizeof(uint32_t));
	ptr += sizeof(uint32_t);
	memcpy(&query->return_id,&data[ptr],sizeof(uint32_t));
	ptr += sizeof(uint32_t);
	memcpy(&query->txn_id,&data[ptr],sizeof(txnid_t));
	ptr += sizeof(txnid_t);
	memcpy(&query->rtype,&data[ptr],sizeof(query->rtype));
	ptr += sizeof(query->rtype);
  */

	if(query->dest_id != _node_id)
		return NULL;

	switch(query->rtype) {
        case RINIT: {
	        memcpy(&query->ts,&data[ptr],sizeof(ts_t));
	        ptr += sizeof(ts_t);
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
	        memcpy(&query->pid,&data[ptr],sizeof(query->pid));
	        ptr += sizeof(query->pid);
	        memcpy(&query->part_cnt,&data[ptr],sizeof(query->part_cnt));
	        ptr += sizeof(query->part_cnt);
            assert(query->part_cnt == 1);
	        query->parts = new uint64_t[query->part_cnt];
	        for (uint64_t i = 0; i < query->part_cnt; i++) {
		        memcpy(&query->parts[i],&data[ptr],sizeof(query->parts[i]));
		        ptr += sizeof(query->parts[i]);
	        }
#endif
#if CC_ALG == AVOID
	        memcpy(&query->t_type,&data[ptr],sizeof(query->txn_type));
	        ptr += sizeof(query->txn_type);
            uint64_t k;
	        memcpy(&query->num_keys,&data[ptr],sizeof(uint64_t));
	        ptr += sizeof(uint64_t);
            query->keys = mem_allocator.alloc(sizeof(uint64_t) * query->num_keys, 0);;
            for(uint64_t i; i < query->num_keys; i++) {
	            memcpy(&query->keys[i],&data[ptr],sizeof(uint64_t));
	            ptr += sizeof(uint64_t);
            }
#endif
#if CC_ALG == VLL
#if WORKLOAD == YCSB
            ycsb_query * m_query = (ycsb_query*) query;
	        memcpy(&m_query->request_cnt,&data[ptr],sizeof(uint64_t));
	        ptr += sizeof(uint64_t);
	        m_query->requests = (ycsb_request *) 
		        mem_allocator.alloc(sizeof(ycsb_request) * m_query->request_cnt, 0);
          memcpy(m_query->requests,&data[ptr],sizeof(ycsb_request)*m_query->request_cnt);
          ptr += sizeof(ycsb_request)*m_query->request_cnt;
#endif
#endif
                    }
            break;
        case RPREPARE:
#if CC_ALG != HSTORE && CC_ALG != HSTORE_SPEC && CC_ALG != VLL
			if (handle_init) {
				query->rc = Abort;
			}
#endif
            break;
		case RQRY: {
#if WORKLOAD == TPCC
            tpcc_query * m_query = new tpcc_query;
#elif WORKLOAD == YCSB
            ycsb_query * m_query = new ycsb_query;
#endif
			m_query->unpack(query,data);
			break;
		}
		case RQRY_RSP: {
#if WORKLOAD == TPCC
            tpcc_query * m_query = new tpcc_query;
#elif WORKLOAD == YCSB
            ycsb_query * m_query = new ycsb_query;
#endif
		    m_query->unpack_rsp(query,data);
			break;
		}
        case RFIN:
            query->unpack_finish(query, data);
            break;
        case RACK:
	        memcpy(&rc,&data[ptr],sizeof(RC));
	        ptr += sizeof(RC);
            if(rc == Abort || query->rc == WAIT || query->rc == WAIT_REM) {
                query->rc = rc;
            }
            break;
        case INIT_DONE:
            break;
        case RTXN: {
#if WORKLOAD == TPCC
            	tpcc_query * m_query = new tpcc_query;
#elif WORKLOAD == YCSB
            	ycsb_query *m_query = new ycsb_query;
#endif
            	m_query->unpack_client(query, data);
				assert(GET_NODE_ID(query->pid) == g_node_id);
            	break;
        }
        case CL_RSP:
          RC rc_tmp;
	        memcpy(&rc_tmp,&data[ptr],sizeof(RC));
	        ptr += sizeof(RC);
          uint64_t client_startts;
	        memcpy(&client_startts,&data[ptr],sizeof(uint64_t));
	        ptr += sizeof(uint64_t);
          INC_STATS(0,client_latency,get_sys_clock() - client_startts);
            	break;
	    	default:
		assert(false);
	}
    return query;
}



