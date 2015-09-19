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
  /*
  for(int i=0;i<MAX_TXN_PER_PART*2*3;i++)
    responses[i] = false;
    */
}

txn_man * Remote_query::get_txn_man(uint64_t thd_id, uint64_t node_id, uint64_t txn_id) {

  txn_man * next_txn = NULL;

  return next_txn;
}

/*
void Remote_query::remote_qry(base_query * query, int type, int dest_id, txn_man * txn) {
#if WORKLOAD == TPCC
	tpcc_query * m_query = (tpcc_query *) query;
	m_query->remote_qry(query,type,dest_id);
#elif WORKLOAD == YCSB
	ycsb_query * m_query = (ycsb_query *) query;
	m_query->remote_qry(query,type,dest_id);
#endif
}
*/

/*
void Remote_query::ack_response(RC rc, txn_man * txn) {

	// Maximum number of parameters
	// NOTE: Adjust if parameters sent is changed
  DEBUG("Sending RACK-1 %ld\n",txn->get_txn_id());
  uint64_t total = 3;
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
  total ++; // For home partition id
  total ++; // For home partition id (2)
#endif

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

#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
	data[num] = &txn->home_part;
	sizes[num++] = sizeof(uint64_t);
	data[num] = &txn->home_part;
	sizes[num++] = sizeof(uint64_t);
#endif

  data[num] = &_rc;
	sizes[num++] = sizeof(RC);

  send_remote_query(GET_NODE_ID(txn->get_pid()),data,sizes,num);
}
*/

/*
void Remote_query::ack_response(base_query * query) {

	// Maximum number of parameters
	// NOTE: Adjust if parameters sent is changed
  DEBUG("Sending RACK-2 %ld\n",query->txn_id);
  uint64_t total = 3;
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
  total += 1;
  total += 1;
#endif
	void ** data = new void *[total];
	int * sizes = new int [total];
	int num = 0;

  txnid_t txn_id = query->txn_id;
	RemReqType rtype = RACK;

	data[num] = &txn_id;
	sizes[num++] = sizeof(txnid_t);

	data[num] = &rtype;
	sizes[num++] = sizeof(RemReqType);

#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
  data[num] = &query->home_part;
	sizes[num++] = sizeof(uint64_t);
  data[num] = &query->home_part;
	sizes[num++] = sizeof(uint64_t);
#endif

  data[num] = &query->rc;
	sizes[num++] = sizeof(RC);

  send_remote_query(query->return_id,data,sizes,num);
}
*/

/*
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
*/

/*
void Remote_query::send_client_rsp(txnid_t txn_id, RC rc, uint64_t client_startts,
		uint32_t client_node_id) {
	// Maximum number of parameters
	// NOTE: Adjust if parameters sent is changed
   	DEBUG("Sending client response (CL_RSP) %ld\n",txn_id);
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
*/

/*
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

*/
/*
void Remote_query::send_exp_done(uint64_t dest_id) {
  uint64_t total = 2;
	void ** data = new void *[total];
	int * sizes = new int [total];
  int num = 0;

  txnid_t txn_id = 0;
	RemReqType rtype = EXP_DONE;

	data[num] = &txn_id;
	sizes[num++] = sizeof(txnid_t);
	data[num] = &rtype;
	sizes[num++] = sizeof(RemReqType);
  send_remote_query(dest_id,data,sizes,num);
}
*/


// TODO: What if in HStore we want to lock multiple partitions at same node?
/*
void Remote_query::send_init(base_query * query,uint64_t dest_part_id) {

	// Maximum number of parameters
	// NOTE: Adjust if parameters sent is changed
  DEBUG("Sending RINIT %ld\n",query->txn_id);

#if CC_ALG == VLL || CC_ALG == AVOID
#if WORKLOAD == TPCC
  tpcc_query * m_query = (tpcc_query *)query;
#elif WORKLOAD == YCSB
  ycsb_query * m_query = (ycsb_query *)query;
#endif
#endif

  uint64_t total = 4;
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
  total += 2; // destination partition, home partition
  total += 2;
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

#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
	data[num] = &query->dest_part;
	sizes[num++] = sizeof(uint64_t);
	data[num] = &query->home_part;
	sizes[num++] = sizeof(uint64_t);
  uint64_t _part_id = query->home_part;
#else
  uint64_t _part_id = GET_PART_ID(0,g_node_id);
#endif

	data[num] = &query->ts;
	sizes[num++] = sizeof(ts_t);

	data[num] = &_part_id;
	sizes[num++] = sizeof(uint64_t);

#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC

  uint64_t _dest_part_id = dest_part_id;
  uint64_t _part_cnt = 1; // TODO: generalize
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
*/

/*
void Remote_query::send_remote_query(uint64_t dest_id, void ** data, int * sizes, int num) {
	tport_man.send_msg(dest_id, data, sizes, num);
}
*/

/*
void Remote_query::remote_rsp(base_query * query, txn_man * txn) {

#if WORKLOAD == TPCC
    tpcc_query * m_query = (tpcc_query *) query;
    m_query->remote_rsp(query);
#elif WORKLOAD == YCSB
    ycsb_query * m_query = (ycsb_query *) query;
    m_query->remote_rsp(query);
#endif

}
*/

/*
void Remote_query::send_remote_rsp(uint64_t dest_id, void ** data, int * sizes, int num) {
	tport_man.send_msg(dest_id, data, sizes, num);
}
*/

// FIXME: unpack batch
/*
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
*/

//base_query * Remote_query::unpack(void * d, int len) {
void Remote_query::unpack(void * d, uint64_t len) {
    base_query * query;
	char * data = (char *) d;
	uint64_t ptr = 0;
  uint32_t dest_id;
  uint32_t return_id;
  uint32_t txn_cnt;
  uint64_t starttime = get_sys_clock();
  assert(len > sizeof(uint32_t) * 3);

  COPY_VAL(dest_id,data,ptr);
  COPY_VAL(return_id,data,ptr);
  COPY_VAL(txn_cnt,data,ptr);
  DEBUG("Received batch %d txns from %d\n",txn_cnt,return_id);

  while(txn_cnt > 0) { 
#if WORKLOAD == TPCC
	    query = (tpcc_query *) mem_allocator.alloc(sizeof(tpcc_query), 0);
	      query = new tpcc_query();
#elif WORKLOAD == YCSB
	    query = (ycsb_query *) mem_allocator.alloc(sizeof(ycsb_query), 0);
        query = new ycsb_query();
#endif


    unpack_query(query,data,ptr,dest_id,return_id);
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
		work_queue.add_query(GET_PART_ID_IDX(query->active_part),query);
#else
		work_queue.add_query(0,query);
#endif
    txn_cnt--;
    query->time_copy += get_sys_clock() - starttime;
  }
  //  return query;
}

void Remote_query::unpack_query(base_query *& query,char * data,  uint64_t & ptr,uint64_t dest_id,uint64_t return_id) {

  uint64_t timespan;
	assert(query != NULL);

  query->dest_id = dest_id;
  query->return_id = return_id;

  COPY_VAL(query->txn_id,data,ptr);
  COPY_VAL(query->rtype,data,ptr);
    /*
    if(query->rtype == INIT_DONE || query->rtype == EXP_DONE)
      return query;
      */

#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
  COPY_VAL(active_part,data,ptr);
  COPY_VAL(home_part,data,ptr);
#endif

	switch(query->rtype) {
    case RINIT: {
      COPY_VAL(query->ts,data,ptr);
      COPY_VAL(query->pid,data,ptr);
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
      COPY_VAL(query->part_cnt,data,ptr);
      assert(query->part_cnt == 1);
	    query->parts = new uint64_t[query->part_cnt];
	    for (uint64_t i = 0; i < query->part_cnt; i++) {
        COPY_VAL(query->parts[i],data,ptr);
      }
#endif
#if CC_ALG == VLL
#if WORKLOAD == YCSB
      ycsb_query * m_query = (ycsb_query*) query;
      COPY_VAL(m_query->request_cnt,data,ptr);
      m_query->requests = (ycsb_request *) 
      mem_allocator.alloc(sizeof(ycsb_request) * m_query->request_cnt, 0);
      COPY_VAL_SIZE(m_query->requests,data,ptr,sizeof(ycsb_request)*m_query->request_cnt);
#endif
#endif
      }
      break;
    case RPREPARE:
      COPY_VAL(query->pid,data,ptr);
      COPY_VAL(query->rc,data,ptr);
      COPY_VAL(query->txn_id,data,ptr);
      break;
		case RQRY: {
#if WORKLOAD == TPCC
      tpcc_query * m_query = (tpcc_query*) query;
#elif WORKLOAD == YCSB
      ycsb_query * m_query = (ycsb_query*) query;
#endif
      assert(WORKLOAD == YCSB);
      COPY_VAL(m_query->txn_rtype,data,ptr);
      COPY_VAL(m_query->pid,data,ptr);
#if CC_ALG == WAIT_DIE || CC_ALG == TIMESTAMP || CC_ALG == MVCC || CC_ALG == VLL
      COPY_VAL(m_query->ts,data,ptr);
#endif
#if CC_ALG == MVCC 
      COPY_VAL(m_query->thd_id,data,ptr);
#elif CC_ALG == OCC
      COPY_VAL(m_query->start_ts,data,ptr);
#endif
      COPY_VAL(m_query->req,data,ptr);
      break;
      }
		case RQRY_RSP: {
      COPY_VAL(query->rc,data,ptr);
      COPY_VAL(query->pid,data,ptr);
			break;
      }
    case RFIN:
      COPY_VAL(query->pid,data,ptr);
      COPY_VAL(query->rc,data,ptr);
      COPY_VAL(query->txn_id,data,ptr);
      break;
    case RACK:
      COPY_VAL(query->rc,data,ptr);
      break;
    case RTXN: {
#if WORKLOAD == TPCC
      tpcc_query * m_query = (tpcc_query*) query;
#elif WORKLOAD == YCSB
      ycsb_query *m_query = (ycsb_query*) query;
#endif
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
      query->home_part = query->active_part;
#endif
      assert(WORKLOAD == YCSB);
      m_query->client_id = m_query->return_id;
      COPY_VAL(m_query->pid,data,ptr);
      COPY_VAL(m_query->client_startts,data,ptr);
#if CC_ALG == CALVIN
      uint64_t batch_num __attribute__ ((unused));
      COPY_VAL(batch_num,data,ptr);
#endif
      COPY_VAL(m_query->part_num,data,ptr);
      m_query->part_to_access = (uint64_t *)
            mem_allocator.alloc(sizeof(uint64_t) * m_query->part_num, 0);
      for (uint64_t i = 0; i < m_query->part_num; ++i) {
        COPY_VAL(m_query->part_to_access[i],data,ptr);
      }
      COPY_VAL(m_query->request_cnt,data,ptr);
      m_query->requests = (ycsb_request *) 
            mem_allocator.alloc(sizeof(ycsb_request) * m_query->request_cnt, 0);
      for (uint64_t i = 0; i < m_query->request_cnt; ++i) {
        COPY_VAL_SIZE(m_query->requests[i],data,ptr,sizeof(ycsb_request));
      }
      assert(GET_NODE_ID(m_query->pid) == g_node_id);
      break;
      }
    case CL_RSP:
      RC rc_tmp;
      COPY_VAL(rc_tmp,data,ptr);
      uint64_t client_startts;
      COPY_VAL(client_startts,data,ptr);
      timespan = get_sys_clock() - client_startts;
      INC_STATS(0,client_latency,timespan);
      INC_STATS_ARR(0,all_lat,timespan);
      INC_STATS(0,txn_cnt,1);
      DEBUG("Received CL_RSP from %ld -- %ld %f\n", query->return_id,query->txn_id,(float)timespan/BILLION);
      break;
    case INIT_DONE: break;
    case EXP_DONE: break;
    case RPASS: break;
    case RLK: break;
    case RULK: break;
    case RLK_RSP: break;
    case RULK_RSP: break;
    case NO_MSG: assert(false);
    default: assert(false);
	}

}
