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
