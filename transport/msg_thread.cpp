#include "msg_thread.h"
#include "msg_queue.h"
#include "mem_alloc.h"
#include "transport.h"
#include "query.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "txn_pool.h"
#include "global.h"

void MessageThread::init(uint64_t thd_id) { 
  buffer_cnt = g_node_cnt + g_client_node_cnt;
#if CC_ALG == CALVIN
  buffer_cnt++;
#endif
  buffer = (mbuf **) mem_allocator.alloc(sizeof(mbuf*) * buffer_cnt,0);
  for(uint64_t n = 0; n < buffer_cnt; n++) {
    buffer[n] = (mbuf *)mem_allocator.alloc(sizeof(mbuf),0);
    buffer[n]->init(n);
    buffer[n]->reset(n);
  }
  _thd_id = thd_id;
}

void MessageThread::run() {
  
  base_query * qry;
  RemReqType type;
  uint64_t dest;
  mbuf * sbuf;
  uint64_t sthd_prof_start = get_sys_clock();

  type = msg_queue.dequeue(qry,dest);


  if( type == NO_MSG ) {
  INC_STATS(_thd_id,sthd_prof_1b,get_sys_clock() - sthd_prof_start);
  sthd_prof_start = get_sys_clock();
    goto end;
  }

  INC_STATS(_thd_id,sthd_prof_1a,get_sys_clock() - sthd_prof_start);
  sthd_prof_start = get_sys_clock();

  assert(dest != g_node_id);
  sbuf = buffer[dest];
  if(!sbuf->fits(get_msg_size(type,qry))) {
    // send message
    DEBUG("Sending batch %ld txns to %ld\n",sbuf->cnt,dest);
    DEBUG_FLUSH();
	  ((uint32_t*)sbuf->buffer)[2] = sbuf->cnt;
    assert(sbuf->cnt > 0);
    INC_STATS(_thd_id,mbuf_send_time,get_sys_clock() - sbuf->starttime);
    //if(_thd_id == g_thread_cnt) 
    //  printf("Sthd Send1 %ld %ld\n",dest,get_sys_clock()-sbuf->starttime);
    tport_man.send_msg(_thd_id,dest,sbuf->buffer,sbuf->ptr);
    INC_STATS(_thd_id,msg_batch_size,sbuf->cnt);
    INC_STATS(_thd_id,msg_batch_bytes,sbuf->ptr);
    INC_STATS(_thd_id,msg_batch_cnt,1);
    sbuf->reset(dest);
  }

  INC_STATS(_thd_id,sthd_prof_2,get_sys_clock() - sthd_prof_start);
  sthd_prof_start = get_sys_clock();


  copy_to_buffer(sbuf,type,qry);

  //if(_thd_id == g_thread_cnt) 
  //  printf("Sthd Add %ld %ld\n",dest,get_sys_clock()-sbuf->starttime);

  INC_STATS(_thd_id,sthd_prof_3,get_sys_clock() - sthd_prof_start);
  sthd_prof_start = get_sys_clock();
  // This is the end for final RACKs and CL_RSP; delete from txn pool
  if((type == RACK && qry->rtype==RFIN) || (type == CL_RSP)) {
    if(ISSEQUENCER) {
      mem_allocator.free(qry,sizeof(base_query));
    } else {
#if MODE==SIMPLE_MODE
      // Need to free the original query
      //  that was not placed in txn pool
      //mem_allocator.free(qry,sizeof(ycsb_query));
      qry_pool.put(qry);
#else
      txn_table.delete_txn(qry->return_id, qry->txn_id);
#endif
    }
  }
#if MODE==QRY_ONLY_MODE || MODE == SETUP_MODE || MODE == SIMPLE_MODE
  if(!ISSEQUENCER && type == RQRY_RSP && qry->max_done) {
    txn_table.delete_txn(qry->return_id, qry->txn_id);
  }
#endif

  INC_STATS(_thd_id,sthd_prof_4,get_sys_clock() - sthd_prof_start);

end:
  sthd_prof_start = get_sys_clock();
  bool sent = false;
  for(uint64_t n = 0; n < buffer_cnt; n++) {
    if(buffer[n]->ready()) {
      assert(buffer[n]->cnt > 0);
	    ((uint32_t*)buffer[n]->buffer)[2] = buffer[n]->cnt;
      //DEBUG("Sending batch %ld txns to %ld\n",buffer[n]->cnt,n);
      INC_STATS(_thd_id,mbuf_send_time,get_sys_clock() - buffer[n]->starttime);
      //if(_thd_id == g_thread_cnt) 
      //  printf("Sthd Send2 %ld %ld\n",n,get_sys_clock()-buffer[n]->starttime);
      tport_man.send_msg(_thd_id,n,buffer[n]->buffer,buffer[n]->ptr);
      INC_STATS(_thd_id,msg_batch_size,buffer[n]->cnt);
      INC_STATS(_thd_id,msg_batch_bytes,buffer[n]->ptr);
      INC_STATS(_thd_id,msg_batch_cnt,1);
      buffer[n]->reset(n);
      sent = true;
    }
  }
  if(sent) {
    INC_STATS(_thd_id,sthd_prof_5a,get_sys_clock() - sthd_prof_start);
  } else {
    INC_STATS(_thd_id,sthd_prof_5b,get_sys_clock() - sthd_prof_start);
  }


}

void MessageThread::copy_to_buffer(mbuf * sbuf, RemReqType type, base_query * qry) {
  if(sbuf->cnt == 0)
    sbuf->starttime = get_sys_clock();
  sbuf->cnt++;

  if(ISCLIENT || type == INIT_DONE || type == EXP_DONE) {
    uint64_t tmp = UINT64_MAX;
  COPY_BUF(sbuf->buffer,tmp,sbuf->ptr);
  COPY_BUF(sbuf->buffer,type,sbuf->ptr);
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
  COPY_BUF(sbuf->buffer,tmp,sbuf->ptr);
  COPY_BUF(sbuf->buffer,tmp,sbuf->ptr);
#endif
  } else {
  assert(qry);
  COPY_BUF(sbuf->buffer,qry->txn_id,sbuf->ptr);
  COPY_BUF(sbuf->buffer,type,sbuf->ptr);
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
  COPY_BUF(sbuf->buffer,qry->dest_part,sbuf->ptr);
  COPY_BUF(sbuf->buffer,qry->hoem_part,sbuf->ptr);
#endif
  }

  switch(type) {
    case INIT_DONE:   DEBUG("Sending INIT_DONE\n");break;
    case EXP_DONE:    sbuf->wait = true; DEBUG("Sending EXP_DONE\n");break;
    case RLK:         /* TODO */ break;
    case  RULK:       /* TODO */ break;
    case  RQRY:       rqry(sbuf,qry);break;
    case  RFIN:       rfin(sbuf,qry);break;
    case  RLK_RSP:    /* TODO */ break;
    case  RULK_RSP:   /* TODO */ break;
    case  RQRY_RSP:   rqry_rsp(sbuf,qry);break;
    case  RACK:       rack(sbuf,qry);break;
    case  RTXN:       if(ISSEQUENCER) rtxn_seq(sbuf,qry); else rtxn(sbuf,qry);break;
    case  RINIT:      rinit(sbuf,qry);break;
    case  RPREPARE:   rprepare(sbuf,qry);break;
    case RPASS:       break;
    case CL_RSP:      cl_rsp(sbuf,qry);break;
    case NO_MSG: assert(false);
    default: assert(false);
  }
}

uint64_t MessageThread::get_msg_size(RemReqType type,base_query * qry) {
  uint64_t size = 0;
#if WORKLOAD == TPCC
  tpcc_client_query * m_qry2 = (tpcc_client_query*) qry;
  tpcc_query * m_qry = (tpcc_query *)qry;
#elif WORKLOAD == YCSB
  ycsb_client_query * m_qry2 = (ycsb_client_query*) qry;
  ycsb_query * m_qry __attribute__((unused));
  m_qry = (ycsb_query *)qry;
#endif
  size += sizeof(txnid_t) + sizeof(RemReqType); //12
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
  size += sizeof(uint64_t) * 2;
#endif
  switch(type) {
        case NO_MSG: break;
        case INIT_DONE:   break;
        case EXP_DONE:    break;
        case RLK:         /* TODO */ break;
        case  RULK:       /* TODO */ break;
        case  RQRY:       
#if WORKLOAD == TPCC
                          size +=sizeof(TPCCRemTxnType);
                          size +=sizeof(TPCCTxnType);
  switch(m_qry->txn_rtype) {
    case TPCC_PAYMENT0 :
      size += sizeof(uint64_t)*3 + sizeof(double);
      break;
    case TPCC_PAYMENT4 :
      size += sizeof(uint64_t)*5 + sizeof(char)*LASTNAME_LEN + sizeof(double) + sizeof(bool);
      break;
    case TPCC_NEWORDER0 :
      size += sizeof(uint64_t)*4 + sizeof(bool);
      break;
    case TPCC_NEWORDER6 :
      size += sizeof(uint64_t);
      break;
    case TPCC_NEWORDER8 :
      size += sizeof(uint64_t)*7 + sizeof(bool);
      break;
    default: assert(false);
  }
#elif WORKLOAD == YCSB
                          size +=sizeof(YCSBRemTxnType);
                          size += sizeof(ycsb_request);
#endif
                          size +=sizeof(uint64_t);
#if CC_ALG == WAIT_DIE || CC_ALG == TIMESTAMP || CC_ALG == MVCC || CC_ALG == VLL
                          size += sizeof(uint64_t);
#endif
#if CC_ALG == MVCC  || CC_ALG == OCC
                          size += sizeof(uint64_t);
#endif
                          break;
        case  RFIN:       size +=sizeof(uint64_t) + sizeof(RC) + sizeof(uint64_t);break;
        case  RLK_RSP:    /* TODO */ break;
        case  RULK_RSP:   /* TODO */ break;
        case  RQRY_RSP:   size +=sizeof(RC) + sizeof(uint64_t);break;
        case  RACK:       size += sizeof(RC);break;
        case  RTXN:       
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
                          size += sizeof(uint64_t);
#endif
                          size += sizeof(uint64_t) * 2; //24
#if CC_ALG == CALVIN
                          size += sizeof(uint64_t)*2; // 24
#endif
                          if(ISSEQUENCER) 
                            size += sizeof(uint64_t) * (m_qry->part_num + 1); //24
                          else
                            size += sizeof(uint64_t) * (m_qry2->part_num + 1); //24
                          size += sizeof(uint64_t); //8

#if WORKLOAD == TPCC
                          size += sizeof(TPCCTxnType);
                          size += sizeof(uint64_t)*3;
  switch (m_qry2->txn_type) {
    case TPCC_PAYMENT:
      size += sizeof(uint64_t)*3 + sizeof(char)*LASTNAME_LEN+ sizeof(double) + sizeof(bool);
      break;
    case TPCC_NEW_ORDER:
      size += sizeof(uint64_t)*4 + sizeof(Item_no)*m_qry2->ol_cnt + sizeof(bool)*2;
      break;
    default:
      assert(false);
  }
#elif WORKLOAD == YCSB
                          if(ISSEQUENCER) 
                            size += sizeof(ycsb_request) * (m_qry->request_cnt); // 240
                          else
                            size += sizeof(ycsb_request) * (m_qry2->request_cnt); // 240
#endif
                          break;
        case  RINIT:      
                          size +=sizeof(ts_t) + sizeof(uint64_t);
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
                          size += sizeof(uint64_t) + sizeof(uint64_t);
#endif
#if CC_ALG == VLL 
#if WORKLOAD == TPCC
#elif WORKLOAD == YCSB
                          size += sizeof(uint64_t) + sizeof(ycsb_request) * m_qry->request_cnt;
#endif
#endif
                          break;
        case  RPREPARE:   size +=sizeof(uint64_t) + sizeof(RC) + sizeof(uint64_t);break;
        case RPASS:       break;
        case CL_RSP:      size +=sizeof(RC) + sizeof(uint64_t);break;
        default: assert(false);
  }

  return size;
}
void MessageThread::rack(mbuf * sbuf,base_query * qry) {
  DEBUG("Sending RACK %ld\n",qry->txn_id);
  assert(IS_REMOTE(qry->txn_id));
  COPY_BUF(sbuf->buffer,qry->rc,sbuf->ptr);
}

void MessageThread::rprepare(mbuf * sbuf,base_query * qry) {
  DEBUG("Sending RPREPARE %ld\n",qry->txn_id);
  assert(IS_LOCAL(qry->txn_id));
  COPY_BUF(sbuf->buffer,qry->pid,sbuf->ptr);
  COPY_BUF(sbuf->buffer,qry->rc,sbuf->ptr);
  COPY_BUF(sbuf->buffer,qry->txn_id,sbuf->ptr);
}

void MessageThread::rfin(mbuf * sbuf,base_query * qry) {
  DEBUG("Sending RFIN %ld\n",qry->txn_id);
  assert(IS_LOCAL(qry->txn_id));
  COPY_BUF(sbuf->buffer,qry->pid,sbuf->ptr);
  COPY_BUF(sbuf->buffer,qry->rc,sbuf->ptr);
  COPY_BUF(sbuf->buffer,qry->txn_id,sbuf->ptr);
}

void MessageThread::cl_rsp(mbuf * sbuf, base_query *qry) {
  DEBUG("Sending CL_RSP %ld\n",qry->txn_id);
  assert(IS_LOCAL(qry->txn_id));
  COPY_BUF(sbuf->buffer,qry->rc,sbuf->ptr);
  COPY_BUF(sbuf->buffer,qry->client_startts,sbuf->ptr);

}

void MessageThread::rinit(mbuf * sbuf,base_query * qry) {
  DEBUG("Sending RINIT %ld\n",qry->txn_id);
  assert(IS_LOCAL(qry->txn_id));
  uint64_t part_id;
  if(CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC) {
    part_id = qry->home_part;
  } else {
    part_id = GET_PART_ID(0,g_node_id);
  }
  COPY_BUF(sbuf->buffer,qry->ts,sbuf->ptr);
  COPY_BUF(sbuf->buffer,part_id,sbuf->ptr);
#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
  uint64_t part_cnt = 1; // TODO: generalize?
  COPY_BUF(sbuf->buffer,qry->dest_part_id,sbuf->ptr);
  COPY_BUF(sbuf->buffer,part_cnt,sbuf->ptr);
#elif CC_ALG == VLL
#if WORKLOAD == TPCC
  //tpcc_query * m_qry = (tpcc_query *)qry;
#elif WORKLOAD == YCSB
  ycsb_query * m_qry = (ycsb_query *)qry;
  COPY_BUF(sbuf->buffer,m_qry->request_cnt,sbuf->ptr);
	for (uint64_t i = 0; i < m_qry->request_cnt; i++) {
    COPY_BUF_SIZE(sbuf->buffer,m_qry->requests[i],sbuf->ptr,sizeof(ycsb_request));
  }
  //COPY_BUF_SIZE(sbuf->buffer,m_qry->requests,sbuf->ptr,sizeof(ycsb_request) * m_qry->request_cnt);
#endif
#endif
}

void MessageThread::rqry(mbuf * sbuf, base_query *qry) {
  DEBUG("Sending RQRY %ld\n",qry->txn_id);
  assert(IS_LOCAL(qry->txn_id));
#if WORKLOAD == TPCC
  tpcc_query * m_qry = (tpcc_query *)qry;
#elif WORKLOAD == YCSB
  ycsb_query * m_qry = (ycsb_query *)qry;
#endif

  COPY_BUF(sbuf->buffer,m_qry->txn_rtype,sbuf->ptr);
  COPY_BUF(sbuf->buffer,qry->pid,sbuf->ptr);
#if CC_ALG == WAIT_DIE || CC_ALG == TIMESTAMP || CC_ALG == MVCC || CC_ALG == VLL
  COPY_BUF(sbuf->buffer,qry->ts,sbuf->ptr);
#endif
#if CC_ALG == MVCC 
  COPY_BUF(sbuf->buffer,qry->thd_id,sbuf->ptr);
#elif CC_ALG == OCC 
  COPY_BUF(sbuf->buffer,qry->start_ts,sbuf->ptr);
#endif

#if WORKLOAD == TPCC
  switch(m_qry->txn_rtype) {
    case TPCC_PAYMENT0 :
      COPY_BUF(sbuf->buffer,m_qry->w_id,sbuf->ptr);
      COPY_BUF(sbuf->buffer,m_qry->d_id,sbuf->ptr);
      COPY_BUF(sbuf->buffer,m_qry->d_w_id,sbuf->ptr);
      COPY_BUF(sbuf->buffer,m_qry->h_amount,sbuf->ptr);
      break;
    case TPCC_PAYMENT4 :
      COPY_BUF(sbuf->buffer,m_qry->w_id,sbuf->ptr);
      COPY_BUF(sbuf->buffer,m_qry->d_id,sbuf->ptr);
      COPY_BUF(sbuf->buffer,m_qry->c_id,sbuf->ptr);
      COPY_BUF(sbuf->buffer,m_qry->c_w_id,sbuf->ptr);
      COPY_BUF(sbuf->buffer,m_qry->c_d_id,sbuf->ptr);
      COPY_BUF(sbuf->buffer,m_qry->c_last,sbuf->ptr);
      COPY_BUF(sbuf->buffer,m_qry->h_amount,sbuf->ptr);
      COPY_BUF(sbuf->buffer,m_qry->by_last_name,sbuf->ptr);
      break;
    case TPCC_NEWORDER0 :
      COPY_BUF(sbuf->buffer,m_qry->w_id,sbuf->ptr);
      COPY_BUF(sbuf->buffer,m_qry->d_id,sbuf->ptr);
      COPY_BUF(sbuf->buffer,m_qry->c_id,sbuf->ptr);
      COPY_BUF(sbuf->buffer,m_qry->remote,sbuf->ptr);
      COPY_BUF(sbuf->buffer,m_qry->ol_cnt,sbuf->ptr);
      break;
    case TPCC_NEWORDER6 :
      COPY_BUF(sbuf->buffer,m_qry->ol_i_id,sbuf->ptr);
      break;
    case TPCC_NEWORDER8 :
      COPY_BUF(sbuf->buffer,m_qry->w_id,sbuf->ptr);
      COPY_BUF(sbuf->buffer,m_qry->d_id,sbuf->ptr);
      COPY_BUF(sbuf->buffer,m_qry->remote,sbuf->ptr);
      COPY_BUF(sbuf->buffer,m_qry->ol_i_id,sbuf->ptr);
      COPY_BUF(sbuf->buffer,m_qry->ol_supply_w_id,sbuf->ptr);
      COPY_BUF(sbuf->buffer,m_qry->ol_quantity,sbuf->ptr);
      COPY_BUF(sbuf->buffer,m_qry->ol_number,sbuf->ptr);
      COPY_BUF(sbuf->buffer,m_qry->o_id,sbuf->ptr);
      break;
    default: assert(false);

  }
#elif WORKLOAD == YCSB
  COPY_BUF(sbuf->buffer,m_qry->req,sbuf->ptr);
#endif



#if MODE==QRY_ONLY_MODE
  COPY_BUF(sbuf->buffer,m_qry->max_access,sbuf->ptr);
#endif
}

void MessageThread::rqry_rsp(mbuf * sbuf, base_query *qry) {
  DEBUG("Sending RQRY_RSP %ld\n",qry->txn_id);
  assert(IS_REMOTE(qry->txn_id));
  COPY_BUF(sbuf->buffer,qry->rc,sbuf->ptr);
  COPY_BUF(sbuf->buffer,qry->pid,sbuf->ptr);
}

void MessageThread::rtxn(mbuf * sbuf, base_query *qry) {
  DEBUG("Sending RTXN\n");
  //assert(ISCLIENT);
#if WORKLOAD == TPCC
    tpcc_client_query * m_qry = (tpcc_client_query *)qry;
#elif WORKLOAD == YCSB
    ycsb_client_query * m_qry = (ycsb_client_query *)qry;
#endif
  uint64_t ts = get_sys_clock();

#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
  COPY_BUF(sbuf->buffer,m_qry->part_to_access[0],sbuf->ptr);
#endif
  COPY_BUF(sbuf->buffer,m_qry->pid,sbuf->ptr);
  COPY_BUF(sbuf->buffer,ts,sbuf->ptr);
#if CC_ALG == CALVIN
  COPY_BUF(sbuf->buffer,m_qry->batch_id,sbuf->ptr);
  COPY_BUF(sbuf->buffer,m_qry->txn_id,sbuf->ptr);
#endif
  COPY_BUF(sbuf->buffer,m_qry->part_num,sbuf->ptr);
  for (uint64_t i = 0; i < m_qry->part_num; ++i) {
    COPY_BUF(sbuf->buffer,m_qry->part_to_access[i],sbuf->ptr);
  }

#if WORKLOAD == TPCC
	COPY_BUF(sbuf->buffer,m_qry->txn_type,sbuf->ptr);
  COPY_BUF(sbuf->buffer,m_qry->w_id,sbuf->ptr);
  COPY_BUF(sbuf->buffer,m_qry->d_id,sbuf->ptr);
  COPY_BUF(sbuf->buffer,m_qry->c_id,sbuf->ptr);
  switch (m_qry->txn_type) {
    case TPCC_PAYMENT:
      COPY_BUF(sbuf->buffer,m_qry->d_w_id,sbuf->ptr);
      COPY_BUF(sbuf->buffer,m_qry->c_w_id,sbuf->ptr);
      COPY_BUF(sbuf->buffer,m_qry->c_d_id,sbuf->ptr);
      COPY_BUF(sbuf->buffer,m_qry->c_last,sbuf->ptr);
      COPY_BUF(sbuf->buffer,m_qry->h_amount,sbuf->ptr);
      COPY_BUF(sbuf->buffer,m_qry->by_last_name,sbuf->ptr);
      break;
    case TPCC_NEW_ORDER:
      COPY_BUF(sbuf->buffer,m_qry->ol_cnt,sbuf->ptr);
      for (uint64_t j = 0; j < m_qry->ol_cnt; ++j) {
          COPY_BUF(sbuf->buffer,m_qry->items[j],sbuf->ptr);
      }
      COPY_BUF(sbuf->buffer,m_qry->rbk,sbuf->ptr);
      COPY_BUF(sbuf->buffer,m_qry->remote,sbuf->ptr);
      COPY_BUF(sbuf->buffer,m_qry->o_entry_d,sbuf->ptr);
      COPY_BUF(sbuf->buffer,m_qry->o_carrier_id,sbuf->ptr);
      COPY_BUF(sbuf->buffer,m_qry->ol_delivery_d,sbuf->ptr);
      break;
    default:
      assert(false);
  }
#elif WORKLOAD == YCSB
  COPY_BUF(sbuf->buffer,m_qry->request_cnt,sbuf->ptr);
  for (uint64_t i = 0; i < m_qry->request_cnt; ++i) {
    COPY_BUF_SIZE(sbuf->buffer,m_qry->requests[i],sbuf->ptr,sizeof(ycsb_request));
  }
#endif

}

void MessageThread::rtxn_seq(mbuf * sbuf, base_query *qry) {
  DEBUG("Sending RTXN\n");
#if WORKLOAD == TPCC
    tpcc_query * m_qry = (tpcc_query *)qry;
#elif WORKLOAD == YCSB
    ycsb_query * m_qry = (ycsb_query *)qry;
#endif
  uint64_t ts = get_sys_clock();

#if CC_ALG == HSTORE || CC_ALG == HSTORE_SPEC
  COPY_BUF(sbuf->buffer,m_qry->part_to_access[0],sbuf->ptr);
#endif
  COPY_BUF(sbuf->buffer,m_qry->pid,sbuf->ptr);
  COPY_BUF(sbuf->buffer,ts,sbuf->ptr);
#if CC_ALG == CALVIN
  uint64_t batch_num = 0;
  COPY_BUF(sbuf->buffer,batch_num,sbuf->ptr);
  COPY_BUF(sbuf->buffer,m_qry->txn_id,sbuf->ptr);
#endif
  COPY_BUF(sbuf->buffer,m_qry->part_num,sbuf->ptr);
  for (uint64_t i = 0; i < m_qry->part_num; ++i) {
    COPY_BUF(sbuf->buffer,m_qry->part_to_access[i],sbuf->ptr);
  }

#if WORKLOAD == TPCC
	COPY_BUF(sbuf->buffer,m_qry->txn_type,sbuf->ptr);
  COPY_BUF(sbuf->buffer,m_qry->w_id,sbuf->ptr);
  COPY_BUF(sbuf->buffer,m_qry->d_id,sbuf->ptr);
  COPY_BUF(sbuf->buffer,m_qry->c_id,sbuf->ptr);
  switch (m_qry->txn_type) {
    case TPCC_PAYMENT:
      COPY_BUF(sbuf->buffer,m_qry->d_w_id,sbuf->ptr);
      COPY_BUF(sbuf->buffer,m_qry->c_w_id,sbuf->ptr);
      COPY_BUF(sbuf->buffer,m_qry->c_d_id,sbuf->ptr);
      COPY_BUF(sbuf->buffer,m_qry->c_last,sbuf->ptr);
      COPY_BUF(sbuf->buffer,m_qry->h_amount,sbuf->ptr);
      COPY_BUF(sbuf->buffer,m_qry->by_last_name,sbuf->ptr);
      break;
    case TPCC_NEW_ORDER:
      COPY_BUF(sbuf->buffer,m_qry->ol_cnt,sbuf->ptr);
      for (uint64_t j = 0; j < m_qry->ol_cnt; ++j) {
          COPY_BUF(sbuf->buffer,m_qry->items[j],sbuf->ptr);
      }
      COPY_BUF(sbuf->buffer,m_qry->rbk,sbuf->ptr);
      COPY_BUF(sbuf->buffer,m_qry->remote,sbuf->ptr);
      COPY_BUF(sbuf->buffer,m_qry->o_entry_d,sbuf->ptr);
      COPY_BUF(sbuf->buffer,m_qry->o_carrier_id,sbuf->ptr);
      COPY_BUF(sbuf->buffer,m_qry->ol_delivery_d,sbuf->ptr);
      break;
    default:
      assert(false);
  }
#elif WORKLOAD == YCSB
  COPY_BUF(sbuf->buffer,m_qry->request_cnt,sbuf->ptr);
  for (uint64_t i = 0; i < m_qry->request_cnt; ++i) {
    COPY_BUF_SIZE(sbuf->buffer,m_qry->requests[i],sbuf->ptr,sizeof(ycsb_request));
  }
#endif

}
