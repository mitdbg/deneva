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

#include "mem_alloc.h"
#include "query.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "global.h"
#include "message.h"

Message * Message::create_message(char * buf) {
 RemReqType rtype = NO_MSG;
 uint64_t ptr = 0;
 COPY_VAL(rtype,buf,ptr);
 Message * msg = create_message(rtype);
 msg->copy_from_buf(buf);
 return msg;
}

Message * Message::create_message(BaseQuery * qry, RemReqType rtype) {
 Message * msg = create_message(rtype);
 msg->copy_from_query(qry);
 return msg;
}


Message * Message::create_message(RemReqType rtype) {
  Message * msg;
  switch(rtype) {
    case INIT_DONE:
      msg = new InitDoneMessage;
      break;
    case RQRY:
#if WORKLOAD == YCSB
      msg = new YCSBQueryMessage;
      msg->init();
#endif
      break;
    case RFIN:
      msg = new FinishMessage;
      break;
    case RQRY_RSP:
      msg = new QueryResponseMessage;
      break;
    case RACK:
      msg = new AckMessage;
      break;
    case RTXN:
      msg = new ClientQueryMessage;
      msg->init();
      break;
    case RINIT:
      msg = new InitMessage;
      break;
    case RPREPARE:
      msg = new PrepareMessage;
      break;
    case RFWD:
      msg = new ForwardMessage;
      break;
    case RDONE:
      msg = new DoneMessage;
      break;
    case CL_RSP:
      msg = new ClientResponseMessage;
      break;
    default: assert(false);
  }
  assert(msg);
  msg->rtype = rtype;
  return msg;
}

uint64_t Message::mget_size() {
  uint64_t size = sizeof(Message);
  return size;
}

void Message::mcopy_from_query(BaseQuery * qry) {
  //rtype = qry->rtype;
  txn_id = qry->txn_id;
}

void Message::mcopy_from_buf(char * buf) {
  uint64_t ptr = 0;
  COPY_VAL(rtype,buf,ptr);
  COPY_VAL(txn_id,buf,ptr);
}

void Message::mcopy_to_buf(char * buf) {
  uint64_t ptr = 0;
  COPY_BUF(buf,rtype,ptr);
  COPY_BUF(buf,txn_id,ptr);
}

/************************/

uint64_t QueryMessage::get_size() {
  uint64_t size = sizeof(QueryMessage);
  return size;
}

void QueryMessage::copy_from_query(BaseQuery * qry) {
  Message::mcopy_from_query(qry);
  pid = qry->pid;
#if CC_ALG == WAIT_DIE || CC_ALG == TIMESTAMP || CC_ALG == MVCC || CC_ALG == VLL
  ts = qry->ts;
#endif
#if CC_ALG == MVCC 
  thd_id = qry->thd_id;
#elif CC_ALG == OCC 
  start_ts = qry->start_ts;
#endif
#if MODE==QRY_ONLY_MODE
  max_access = qry->max_access;
#endif
}

void QueryMessage::copy_to_query(BaseQuery * qry) {
  Message::mcopy_to_query(qry);
  qry->pid = pid;
#if CC_ALG == WAIT_DIE || CC_ALG == TIMESTAMP || CC_ALG == MVCC || CC_ALG == VLL
  qry->ts = ts;
#endif
#if CC_ALG == MVCC 
  qry->thd_id = thd_id;
#elif CC_ALG == OCC 
  qry->start_ts = start_ts;
#endif
#if MODE==QRY_ONLY_MODE
  qry->max_access = max_access;
#endif

}

void QueryMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
 COPY_VAL(pid,buf,ptr);
#if CC_ALG == WAIT_DIE || CC_ALG == TIMESTAMP || CC_ALG == MVCC || CC_ALG == VLL
 COPY_VAL(ts,buf,ptr);
#endif
#if CC_ALG == MVCC 
 COPY_VAL(thd_id,buf,ptr);
#elif CC_ALG == OCC 
 COPY_VAL(start_ts,buf,ptr);
#endif
#if MODE==QRY_ONLY_MODE
 COPY_VAL(max_access,buf,ptr);
#endif
}

void QueryMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
 COPY_BUF(buf,pid,ptr);
#if CC_ALG == WAIT_DIE || CC_ALG == TIMESTAMP || CC_ALG == MVCC || CC_ALG == VLL
 COPY_BUF(buf,ts,ptr);
#endif
#if CC_ALG == MVCC 
 COPY_BUF(buf,thd_id,ptr);
#elif CC_ALG == OCC 
 COPY_BUF(buf,start_ts,ptr);
#endif
#if MODE==QRY_ONLY_MODE
 COPY_BUF(buf,max_access,ptr);
#endif
}

/************************/

void YCSBClientQueryMessage::init() {
  requests = (ycsb_request*)mem_allocator.alloc(sizeof(ycsb_request)*g_req_per_query);
}

uint64_t YCSBClientQueryMessage::get_size() {
  uint64_t size = sizeof(YCSBClientQueryMessage);
  size += sizeof(ycsb_request) * req_cnt;
  return size;
}

void YCSBClientQueryMessage::copy_from_query(BaseQuery * qry) {
  ClientQueryMessage::mcopy_from_query(qry);
  req_cnt = ((YCSBQuery*)qry)->request_cnt;
  for(uint64_t i = 0 ; i < req_cnt;i++) {
    requests[i] = ((YCSBQuery*)qry)->requests[i];
  }
}

void YCSBClientQueryMessage::copy_to_query(BaseQuery * qry) {
  ClientQueryMessage::copy_to_query(qry);
  ((YCSBQuery*)qry)->request_cnt = req_cnt;
  for(uint64_t i = 0 ; i < req_cnt;i++) {
    ((YCSBQuery*)qry)->requests[i] = requests[i];
  }
}

void YCSBClientQueryMessage::copy_from_buf(char * buf) {
  ClientQueryMessage::copy_from_buf(buf);
  uint64_t ptr = ClientQueryMessage::get_size();
  COPY_VAL(req_cnt,buf,ptr);
  for(uint64_t i = 0 ; i < req_cnt;i++) {
    COPY_VAL(requests[i],buf,ptr);
  }
}

void YCSBClientQueryMessage::copy_to_buf(char * buf) {
  ClientQueryMessage::copy_to_buf(buf);
  uint64_t ptr = ClientQueryMessage::get_size();
  COPY_BUF(buf,req_cnt,ptr);
  for(uint64_t i = 0; i < req_cnt; i++) {
    COPY_BUF(buf,requests[i],ptr);
  }
}

/************************/

void ClientQueryMessage::init() {
  part_to_access = (uint64_t*)mem_allocator.alloc(sizeof(uint64_t)*g_part_cnt);
}



uint64_t ClientQueryMessage::get_size() {
  uint64_t size = sizeof(ClientQueryMessage);
  size += sizeof(uint64_t) * part_num;
  return size;
}

void ClientQueryMessage::copy_from_query(BaseQuery * qry) {
  Message::mcopy_from_query(qry);
  pid = qry->pid;
  ts = qry->ts;
#if CC_ALG == CALVIN
  batch_id = qry->batch_id;
  txn_id = qry->txn_id;
#endif
  part_num = qry->part_num;
  for(uint64_t i = 0; i < part_num; i++) {
    part_to_access[i] = qry->part_to_access[i];
  }
}

void ClientQueryMessage::copy_to_query(BaseQuery * qry) {
  Message::mcopy_to_query(qry);
  qry->pid = pid;
  qry->ts = ts;
#if CC_ALG == CALVIN
  qry->batch_id = batch_id;
  qry->txn_id = txn_id;
#endif
  qry->part_num = part_num;
  for(uint64_t i = 0; i < part_num; i++) {
    qry->part_to_access[i] = part_to_access[i];
  }
}

void ClientQueryMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_VAL(pid,buf,ptr);
  COPY_VAL(ts,buf,ptr);
#if CC_ALG == CALVIN
  COPY_VAL(batch_id,buf,ptr);
  COPY_VAL(txn_id,buf,ptr);
#endif
  COPY_VAL(part_num,buf,ptr);
  for(uint64_t i = 0; i < part_num; i++) {
    COPY_VAL(part_to_access[i],buf,ptr);
  }
}

void ClientQueryMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_BUF(buf,pid,ptr);
  COPY_BUF(buf,ts,ptr);
#if CC_ALG == CALVIN
  COPY_BUF(buf,batch_id,ptr);
  COPY_BUF(buf,txn_id,ptr);
#endif
  COPY_BUF(buf,part_num,ptr);
  for(uint64_t i = 0; i < part_num; i++) {
    COPY_BUF(buf,part_to_access[i],ptr);
  }
}

/************************/


uint64_t ClientResponseMessage::get_size() {
  uint64_t size = sizeof(ClientResponseMessage);
  return size;
}

void ClientResponseMessage::copy_from_query(BaseQuery * qry) {
  Message::mcopy_from_query(qry);
  rc = qry->rc;
  client_startts = qry->client_startts;
}

void ClientResponseMessage::copy_to_query(BaseQuery * qry) {
  Message::mcopy_to_query(qry);
  qry->rc = rc;
  qry->client_startts = client_startts;
}

void ClientResponseMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_VAL(rc,buf,ptr);
  COPY_VAL(client_startts,buf,ptr);
}

void ClientResponseMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_BUF(buf,rc,ptr);
  COPY_BUF(buf,client_startts,ptr);
}

/************************/


uint64_t DoneMessage::get_size() {
  uint64_t size = sizeof(DoneMessage);
  return size;
}

void DoneMessage::copy_from_query(BaseQuery * qry) {
  Message::mcopy_from_query(qry);
  batch_id = qry->batch_id;
}

void DoneMessage::copy_to_query(BaseQuery * qry) {
  Message::mcopy_to_query(qry);
  qry->batch_id = batch_id;
}

void DoneMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_VAL(batch_id,buf,ptr);
}

void DoneMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_BUF(buf,batch_id,ptr);
}

/************************/


uint64_t ForwardMessage::get_size() {
  uint64_t size = sizeof(ForwardMessage);
  return size;
}

void ForwardMessage::copy_from_query(BaseQuery * qry) {
  Message::mcopy_from_query(qry);
  txn_id = qry->txn_id;
  batch_id = qry->batch_id;
#if WORKLOAD == TPCC
  o_id = ((TPCCQuery*)qry)->o_id;
#endif
}

void ForwardMessage::copy_to_query(BaseQuery * qry) {
  Message::mcopy_to_query(qry);
  qry->txn_id = txn_id;
  qry->batch_id = batch_id;
#if WORKLOAD == TPCC
  ((TPCCQuery*)qry)->o_id = o_id;
#endif
}

void ForwardMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_VAL(txn_id,buf,ptr);
  COPY_VAL(batch_id,buf,ptr);
#if WORKLOAD == TPCC
  COPY_VAL(o_id,buf,ptr);
#endif
}

void ForwardMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_BUF(buf,txn_id,ptr);
  COPY_BUF(buf,batch_id,ptr);
#if WORKLOAD == TPCC
  COPY_BUF(buf,o_id,ptr);
#endif
}

/************************/

uint64_t PrepareMessage::get_size() {
  uint64_t size = sizeof(PrepareMessage);
  return size;
}

void PrepareMessage::copy_from_query(BaseQuery * qry) {
  Message::mcopy_from_query(qry);
  pid = qry->pid;
  rc = qry->rc;
  txn_id = qry->txn_id;
}

void PrepareMessage::copy_to_query(BaseQuery * qry) {
  Message::mcopy_to_query(qry);
  qry->pid = pid;
  qry->rc = rc;
  qry->txn_id = txn_id;
}

void PrepareMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_VAL(pid,buf,ptr);
  COPY_VAL(rc,buf,ptr);
  COPY_VAL(txn_id,buf,ptr);
}

void PrepareMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_BUF(buf,pid,ptr);
  COPY_BUF(buf,rc,ptr);
  COPY_BUF(buf,txn_id,ptr);
}

/************************/




uint64_t InitMessage::get_size() {
  uint64_t size = sizeof(InitMessage);
  return size;
}

void InitMessage::copy_from_query(BaseQuery * qry) {
  Message::mcopy_from_query(qry);
  ts = qry->ts;
  part_id = GET_PART_ID(0,g_node_id);
}

void InitMessage::copy_to_query(BaseQuery * qry) {
  Message::mcopy_to_query(qry);
  qry->ts = ts;
}

void InitMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_VAL(ts,buf,ptr);
  COPY_VAL(part_id,buf,ptr);
}

void InitMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_BUF(buf,ts,ptr);
  COPY_BUF(buf,part_id,ptr);
}

/************************/


uint64_t AckMessage::get_size() {
  uint64_t size = sizeof(AckMessage);
  return size;
}

void AckMessage::copy_from_query(BaseQuery * qry) {
  Message::mcopy_from_query(qry);
  rc = qry->rc;
#if CC_ALG == CALVIN
  batch_id = qry->batch_id;
#endif
}

void AckMessage::copy_to_query(BaseQuery * qry) {
  Message::mcopy_to_query(qry);
  qry->rc = rc;
#if CC_ALG == CALVIN
  qry->batch_id = batch_id;
#endif
}

void AckMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_VAL(rc,buf,ptr);
#if CC_ALG == CALVIN
  COPY_VAL(batch_id,buf,ptr);
#endif
}

void AckMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_BUF(buf,rc,ptr);
#if CC_ALG == CALVIN
  COPY_BUF(buf,batch_id,ptr);
#endif
}

/************************/

uint64_t QueryResponseMessage::get_size() {
  uint64_t size = sizeof(QueryResponseMessage);
  return size;
}

void QueryResponseMessage::copy_from_query(BaseQuery * qry) {
  Message::mcopy_from_query(qry);
  rc = qry->rc;
  pid = qry->pid;
}

void QueryResponseMessage::copy_to_query(BaseQuery * qry) {
  Message::mcopy_to_query(qry);
  qry->rc = rc;
  qry->pid = pid;
}

void QueryResponseMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_VAL(rc,buf,ptr);
  COPY_VAL(pid,buf,ptr);
}

void QueryResponseMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_BUF(buf,rc,ptr);
  COPY_BUF(buf,pid,ptr);
}

/************************/



uint64_t FinishMessage::get_size() {
  uint64_t size = sizeof(FinishMessage);
  return size;
}

void FinishMessage::copy_from_query(BaseQuery * qry) {
  Message::mcopy_from_query(qry);
  pid = qry->pid;
  rc = qry->rc;
  txn_id = qry->txn_id;
  batch_id = qry->batch_id;
  ro = qry->ro;
}

void FinishMessage::copy_to_query(BaseQuery * qry) {
  Message::mcopy_to_query(qry);
  qry->pid = pid;
  qry->rc = rc;
  qry->txn_id = txn_id;
  qry->batch_id = batch_id;
  qry->ro = ro;
}

void FinishMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_VAL(pid,buf,ptr);
  COPY_VAL(rc,buf,ptr);
  COPY_VAL(txn_id,buf,ptr);
  COPY_VAL(batch_id,buf,ptr);
  COPY_VAL(ro,buf,ptr);
}

void FinishMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_BUF(buf,pid,ptr);
  COPY_BUF(buf,rc,ptr);
  COPY_BUF(buf,txn_id,ptr);
  COPY_BUF(buf,batch_id,ptr);
  COPY_BUF(buf,ro,ptr);
}

/************************/

uint64_t InitDoneMessage::get_size() {
  uint64_t size = sizeof(InitDoneMessage);
  return size;
}

void InitDoneMessage::copy_from_query(BaseQuery * qry) {
}

void InitDoneMessage::copy_to_query(BaseQuery * qry) {
  Message::mcopy_to_query(qry);
}

void InitDoneMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
}

void InitDoneMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
}

/************************/

void YCSBQueryMessage::init() {
  requests = (ycsb_request*)mem_allocator.alloc(sizeof(ycsb_request)*g_req_per_query);
}

uint64_t YCSBQueryMessage::get_size() {
  uint64_t size = sizeof(YCSBQueryMessage);
  size += req_cnt * sizeof(ycsb_request);
  return size;
}

void YCSBQueryMessage::copy_from_query(BaseQuery * qry) {
  QueryMessage::copy_from_query(qry);
  req_cnt = ((YCSBQuery*)qry)->request_cnt;
  for(uint64_t i = 0 ; i < req_cnt;i++) {
    requests[i] = ((YCSBQuery*)qry)->requests[i];
  }
}

void YCSBQueryMessage::copy_to_query(BaseQuery * qry) {
  QueryMessage::copy_to_query(qry);
  ((YCSBQuery*)qry)->request_cnt = req_cnt;
  for(uint64_t i = 0 ; i < req_cnt;i++) {
    ((YCSBQuery*)qry)->requests[i] = requests[i];
  }
}


void YCSBQueryMessage::copy_from_buf(char * buf) {
  QueryMessage::copy_from_buf(buf);
  uint64_t ptr = QueryMessage::get_size();

  COPY_VAL(req_cnt,buf,ptr);
  for(uint64_t i = 0 ; i < req_cnt;i++) {
    COPY_VAL(requests[i],buf,ptr);
  }
}

void YCSBQueryMessage::copy_to_buf(char * buf) {
  QueryMessage::copy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_BUF(buf,req_cnt,ptr);
  for(uint64_t i = 0; i < req_cnt; i++) {
    COPY_BUF(buf,requests[i],ptr);
  }
}

