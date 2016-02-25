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

std::vector<Message*> Message::create_messages(char * buf) {
  std::vector<Message*> all_msgs;
  char * data = buf;
	uint64_t ptr = 0;
  uint32_t dest_id;
  uint32_t return_id;
  uint32_t txn_cnt;
  COPY_VAL(dest_id,data,ptr);
  COPY_VAL(return_id,data,ptr);
  COPY_VAL(txn_cnt,data,ptr);
  assert(dest_id == g_node_id);
  assert(ISCLIENTN(return_id) || ISSERVERN(return_id));
  while(txn_cnt > 0) {
    Message * msg = create_message(&data[ptr]);
    msg->return_node_id = return_id;
    ptr += msg->get_size();
    all_msgs.push_back(msg);
    --txn_cnt;
  }
  return all_msgs;
}

Message * Message::create_message(char * buf) {
 RemReqType rtype = NO_MSG;
 uint64_t ptr = 0;
 COPY_VAL(rtype,buf,ptr);
 Message * msg = create_message(rtype);
 msg->copy_from_buf(buf);
 return msg;
}

Message * Message::create_message(TxnManager * txn, RemReqType rtype) {
 Message * msg = create_message(rtype);
 msg->mcopy_from_txn(txn);
 return msg;
}

Message * Message::create_message(BaseQuery * query, RemReqType rtype) {
  assert(rtype == RTXN);
 Message * msg = create_message(rtype);
 ((YCSBClientQueryMessage*)msg)->copy_from_query(query);
 assert(false);// FIXME
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

void Message::mcopy_from_txn(TxnManager * txn) {
  //rtype = query->rtype;
  txn_id = txn->get_txn_id();
}

void Message::mcopy_to_txn(TxnManager * txn) {
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

void QueryMessage::copy_from_txn(TxnManager * txn) {
  Message::mcopy_from_txn(txn);
#if CC_ALG == WAIT_DIE || CC_ALG == TIMESTAMP || CC_ALG == MVCC || CC_ALG == VLL
  ts = txn->get_timestamp();
#endif
#if CC_ALG == OCC 
  start_ts = txn->get_start_timestamp();
#endif
}

void QueryMessage::copy_to_txn(TxnManager * txn) {
  Message::mcopy_to_txn(txn);
#if CC_ALG == WAIT_DIE || CC_ALG == TIMESTAMP || CC_ALG == MVCC || CC_ALG == VLL
  query->ts = ts;
#endif
#if CC_ALG == OCC 
  query->start_ts = start_ts;
#endif

}

void QueryMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr __attribute__ ((unused));
  ptr = Message::mget_size();
#if CC_ALG == WAIT_DIE || CC_ALG == TIMESTAMP || CC_ALG == MVCC || CC_ALG == VLL
 COPY_VAL(ts,buf,ptr);
#endif
#if CC_ALG == OCC 
 COPY_VAL(start_ts,buf,ptr);
#endif
}

void QueryMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr __attribute__ ((unused));
  ptr = Message::mget_size();
#if CC_ALG == WAIT_DIE || CC_ALG == TIMESTAMP || CC_ALG == MVCC || CC_ALG == VLL
 COPY_BUF(buf,ts,ptr);
#endif
#if CC_ALG == OCC 
 COPY_BUF(buf,start_ts,ptr);
#endif
}

/************************/

void YCSBClientQueryMessage::init() {
}

uint64_t YCSBClientQueryMessage::get_size() {
  uint64_t size = sizeof(YCSBClientQueryMessage);
  size += sizeof(size_t);
  size += sizeof(ycsb_request) * requests.size();
  return size;
}

void YCSBClientQueryMessage::copy_from_query(BaseQuery * query) {
  //ClientQueryMessage::mcopy_from_txn(txn);
  requests.clear();
  requests.insert(requests.begin(),((YCSBQuery*)(query))->requests.begin(),((YCSBQuery*)(query))->requests.end());
}


void YCSBClientQueryMessage::copy_from_txn(TxnManager * txn) {
  ClientQueryMessage::mcopy_from_txn(txn);
  requests.clear();
  requests.insert(requests.begin(),((YCSBQuery*)(txn->query))->requests.begin(),((YCSBQuery*)(txn->query))->requests.end());
}

void YCSBClientQueryMessage::copy_to_txn(TxnManager * txn) {
  ClientQueryMessage::copy_to_txn(txn);
  ((YCSBQuery*)(txn->query))->requests.clear();
  ((YCSBQuery*)(txn->query))->requests.insert(((YCSBQuery*)(txn->query))->requests.begin(),requests.begin(),requests.end());
}

void YCSBClientQueryMessage::copy_from_buf(char * buf) {
  ClientQueryMessage::copy_from_buf(buf);
  uint64_t ptr = ClientQueryMessage::get_size();
  size_t size;
  COPY_VAL(size,buf,ptr);
  requests.resize(size);
  for(uint64_t i = 0 ; i < size;i++) {
    COPY_VAL(requests[i],buf,ptr);
  }
}

void YCSBClientQueryMessage::copy_to_buf(char * buf) {
  ClientQueryMessage::copy_to_buf(buf);
  uint64_t ptr = ClientQueryMessage::get_size();
  size_t size = requests.size();
  COPY_BUF(buf,size,ptr);
  for(uint64_t i = 0; i < requests.size(); i++) {
    COPY_BUF(buf,requests[i],ptr);
  }
}

/************************/

void ClientQueryMessage::init() {
}

uint64_t ClientQueryMessage::get_size() {
  uint64_t size = sizeof(ClientQueryMessage);
  size += sizeof(size_t);
  size += sizeof(uint64_t) * partitions.size();
  return size;
}

void ClientQueryMessage::copy_from_txn(TxnManager * txn) {
  Message::mcopy_from_txn(txn);
  ts = txn->txn->timestamp;
#if CC_ALG == CALVIN
  batch_id = txn->txn->batch_id;
  txn_id = txn->txn->txn_id;
#endif
  partitions.clear();
  partitions.insert(partitions.begin(),txn->query->partitions.begin(),txn->query->partitions.end());
}

void ClientQueryMessage::copy_to_txn(TxnManager * txn) {
  Message::mcopy_to_txn(txn);
  txn->txn->timestamp = ts;
#if CC_ALG == CALVIN
  txn->txn->batch_id = batch_id;
  txn->txn->txn_id = txn_id;
#endif
  txn->query->partitions.clear();
  txn->query->partitions.insert(txn->query->partitions.begin(),partitions.begin(),partitions.end());
}

void ClientQueryMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_VAL(ts,buf,ptr);
#if CC_ALG == CALVIN
  COPY_VAL(batch_id,buf,ptr);
  COPY_VAL(txn_id,buf,ptr);
#endif
  size_t size;
  COPY_VAL(size,buf,ptr);
  partitions.resize(size);
  for(uint64_t i = 0; i < size; i++) {
    COPY_VAL(partitions[i],buf,ptr);
  }
}

void ClientQueryMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_BUF(buf,ts,ptr);
#if CC_ALG == CALVIN
  COPY_BUF(buf,batch_id,ptr);
  COPY_BUF(buf,txn_id,ptr);
#endif
  size_t size;
  COPY_BUF(buf,size,ptr);
  partitions.resize(size,UINT64_MAX);
  for(uint64_t i = 0; i < size; i++) {
    COPY_BUF(buf,partitions[i],ptr);
  }
}

/************************/


uint64_t ClientResponseMessage::get_size() {
  uint64_t size = sizeof(ClientResponseMessage);
  return size;
}

void ClientResponseMessage::copy_from_txn(TxnManager * txn) {
  Message::mcopy_from_txn(txn);
  client_startts = txn->client_startts;
}

void ClientResponseMessage::copy_to_txn(TxnManager * txn) {
  Message::mcopy_to_txn(txn);
  txn->client_startts = client_startts;
}

void ClientResponseMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_VAL(client_startts,buf,ptr);
}

void ClientResponseMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_BUF(buf,client_startts,ptr);
}

/************************/


uint64_t DoneMessage::get_size() {
  uint64_t size = sizeof(DoneMessage);
  return size;
}

void DoneMessage::copy_from_txn(TxnManager * txn) {
  Message::mcopy_from_txn(txn);
  batch_id = txn->txn->batch_id;
}

void DoneMessage::copy_to_txn(TxnManager * txn) {
  Message::mcopy_to_txn(txn);
  txn->txn->batch_id = batch_id;
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

void ForwardMessage::copy_from_txn(TxnManager * txn) {
  Message::mcopy_from_txn(txn);
  txn_id = txn->txn->txn_id;
  batch_id = txn->txn->batch_id;
#if WORKLOAD == TPCC
  o_id = ((TPCCQuery*)txn->query)->o_id;
#endif
}

void ForwardMessage::copy_to_txn(TxnManager * txn) {
  Message::mcopy_to_txn(txn);
  txn->txn->txn_id = txn_id;
  txn->txn->batch_id = batch_id;
#if WORKLOAD == TPCC
  ((TPCCQuery*)txn->query)->o_id = o_id;
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

void PrepareMessage::copy_from_txn(TxnManager * txn) {
  Message::mcopy_from_txn(txn);
  txn_id = txn->txn->txn_id;
}

void PrepareMessage::copy_to_txn(TxnManager * txn) {
  Message::mcopy_to_txn(txn);
  txn->txn->txn_id = txn_id;
}

void PrepareMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_VAL(txn_id,buf,ptr);
}

void PrepareMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_BUF(buf,txn_id,ptr);
}

/************************/

uint64_t AckMessage::get_size() {
  uint64_t size = sizeof(AckMessage);
  return size;
}

void AckMessage::copy_from_txn(TxnManager * txn) {
  Message::mcopy_from_txn(txn);
  //rc = query->rc;
#if CC_ALG == CALVIN
  batch_id = txn->txn->batch_id;
#endif
}

void AckMessage::copy_to_txn(TxnManager * txn) {
  Message::mcopy_to_txn(txn);
  //query->rc = rc;
#if CC_ALG == CALVIN
  txn->txn->batch_id = batch_id;
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

void QueryResponseMessage::copy_from_txn(TxnManager * txn) {
  Message::mcopy_from_txn(txn);
  //rc = query->rc;
}

void QueryResponseMessage::copy_to_txn(TxnManager * txn) {
  Message::mcopy_to_txn(txn);
  //query->rc = rc;
}

void QueryResponseMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_VAL(rc,buf,ptr);
}

void QueryResponseMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  COPY_BUF(buf,rc,ptr);
}

/************************/



uint64_t FinishMessage::get_size() {
  uint64_t size = sizeof(FinishMessage);
  return size;
}

void FinishMessage::copy_from_txn(TxnManager * txn) {
  Message::mcopy_from_txn(txn);
  //rc = query->rc;
  txn_id = txn->txn->txn_id;
  batch_id = txn->txn->batch_id;
}

void FinishMessage::copy_to_txn(TxnManager * txn) {
  Message::mcopy_to_txn(txn);
  //query->rc = rc;
  txn->txn->txn_id = txn_id;
  txn->txn->batch_id = batch_id;
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

void InitDoneMessage::copy_from_txn(TxnManager * txn) {
}

void InitDoneMessage::copy_to_txn(TxnManager * txn) {
  Message::mcopy_to_txn(txn);
}

void InitDoneMessage::copy_from_buf(char * buf) {
  Message::mcopy_from_buf(buf);
}

void InitDoneMessage::copy_to_buf(char * buf) {
  Message::mcopy_to_buf(buf);
}

/************************/

void YCSBQueryMessage::init() {
}

uint64_t YCSBQueryMessage::get_size() {
  uint64_t size = sizeof(YCSBQueryMessage);
  size += sizeof(size_t);
  size += sizeof(ycsb_request) * requests.size();
  return size;
}

void YCSBQueryMessage::copy_from_txn(TxnManager * txn) {
  QueryMessage::copy_from_txn(txn);
  requests.clear();
  requests.insert(requests.begin(),((YCSBQuery*)(txn->query))->requests.begin(),((YCSBQuery*)(txn->query))->requests.end());
}

void YCSBQueryMessage::copy_to_txn(TxnManager * txn) {
  QueryMessage::copy_to_txn(txn);
  ((YCSBQuery*)(txn->query))->requests.clear();
  ((YCSBQuery*)(txn->query))->requests.insert(((YCSBQuery*)(txn->query))->requests.begin(),requests.begin(),requests.end());
}


void YCSBQueryMessage::copy_from_buf(char * buf) {
  QueryMessage::copy_from_buf(buf);
  uint64_t ptr = QueryMessage::get_size();
  size_t size;
  COPY_VAL(size,buf,ptr);
  requests.resize(size);
  for(uint64_t i = 0 ; i < size;i++) {
    COPY_VAL(requests[i],buf,ptr);
  }
}

void YCSBQueryMessage::copy_to_buf(char * buf) {
  QueryMessage::copy_to_buf(buf);
  uint64_t ptr = Message::mget_size();
  size_t size = requests.size();
  COPY_BUF(buf,size,ptr);
  for(uint64_t i = 0; i < requests.size(); i++) {
    COPY_BUF(buf,requests[i],ptr);
  }
}

