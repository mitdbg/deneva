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

#ifndef _MESSAGE_H_
#define _MESSAGE_H_

#include "global.h"
#include "helper.h"
#include "tpcc.h"
#include "tpcc_query.h"

class ycsb_request;

class Message {
public:
  static Message * create_message(char * buf); 
  static Message * create_message(BaseQuery * query, RemReqType rtype); 
  static Message * create_message(TxnManager * txn, RemReqType rtype); 
  static Message * create_message(RemReqType rtype); 
  static std::vector<Message*> create_messages(char * buf); 
  RemReqType rtype;
  uint64_t txn_id;
  uint64_t batch_id;
  uint64_t return_node_id;

  uint64_t mget_size();
  uint64_t get_txn_id() {return txn_id;}
  void mcopy_from_buf(char * buf);
  void mcopy_to_buf(char * buf);
  void mcopy_from_txn(TxnManager * txn);
  void mcopy_to_txn(TxnManager * txn);
  RemReqType get_rtype() {return rtype;}

  virtual uint64_t get_size() = 0;
  virtual void copy_from_buf(char * buf) = 0;
  virtual void copy_to_buf(char * buf) = 0;
  virtual void copy_to_txn(TxnManager * txn) = 0;
  virtual void init() = 0;
  virtual void release() = 0;
};

// Message types
class InitDoneMessage : public Message {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_txn(TxnManager * txn);
  void copy_to_txn(TxnManager * txn);
  uint64_t get_size();
  void init() {}
  void release() {}
};

class FinishMessage : public Message {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_txn(TxnManager * txn);
  void copy_to_txn(TxnManager * txn);
  uint64_t get_size();
  void init() {}
  void release() {}
  bool is_abort() { return rc == Abort;}

  uint64_t pid;
  RC rc;
  uint64_t txn_id;
  uint64_t batch_id;
  bool ro;
};

class QueryResponseMessage : public Message {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_txn(TxnManager * txn);
  void copy_to_txn(TxnManager * txn);
  uint64_t get_size();
  void init() {}
  void release() {}

  RC rc;
  uint64_t pid;
};

class AckMessage : public Message {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_txn(TxnManager * txn);
  void copy_to_txn(TxnManager * txn);
  uint64_t get_size();
  void init() {}
  void release() {}

  RC rc;
#if CC_ALG == CALVIN
  uint64_t batch_id;
#endif
};

class PrepareMessage : public Message {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_txn(TxnManager * txn);
  void copy_to_txn(TxnManager * txn);
  uint64_t get_size();
  void init() {}
  void release() {}

  uint64_t pid;
  RC rc;
  uint64_t txn_id;
};

class ForwardMessage : public Message {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_txn(TxnManager * txn);
  void copy_to_txn(TxnManager * txn);
  uint64_t get_size();
  void init() {}
  void release() {}

  uint64_t txn_id;
  uint64_t batch_id;
#if WORKLOAD == TPCC
	uint64_t o_id;
#endif
};


class DoneMessage : public Message {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_txn(TxnManager * txn);
  void copy_to_txn(TxnManager * txn);
  uint64_t get_size();
  void init() {}
  void release() {}
  uint64_t batch_id;
};

class ClientResponseMessage : public Message {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_txn(TxnManager * txn);
  void copy_to_txn(TxnManager * txn);
  uint64_t get_size();
  void init() {}
  void release() {}

  RC rc;
  uint64_t client_startts;
};

class ClientQueryMessage : public Message {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_query(BaseQuery * query); 
  void copy_from_txn(TxnManager * txn);
  void copy_to_txn(TxnManager * txn);
  uint64_t get_size();
  void init();
  void release();

  uint64_t pid;
  uint64_t ts;
#if CC_ALG == CALVIN
  uint64_t batch_id;
  uint64_t txn_id;
#endif
  Array<uint64_t> partitions;
};

class YCSBClientQueryMessage : public ClientQueryMessage {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_query(BaseQuery * query);
  void copy_from_txn(TxnManager * txn);
  void copy_to_txn(TxnManager * txn);
  uint64_t get_size();
  void init(); 
  void release(); 

  Array<ycsb_request*> requests;

};
class QueryMessage : public Message {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_txn(TxnManager * txn);
  void copy_to_txn(TxnManager * txn);
  uint64_t get_size();
  void init() {}
  void release() {}

  uint64_t pid;
#if CC_ALG == WAIT_DIE || CC_ALG == TIMESTAMP || CC_ALG == MVCC || CC_ALG == VLL
  uint64_t ts;
#endif
#if CC_ALG == MVCC 
  uint64_t thd_id;
#elif CC_ALG == OCC 
  uint64_t start_ts;
#endif
#if MODE==QRY_ONLY_MODE
  uint64_t max_access;
#endif
};

class YCSBQueryMessage : public QueryMessage {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_txn(TxnManager * txn);
  void copy_to_txn(TxnManager * txn);
  uint64_t get_size();
  void init();
  void release(); 

 Array<ycsb_request*> requests;

};

class TPCCQueryMessage : public QueryMessage {
public:
  void copy_from_buf(char * buf);
  void copy_to_buf(char * buf);
  void copy_from_txn(TxnManager * txn);
  void copy_to_txn(TxnManager * txn);
  uint64_t get_size();
  void init();
  void release() {assert(false);} // FIXME

	TPCCRemTxnType state;
  Array<Item_no*> items;

};



#endif
