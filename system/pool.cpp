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

#include "pool.h"
#include "global.h"
#include "helper.h"
#include "txn.h"
#include "mem_alloc.h"
#include "wl.h"
#include "ycsb_query.h"
#include "ycsb.h"
#include "tpcc_query.h"
#include "query.h"
#include "msg_queue.h"

void TxnManPool::init(Workload * wl, uint64_t size) {
  _wl = wl;
  TxnManager * txn;
  for(uint64_t i = 0; i < size; i++) {
    //put(items[i]);
    _wl->get_txn_man(txn);
    txn->init(_wl);
    put(txn);
  }
}

void TxnManPool::get(TxnManager *& item) {
  bool r = pool.pop(item);
  if(!r) {
    _wl->get_txn_man(item);
  }
  item->init(_wl);
}

void TxnManPool::put(TxnManager * item) {
  item->release();
  if(!pool.push(item)) {
    mem_allocator.free(item,sizeof(TxnManager));
  }
}

void TxnManPool::free_all() {
  TxnManager * item;
  while(pool.pop(item)) {
    mem_allocator.free(item,sizeof(TxnManager));
  }
}

void TxnPool::init(Workload * wl, uint64_t size) {
  _wl = wl;
  Transaction * txn;
  for(uint64_t i = 0; i < size; i++) {
    //put(items[i]);
    txn = (Transaction*) mem_allocator.alloc(sizeof(Transaction));
    txn->init();
    put(txn);
  }
}

void TxnPool::get(Transaction *& item) {
  bool r = pool.pop(item);
  if(!r) {
    item = (Transaction*) mem_allocator.alloc(sizeof(Transaction));
  }
  item->init();
}

void TxnPool::put(Transaction * item) {
  item->release();
  if(!pool.push(item)) {
    mem_allocator.free(item,sizeof(Transaction));
  }
}

void TxnPool::free_all() {
  TxnManager * item;
  while(pool.pop(item)) {
    mem_allocator.free(item,sizeof(item));

  }
}


void AccessPool::init(Workload * wl, uint64_t size) {
  _wl = wl;
  DEBUG_M("AccessPool alloc init\n");
  for(uint64_t i = 0; i < size; i++) {
    Access * item = (Access*)mem_allocator.alloc(sizeof(Access));
    put(item);
  }
}

void AccessPool::get(Access *& item) {
  bool r = pool.pop(item);
  if(!r) {
    DEBUG_M("access_pool alloc\n");
    item = (Access*)mem_allocator.alloc(sizeof(Access));
  }
}

void AccessPool::put(Access * item) {
  if(!pool.push(item)) {
    mem_allocator.free(item,sizeof(Access));
  }
}

void AccessPool::free_all() {
  Access * item;
  DEBUG_M("access_pool free\n");
  while(pool.pop(item)) {
    mem_allocator.free(item,sizeof(item));
  }
}

void TxnTablePool::init(Workload * wl, uint64_t size) {
  _wl = wl;
  DEBUG_M("TxnTablePool alloc init\n");
  for(uint64_t i = 0; i < size; i++) {
    txn_node * t_node = (txn_node *) mem_allocator.align_alloc(sizeof(struct txn_node));
    //put(new txn_node());
    put(&t_node[i]);
  }
}

void TxnTablePool::get(txn_node *& item) {
  bool r = pool.pop(item);
  if(!r) {
    DEBUG_M("txn_table_pool alloc\n");
    item = (txn_node *) mem_allocator.align_alloc(sizeof(struct txn_node));
  }
}

void TxnTablePool::put(txn_node * item) {
  if(!pool.push(item)) {
    mem_allocator.free(item,sizeof(txn_node));
  }
}

void TxnTablePool::free_all() {
  txn_node * item;
  DEBUG_M("txn_table_pool free\n");
  while(pool.pop(item)) {
    mem_allocator.free(item,sizeof(item));
  }
}

void QryPool::init(Workload * wl, uint64_t size) {
  _wl = wl;
  BaseQuery * qry=NULL;
  DEBUG_M("QryPool alloc init\n");
  for(uint64_t i = 0; i < size; i++) {
    //put(items[i]);
#if WORKLOAD==TPCC
    TPCCQuery * m_qry = (TPCCQuery *) mem_allocator.alloc(sizeof(TPCCQuery));
    m_qry = new TPCCQuery();
#elif WORKLOAD==YCSB
    YCSBQuery * m_qry = (YCSBQuery *) mem_allocator.alloc(sizeof(YCSBQuery));
    m_qry = new YCSBQuery();
#endif
    m_qry->init();
    qry = m_qry;
    put(qry);
  }
}

void QryPool::get(BaseQuery *& item) {
  bool r = pool.pop(item);
  if(!r) {
    DEBUG_M("query_pool alloc\n");
#if WORKLOAD==TPCC
    TPCCQuery * qry = (TPCCQuery *) mem_allocator.alloc(sizeof(TPCCQuery));
    qry = new TPCCQuery();
#elif WORKLOAD==YCSB
    YCSBQuery * qry = NULL;
    qry = (YCSBQuery *) mem_allocator.alloc(sizeof(YCSBQuery));
    qry = new YCSBQuery();
#endif
    item = (BaseQuery*)qry;
  }
#if WORKLOAD == YCSB
  ((YCSBQuery*)item)->init();
#elif WORKLOAD == TPCC
  ((TPCCQuery*)item)->init();
#endif
  DEBUG_R("get 0x%lx\n",(uint64_t)item);
}

void QryPool::put(BaseQuery * item) {
  assert(item);
#if WORKLOAD == YCSB
  ((YCSBQuery*)item)->release();
#elif WORKLOAD == TPCC
  ((TPCCQuery*)item)->release();
#endif
  //DEBUG_M("put 0x%lx\n",(uint64_t)item);
  DEBUG_R("put 0x%lx\n",(uint64_t)item);
  //mem_allocator.free(item,sizeof(item));
  if(!pool.push(item)) {
    mem_allocator.free(item,sizeof(BaseQuery));
  }
}

void QryPool::free_all() {
  BaseQuery * item;
  DEBUG_M("query_pool free\n");
  while(pool.pop(item)) {
    mem_allocator.free(item,sizeof(item));
  }
}

void MsgPool::init(Workload * wl, uint64_t size) {
  _wl = wl;
  msg_entry* entry;
  DEBUG_M("MsgPool alloc init\n");
  for(uint64_t i = 0; i < size; i++) {
    entry = (msg_entry*) mem_allocator.alloc(sizeof(struct msg_entry));
    put(entry);
  }
}

void MsgPool::get(msg_entry* & item) {
  bool r = pool.pop(item);
  if(!r) {
    DEBUG_M("msg_pool alloc\n");
    item = (msg_entry*) mem_allocator.alloc(sizeof(struct msg_entry));
  }
}

void MsgPool::put(msg_entry* item) {
  if(!pool.push(item)) {
    mem_allocator.free(item,sizeof(msg_entry));
  }
}

void MsgPool::free_all() {
  msg_entry * item;
  DEBUG_M("msg_pool free\n");
  while(pool.pop(item)) {
    mem_allocator.free(item,sizeof(item));
  }
}

