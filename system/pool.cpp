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

void TxnPool::init(Workload * wl, uint64_t size) {
  _wl = wl;
  return; //FIXME?
  //TxnManager * items = (TxnManager*)mem_allocator.alloc(sizeof(TxnManager)*size);
  TxnManager * txn;
  for(uint64_t i = 0; i < size; i++) {
    //put(items[i]);
    _wl->get_txn_man(txn);
    txn->init(wl);
    put(txn);
  }
}

void TxnPool::get(TxnManager *& item) {
  bool r = pool.try_dequeue(item);
  if(!r) {
    _wl->get_txn_man(item);
  }
  item->reset();
}

void TxnPool::put(TxnManager * item) {
  pool.enqueue(item);
}

void TxnPool::free_all() {
  TxnManager * item;
  while(pool.try_dequeue(item)) {
    mem_allocator.free(item,sizeof(item));
  }
}

void AccessPool::init(Workload * wl, uint64_t size) {
  _wl = wl;
  Access * items = (Access*)mem_allocator.alloc(sizeof(Access)*size);
  for(uint64_t i = 0; i < size; i++) {
    put(&items[i]);
  }
}

void AccessPool::get(Access *& item) {
  bool r = pool.try_dequeue(item);
  if(!r) {
    DEBUG_M("access_pool\n");
    item = (Access*)mem_allocator.alloc(sizeof(Access));
  }
}

void AccessPool::put(Access * item) {
  pool.enqueue(item);
}

void AccessPool::free_all() {
  Access * item;
  while(pool.try_dequeue(item)) {
    mem_allocator.free(item,sizeof(item));
  }
}

void TxnTablePool::init(Workload * wl, uint64_t size) {
  _wl = wl;
  //TxnManager * items = (TxnManager*)mem_allocator.alloc(sizeof(TxnManager)*size);
  txn_node * t_node = (txn_node *) mem_allocator.alloc(sizeof(struct txn_node) * size);
  for(uint64_t i = 0; i < size; i++) {
    //put(new txn_node());
    put(&t_node[i]);
  }
}

void TxnTablePool::get(txn_node *& item) {
  bool r = pool.try_dequeue(item);
  if(!r) {
    DEBUG_M("txn_table_pool\n");
    item = (txn_node *) mem_allocator.alloc(sizeof(struct txn_node));
    //item = new txn_node;
    //item->txn_man = new YCSBTxnManager;
  }
  item->txn_man->txn = NULL;
  item->txn_man->query = NULL;
}

void TxnTablePool::put(txn_node * item) {
  pool.enqueue(item);
}

void TxnTablePool::free_all() {
  txn_node * item;
  while(pool.try_dequeue(item)) {
    mem_allocator.free(item,sizeof(item));
  }
}

void QryPool::init(Workload * wl, uint64_t size) {
  _wl = wl;
  BaseQuery * qry=NULL;
  for(uint64_t i = 0; i < size; i++) {
    //put(items[i]);
#if WORKLOAD==TPCC
    TPCCQuery * m_qry = (TPCCQuery *) mem_allocator.alloc(sizeof(TPCCQuery));
    m_qry = new TPCCQuery();
    //m_qry->items = (Item_no*) mem_allocator.alloc(sizeof(Item_no)*g_max_items_per_txn);
#elif WORKLOAD==YCSB
    YCSBQuery * m_qry = (YCSBQuery *) mem_allocator.alloc(sizeof(YCSBQuery));
    m_qry = new YCSBQuery();
    //m_qry->requests = (ycsb_request*)mem_allocator.alloc(sizeof(ycsb_request)*g_req_per_query);
#endif
    qry = m_qry;
    //qry->part_to_access = (uint64_t*)mem_allocator.alloc(sizeof(uint64_t)*g_part_per_txn);
    //qry->part_touched = (uint64_t*)mem_allocator.alloc(sizeof(uint64_t)*g_part_per_txn);
    qry->partitions.init(g_part_cnt);
    qry->partitions_touched.init(g_part_cnt);
    qry->active_nodes.init(g_node_cnt);
    qry->participant_nodes.init(g_node_cnt);
    put(qry);
  }
}

void QryPool::get(BaseQuery *& item) {
  bool r = pool.try_dequeue(item);
  if(!r) {
    DEBUG_M("query_pool\n");
#if WORKLOAD==TPCC
    TPCCQuery * qry = (TPCCQuery *) mem_allocator.alloc(sizeof(TPCCQuery));
    qry = new TPCCQuery();
    //qry->items = (Item_no*) mem_allocator.alloc(sizeof(Item_no)*g_max_items_per_txn);
#elif WORKLOAD==YCSB
    YCSBQuery * qry = NULL;
    qry = (YCSBQuery *) mem_allocator.alloc(sizeof(YCSBQuery));
    qry = new YCSBQuery();
    //qry->requests = (ycsb_request*)mem_allocator.alloc(sizeof(ycsb_request)*g_req_per_query);
#endif
    //qry->part_to_access = (uint64_t*)mem_allocator.alloc(sizeof(uint64_t)*g_part_per_txn);
    //qry->part_touched = (uint64_t*)mem_allocator.alloc(sizeof(uint64_t)*g_part_per_txn);
    qry->partitions.init(g_part_cnt);
    qry->partitions_touched.init(g_part_cnt);
    qry->active_nodes.init(g_node_cnt);
    qry->participant_nodes.init(g_node_cnt);
    item = (BaseQuery*)qry;
  }
  //DEBUG_M("get 0x%lx\n",(uint64_t)item);
  DEBUG_R("get 0x%lx\n",(uint64_t)item);
}

void QryPool::put(BaseQuery * item) {
  assert(item);
  item->partitions.clear();
  item->partitions_touched.clear();
  item->active_nodes.clear();
  item->participant_nodes.clear();
  //DEBUG_M("put 0x%lx\n",(uint64_t)item);
  DEBUG_R("put 0x%lx\n",(uint64_t)item);
  //mem_allocator.free(item,sizeof(item));
  pool.enqueue(item);
}

void QryPool::free_all() {
  BaseQuery * item;
  while(pool.try_dequeue(item)) {
    mem_allocator.free(item,sizeof(item));
  }
}

void MsgPool::init(Workload * wl, uint64_t size) {
  _wl = wl;
  msg_entry* entry;
  for(uint64_t i = 0; i < size; i++) {
    entry = (msg_entry*) mem_allocator.alloc(sizeof(struct msg_entry));
    put(entry);
  }
}

void MsgPool::get(msg_entry* & item) {
  bool r = pool.try_dequeue(item);
  if(!r) {
    DEBUG_M("msg_pool\n");
    item = (msg_entry*) mem_allocator.alloc(sizeof(struct msg_entry));
  }
}

void MsgPool::put(msg_entry* item) {
  pool.enqueue(item);
}

void MsgPool::free_all() {
  msg_entry * item;
  while(pool.try_dequeue(item)) {
    mem_allocator.free(item,sizeof(item));
  }
}

