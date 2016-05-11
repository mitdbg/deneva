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

#ifndef _TXN_POOL_H_
#define _TXN_POOL_H_


#include "global.h"
#include "helper.h"
#include <boost/lockfree/queue.hpp>

class TxnManager;
class BaseQuery;
class Workload;
struct msg_entry;
struct txn_node;
class Access;
class Transaction;


class TxnManPool {
public:
  void init(Workload * wl, uint64_t size);
  void get(TxnManager *& item);
  void put(TxnManager * items);
  void free_all();

private:
  boost::lockfree::queue<TxnManager*, boost::lockfree::capacity<65526> > pool;
  Workload * _wl;

};


class TxnPool {
public:
  void init(Workload * wl, uint64_t size);
  void get(Transaction *& item);
  void put(Transaction * items);
  void free_all();

private:
  boost::lockfree::queue<Transaction*, boost::lockfree::capacity<65526> > pool;
  Workload * _wl;

};

class AccessPool {
public:
  void init(Workload * wl, uint64_t size);
  void get(Access *& item);
  void put(Access * items);
  void free_all();

private:
  boost::lockfree::queue<Access*, boost::lockfree::capacity<65526> > pool;
  Workload * _wl;

};


class TxnTablePool {
public:
  void init(Workload * wl, uint64_t size);
  void get(txn_node *& item);
  void put(txn_node * items);
  void free_all();

private:
  boost::lockfree::queue<txn_node*, boost::lockfree::capacity<65526> > pool;
  Workload * _wl;

};
class QryPool {
public:
  void init(Workload * wl, uint64_t size);
  void get(BaseQuery *& item);
  void put(BaseQuery * items);
  void free_all();

private:
  boost::lockfree::queue<BaseQuery*, boost::lockfree::capacity<65526> > pool;
  Workload * _wl;

};

class MsgPool {
public:
  void init(Workload * wl, uint64_t size);
  void get(msg_entry *& item);
  void put(msg_entry * items);
  void free_all();

private:
  boost::lockfree::queue<msg_entry*, boost::lockfree::capacity<65526> > pool;
  Workload * _wl;

};

#endif
