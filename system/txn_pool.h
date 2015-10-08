#ifndef _TXN_POOL_H_
#define _TXN_POOL_H_

#include "global.h"
#include "helper.h"
#include "concurrentqueue.h"

class txn_man;
class base_query;
class workload;
struct msg_entry;
struct txn_node;
class Access;

class TxnPool {
public:
  void init(workload * wl, uint64_t size);
  void get(txn_man *& item);
  void put(txn_man * items);
  void free_all();

private:
  moodycamel::ConcurrentQueue<txn_man*,moodycamel::ConcurrentQueueDefaultTraits> pool;
  workload * _wl;

};

class AccessPool {
public:
  void init(workload * wl, uint64_t size);
  void get(Access *& item);
  void put(Access * items);
  void free_all();

private:
  moodycamel::ConcurrentQueue<Access*,moodycamel::ConcurrentQueueDefaultTraits> pool;
  workload * _wl;

};


class TxnTablePool {
public:
  void init(workload * wl, uint64_t size);
  void get(txn_node *& item);
  void put(txn_node * items);
  void free_all();

private:
  moodycamel::ConcurrentQueue<txn_node*,moodycamel::ConcurrentQueueDefaultTraits> pool;
  workload * _wl;

};
class QryPool {
public:
  void init(workload * wl, uint64_t size);
  void get(base_query *& item);
  void put(base_query * items);
  void free_all();

private:
  moodycamel::ConcurrentQueue<base_query*,moodycamel::ConcurrentQueueDefaultTraits> pool;
  workload * _wl;

};

class MsgPool {
public:
  void init(workload * wl, uint64_t size);
  void get(msg_entry *& item);
  void put(msg_entry * items);
  void free_all();

private:
  moodycamel::ConcurrentQueue<msg_entry*,moodycamel::ConcurrentQueueDefaultTraits> pool;
  workload * _wl;

};

/*
class BackupRowPool {
public:
  void init(workload * wl, uint64_t size);
  void get(row_t *& item);
  void put(row_t * items);
  void free_all();

private:
  moodycamel::ConcurrentQueue<row_t*,moodycamel::ConcurrentQueueDefaultTraits> pool;
  workload * _wl;

};
*/
/*
template <class T>
class TxnPool {
public:
  void init(uint64_t size);
  void get(T *& item);
  void put(T * items);
  void free_all();

private:
  moodycamel::ConcurrentQueue<T,moodycamel::ConcurrentQueueDefaultTraits> pool;

};
*/

#endif
