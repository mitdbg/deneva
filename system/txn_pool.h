#ifndef _TXN_POOL_H_
#define _TXN_POOL_H_

#include "global.h"
#include "helper.h"
#include "concurrentqueue.h"

class txn_man;
class base_query;
class workload;

class TxnPool {
public:
  void init(workload * wl, uint64_t size);
  void get(txn_man *& item);
  void put(txn_man * items);

private:
  moodycamel::ConcurrentQueue<txn_man*,moodycamel::ConcurrentQueueDefaultTraits> pool;
  workload * _wl;

};

class QryPool {
public:
  void init(workload * wl, uint64_t size);
  void get(base_query *& item);
  void put(base_query * items);

private:
  moodycamel::ConcurrentQueue<base_query*,moodycamel::ConcurrentQueueDefaultTraits> pool;
  workload * _wl;

};

/*
template <class T>
class TxnPool {
public:
  void init(uint64_t size);
  void get(T *& item);
  void put(T * items);

private:
  moodycamel::ConcurrentQueue<T,moodycamel::ConcurrentQueueDefaultTraits> pool;

};
*/

#endif
