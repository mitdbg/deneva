#include "txn_pool.h"
#include "global.h"
#include "helper.h"
#include "txn.h"
#include "mem_alloc.h"
#include "wl.h"

void TxnPool::init(workload * wl, uint64_t size) {
  _wl = wl;
  //txn_man * items = (txn_man*)mem_allocator.alloc(sizeof(txn_man)*size,0);
  txn_man * txn;
  for(uint64_t i = 0; i < size; i++) {
    //put(items[i]);
    _wl->get_txn_man(txn);
    put(txn);
  }
}

void TxnPool::get(txn_man *& item) {
  bool r = pool.try_dequeue(item);
  if(!r) {
    _wl->get_txn_man(item);
  }
  item->reset();
}

void TxnPool::put(txn_man * item) {
  pool.enqueue(item);
}
/*
template <class T>
void TxnPool::init(uint64_t size) {
  T * items = (T*)mem_allocator.alloc(sizeof(T)*size,0);
  for(uint64_t i = 0; i < size; i++) {
    put(items[i]);
  }
}

template <class T>
void TxnPool::get(T *& item) {
  bool r = pool.try_dequeue(item);
  if(!r) {
    item = (T)mem_allocator.alloc(sizeof(T),0);
  }
}

template <class T>
void TxnPool::put(T * item) {
  pool.enqueue(item);
}
*/
