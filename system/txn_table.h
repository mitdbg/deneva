#ifndef _TXN_TABLE_H_
#define _TXN_TABLE_H_

#include "global.h"
#include "helper.h"

class txn_man;
class base_query;
class row_t;

struct txn_node {
 public:
    txn_man * txn;
    base_query * qry;
    struct txn_node * next;
    struct txn_node * prev;
};

typedef txn_node * txn_node_t;

struct pool_node {
 public:
   txn_node_t head;
   txn_node_t tail;
   pthread_mutex_t mtx;
  pthread_cond_t cond_m;
  pthread_cond_t cond_a;
  volatile bool modify;
  int access;
  uint64_t cnt;
  uint64_t min_ts;

};
typedef pool_node * pool_node_t;

class TxnTable {
public:
  void init();
  uint64_t get_cnt() {return cnt;}
  bool empty(uint64_t node_id);
  void add_txn(uint64_t node_id, txn_man * txn, base_query * qry);
  void get_txn(uint64_t node_id, uint64_t txn_id,txn_man *& txn,base_query *& qry);
  //txn_man * get_txn(uint64_t node_id, uint64_t txn_id);
  //base_query * get_qry(uint64_t node_id, uint64_t txn_id);
  void restart_txn(uint64_t txn_id);
  void delete_all();
  void delete_txn(uint64_t node_id, uint64_t txn_id);
  uint64_t get_min_ts(); 
  void snapshot(); 

  void spec_next(uint64_t tid);
  void start_spec_ex(uint64_t tid);
  void end_spec_ex();
  void commit_spec_ex(int r,uint64_t tid);

  //uint64_t inflight_cnt;
  bool * spec_mode;

private:
	uint64_t _node_id;

  pthread_mutex_t mtx;
  pthread_cond_t cond_m;
  pthread_cond_t cond_a;
  volatile bool modify;
  int access;
  uint64_t cnt;

  uint64_t pool_size;
  pool_node_t pool;
  uint64_t last_min_ts_time;
  uint64_t table_min_ts;
};

#endif
