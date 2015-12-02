#ifndef _PLOCK_H_
#define _PLOCK_H_

#include "global.h"
#include "helper.h"
#include "remote_query.h"
#include "query.h"

class txn_man;

// Parition manager for HSTORE / HSTORE_SPEC
class PartMan {
public:
	void init(uint64_t node_id,uint64_t part_id);
  void start_spec_ex(); 
	RC lock(txn_man * txn);
	void unlock(txn_man * txn);
	void remote_rsp(bool l, RC rc, txn_man * txn);
private:
	uint64_t _node_id;
	pthread_mutex_t latch;
  // TODO: Change back to owner / waiters
	txn_man * owner;
	txn_man ** waiters;
	UInt32 waiter_cnt;
  uint64_t _part_id;
  /*
	uint64_t owner;
	uint64_t owner_ts;
	uint64_t * owner_rp;
	uint64_t * waiters;
	uint64_t * waiters_ts;
	uint64_t ** waiters_rp;
  */
};

struct plock_node {
 public:
   uint64_t txn_id;
	 uint64_t _ready_parts;
	 uint64_t _ready_ulks;
	 RC  _rcs;
	 uint64_t ts;
   struct plock_node * next;
};

typedef plock_node * plock_node_t;

class PlockLL {
public:
  void init();
  plock_node_t add_node(uint64_t txn_id, uint64_t ts);
  plock_node_t get_node(uint64_t txn_id);
  void delete_node(uint64_t txn_id);
private:
  pthread_mutex_t mtx;
  plock_node_t locks;
};


// Partition Level Locking
class Plock {
public:
	void init(uint64_t node_id);
  void start_spec_ex(uint64_t * parts, uint64_t part_cnt); 
	// lock all partitions in parts
	RC lock(uint64_t * parts, uint64_t part_cnt, txn_man * txn);
	RC unlock(uint64_t * parts, uint64_t part_cnt, txn_man * txn);

	void unpack_rsp(base_query * query, void * d);
	void unpack(base_query * query, char * data);
	void remote_qry(bool l, uint64_t lid, txn_man * txn);
	uint64_t get_node_id() {return _node_id;};
	RC rem_lock(uint64_t * parts, uint64_t part_cnt, txn_man * txn); 
	void rem_unlock(uint64_t * parts, uint64_t part_cnt, txn_man * txn);
	void rem_lock_rsp(RC rc, txn_man * txn);
  void rem_unlock_rsp(txn_man * txn);
private:
	uint64_t _node_id;
	PartMan ** part_mans;
	// make sure these are on different cache lines
  PlockLL locks;
};

#endif
