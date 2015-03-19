#ifndef _PLOCK_H_
#define _PLOCK_H_

#include "global.h"
#include "helper.h"
#include "remote_query.h"
#include "query.h"

class txn_man;

// Parition manager for HSTORE
class PartMan {
public:
	void init(uint64_t node_id);
	RC lock(uint64_t pid,  uint64_t * rp, uint64_t ts);
	void unlock(uint64_t pid,  uint64_t * rp, ts_t ts);
	void remote_rsp(bool l, RC rc, uint64_t node_id, uint64_t pid, uint64_t ts);
private:
	uint64_t _node_id;
	pthread_mutex_t latch;
	//txn_man * owner;
	//txn_man ** waiters;
	UInt32 waiter_cnt;
	uint64_t owner;
	uint64_t owner_ts;
	uint64_t * owner_rp;
	uint64_t * waiters;
	uint64_t * waiters_ts;
	uint64_t ** waiters_rp;
};

// Partition Level Locking
class Plock {
public:
	void init(uint64_t node_id);
	// lock all partitions in parts
	RC lock(txn_man * txn, uint64_t * parts, uint64_t part_cnt);
	void unlock(txn_man * txn, uint64_t * parts, uint64_t part_cnt);

	void unpack_rsp(base_query * query, void * d);
	void unpack(base_query * query, char * data);
	void remote_qry(bool l, uint64_t pid, uint64_t lid, uint64_t ts);
	uint64_t get_node_id() {return _node_id;};
	void rem_unlock(uint64_t pid, uint64_t * parts, uint64_t part_cnt, ts_t ts);
	void rem_lock(uint64_t pid, uint64_t ts, uint64_t * parts, uint64_t part_cnt); 
	void rem_lock_rsp(uint64_t pid, RC rc, uint64_t ts);
  void rem_unlock_rsp(uint64_t pid, RC rc, uint64_t ts);
private:
	uint64_t _node_id;
	PartMan ** part_mans;
	// make sure these are on different cache lines
	 uint64_t *_ready_parts;
	 uint64_t *_ready_ulks;
	RC * _rcs;
	uint64_t * _ts;
};

#endif
