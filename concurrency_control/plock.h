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
	void init();
	RC lock(txn_man * txn);
	void unlock(txn_man * txn);
private:
	pthread_mutex_t latch;
	txn_man * owner;
	txn_man ** waiters;
	UInt32 waiter_cnt;
};

// Partition Level Locking
class Plock {
public:
	void init(uint64_t node_id);
	// lock all partitions in parts
	RC lock(txn_man * txn, uint64_t * parts, uint64_t part_cnt);
	void unlock(txn_man * txn, uint64_t * parts, uint64_t part_cnt);

	RC unpack_rsp(void * d);
	void unpack(base_query * query, char * data);
	RC remote_qry(bool l, uint64_t * part_id);
	void remote_rsp(bool l, RC * rc, uint64_t node_id);
	uint64_t get_node_id() {return _node_id;};
private:
	uint64_t _node_id;
	PartMan ** part_mans;
};

#endif
